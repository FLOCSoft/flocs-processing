#!/usr/bin/env python
from .db_utils import FACET_STEPS, STEP_COLUMNS, FlocsDB
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from enum import Enum
import cyclopts
import functools
import math
import re
import sqlite3
import structlog
from typing import Annotated, Literal, Optional, get_args

app = cyclopts.App()

logger = structlog.getLogger()


def parse_target_coordinates(target_name):
    """Parse compact IAU target names into an Astropy SkyCoord."""
    match = re.fullmatch(
        r"(?:ILT)?J(?P<rah>\d{2})(?P<ram>\d{2})(?P<ras>\d{2}(?:\.\d+)?)"
        r"(?P<sign>[+-])(?P<decd>\d{2})(?P<decm>\d{2})(?P<decs>\d{2}(?:\.\d+)?)",
        (target_name or "").strip().replace(" ", ""),
        re.IGNORECASE,
    )
    if not match:
        return None
    from astropy.coordinates import SkyCoord

    ra = f"{match['rah']}h{match['ram']}m{match['ras']}s"
    dec = f"{match['sign']}{match['decd']}d{match['decm']}m{match['decs']}s"
    return SkyCoord(f"{ra} {dec}", frame="icrs")


def get_target_products(lookup, target_name, sas_id_target, minfreq, maxfreq):
    """Resolve the target beam and its valid archive products within the band."""
    from awlofar.main.aweimports import AveragingPipeline, CorrelatedDataProduct

    pointings = list(lookup.target.subArrayPointings)
    pipeline_id = None
    if str(lookup.obsid) != str(sas_id_target):
        pipelines = list(
            (AveragingPipeline.observationId == sas_id_target)
            & (AveragingPipeline.isValid == 1)
        )
        if len(pipelines) != 1:
            raise ValueError(f"Cannot identify a unique averaging pipeline for {sas_id_target}.")
        sapids = {source.subArrayPointingIdentifier for source in pipelines[0].sourceData}
        pointings = [sap for sap in pointings if sap.subArrayPointingIdentifier in sapids]
        pipeline_id = sas_id_target
    if lookup.sapid is not None:
        pointings = [
            sap for sap in pointings
            if str(sap.subArrayPointingIdentifier) == str(lookup.sapid)
        ]
    else:
        named = [sap for sap in pointings if sap.targetName == target_name]
        if named:
            pointings = named
        elif len(pointings) > 1:
            target = parse_target_coordinates(target_name)
            if target is not None:
                from astropy.coordinates import SkyCoord

                pointings = [sap for sap in pointings if sap.pointing is not None]
                if pointings:
                    pointing = min(
                        pointings,
                        key=lambda sap: target.separation(SkyCoord(
                            sap.pointing.rightAscension, sap.pointing.declination, unit="deg"
                        )).deg,
                    )
                    pointings = [pointing]
    if len(pointings) != 1:
        raise ValueError(f"Cannot identify a unique pointing for {target_name}, target {sas_id_target}.")
    pointing = pointings[0]
    query = CorrelatedDataProduct.isValid == 1
    query &= (
        CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
        == pointing.subArrayPointingIdentifier
    )
    if pipeline_id:
        query &= CorrelatedDataProduct.pipeline.observationId == pipeline_id
    else:
        query &= CorrelatedDataProduct.observation.observationId == lookup.obsid
    query &= CorrelatedDataProduct.minimumFrequency >= minfreq
    query &= CorrelatedDataProduct.maximumFrequency <= maxfreq
    return pointing, list(query)


def set_target_surls(lookup, field, minfreq, maxfreq):
    """Use the registered target selection for archive staging."""
    from awlofar.main.aweimports import FileObject

    pointing, products = get_target_products(
        lookup, field["target_name"], field["sas_id_target"], minfreq, maxfreq
    )
    uris = set()
    for product in products:
        file = ((FileObject.data_object == product) & (FileObject.isValid > 0)).max("creation_date")
        if file is None or not file.URI:
            raise ValueError(f"No valid archive file for target {field['sas_id_target']}, subband {product.subband}.")
        uris.add(file.URI)
    if not uris or len(uris) != len(products):
        raise ValueError(f"Cannot identify one archive file per subband for target {field['sas_id_target']}.")
    lookup.sapid = str(pointing.subArrayPointingIdentifier)
    lookup.target_uris = uris
    lookup.get_surls = True
    lookup.srm_prefix = f"srms_{field['sas_id_target']}"
    with open(f"{lookup.srm_prefix}.txt", "w") as stream:
        stream.write("".join(uri + "\n" for uri in sorted(uris)))


PIPELINES = Literal[
    "all",
    "download",
    "calibrator1",
    "calibrator2",
    "target",
    "vlbi_delay",
    "prepare_ddf",
    "ddf",
    "prepare_ddf_subtract",
    "vlbi_ddf_subtract",
    "vlbi_dd",
    "vlbi_intermediate_img",
    "vlbi_facet_subtract",
    "vlbi_facet_imaging",
    "vlbi_facet_imaging_6.0",
    "vlbi_facet_imaging_3.0",
    "vlbi_facet_imaging_1.5",
    "vlbi_facet_imaging_0.6",
    "vlbi_facet_imaging_0.3",
]


class FIELD_STATUS(Enum):
    nothing = "not started"
    downloading = "downloading"
    finished = "finished"
    processing = "processing"
    failed = "failed"

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value == other.value


@functools.total_ordering
class PIPELINE_STATUS(Enum):
    nothing = 0
    downloaded = 1
    finished = 2
    await_approval = 3
    processing = 98
    error = 99

    def __eq__(self, other):
        if other.__class__ is int:
            return self.value == other
        elif other.__class__ is self.__class__:
            return self.value == other.value
        else:
            raise NotImplementedError

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value < other.value


@app.command()
def create_database(
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
    pipelines: Annotated[
        Literal["vlbi-widefield", "vlbi-single-target"],
        Parameter(help="Pipeline this database will process."),
    ] = "vlbi-widefield",
):
    if pipelines not in {"vlbi-widefield", "vlbi-single-target"}:
        raise ValueError(f"Unknown processing mode: {pipelines}")
    table = '"' + table_name.replace('"', '""') + '"'
    status_columns = ", ".join(
        f"{column} SMALLINT NOT NULL DEFAULT 0" for column in STEP_COLUMNS.values()
    )
    facets = ", ".join(STEP_COLUMNS[step] for step in FACET_STEPS)
    dbstr = f"""CREATE TABLE {table} (
        target_name TEXT NOT NULL,
        sas_id_target TEXT PRIMARY KEY NOT NULL,
        project_code TEXT,
        observation_date TEXT,
        ra_deg REAL,
        dec_deg REAL,
        integration_hours REAL,
        number_subbands INTEGER,
        freq_min_mhz REAL,
        freq_max_mhz REAL,
        frequency_resolution_hz REAL,
        time_resolution_s REAL,
        nr_international INTEGER,
        elevation_mean_deg REAL,
        sas_id_calibrator1 TEXT,
        sas_id_calibrator2 TEXT,
        sas_id_calibrator_final TEXT,
        selected_delay_calibrator TEXT,
        pipelines TEXT NOT NULL DEFAULT '{pipelines}',
        priority INTEGER NOT NULL DEFAULT 0,
        processing_step TEXT NOT NULL DEFAULT 'pending',
        status TEXT NOT NULL DEFAULT 'queued',
        processing_start_time TEXT,
        processing_end_time TEXT,
        processing_cluster TEXT,
        processing_user TEXT,
        processing_host TEXT,
        workflow_run_id TEXT,
        last_updated_at TEXT,
        input_location TEXT,
        work_directory TEXT,
        output_location TEXT,
        last_error TEXT,
        delay_calibrator_phase_score REAL,
        delay_calibrator_amp_score REAL,
        n_dd_calibrators INTEGER,
        dd_calibrator_phase_score_min REAL,
        dd_calibrator_phase_score_max REAL,
        dd_calibrator_phase_score_median REAL,
        dd_calibrator_amp_score_min REAL,
        dd_calibrator_amp_score_max REAL,
        dd_calibrator_amp_score_median REAL,
        {status_columns},
        downloaded INTEGER GENERATED ALWAYS AS (status_download IN (2, 3)) VIRTUAL,
        status_vlbi_facet_imaging SMALLINT GENERATED ALWAYS AS (
            CASE WHEN 99 IN ({facets}) THEN 99
                 WHEN 98 IN ({facets}) THEN 98
                 WHEN 3 IN ({facets}) THEN 3
                 WHEN min({facets}) = max({facets}) THEN min({facets})
                 ELSE 98 END) VIRTUAL
    )"""
    with sqlite3.connect(dbname) as db:
        db.execute(dbstr)


@app.command()
def add_field(
    field_name: Annotated[str, Parameter(help="Name of the source/field to add.")],
    sas_id_calibrators: Annotated[
        list[str],
        Parameter(help="SAS IDs of the calibrators to add.", consume_multiple=True),
    ],
    sas_id_target: Annotated[
        str,
        Parameter(help="SAS ID of the target to add.", consume_multiple=True),
    ],
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
    pipelines: Annotated[
        Optional[Literal["vlbi-widefield", "vlbi-single-target"]],
        Parameter(help="Pipeline this field needs to be processed with."),
    ] = None,
    freq_min_mhz: Annotated[
        Optional[float], Parameter(help="Minimum target frequency in MHz (120 by default).")
    ] = None,
    freq_max_mhz: Annotated[
        Optional[float], Parameter(help="Maximum target frequency in MHz (168 by default).")
    ] = None,
    number_subbands: Annotated[
        Optional[int], Parameter(help="Select this many lowest-frequency target subbands within the frequency range.")
    ] = None,
    fetch_metadata: Annotated[
        bool, Parameter(help="Read observation metadata from the LTA.")
    ] = True,
):
    minfreq = 120 if freq_min_mhz is None else freq_min_mhz
    maxfreq = 168 if freq_max_mhz is None else freq_max_mhz
    if not (math.isfinite(minfreq) and math.isfinite(maxfreq) and 0 < minfreq < maxfreq):
        raise ValueError("Target frequency limits must be finite, positive and increasing.")
    if number_subbands is not None and number_subbands <= 0:
        raise ValueError("number_subbands must be positive.")
    if len(sas_id_calibrators) not in (1, 2):
        raise ValueError("Provide one or two calibrator SAS IDs.")
    values = dict(target_name=field_name, sas_id_target=sas_id_target)
    values.update(zip(("sas_id_calibrator1", "sas_id_calibrator2"), sas_id_calibrators))
    if pipelines:
        values["pipelines"] = pipelines
    if fetch_metadata:
        from flocs_lta.lta_search import ObservationStager

        lookup = ObservationStager(get_surls=False)
        try:
            lookup.find_observation_by_sasid("ALL", sas_id_target)
        except SystemExit as error:
            raise RuntimeError(f"No LTA metadata found for target {sas_id_target}.") from error
        observation = lookup.target
        pointing, products = get_target_products(lookup, field_name, sas_id_target, minfreq, maxfreq)
        if not products:
            raise ValueError(f"No valid correlated data products in the requested frequency range for {sas_id_target}.")
        products.sort(key=lambda product: product.minimumFrequency)
        candidates = products
        available = list(dict.fromkeys(product.subband for product in products))
        if number_subbands is not None:
            if len(available) < number_subbands:
                raise ValueError(f"Requested {number_subbands} subbands, found {len(available)} in the frequency range.")
            selected_bands = available[:number_subbands]
            if None in selected_bands or min(selected_bands) < 0:
                raise ValueError("Cannot select subbands whose archive identifiers are unknown.")
            selected = set(selected_bands)
            products = [product for product in products if product.subband in selected]
        subbands = {product.subband for product in products}
        known_subbands = [
            product.subband for product in products
            if product.subband is not None and product.subband >= 0
        ]
        if len(set(known_subbands)) != len(known_subbands):
            raise ValueError("The target selection must contain one data product per subband.")
        frequencies = [(product.minimumFrequency, product.maximumFrequency) for product in products]
        valid_frequencies = all(
            low is not None and high is not None and 0 < low < high < float("inf")
            for low, high in frequencies
        )
        widths = {product.channelWidth for product in products}
        intervals = {product.integrationInterval for product in products}
        if not valid_frequencies:
            raise ValueError("The target selection has invalid archive frequency limits.")
        selected_min = min(low for low, high in frequencies)
        selected_max = max(high for low, high in frequencies)
        replay = [
            product for product in candidates
            if product.minimumFrequency >= selected_min and product.maximumFrequency <= selected_max
        ]
        if {product.subband for product in replay} != subbands or len(replay) != len(products):
            raise ValueError("The selected subbands cannot be represented by a single frequency range.")
        date = observation.startTime
        duration = observation.duration
        stations = observation.nrStationsInternational
        width = next(iter(widths)) if len(widths) == 1 else None
        interval = next(iter(intervals)) if len(intervals) == 1 else None
        values.update(
            project_code=str(lookup.project) if lookup.project is not None else None,
            observation_date=date.isoformat() if date and date.year > 1990 else None,
            integration_hours=duration / 3600 if duration is not None and 0 < duration < float("inf") else None,
            ra_deg=pointing.pointing.rightAscension if pointing.pointing else None,
            dec_deg=pointing.pointing.declination if pointing.pointing else None,
            nr_international=stations if stations is not None and stations >= 0 else None,
            number_subbands=len(subbands) if None not in subbands and min(subbands) >= 0 else None,
            freq_min_mhz=selected_min,
            freq_max_mhz=selected_max,
            frequency_resolution_hz=width if width is not None and 0 < width < float("inf") else None,
            time_resolution_s=interval if interval is not None and 0 < interval < float("inf") else None,
        )
    else:
        values.update(freq_min_mhz=freq_min_mhz, freq_max_mhz=freq_max_mhz, number_subbands=number_subbands)
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    print(f"Adding field {field_name} to {table_name} in {dbname}")
    with sqlite3.connect(dbname) as database:
        table = '"' + table_name.replace('"', '""') + '"'
        database.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values.values()))


@app.command()
def update_field_status(
    field_name: Annotated[str, Parameter(help="Name of the source/field to update.")],
    sas_id_target: Annotated[
        str,
        Parameter(help="SAS ID of the target to add.", consume_multiple=True),
    ],
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    set_status: Annotated[
        Literal["not started", "downloaded", "processing", "failed", "finished"],
        Parameter(help="Set the status of the given pipeline."),
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
):
    db = FlocsDB(dbname=dbname, db_table=table_name)
    logger.info(f"Setting field {field_name} to status {set_status}")
    match set_status:
        case "not started":
            db.reset_field(field_name, sas_id_target)
        case "downloaded":
            db.set_field_downloaded(field_name, sas_id_target)
        case "processing":
            db.set_field_processing(field_name, sas_id_target)
        case "failed":
            db.set_field_failed(field_name, sas_id_target)
        case "finished":
            db.set_field_finished(field_name, sas_id_target)


@app.command()
def update_pipeline_status(
    field_name: Annotated[str, Parameter(help="Name of the source/field to update.")],
    sas_id_target: Annotated[
        str,
        Parameter(help="SAS ID of the target to add.", consume_multiple=True),
    ],
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    pipeline: Annotated[
        PIPELINES,
        Parameter(help="Pipeline of which to update the status"),
    ],
    set_status: Annotated[
        Literal["nothing", "downloaded", "processing", "failed", "success"],
        Parameter(help="Set the status of the given pipeline."),
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
):
    db = FlocsDB(dbname=dbname, db_table=table_name)
    if pipeline == "all":
        p_list = [p for p in get_args(PIPELINES) if p != "all" and p not in FACET_STEPS]
    else:
        p_list = [pipeline]
    for p in p_list:
        logger.info(f"Setting pipeline {p} to status {set_status}")
        match set_status:
            case "nothing":
                db.set_status_nothing(field_name, p, sas_id_target)
            case "downloaded":
                db.set_status_downloaded(field_name, p, sas_id_target)
            case "processing":
                db.set_status_processing(field_name, p, sas_id_target)
            case "failed":
                db.set_status_failed(field_name, p, sas_id_target)
            case "success":
                db.set_status_finished(field_name, p, sas_id_target)


@app.command()
def deploy_airflow(
    airflow_cores: Annotated[
        int, Parameter(help="Number of cores to give to Airflow.")
    ] = 6,
    custom_airflow_home: Annotated[
        Optional[str],
        Parameter(
            help="Directory where Airflow stores its internal data. If given this overrides $AIRFLOW_HOME. If not given, $AIRFLOW_HOME is used to define other related variables."
        ),
    ] = "",
):
    fp = FlocsAirflowProcessor(custom_airflow_home, airflow_cores)
    fp.generate_jwt_secret()
    fp.check_environment()
    fp.check_airflow_init()
    fp.write_source_file()
    fp.deploy_airflow_tmux()


def main():
    app()


if __name__ == "__main__":
    main()
