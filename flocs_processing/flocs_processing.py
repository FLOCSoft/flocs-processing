#!/usr/bin/env python
from .db_utils import FlocsDB
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from datetime import datetime, timezone
from enum import Enum
import cyclopts
import functools
import oracledb
import subprocess
import structlog
from typing import Annotated, Literal, Optional, get_args

try:
    from flocs_lta.lta_search import ObservationStager
    from awlofar.main.aweimports import CorrelatedDataProduct
except oracledb.OperationalError as error:
    (error_obj,) = error.args
    if error_obj.full_code != "DPY-6005":
        raise
    _lta_connection_error = error
else:
    _lta_connection_error = None

app = cyclopts.App()

logger = structlog.getLogger()


PIPELINES = Literal[
    "all",
    "calibrator1",
    "calibrator2",
    "target",
    "vlbi_delay",
    "ddf",
    "vlbi_ddf_subtract",
    "vlbi_dd",
    "vlbi_intermediate_img",
    "vlbi_facet_subtract",
    "vlbi_facet_imaging",
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
):
    dbstr = f"create table {table_name}(target_name text default NULL, pipelines text default NULL, priority int default 0, status text default '{FIELD_STATUS.nothing.value}', downloaded bit default 0"

    dbstr += f", sas_id_calibrator1 text default NULL, sas_id_calibrator2 text default NULL, sas_id_calibrator_final text default NULL, sas_id_target text primary key default NULL, status_calibrator1 smallint default {PIPELINE_STATUS.nothing.value}, status_calibrator2 smallint default {PIPELINE_STATUS.nothing.value}, status_target smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_ddf smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_delay smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_dd smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_ddf_subtract smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_intermediate_img smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_facet_subtract smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += f", status_vlbi_facet_imaging smallint default {PIPELINE_STATUS.nothing.value}"
    dbstr += """,
        project_code TEXT,
        observation_date TEXT,
        pointing_ra_deg REAL,
        pointing_dec_deg REAL,
        integration_hours REAL,
        number_subbands_requested INTEGER,
        number_subbands_downloaded INTEGER,
        freq_min_mhz REAL,
        freq_max_mhz REAL,
        frequency_resolution_hz REAL,
        time_resolution_s REAL,
        nr_core INTEGER,
        nr_remote INTEGER,
        nr_international INTEGER,
        elevation_mean_deg REAL,
        selected_delay_calibrator TEXT,
        intent TEXT,
        processing_step TEXT NOT NULL DEFAULT 'pending',
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
        dd_calibrator_amp_score_median REAL
    );"""

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Creating table via: {' '.join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to create table {table_name} in database {dbname}.")


def get_target_products(lookup, field_name, sas_id_target, minfreq=None, maxfreq=None):
    products = CorrelatedDataProduct.isValid == 1
    if str(lookup.obsid) == str(sas_id_target):
        products &= CorrelatedDataProduct.observation.observationId == lookup.obsid
    else:
        products &= CorrelatedDataProduct.pipeline.observationId == sas_id_target
    if minfreq is not None:
        products &= CorrelatedDataProduct.minimumFrequency >= minfreq
    if maxfreq is not None:
        products &= CorrelatedDataProduct.maximumFrequency <= maxfreq
    products = list(products)
    pointings = list(lookup.target.subArrayPointings)
    named = [pointing for pointing in pointings if pointing.targetName == field_name]
    if named:
        pointings = named
    sapids = {product.subArrayPointing.subArrayPointingIdentifier for product in products}
    pointings = [pointing for pointing in pointings if pointing.subArrayPointingIdentifier in sapids]
    if len(pointings) != 1:
        raise ValueError(f"Cannot identify a unique beam for {field_name}, target {sas_id_target}.")
    pointing = pointings[0]
    products = [product for product in products
                if product.subArrayPointing.subArrayPointingIdentifier == pointing.subArrayPointingIdentifier]
    return pointing, products


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
    intent: Annotated[
        Literal["vlbi-widefield", "vlbi-single-target"],
        Parameter(help="Workflow this field needs to be processed with."),
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
    pipelines: Annotated[
        str, Parameter(help="Pipelines this field needs to be processed with.")
    ] = "",
    freq_min_mhz: Annotated[
        Optional[float], Parameter(help="Minimum target frequency in MHz.")
    ] = None,
    freq_max_mhz: Annotated[
        Optional[float], Parameter(help="Maximum target frequency in MHz.")
    ] = None,
):
    if intent != "vlbi-widefield" and (freq_min_mhz is not None or freq_max_mhz is not None):
        raise ValueError("Frequency bounds are currently supported for vlbi-widefield.")
    if freq_min_mhz is not None and freq_max_mhz is not None and freq_min_mhz >= freq_max_mhz:
        raise ValueError("Minimum target frequency must be below maximum target frequency.")
    if not pipelines and intent == "vlbi-widefield":
        pipelines = "linc,ddf-pipeline,vlbi-delay-widefield"
    elif not pipelines and intent == "vlbi-single-target":
        pipelines = "linc,vlbi-delay-single-target"

    if _lta_connection_error is not None:
        raise _lta_connection_error

    lookup = ObservationStager(get_surls=False)
    try:
        lookup.find_observation_by_sasid("ALL", sas_id_target)
    except SystemExit as error:
        raise LookupError(f"No LTA metadata found for target {sas_id_target}.") from error
    observation = lookup.target
    pointing, products = get_target_products(lookup, field_name, sas_id_target, freq_min_mhz, freq_max_mhz)
    widths = {product.channelWidth for product in products}
    intervals = {product.integrationInterval for product in products}
    coordinates = pointing.pointing
    date = observation.startTime
    duration = observation.duration
    metadata = {
        "intent": intent,
        "pipelines": pipelines,
        "project_code": lookup.project,
        "pointing_ra_deg": coordinates.rightAscension if coordinates is not None else None,
        "pointing_dec_deg": coordinates.declination if coordinates is not None else None,
        "number_subbands_requested": len({product.subband for product in products}),
        "freq_min_mhz": min(product.minimumFrequency for product in products),
        "freq_max_mhz": max(product.maximumFrequency for product in products),
        "frequency_resolution_hz": widths.pop() if len(widths) == 1 else None,
        "time_resolution_s": intervals.pop() if len(intervals) == 1 else None,
        "observation_date": date.isoformat() if date is not None else None,
        "integration_hours": duration / 3600 if duration is not None else None,
        "nr_core": observation.nrStationsCore,
        "nr_remote": observation.nrStationsRemote,
        "nr_international": observation.nrStationsInternational,
        "last_updated_at": datetime.now(timezone.utc).isoformat(),
    }

    dbstr = f"insert into {table_name} (target_name"
    if len(sas_id_calibrators) == 1:
        dbstr += ", sas_id_calibrator1"
    if len(sas_id_calibrators) == 2:
        dbstr += ", sas_id_calibrator1, sas_id_calibrator2"
    dbstr += f", sas_id_target, {', '.join(metadata)}) values ('{field_name}', "
    if len(sas_id_calibrators) == 1:
        dbstr += f"'{sas_id_calibrators[0]}', "
    if len(sas_id_calibrators) == 2:
        dbstr += f"'{sas_id_calibrators[0]}', '{sas_id_calibrators[1]}', "
    dbstr += f"'{sas_id_target}', "
    dbstr += ", ".join(
        "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"
        for value in metadata.values()
    ) + ")"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Adding field {field_name} to {table_name} via: {' '.join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to update table {table_name} in database {dbname}.")


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
        p_list = list(get_args(PIPELINES))
        p_list.remove("all")
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
    else:
        logger.info(f"Setting pipeline {pipeline} to status {set_status}")
        match set_status:
            case "nothing":
                db.set_status_nothing(field_name, pipeline, sas_id_target)
            case "downloaded":
                db.set_status_downloaded(field_name, pipeline, sas_id_target)
            case "processing":
                db.set_status_processing(field_name, pipeline, sas_id_target)
            case "failed":
                db.set_status_failed(field_name, pipeline, sas_id_target)
            case "success":
                db.set_status_finished(field_name, pipeline, sas_id_target)


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
