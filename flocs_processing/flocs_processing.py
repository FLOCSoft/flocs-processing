#!/usr/bin/env python
from .db_utils import FlocsDB
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from datetime import datetime, timezone
from enum import Enum
import cyclopts
import functools
import math
import sqlite3
import subprocess
import structlog
from typing import Annotated, Literal, Optional, get_args

app = cyclopts.App()

logger = structlog.getLogger()


INTENT_PIPELINES = {
    "PILOT-widefield": ("linc", "ddf-pipeline", "vlbi-delay-widefield"),
    "PILOT-single-target": ("linc", "vlbi-delay-single-target"),
}
PIPELINE_CHECKPOINTS = {
    "linc": ("calibrator1", "calibrator2", "target"),
    "ddf-pipeline": ("ddf",),
    "vlbi-delay-widefield": (
        "vlbi_delay", "vlbi_dd", "vlbi_ddf_subtract", "vlbi_intermediate_img",
        "vlbi_facet_subtract", "vlbi_facet_imaging",
    ),
    "vlbi-delay-single-target": ("vlbi_delay", "vlbi_dd"),
}
OBSERVATION_METADATA_COLUMNS = {
    "project_code", "observation_date", "integration_hours",
    "nr_core", "nr_remote", "nr_international",
}


def get_observation_metadata(sas_id_target):
    from flocs_lta.lta_search import ObservationStager

    lookup = ObservationStager(get_surls=False)
    try:
        lookup.find_observation_by_sasid("ALL", sas_id_target)
    except SystemExit as error:
        raise LookupError(f"No LTA metadata found for target {sas_id_target}.") from error
    observation = lookup.target
    date = observation.startTime
    duration = observation.duration
    values = {
        "project_code": str(lookup.project) if lookup.project is not None else None,
        "observation_date": date.isoformat() if date and date.year > 1 else None,
        "integration_hours": duration / 3600
        if duration is not None and math.isfinite(duration) and duration > 0 else None,
    }
    for column, attribute in (
        ("nr_core", "nrStationsCore"),
        ("nr_remote", "nrStationsRemote"),
        ("nr_international", "nrStationsInternational"),
    ):
        count = getattr(observation, attribute, None)
        values[column] = count if (
            count is not None and math.isfinite(count) and count >= 0 and int(count) == count
        ) else None
    return values


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
    pipelines: Annotated[list[str], Parameter(help="", consume_multiple=True)] = [
        "linc"
    ],
):
    pipeline_str = ",".join(pipelines)
    pipelines = list(map(str.lower, pipelines))
    dbstr = f"create table {table_name}(target_name text default NULL, pipelines text default '{pipeline_str}', priority int default 0, status text default '{FIELD_STATUS.nothing.value}', downloaded bit default 0"

    if "linc" in pipelines:
        dbstr += f", sas_id_calibrator1 text default NULL, sas_id_calibrator2 text default NULL, sas_id_calibrator_final text default NULL, sas_id_target text primary key default NULL, status_calibrator1 smallint default {PIPELINE_STATUS.nothing.value}, status_calibrator2 smallint default {PIPELINE_STATUS.nothing.value}, status_target smallint default {PIPELINE_STATUS.nothing.value}"
    if "ddf-pipeline" in pipelines:
        dbstr += f", status_ddf smallint default {PIPELINE_STATUS.nothing.value}"
    if "vlbi-delay-widefield" in pipelines:
        dbstr += f", status_vlbi_delay smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_dd smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_ddf_subtract smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_intermediate_img smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_facet_subtract smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_facet_imaging smallint default {PIPELINE_STATUS.nothing.value}"
    if "vlbi-delay-single-target" in pipelines:
        dbstr += f", status_vlbi_delay smallint default {PIPELINE_STATUS.nothing.value}"
        dbstr += f", status_vlbi_dd smallint default {PIPELINE_STATUS.nothing.value}"
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
        str, Parameter(help="Pipelines this field needs to be processed with.")
    ] = "",
    intent: Annotated[
        Optional[Literal["PILOT-widefield", "PILOT-single-target"]],
        Parameter(help="Workflow to register with its component pipelines."),
    ] = None,
    fetch_metadata: Annotated[
        bool, Parameter(help="Read observation metadata from the LTA at registration.")
    ] = False,
):
    if len(sas_id_calibrators) not in (1, 2):
        raise ValueError("Provide one or two calibrator SAS IDs.")
    components = tuple(part.strip().lower() for part in pipelines.split(",")) if pipelines else None
    if components is not None and any(part not in PIPELINE_CHECKPOINTS for part in components):
        raise ValueError("Provide valid comma-separated component pipeline names.")
    if intent is not None:
        expected = INTENT_PIPELINES[intent]
        if components is not None and set(components) != set(expected):
            raise ValueError("The component pipelines do not match the selected workflow.")
        components = expected
    table = '"' + table_name.replace('"', '""') + '"'
    values = dict(target_name=field_name, sas_id_target=sas_id_target)
    values.update(zip(("sas_id_calibrator1", "sas_id_calibrator2"), sas_id_calibrators))
    with sqlite3.connect(dbname) as database:
        columns = {row[1] for row in database.execute(f"PRAGMA table_info({table})")}
        if components is not None:
            required = {f"status_{step}" for part in components for step in PIPELINE_CHECKPOINTS[part]}
            if not required.issubset(columns):
                raise ValueError("The table lacks checkpoint columns for the selected pipelines.")
            values["pipelines"] = ",".join(components)
            if intent is None:
                intent = next((name for name, parts in INTENT_PIPELINES.items()
                               if set(parts) == set(components)), None)
        if intent is not None:
            if intent == "PILOT-single-target" and "status_ddf" in columns:
                raise ValueError("Single-target processing requires a table without DDF columns.")
            values["intent"] = intent
        values["last_updated_at"] = datetime.now(timezone.utc).isoformat()
        required = values.keys() | (
            OBSERVATION_METADATA_COLUMNS | {"last_updated_at"} if fetch_metadata else set()
        )
        missing = required - columns
        if missing:
            raise ValueError(f"The table lacks required columns: {', '.join(sorted(missing))}.")
        if fetch_metadata:
            values.update(get_observation_metadata(sas_id_target))
            values["last_updated_at"] = datetime.now(timezone.utc).isoformat()
        placeholders = ", ".join("?" for _ in values)
        database.execute(f"INSERT INTO {table} ({', '.join(values)}) VALUES ({placeholders})", list(values.values()))
        if components is None:
            registered = database.execute(
                f"SELECT pipelines FROM {table} WHERE sas_id_target=?", (sas_id_target,)
            ).fetchone()[0]
            selected = {part.strip().lower() for part in (registered or "").split(",")}
            inferred = next((name for name, parts in INTENT_PIPELINES.items() if set(parts) == selected), None)
            if inferred is not None:
                database.execute(f"UPDATE {table} SET intent=? WHERE sas_id_target=?", (inferred, sas_id_target))
    print(f"Added field {field_name} to {table_name} in {dbname}")


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
