#!/usr/bin/env python
from .db_utils import FlocsDB
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from enum import Enum
import cyclopts
import functools
import subprocess
import structlog
from typing import Annotated, Literal, Optional, get_args

app = cyclopts.App()

logger = structlog.getLogger()


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
    dbstr = f"""CREATE TABLE {table_name} (
        field_name TEXT NOT NULL,
        target_sas_id TEXT PRIMARY KEY NOT NULL,
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
        calibrator1_sas_id TEXT,
        calibrator2_sas_id TEXT,
        selected_calibrator_sas_id TEXT,
        selected_delay_calibrator TEXT,
        pipelines TEXT NOT NULL DEFAULT '{pipelines}',
        priority INTEGER NOT NULL DEFAULT 0,
        processing_step TEXT NOT NULL DEFAULT 'pending',
        processing_status TEXT NOT NULL DEFAULT 'queued',
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
        last_error TEXT
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
        Optional[Literal["vlbi-widefield", "vlbi-single-target"]],
        Parameter(help="Pipeline this field needs to be processed with."),
    ] = None,
):
    dbstr = f"insert into {table_name} (field_name"
    if len(sas_id_calibrators) == 1:
        dbstr += ", calibrator1_sas_id"
    if len(sas_id_calibrators) == 2:
        dbstr += ", calibrator1_sas_id, calibrator2_sas_id"
    dbstr += ", target_sas_id"
    if pipelines:
        dbstr += ", pipelines"
    dbstr += f") values ('{field_name}', "
    if len(sas_id_calibrators) == 1:
        dbstr += f"'{sas_id_calibrators[0]}', "
    if len(sas_id_calibrators) == 2:
        dbstr += f"'{sas_id_calibrators[0]}', '{sas_id_calibrators[1]}', "
    dbstr += f"'{sas_id_target}'"
    if pipelines:
        dbstr += f", '{pipelines}'"
    dbstr += ")"

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
        p_list = [p for p in get_args(PIPELINES) if p not in {"all", "vlbi_facet_imaging"}]
    elif pipeline == "vlbi_facet_imaging":
        p_list = [p for p in get_args(PIPELINES) if p.startswith("vlbi_facet_imaging_")]
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
