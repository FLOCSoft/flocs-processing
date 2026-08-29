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


@functools.total_ordering
class PIPELINE_STATUS(Enum):
    nothing = 0
    downloaded = 1
    finished = 2
    running = 3
    processing = 98
    error = 99

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value == other.value

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
    dbstr = f"create table {table_name}(target_name text default NULL, pipelines text default '{pipeline_str}', priority int default 0, finished bit default 0, downloaded bit default 0"

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
    dbstr += ");"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Creating table via: {" ".join(cmd)}")

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
):
    dbstr = f"insert into {table_name} (target_name"
    if len(sas_id_calibrators) == 1:
        dbstr += ", sas_id_calibrator1"
    if len(sas_id_calibrators) == 2:
        dbstr += ", sas_id_calibrator1, sas_id_calibrator2"
    dbstr += f", sas_id_target) values ('{field_name}', "
    if len(sas_id_calibrators) == 1:
        dbstr += f"'{sas_id_calibrators[0]}', "
    if len(sas_id_calibrators) == 2:
        dbstr += f"'{sas_id_calibrators[0]}', '{sas_id_calibrators[1]}', "
    dbstr += f"'{sas_id_target}')"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Adding field {field_name} to {table_name} via: {" ".join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to update table {table_name} in database {dbname}.")


@app.command()
def update_field(
    field_name: Annotated[str, Parameter(help="Name of the source/field to add.")],
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
                    db.set_status_downloaded(field_name, sas_id_target)
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
                db.set_status_downloaded(field_name, sas_id_target)
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
