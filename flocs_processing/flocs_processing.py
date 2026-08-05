#!/usr/bin/env python
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from enum import Enum
import cyclopts
import functools
import subprocess
import structlog
from typing import Optional, Annotated

app = cyclopts.App()

logger = structlog.getLogger()


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
def start_airflow_processing(
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
