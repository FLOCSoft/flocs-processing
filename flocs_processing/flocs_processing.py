#!/usr/bin/env python
from .db_utils import FACET_STEPS, STEP_COLUMNS, FlocsDB
from .processors import FlocsAirflowProcessor
from cyclopts import Parameter
from enum import Enum
import cyclopts
import functools
import json
from pathlib import Path
import sqlite3
import subprocess
import structlog
from typing import Annotated, Literal, Optional, get_args
from urllib.parse import quote

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


# These columns also form the additive migration for databases created by this branch.
CALIBRATOR_COLUMNS = {
    "delay_calibrator_phase_score": "REAL",
    "delay_calibrator_amp_score": "REAL",
    "n_dd_calibrators": "INTEGER",
    **{f"dd_calibrator_{score}_score_{stat}": "REAL"
       for score in ("phase", "amp") for stat in ("min", "max", "median")},
}


def _status_columns() -> dict[str, str]:
    facets = ", ".join(STEP_COLUMNS[step] for step in FACET_STEPS)
    # The summary can only become finished once all five imaging jobs are finished.
    return {
        **dict.fromkeys(STEP_COLUMNS.values(), "SMALLINT NOT NULL DEFAULT 0"),
        "downloaded": "INTEGER GENERATED ALWAYS AS (status_download IN (2, 3)) VIRTUAL",
        "status_vlbi_facet_imaging": f"""SMALLINT GENERATED ALWAYS AS (
            CASE WHEN 99 IN ({facets}) THEN 99
                 WHEN 98 IN ({facets}) THEN 98
                 WHEN 3 IN ({facets}) THEN 3
                 WHEN min({facets}) = max({facets}) THEN min({facets})
                 ELSE 98 END) VIRTUAL""",
    }


def _quote_table(table_name: str) -> str:
    return '"' + table_name.replace('"', '""') + '"'


def _create_table(db: sqlite3.Connection, table: str, pipelines: str):
    if pipelines not in {"vlbi-widefield", "vlbi-single-target"}:
        raise ValueError(f"Unknown processing mode: {pipelines}")
    extra_columns = ", ".join(
        f"{name} {definition}" for name, definition in (CALIBRATOR_COLUMNS | _status_columns()).items()
    )
    db.execute(f"""CREATE TABLE {table} (
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
        last_error TEXT,
        {extra_columns}
    )""")


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
    with sqlite3.connect(dbname) as db:
        _create_table(db, _quote_table(table_name), pipelines)


def _import_step_files(db: sqlite3.Connection, table: str, dbname: str, new_statuses: set[str]):
    if not new_statuses:
        return
    database = Path(dbname).expanduser().resolve()
    steps = database.with_name(database.name + ".steps")
    if steps.exists() and not steps.is_dir():
        raise ValueError(f"Expected a directory at {steps}.")
    valid_statuses = {state.value for state in PIPELINE_STATUS}
    for row in db.execute(f"SELECT * FROM {table}").fetchall():
        target = row["target_sas_id"]
        component = quote(str(target), safe="").replace(".", "%2E") or "%EMPTY"
        directory = steps / component
        # The previous CLI could queue, start a download or manually finish a field
        # before creating its first step file. Every other phase needed this directory.
        if not directory.is_dir():
            if directory.exists():
                raise ValueError(f"Expected a directory at {directory}.")
            phase = row["processing_step"]
            if phase in {"pending", "download"} or (phase == "finished" and not row["workflow_run_id"]):
                continue
            raise ValueError(f"Cannot import previous step statuses: {directory} is missing.")
        for path in directory.glob("*.json"):
            column = STEP_COLUMNS.get(path.stem)
            if column is None:
                raise ValueError(f"Unknown pipeline step in {path}.")
            status = json.loads(path.read_text())["status"]
            if type(status) is not int or status not in valid_statuses:
                raise ValueError(f"Invalid pipeline status in {path}: {status!r}.")
            if column not in new_statuses and status != row[column]:
                raise ValueError(f"Conflicting SQLite and file statuses for {path}.")
            db.execute(f"UPDATE {table} SET {column}=? WHERE target_sas_id=?", (status, target))


def _copy_legacy_fields(db: sqlite3.Connection, table: str, legacy_table: str, legacy_pipeline: Optional[str]):
    routes = {
        "vlbi-widefield": "vlbi-widefield", "vlbi-delay-widefield": "vlbi-widefield",
        "vlbi-single-target": "vlbi-single-target", "vlbi-delay-single-target": "vlbi-single-target",
    }
    statuses = {
        "not started": "queued", "downloaded": "queued", "downloading": "running",
        "processing": "running", "failed": "failed", "finished": "finished",
    }
    for record in db.execute(f"SELECT * FROM {legacy_table}"):
        old = dict(record)
        target = old["sas_id_target"]
        tokens = {token.strip().lower() for token in (old["pipelines"] or "").split(",")}
        pipelines = {routes[token] for token in tokens if token in routes}
        pipeline = pipelines.pop() if len(pipelines) == 1 else legacy_pipeline
        if not pipeline:
            raise ValueError(f"Target {target} has ambiguous pipelines {old['pipelines']!r}. Specify --legacy-pipeline.")
        values = {
            "field_name": old["target_name"],
            "target_sas_id": target,
            "calibrator1_sas_id": old.get("sas_id_calibrator1"),
            "calibrator2_sas_id": old.get("sas_id_calibrator2"),
            "selected_calibrator_sas_id": old.get("sas_id_calibrator_final"),
            "priority": old["priority"] or 0,
            "pipelines": pipeline,
            "processing_status": statuses[old["status"]],
        }
        if old["downloaded"] or old["status"] == "downloaded":
            values["status_download"] = PIPELINE_STATUS.finished.value
        elif old["status"] == "downloading":
            values["status_download"] = PIPELINE_STATUS.processing.value
        for column in STEP_COLUMNS.values():
            if column in old:
                values[column] = old[column] or 0
        if old["status"] == "finished":
            values["processing_step"] = "finished"
        elif old["status"] == "downloading":
            values["processing_step"] = "download"
        elif old["status"] in {"processing", "failed"}:
            # Legacy rows contain no Airflow run ID and can have several active steps.
            active = [step for step, column in STEP_COLUMNS.items() if values.get(column) in {3, 98, 99}]
            values["processing_step"] = active[0] if len(active) == 1 else "unknown"
            stage_statuses = {values.get(column) for column in STEP_COLUMNS.values()}
            if old["status"] == "processing" and 3 in stage_statuses and 98 not in stage_statuses:
                values["processing_status"] = "awaiting_approval"
        columns = ", ".join(values)
        placeholders = ", ".join("?" for _ in values)
        db.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values.values()))


@app.command()
def migrate_database(
    dbname: Annotated[str, Parameter(help="Existing processing database to migrate.")],
    table_name: Annotated[str, Parameter(help="Processing table to migrate.")] = "processing_flocs",
    legacy_pipeline: Annotated[
        Optional[Literal["vlbi-widefield", "vlbi-single-target"]],
        Parameter(help="Route for master-branch rows whose pipelines do not specify a unique route."),
    ] = None,
):
    """Import previous step statuses and add columns for final calibration scores."""
    table = _quote_table(table_name)
    with sqlite3.connect(Path(dbname).resolve().as_uri() + "?mode=rw", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE")
        existing = {row["name"] for row in db.execute(f"PRAGMA table_xinfo({table})")}
        if "sas_id_target" in existing and "target_name" in existing:
            # Keep the original table, including data the new schema cannot represent,
            # such as a shared facet status with no per-resolution completion records.
            legacy_table = _quote_table(table_name + "_legacy")
            db.execute(f"ALTER TABLE {table} RENAME TO {legacy_table}")
            _create_table(db, table, legacy_pipeline or "vlbi-widefield")
            _copy_legacy_fields(db, table, legacy_table, legacy_pipeline)
        elif "target_sas_id" in existing and "processing_status" in existing:
            for name, definition in (CALIBRATOR_COLUMNS | _status_columns()).items():
                if name not in existing:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            _import_step_files(db, table, dbname, set(STEP_COLUMNS.values()) - existing)
        else:
            raise ValueError(f"Unrecognised processing schema for {table_name} in {dbname}.")


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
