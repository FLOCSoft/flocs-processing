from enum import Enum
import functools
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime, timezone
from urllib.parse import quote
from uuid import uuid4


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


class FlocsDB:
    def __init__(self, dbname: str, db_table: str):
        self.DATABASE = dbname
        self.TABLE_NAME = db_table
        self._table = '"' + db_table.replace('"', '""') + '"'
        database_path = Path(str(dbname)).expanduser().resolve()
        self._steps = database_path.with_name(database_path.name + ".steps")

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _run_id():
        try:
            from airflow.sdk import get_current_context
        except ImportError:
            return None
        try:
            context = get_current_context()
        except RuntimeError:
            return None
        run = context.get("dag_run")
        if run is None:
            return None
        return f"{run.dag_id}:{run.run_id}"

    def _receipt(self, target, identifier):
        component = quote(str(target), safe="").replace(".", "%2E") or "%EMPTY"
        return self._steps / component / (quote(str(identifier), safe="") + ".json")

    def _read_receipt(self, target, identifier):
        try:
            return json.loads(self._receipt(target, identifier).read_text())
        except FileNotFoundError:
            return {}

    def _update(self, name, target, values, run_id=None):
        owner = run_id if run_id is not None else self._run_id()
        values = dict(values, last_updated_at=self._now())
        where = "field_name=? AND target_sas_id=?"
        parameters = list(values.values()) + [name, str(target)]
        if owner is not None:
            where += " AND workflow_run_id=?"
            parameters.append(owner)
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.execute(
                f"UPDATE {self._table} SET " + ", ".join(f"{key}=?" for key in values) + f" WHERE {where}",
                parameters,
            )
            if cursor.rowcount != 1:
                raise LookupError(f"Field {name}, target {target} is missing or belongs to another workflow.")

    def get_db_columns(self, obsid: str = "", intent="%"):
        modes = {"widefield": "vlbi-widefield", "single-target": "vlbi-single-target",
                 "vlbi-widefield": "vlbi-widefield", "vlbi-single-target": "vlbi-single-target"}
        if intent != "%" and intent not in modes:
            raise ValueError(f"Unknown processing mode: {intent}")
        where, parameters = [], []
        if obsid:
            where.append("target_sas_id=?")
            parameters.append(str(obsid))
        else:
            where.append("processing_status='queued'")
        if intent != "%":
            where.append("pipelines=?")
            parameters.append(modes[intent])
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                f"SELECT * FROM {self._table} WHERE {' AND '.join(where)} ORDER BY priority DESC, target_sas_id",
                parameters,
            ).fetchall()

    def get_run_field(self, run_id):
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(f"SELECT * FROM {self._table} WHERE workflow_run_id=?", (run_id,)).fetchone()
            return dict(row) if row is not None else None

    def claim_field(self, name, target, run_id, *, pipeline, cluster=None, user=None, host=None,
                    input_location=None, work_directory=None, output_location=None):
        if not run_id:
            raise ValueError("A workflow run ID is required to claim a field.")
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                f"SELECT * FROM {self._table} WHERE field_name=? AND target_sas_id=? AND pipelines=?",
                (name, str(target), pipeline),
            ).fetchone()
            if row is None:
                return None
            if row["workflow_run_id"] == run_id:
                if row["processing_status"] in {"failed", "finished"}:
                    db.execute(
                        f"UPDATE {self._table} SET processing_status='running', "
                        "processing_end_time=NULL, last_error=NULL, last_updated_at=? "
                        "WHERE target_sas_id=? AND workflow_run_id=?",
                        (self._now(), str(target), run_id),
                    )
                    row = db.execute(
                        f"SELECT * FROM {self._table} WHERE target_sas_id=?", (str(target),)
                    ).fetchone()
                return dict(row)
            if row["processing_status"] not in {"queued", "failed"}:
                return None
            values = dict(workflow_run_id=run_id, processing_status="running",
                          processing_start_time=self._now(), processing_end_time=None,
                          processing_cluster=cluster, processing_user=user, processing_host=host,
                          last_updated_at=self._now(), last_error=None)
            for key, value in (("input_location", input_location), ("work_directory", work_directory),
                               ("output_location", output_location)):
                if value is not None:
                    values[key] = str(value)
            db.execute(
                f"UPDATE {self._table} SET " + ", ".join(f"{key}=?" for key in values)
                + " WHERE field_name=? AND target_sas_id=?",
                list(values.values()) + [name, str(target)],
            )
            return dict(db.execute(
                f"SELECT * FROM {self._table} WHERE target_sas_id=?", (str(target),)
            ).fetchone())

    def get_step_status(self, name, identifier, target):
        rows = self.get_db_columns(str(target))
        if not rows or rows[0]["field_name"] != name:
            raise LookupError(f"Field {name}, target {target} is not registered.")
        receipt = self._read_receipt(target, identifier)
        return receipt.get("status", PIPELINE_STATUS.nothing.value)

    def is_step_finished(self, name, identifier, target):
        return self.get_step_status(name, identifier, target) in {
            PIPELINE_STATUS.finished.value, PIPELINE_STATUS.await_approval.value
        }

    def is_step_processing(self, name, identifier, target):
        return self.get_step_status(name, identifier, target) == PIPELINE_STATUS.processing.value

    def _set_step(self, name, identifier, target, status):
        owner = self._run_id()
        receipt_path = self._receipt(target, identifier)
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                f"SELECT * FROM {self._table} WHERE field_name=? AND target_sas_id=?", (name, str(target))
            ).fetchone()
            if row is None or (owner is not None and row["workflow_run_id"] != owner):
                raise LookupError(f"Field {name}, target {target} is missing or belongs to another workflow.")
            values = {"last_updated_at": self._now()}
            if status == PIPELINE_STATUS.nothing.value:
                receipt_path.unlink(missing_ok=True)
            else:
                receipt = {"status": status, "run_id": owner or row["workflow_run_id"]}
                receipt_path.parent.mkdir(parents=True, exist_ok=True)
                temporary = receipt_path.with_name(receipt_path.name + "." + uuid4().hex + ".tmp")
                try:
                    temporary.write_text(json.dumps(receipt))
                    os.replace(temporary, receipt_path)
                finally:
                    temporary.unlink(missing_ok=True)
                phase = "calibrators" if identifier in {"calibrator1", "calibrator2"} else identifier
                if str(identifier).startswith("vlbi_facet_imaging_"):
                    phase = "vlbi_facet_imaging"
                values["processing_step"] = phase
                if status == PIPELINE_STATUS.processing.value:
                    if row["processing_status"] not in {"failed", "finished"}:
                        values.update(processing_status="running", last_error=None)
                elif status == PIPELINE_STATUS.await_approval.value:
                    if row["processing_status"] not in {"failed", "finished"}:
                        values["processing_status"] = "awaiting_approval"
                elif status == PIPELINE_STATUS.finished.value:
                    if row["processing_status"] == "awaiting_approval" and row["processing_step"] == phase:
                        values["processing_status"] = "running"
                elif status == PIPELINE_STATUS.error.value:
                    values["last_error"] = f"{identifier} failed."
            db.execute(
                f"UPDATE {self._table} SET " + ", ".join(f"{key}=?" for key in values)
                + " WHERE field_name=? AND target_sas_id=?",
                list(values.values()) + [name, str(target)],
            )

    def reset_field(self, name, target):
        self._update(name, target, {"processing_step": "pending", "processing_status": "queued",
                                   "workflow_run_id": None, "processing_end_time": None, "last_error": None})

    def set_field_downloading(self, name, target):
        self._update(name, target, {"processing_step": "download", "processing_status": "running"})

    def set_field_processing(self, name, target):
        self._update(name, target, {"processing_status": "running"})

    def set_field_finished(self, name, target, run_id=None):
        self._update(name, target, {"processing_step": "finished", "processing_status": "finished",
                                   "processing_end_time": self._now(), "last_error": None}, run_id)

    def set_field_failed(self, name, target, error=None, run_id=None):
        self._update(name, target, {"processing_status": "failed", "processing_end_time": self._now(),
                                   "last_error": str(error) if error is not None else "Workflow failed."}, run_id)

    def set_status_nothing(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.nothing.value)

    def set_status_failed(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.error.value)

    def set_status_processing(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.processing.value)

    def set_status_await_approval(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.await_approval.value)

    def set_status_finished(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.finished.value)

    def set_status_downloaded(self, name, identifier, target=None):
        if target is None:
            self.set_field_downloaded(name, identifier)
        else:
            self._set_step(name, identifier, target, PIPELINE_STATUS.downloaded.value)

    def set_field_downloaded(self, name, target):
        self._set_step(name, "download", target, PIPELINE_STATUS.finished.value)

    def update_target_sas_id(self, old_sas_id: str, new_sas_id: str):
        if not old_sas_id or not new_sas_id:
            raise ValueError("Both old and new target SAS IDs must be provided.")
        if str(old_sas_id) == str(new_sas_id):
            return
        old_path = self._receipt(old_sas_id, "download").parent
        new_path = self._receipt(new_sas_id, "download").parent
        owner = self._run_id()
        moved = False
        try:
            with sqlite3.connect(self.DATABASE) as db:
                db.execute("BEGIN IMMEDIATE")
                if old_path.exists() and new_path.exists():
                    raise FileExistsError(f"Step receipts already exist for target {new_sas_id}.")
                where = "target_sas_id=?"
                parameters = [str(new_sas_id), str(old_sas_id)]
                if owner is not None:
                    where += " AND workflow_run_id=?"
                    parameters.append(owner)
                cursor = db.execute(
                    f"UPDATE {self._table} SET target_sas_id=? WHERE {where}", parameters
                )
                if cursor.rowcount != 1:
                    raise LookupError(f"Target {old_sas_id} is missing or belongs to another workflow.")
                if old_path.exists():
                    old_path.rename(new_path)
                    moved = True
        except Exception:
            if moved:
                new_path.rename(old_path)
            raise

    def set_final_calibrator(self, name, target, final_cal):
        self._update(name, target, {"selected_calibrator_sas_id": str(final_cal), "last_error": None})

    def set_delay_calibrator(self, name, target, source):
        self._update(name, target, {"selected_delay_calibrator": str(source)})
