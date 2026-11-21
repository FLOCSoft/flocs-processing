from enum import Enum
import functools
import sqlite3
from datetime import datetime, timezone


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


FACET_STEPS = tuple(
    f"vlbi_facet_imaging_{resolution}" for resolution in ("6.0", "3.0", "1.5", "0.6", "0.3")
)
STEP_COLUMNS = {
    step: "status_" + step.replace(".", "p")
    for step in (
        "download", "calibrator1", "calibrator2", "target", "vlbi_delay", "prepare_ddf",
        "ddf", "prepare_ddf_subtract", "vlbi_ddf_subtract", "vlbi_dd", "vlbi_intermediate_img",
        "vlbi_facet_subtract", *FACET_STEPS,
    )
}


class FlocsDB:
    def __init__(self, dbname: str, db_table: str):
        self.DATABASE = dbname
        self.TABLE_NAME = db_table
        self._table = '"' + db_table.replace('"', '""') + '"'

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

    @staticmethod
    def _progress_status(row):
        statuses = {row[column] for column in STEP_COLUMNS.values()}
        if PIPELINE_STATUS.await_approval.value in statuses and PIPELINE_STATUS.processing.value not in statuses:
            return "awaiting_approval"
        return "running"

    def _update(self, name, target, values, run_id=None):
        owner = run_id if run_id is not None else self._run_id()
        values = dict(values, last_updated_at=self._now())
        where = "target_name=? AND sas_id_target=?"
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
        if intent not in {"%", "vlbi-widefield", "vlbi-single-target"}:
            raise ValueError(f"Unknown processing mode: {intent}")
        where, parameters = [], []
        if obsid:
            where.append("sas_id_target=?")
            parameters.append(str(obsid))
        else:
            where.append("status='queued'")
            where.extend(f"COALESCE({column}, 0)<>?" for column in STEP_COLUMNS.values())
            parameters.extend([PIPELINE_STATUS.processing.value] * len(STEP_COLUMNS))
        if intent != "%":
            where.append("pipelines=?")
            parameters.append(intent)
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            return db.execute(
                f"SELECT * FROM {self._table} WHERE {' AND '.join(where)} ORDER BY priority DESC, sas_id_target",
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
                f"SELECT * FROM {self._table} WHERE target_name=? AND sas_id_target=? AND pipelines=?",
                (name, str(target), pipeline),
            ).fetchone()
            if row is None:
                return None
            if row["workflow_run_id"] == run_id:
                if row["status"] in {"failed", "finished"}:
                    db.execute(
                        f"UPDATE {self._table} SET status=?, "
                        "processing_end_time=NULL, last_error=NULL, last_updated_at=? "
                        "WHERE sas_id_target=? AND workflow_run_id=?",
                        (self._progress_status(row), self._now(), str(target), run_id),
                    )
                    row = db.execute(
                        f"SELECT * FROM {self._table} WHERE sas_id_target=?", (str(target),)
                    ).fetchone()
                return dict(row)
            if row["status"] not in {"queued", "failed"}:
                return None
            if any(row[column] == PIPELINE_STATUS.processing.value for column in STEP_COLUMNS.values()):
                return None
            values = dict(workflow_run_id=run_id, status=self._progress_status(row),
                          processing_start_time=self._now(), processing_end_time=None,
                          processing_cluster=cluster, processing_user=user, processing_host=host,
                          last_updated_at=self._now(), last_error=None)
            for key, value in (("input_location", input_location), ("work_directory", work_directory),
                               ("output_location", output_location)):
                if value is not None:
                    values[key] = str(value)
            db.execute(
                f"UPDATE {self._table} SET " + ", ".join(f"{key}=?" for key in values)
                + " WHERE target_name=? AND sas_id_target=?",
                list(values.values()) + [name, str(target)],
            )
            return dict(db.execute(
                f"SELECT * FROM {self._table} WHERE sas_id_target=?", (str(target),)
            ).fetchone())

    def get_step_status(self, name, identifier, target):
        rows = self.get_db_columns(str(target))
        if not rows or rows[0]["target_name"] != name:
            raise LookupError(f"Field {name}, target {target} is not registered.")
        if identifier == "vlbi_facet_imaging":
            return rows[0]["status_vlbi_facet_imaging"]
        return rows[0][STEP_COLUMNS[identifier]]

    def is_step_finished(self, name, identifier, target):
        return self.get_step_status(name, identifier, target) in {
            PIPELINE_STATUS.finished.value, PIPELINE_STATUS.await_approval.value
        }

    def is_step_processing(self, name, identifier, target):
        return self.get_step_status(name, identifier, target) == PIPELINE_STATUS.processing.value

    def _set_step(self, name, identifier, target, status):
        owner = self._run_id()
        steps = FACET_STEPS if identifier == "vlbi_facet_imaging" else (identifier,)
        columns = [STEP_COLUMNS[step] for step in steps]
        with sqlite3.connect(self.DATABASE) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                f"SELECT * FROM {self._table} WHERE target_name=? AND sas_id_target=?", (name, str(target))
            ).fetchone()
            if row is None or (owner is not None and row["workflow_run_id"] != owner):
                raise LookupError(f"Field {name}, target {target} is missing or belongs to another workflow.")
            retrying = (row["status"] == "failed" and status == PIPELINE_STATUS.processing.value
                        and any(row[column] == PIPELINE_STATUS.error.value for column in columns))
            values = dict.fromkeys(columns, status)
            values["last_updated_at"] = self._now()
            if status != PIPELINE_STATUS.nothing.value:
                phase = "calibrators" if identifier in {"calibrator1", "calibrator2"} else identifier
                if identifier in FACET_STEPS:
                    phase = "vlbi_facet_imaging"
                values["processing_step"] = phase
                if status == PIPELINE_STATUS.error.value:
                    values["last_error"] = f"{identifier} failed."
            starting = (status in {PIPELINE_STATUS.processing.value, PIPELINE_STATUS.await_approval.value}
                        and row["status"] not in {"failed", "finished"})
            if row["status"] in {"running", "awaiting_approval"} or starting or retrying:
                values["status"] = self._progress_status(dict(row) | values)
            if status == PIPELINE_STATUS.processing.value and (starting or retrying):
                values["last_error"] = None
                if retrying:
                    values["processing_end_time"] = None
            db.execute(
                f"UPDATE {self._table} SET " + ", ".join(f"{key}=?" for key in values)
                + " WHERE target_name=? AND sas_id_target=?",
                list(values.values()) + [name, str(target)],
            )

    def reset_field(self, name, target):
        self._update(name, target, {"processing_step": "pending", "status": "queued",
                                   "workflow_run_id": None, "processing_end_time": None, "last_error": None})

    def set_field_downloading(self, name, target):
        self.set_status_processing(name, "download", target)

    def set_field_processing(self, name, target):
        self._update(name, target, {"status": "running"})

    def set_field_finished(self, name, target, run_id=None):
        self._update(name, target, {"processing_step": "finished", "status": "finished",
                                   "processing_end_time": self._now(), "last_error": None}, run_id)

    def set_field_failed(self, name, target, error=None, run_id=None):
        self._update(name, target, {"status": "failed", "processing_end_time": self._now(),
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

    def set_status_downloaded(self, name, identifier, target):
        self._set_step(name, identifier, target, PIPELINE_STATUS.downloaded.value)

    def set_field_downloaded(self, name, target):
        self._set_step(name, "download", target, PIPELINE_STATUS.finished.value)

    def update_target_sas_id(self, old_sas_id: str, new_sas_id: str):
        if not old_sas_id or not new_sas_id:
            raise ValueError("Both old and new target SAS IDs must be provided.")
        if str(old_sas_id) == str(new_sas_id):
            return
        owner = self._run_id()
        where = "sas_id_target=?"
        parameters = [str(new_sas_id), str(old_sas_id)]
        if owner is not None:
            where += " AND workflow_run_id=?"
            parameters.append(owner)
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.execute(f"UPDATE {self._table} SET sas_id_target=? WHERE {where}", parameters)
            if cursor.rowcount != 1:
                raise LookupError(f"Target {old_sas_id} is missing or belongs to another workflow.")

    def set_final_calibrator(self, name, target, final_cal):
        self._update(name, target, {"sas_id_calibrator_final": str(final_cal), "last_error": None})

    def set_delay_calibrator(self, name, target, source, phase_score=None, amp_score=None):
        self._update(name, target, {
            "selected_delay_calibrator": str(source),
            "delay_calibrator_phase_score": phase_score,
            "delay_calibrator_amp_score": amp_score,
        })

    def set_dd_calibrator_scores(self, name, target, n_dd_calibrators, phase_min, phase_max,
                                 phase_median, amp_min, amp_max, amp_median):
        self._update(name, target, {
            "n_dd_calibrators": n_dd_calibrators,
            "dd_calibrator_phase_score_min": phase_min,
            "dd_calibrator_phase_score_max": phase_max,
            "dd_calibrator_phase_score_median": phase_median,
            "dd_calibrator_amp_score_min": amp_min,
            "dd_calibrator_amp_score_max": amp_max,
            "dd_calibrator_amp_score_median": amp_median,
        })
