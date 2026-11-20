import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from flocs_processing.db_utils import FACET_STEPS, FlocsDB
from flocs_processing.flocs_processing import create_database, migrate_database


def test_parallel_facet_statuses_survive_database_copy(tmp_path):
    path = tmp_path / "processing.db"
    create_database(str(path))
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO processing_flocs (field_name, target_sas_id) VALUES ('field', '123')")
    db = FlocsDB(str(path), "processing_flocs")
    db.claim_field("field", "123", "ilotss:test", pipeline="vlbi-widefield")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda step: db.set_status_finished("field", step, "123"), FACET_STEPS[:-1]))
    assert db.get_step_status("field", "vlbi_facet_imaging", "123") != 2

    copy = tmp_path / "copy.db"
    with sqlite3.connect(path) as source, sqlite3.connect(copy) as destination:
        source.backup(destination)
    copied = FlocsDB(str(copy), "processing_flocs")
    assert all(copied.get_step_status("field", step, "123") == 2 for step in FACET_STEPS[:-1])
    assert copied.get_step_status("field", FACET_STEPS[-1], "123") == 0
    copied.set_status_finished("field", FACET_STEPS[-1], "123")
    assert copied.get_step_status("field", "vlbi_facet_imaging", "123") == 2
    assert copied.get_db_columns("123")[0]["processing_status"] == "running"
    assert set(tmp_path.iterdir()) == {path, copy}


def test_import_step_files_once_and_roll_back_on_invalid_status(tmp_path):
    path = tmp_path / "processing.db"
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE processing_flocs (
            field_name TEXT, target_sas_id TEXT PRIMARY KEY, processing_step TEXT,
            processing_status TEXT, workflow_run_id TEXT, priority INTEGER)""")
        conn.execute("INSERT INTO processing_flocs VALUES ('field', '123', 'vlbi_delay', 'running', 'run', 0)")
    steps = tmp_path / "processing.db.steps" / "123"
    steps.mkdir(parents=True)
    (steps / "target.json").write_text(json.dumps({"status": 2, "run_id": "previous"}))
    delay = steps / "vlbi_delay.json"
    delay.write_text(json.dumps({"status": "unknown", "run_id": "run"}))

    with pytest.raises(ValueError):
        migrate_database(str(path))
    with sqlite3.connect(path) as conn:
        assert "status_target" not in {col[1] for col in conn.execute("PRAGMA table_info(processing_flocs)")}

    delay.write_text(json.dumps({"status": 3, "run_id": "run"}))
    migrate_database(str(path))
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT status_target, status_vlbi_delay FROM processing_flocs").fetchone() == (2, 3)
        conn.execute("UPDATE processing_flocs SET status_vlbi_delay=2")
    migrate_database(str(path))
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT status_vlbi_delay, workflow_run_id FROM processing_flocs").fetchone() == (2, "run")


def test_migrate_upstream_keeps_original_facet_evidence_and_routes(tmp_path):
    path = tmp_path / "processing.db"
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE processing_flocs (
            target_name TEXT, sas_id_target TEXT PRIMARY KEY, pipelines TEXT,
            priority INTEGER, status TEXT, downloaded INTEGER, status_target SMALLINT,
            status_vlbi_facet_imaging SMALLINT)""")
        conn.execute("""INSERT INTO processing_flocs VALUES (
            'field', '123', 'linc,ddf-pipeline,vlbi-delay-widefield', 3, 'processing', 1, 2, 2)""")
        conn.execute("INSERT INTO processing_flocs VALUES ('other', '456', 'linc', 0, 'not started', 0, 0, 0)")

    with pytest.raises(ValueError, match="legacy-pipeline"):
        migrate_database(str(path))
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT count(*) FROM processing_flocs WHERE target_name='field'").fetchone() == (1,)

    migrate_database(str(path), legacy_pipeline="vlbi-single-target")
    db = FlocsDB(str(path), "processing_flocs")
    field = db.get_db_columns("123")[0]
    assert (field["pipelines"], field["processing_status"], field["workflow_run_id"]) == (
        "vlbi-widefield", "running", None
    )
    assert (field["downloaded"], field["status_target"]) == (1, 2)
    assert db.get_db_columns("456")[0]["pipelines"] == "vlbi-single-target"
    assert all(db.get_step_status("field", step, "123") == 0 for step in FACET_STEPS)
    assert db.claim_field("field", "123", "ilotss:new", pipeline="vlbi-widefield") is None
    with sqlite3.connect(path) as conn:
        assert conn.execute(
            "SELECT status_vlbi_facet_imaging FROM processing_flocs_legacy WHERE sas_id_target='123'"
        ).fetchone() == (2,)
