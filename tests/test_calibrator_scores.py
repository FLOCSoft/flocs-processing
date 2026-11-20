import sqlite3
from unittest.mock import Mock

import pytest
from losoto.h5parm import h5parm

from flocs_processing.db_utils import FlocsDB
from flocs_processing.flocs_processing import create_database


@pytest.fixture
def runners(tmp_path, monkeypatch):
    config = tmp_path / "airflow.cfg"
    config.write_text(
        "SLURM_ACCOUNT=test\nSLURM_QUEUE=test\nDATA_DIR=.\nOUTPUT_DIR=.\n"
        "PROCESSING_DIR=.\nNN_MODEL_CACHE=.\nDDF_CONFIG=.\n"
        "FLUX_CALIBRATOR_TEMPLATE=.\nNEEDS_MANUAL_APPROVAL_DELAY=true\n"
    )
    monkeypatch.setenv("FLOCS_AIRFLOW_CONFIG", str(config))
    from flocs_processing import pipeline_runners

    return pipeline_runners


def write_solution(path, directions):
    h5 = h5parm(str(path), readonly=False)
    try:
        h5.makeSolset("sol000").obj.source.append(directions)
    finally:
        h5.close()


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "test.db"
    create_database(str(path))
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO processing_flocs (field_name, target_sas_id) VALUES ('P136','123')")
    return FlocsDB(str(path), "processing_flocs")


def test_final_dd_scores(tmp_path, runners, database):
    field = {"field_name": "P136", "target_sas_id": "123", "pipelines": "vlbi-widefield"}
    results = tmp_path / "VLBI_dd-calibration_L123_1/results_VLBI_dd-calibration"
    results.mkdir(parents=True)
    first = "ILTJ090611.25+724054.7"
    second = "ILTJ085348.40+722314.2"
    third = "ILTJ085607.21+711850.1"
    positions = [(1.2, 0.4), (1.0, -0.2), (1.3, 0.1)]
    write_solution(results / "merged.h5", list(zip(["Dir01", "Dir00", "Dir02"], positions)))
    for source, position in zip([first, second, third], positions):
        write_solution(results / f"select_best_{source}.h5", [("Dir00", position)])
    (results / "validation.csv").write_text(
        "source_id,phase_score,amp_score,accept_image,accept_solutions\n"
        f"{first},0.9,0.8,True,False\n"  # rejected strong attempt
        f"{second},0.6,,True,True\n"  # phase-only solution
        "unused,1,1,True,True\n"  # absent from merged.h5
        f"{third},0.72,0.92,True,True\n"
        f"{first},0.8,0.98,True,True\n"  # successful weak retry
    )
    runners.finish_dd_calibration(field, database, tmp_path)
    with sqlite3.connect(database.DATABASE) as conn:
        row = conn.execute("""
            SELECT status_vlbi_dd, n_dd_calibrators, dd_calibrator_phase_score_min,
                   dd_calibrator_phase_score_max, dd_calibrator_phase_score_median,
                   dd_calibrator_amp_score_min, dd_calibrator_amp_score_max,
                   dd_calibrator_amp_score_median FROM processing_flocs
        """).fetchone()
    assert row == pytest.approx((2, 3, 0.6, 0.8, 0.72, 0.92, 0.98, 0.95))


def test_unmatched_dd_direction_fails_step(tmp_path, runners):
    field = {"field_name": "P136", "target_sas_id": "123", "pipelines": "vlbi-widefield"}
    results = tmp_path / "VLBI_dd-calibration_L123_1/results_VLBI_dd-calibration"
    results.mkdir(parents=True)
    write_solution(results / "merged.h5", [("Dir00", (1.0, 0.1))])
    write_solution(results / "select_best_source.h5", [("Dir00", (1.1, 0.1))])
    (results / "validation.csv").write_text(
        "source_id,phase_score,amp_score,accept_image,accept_solutions\n"
        "best_source.h5,0.9,,True,True\n"
    )
    db = Mock()
    with pytest.raises(ValueError, match="Cannot match"):
        runners.finish_dd_calibration(field, db, tmp_path)
    db.set_status_failed.assert_called_once_with("P136", "vlbi_dd", "123")
    db.set_dd_calibrator_scores.assert_not_called()
    db.set_status_finished.assert_not_called()


def test_dd_without_validation(tmp_path, runners):
    write_solution(tmp_path / "merged.h5", [("Dir00", (1.0, 0.2))])
    assert runners.get_dd_calibrator_scores(tmp_path) == (1, None, None, None, None, None, None)


def test_mixed_dd_labels_and_non_iltj_source(tmp_path, runners):
    positions = [(1.2, 0.4), (1.0, -0.2)]
    write_solution(tmp_path / "merged.h5", list(zip(["target1", "Dir01"], positions)))
    source = "best_target2.h5.addCS.h5.onedir.h5"
    write_solution(tmp_path / f"select_{source}", [("Dir00", positions[1])])
    (tmp_path / "validation.csv").write_text(
        "source_id,phase_score,accept_image,accept_solutions\n"
        "target1,0.8,True,True\n"
        f"{source},0.9,True,True\n"
    )
    count, minimum, maximum, median, *amplitude = runners.get_dd_calibrator_scores(tmp_path)
    assert (count, minimum, maximum) == (2, 0.8, 0.9)
    assert median == pytest.approx(0.85)
    assert amplitude == [None, None, None]


def test_delay_scores_follow_selected_solution(tmp_path, runners, database, monkeypatch):
    field = {"field_name": "P136", "target_sas_id": "123"}
    results = tmp_path / "VLBI_delay-calibration_L123_1/results_VLBI_delay-calibration"
    results.mkdir(parents=True)
    (tmp_path / "VLBI_delay-calibration_L1234_2").mkdir()
    source = "ILTJ090500.54+723030.5"
    solution = results / f"merged_selfcalcycle019_linearfulljones_{source}.h5"
    write_solution(solution, [("Dir00", (1.0, 0.1))])
    (results / "validation_solutions.csv").write_text(
        "source_id,phase_score,amp_score,phase_score_current_before_flagging\n"
        "unused,0.9,1.0,0.5\n"
        f"{source},0.8,,0.01\n"
    )
    monkeypatch.setattr(runners, "NEEDS_MANUAL_APPROVAL_DELAY", True)
    runners.finish_delay_calibration(field, database, tmp_path)
    with sqlite3.connect(database.DATABASE) as conn:
        row = conn.execute("""
            SELECT status_vlbi_delay, selected_delay_calibrator,
                   delay_calibrator_phase_score, delay_calibrator_amp_score FROM processing_flocs
        """).fetchone()
    assert row == (3, source, 0.8, None)
