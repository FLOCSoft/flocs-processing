from flocs_processing.db_utils import PIPELINE_STATUS, FlocsDB
from flocs_processing.pipeline_runners import (
    get_most_recent_run,
    run_linc_calibrator_cwltool,
    run_linc_calibrator_toil,
    run_linc_target_cwltool,
    run_linc_target_toil,
    run_pilot_delay_cwltool,
    run_pilot_delay_toil,
    run_pilot_ddcal_cwltool,
    run_pilot_ddcal_toil,
    run_pilot_facet_imaging_toil,
    run_pilot_facet_subtract_toil,
    run_pilot_intermediate_image_toil,
    run_pilot_process_ddf_toil,
    run_prepare_ddf,
    run_prepare_ddf_subtract,
)

import configparser
import datetime
import os
import pathlib
import random
import re
import sqlite3
import subprocess
import time

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk.exceptions import AirflowSkipException
from airflow.task.trigger_rule import TriggerRule
from flocs_lta.lta_search import ObservationStager
from ilotss.assess_calibrators import assess_and_compare
from stager_access import get_surls_requested, get_surls_online

if "FLOCS_AIRFLOW_CONFIG" not in os.environ:
    raise RuntimeError(
        "FLOCS_AIRFLOW_CONFIG environment variable not set. Please point this to a valid configuration file."
    )

CONFIG_FILE: str = os.getenv("FLOCS_AIRFLOW_CONFIG") or ""
if not os.path.isfile(CONFIG_FILE):
    raise RuntimeError(f"{CONFIG_FILE} is not a valid file")

parser = configparser.ConfigParser()
parser.optionxform = str  # ty: ignore[invalid-assignment]
with open(CONFIG_FILE, "r") as config:
    parser.read_string("[DEFAULT]\n" + config.read())

print("Config summary:")
for k, v in parser["DEFAULT"].items():
    print(f"{k}: {v}")

TABLE_NAME = parser["DEFAULT"]["TABLE_NAME"]
DATABASE = parser["DEFAULT"]["DATABASE"]
SLURM_ACCOUNT = parser["DEFAULT"]["SLURM_ACCOUNT"]
SLURM_QUEUE = parser["DEFAULT"]["SLURM_QUEUE"]
DATA_DIR = parser["DEFAULT"]["DATA_DIR"]
OUTPUT_DIR = parser["DEFAULT"]["OUTPUT_DIR"]
PROCESSING_DIR = parser["DEFAULT"]["PROCESSING_DIR"]
NN_MODEL_CACHE = parser["DEFAULT"]["NN_MODEL_CACHE"]
DDF_CONFIG = parser["DEFAULT"]["DDF_CONFIG"]
FLUX_CALIBRATOR_TEMPLATE = parser["DEFAULT"]["FLUX_CALIBRATOR_TEMPLATE"]
NEEDS_MANUAL_APPROVAL_DELAY = bool(parser["DEFAULT"]["NEEDS_MANUAL_APPROVAL_DELAY"])

CWL_RUNNER_LINC_CALIBRATOR = "cwltool"
CWL_RUNNER_LINC_TARGET = "toil"
CWL_RUNNER_PILOT_DELAY = "toil"
CWL_RUNNER_PILOT_DDCAL = "toil"

CURRENT_DB = FlocsDB(DATABASE, TABLE_NAME)


def get_approval(field, identifier, needs_approval):
    if not needs_approval:
        return field
    with sqlite3.connect(DATABASE) as db:
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        columns = f"sas_id_target,status_{identifier}"
        field = cursor.execute(
            f"select {columns} from {TABLE_NAME} where sas_id_target=='{field['sas_id_target']}'"
        ).fetchall()
        status = field[0][f"status_{identifier}"]
    if status == PIPELINE_STATUS.finished.value:
        return field


@dag(max_active_runs=1)
def pilot_widefield():
    @task
    def get_unprocessed_target():
        field = None
        for dbrow in CURRENT_DB.get_db_columns():
            is_processing = False
            row = dict(dbrow)
            status_keys = filter(lambda x: x.startswith("status_"), row.keys())
            for key in status_keys:
                if row[key] == PIPELINE_STATUS.processing.value:
                    is_processing = True
                    break
            if not is_processing:
                # Only select a field if nothing is processing it.
                field = dict(row)
                break
        if not field:
            raise AirflowSkipException("No unprocessed fields found.")
        print(field["target_name"])
        return field

    @task.short_circuit
    def check_fields():
        fields = CURRENT_DB.get_db_columns()
        return bool(fields)

    @task
    def download_field(field):
        if field["downloaded"]:
            return field
        else:
            stage_calibrators = False
            num_downloaded_calib1 = 0
            num_downloaded_calib2 = 0
            num_staged_calib = 0
            num_staged_targ = 0
            if os.path.exists(f"srms_{field['sas_id_target']}_calibrators.txt"):
                print("Found srm file; counting calibrator SRMs.")
                out = subprocess.check_output(
                    f"wc -l srms_{field['sas_id_target']}_calibrators.txt | cut -f 1 -d ' '",
                    text=True,
                    shell=True,
                )
                num_staged_calib = int(out.strip())

            if os.path.exists(f"srms_{field['sas_id_target']}.txt"):
                print("Found srm file; counting target SRMs.")
                out = subprocess.check_output(
                    f"wc -l srms_{field['sas_id_target']}.txt | cut -f 1 -d ' '",
                    text=True,
                    shell=True,
                )
                num_staged_targ = int(out.strip())

            if field["sas_id_calibrator1"]:
                ms_folder = f"L{field['sas_id_calibrator1']}"
                cal1_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "calibrator", ms_folder
                )
                if os.path.exists(cal1_full_path):
                    num_downloaded_calib1 = len(
                        list(pathlib.Path(cal1_full_path).glob("*.MS"))
                    )
                else:
                    stage_calibrators = True

            if field["sas_id_calibrator2"]:
                ms_folder = f"L{field['sas_id_calibrator2']}"
                cal2_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "calibrator", ms_folder
                )
                if os.path.exists(cal2_full_path):
                    num_downloaded_calib2 = len(
                        list(pathlib.Path(cal2_full_path).glob("*.MS"))
                    )
                else:
                    stage_calibrators = True

            num_downloaded_calib = num_downloaded_calib1 + num_downloaded_calib2
            if num_downloaded_calib == num_staged_calib:
                print(
                    f"Number of staged calibrator MSes ({num_staged_calib}) equals number of downloaded MSes ({num_downloaded_calib}); not staging calibrators again."
                )
                stage_calibrators = False
            else:
                print(
                    f"Number of staged calibrator MSes ({num_staged_calib}) does NOT equal number of downloaded MSes ({num_downloaded_calib}); restaging calibrators and resuming download."
                )
                stage_calibrators = True

            stage_target = False
            if field["sas_id_target"]:
                ms_folder = f"L{field['sas_id_target']}"
                target_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "target", ms_folder
                )
                if os.path.exists(target_full_path):
                    num_downloaded_targ = len(
                        list(pathlib.Path(target_full_path).glob("*.MS"))
                    )
                    if num_downloaded_targ == num_staged_targ:
                        print(
                            f"Number of staged target MSes ({num_staged_targ}) equals number of downloaded MSes ({num_downloaded_targ}); not staging target again."
                        )
                        stage_target = False
                    else:
                        print(
                            f"Number of staged target MSes ({num_staged_targ}) does NOT equal number of downloaded MSes ({num_downloaded_targ}); staging target again and resuming download."
                        )
                        stage_target = True
            else:
                raise AirflowFailException(
                    f"No target SAS ID in database for field {field['target_name']}"
                )

            if stage_calibrators or stage_target:
                print(f"Field {field['sas_id_target']} is not downloaded.")
                stager = ObservationStager(get_surls=True)
                stager.find_observation_by_sasid(
                    "ALL",
                    field["sas_id_target"],
                    None,
                    120,
                    168,
                )
                if stage_calibrators:
                    stager.find_nearest_calibrators(2, 120, 168)
                    stage_id_calibrators = stager.stage_calibrators()
                if stage_target:
                    stage_id_target = stager.stage_target()
            else:
                return field

            calibrator_staged = False
            target_staged = False
            calibrator_downloaded = not stage_calibrators
            target_downloaded = not stage_target
            while True:
                if not calibrator_downloaded:
                    if len(get_surls_online(stage_id_calibrators)) == len(
                        get_surls_requested(stage_id_calibrators)
                    ):
                        calibrator_staged = True
                    if calibrator_staged and not calibrator_downloaded:
                        dl_path = os.path.join(
                            DATA_DIR, field["target_name"], "calibrator"
                        )
                        cmd = f"flocs-lta download --outdir {dl_path} {stage_id_calibrators}"
                        with (
                            open(
                                f"log_download_calibrators_{field['target_name']}.txt",
                                "w+",
                            ) as f_out,
                            open(
                                f"log_download_calibrators_{field['target_name']}_err.txt",
                                "w+",
                            ) as f_err,
                        ):
                            proc = subprocess.run(
                                cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                            )
                            if not proc.returncode:
                                calibrator_downloaded = True
                            else:
                                raise RuntimeError

                if not target_downloaded:
                    if len(get_surls_online(stage_id_target)) == len(
                        get_surls_requested(stage_id_target)
                    ):
                        target_staged = True
                    if target_staged and not target_downloaded:
                        dl_path = os.path.join(DATA_DIR, field["target_name"], "target")
                        cmd = f"flocs-lta download --outdir {dl_path} {stage_id_target}"
                        with (
                            open(
                                f"log_download_target_{field['target_name']}.txt",
                                "w+",
                            ) as f_out,
                            open(
                                f"log_download_target_{field['target_name']}_err.txt",
                                "w+",
                            ) as f_err,
                        ):
                            proc = subprocess.run(
                                cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                            )
                            if not proc.returncode:
                                CURRENT_DB.set_status_downloaded(
                                    field["target_name"],
                                    field["sas_id_target"],
                                )
                                target_downloaded = True
                            else:
                                raise RuntimeError
                if calibrator_downloaded and target_downloaded:
                    break
                time.sleep(60)

    @task
    def run_linc_calibrator1(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if not field["sas_id_calibrator1"]:
            raise AirflowSkipException("Calibrator 1 does not exist, skipping.")
        if field["status_calibrator1"] == PIPELINE_STATUS.finished:
            print(
                f"Flux density calibrator {field['sas_id_calibrator1']} for observation {field['target_name']} {field['sas_id_target']} already processed."
            )
            return field
        else:
            if CWL_RUNNER_LINC_CALIBRATOR == "cwltool":
                run_linc_calibrator_cwltool(field, calibrator_field=1, db=CURRENT_DB)
            elif CWL_RUNNER_LINC_CALIBRATOR == "toil":
                run_linc_calibrator_toil(field, calibrator_field=1, db=CURRENT_DB)
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def run_linc_calibrator2(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if not field["sas_id_calibrator2"]:
            raise AirflowSkipException("Calibrator 2 does not exist, skipping.")
        if field["status_calibrator2"] == PIPELINE_STATUS.finished:
            print(
                f"Flux density calibrator {field['sas_id_calibrator2']} for observation {field['target_name']} {field['sas_id_target']} already processed."
            )
            return field
        else:
            if CWL_RUNNER_LINC_CALIBRATOR == "cwltool":
                run_linc_calibrator_cwltool(field, calibrator_field=2, db=CURRENT_DB)
            elif CWL_RUNNER_LINC_CALIBRATOR == "toil":
                run_linc_calibrator_toil(field, calibrator_field=2, db=CURRENT_DB)
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def select_best_calibrator(result1, result2):
        if result1["sas_id_calibrator_final"]:
            return result1
        elif result2["sas_id_calibrator_final"]:
            return result2
        elif result1 and result2:
            cal_template = pathlib.Path(FLUX_CALIBRATOR_TEMPLATE)
            if not cal_template.is_file():
                cal = random.randint(1, 2)
                print(
                    f"No flux density calibrator template found. Randomly selected calibrator{cal}"
                )
                if cal == 1:
                    CURRENT_DB.set_final_calibrator(
                        result1["target_name"],
                        result1["sas_id_target"],
                        result1["sas_id_calibrator1"],
                    )
                    return result1
                elif cal == 2:
                    CURRENT_DB.set_final_calibrator(
                        result2["target_name"],
                        result2["sas_id_target"],
                        result2["sas_id_calibrator2"],
                    )
                    return result2
            else:
                outdir = os.path.join(OUTPUT_DIR, result1["target_name"])
                calibrator1_path = get_most_recent_run(
                    outdir, result1["sas_id_calibrator1"], "LINC_calibrator"
                )
                calibrator1_solutions = (
                    calibrator1_path / "results_LINC_calibrator" / "cal_solutions.h5"
                )

                calibrator2_path = get_most_recent_run(
                    outdir, result2["sas_id_calibrator2"], "LINC_calibrator"
                )
                calibrator2_solutions = (
                    calibrator2_path / "results_LINC_calibrator" / "cal_solutions.h5"
                )
                assess_cal1 = assess_and_compare(
                    FLUX_CALIBRATOR_TEMPLATE,
                    [calibrator1_solutions],
                )
                assess_cal2 = assess_and_compare(
                    FLUX_CALIBRATOR_TEMPLATE,
                    [calibrator2_solutions],
                )
                score1 = assess_cal1[0]["score"]
                score2 = assess_cal2[0]["score"]
                print(f"Calibrator 1 score: {score1}")
                print(f"Calibrator 2 score: {score2}")
                match score1 <= score2:
                    case True:
                        print("Best score for calibrator1")
                        CURRENT_DB.set_final_calibrator(
                            result1["target_name"],
                            result1["sas_id_target"],
                            result1["sas_id_calibrator1"],
                        )
                        return result1
                    case False:
                        print("Best score for calibrator2")
                        CURRENT_DB.set_final_calibrator(
                            result2["target_name"],
                            result2["sas_id_target"],
                            result2["sas_id_calibrator2"],
                        )
                        return result2
        elif result1 and (not result2):
            print("Only cal 1 succeeded, continuing with that")
            CURRENT_DB.set_final_calibrator(
                result1["target_name"],
                result1["sas_id_target"],
                result1["sas_id_calibrator1"],
            )
            return result1
        elif (not result1) and result2:
            print("Only cal 2 succeeded, continuing with that")
            CURRENT_DB.set_final_calibrator(
                result2["target_name"],
                result2["sas_id_target"],
                result2["sas_id_calibrator2"],
            )
            return result2
        else:
            raise AirflowFailException("No calibrators succeeded; stopping processing.")

    @task
    def run_linc_target(field):
        if (field["status_target"] == PIPELINE_STATUS.finished) or (
            field["status_target"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            if CWL_RUNNER_LINC_TARGET == "cwltool":
                run_linc_target_cwltool(field, CURRENT_DB)
            elif CWL_RUNNER_LINC_TARGET == "toil":
                run_linc_target_toil(field, CURRENT_DB)
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def validate_linc_target(field):
        return field

    @task(retries=0, retry_delay=datetime.timedelta(seconds=5))
    def run_vlbi_delay(field):
        if field["status_vlbi_delay"] == PIPELINE_STATUS.finished:
            return field
        else:
            if CWL_RUNNER_PILOT_DELAY == "cwltool":
                run_pilot_delay_cwltool(field, CURRENT_DB)
            elif CWL_RUNNER_PILOT_DELAY == "toil":
                run_pilot_delay_toil(field, CURRENT_DB)
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def run_ddf_pipeline(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_ddf"] == PIPELINE_STATUS.processing:
            print(
                f"ddf-pipeline for {field['target_name']} {field['sas_id_target']} should be running, attempting to resume polling..."
            )
            with (
                open(
                    f"log_DDF-pipeline_{field['target_name']}_{field['sas_id_target']}.txt",
                    "r",
                ) as f_out,
                open(
                    f"log_DDF-pipeline_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "r",
                ) as f_err,
            ):
                jobid = None
                for line in f_out.readlines():
                    if "Submitted batch job" in line:
                        jobid = line.strip().split()[-1]
                    else:
                        raise AirflowFailException("Failed to recover job id from log.")

                while True:
                    print(f"Polling DDF-pipeine job {jobid}")
                    poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                    status = subprocess.run(
                        poll_cmd, shell=True, text=True, capture_output=True
                    ).stdout.strip()
                    if (status == "RUNNING") or (status == "PENDING"):
                        time.sleep(60)
                    elif status == "COMPLETED":
                        CURRENT_DB.set_status_finished(
                            field["target_name"], "ddf_subtract", field["sas_id_target"]
                        )
                        return field
                    elif (status == "FAILED") or ("TIMEOUT" in status):
                        raise RuntimeError(
                            f"DDF-pipeline for {field['target_name']} {field['sas_id_target']} failed."
                        )
        if field["status_ddf"] == PIPELINE_STATUS.finished:
            return field
        else:
            launch_ddf_pipeline(field, CURRENT_DB)
            return field

    @task
    def run_ddf_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_ddf_subtract"] == PIPELINE_STATUS.finished:
            return field
        else:
            run_pilot_process_ddf_toil(field, CURRENT_DB)
            return field

    @task
    def run_vlbi_ddcal(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_dd"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_dd"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            if CWL_RUNNER_PILOT_DDCAL == "cwltool":
                run_pilot_ddcal_cwltool(field, CURRENT_DB)
            elif CWL_RUNNER_PILOT_DDCAL == "toil":
                run_pilot_ddcal_toil(field, CURRENT_DB)
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def prepare_ddf(field):
        mses_averaged = run_prepare_ddf(field)
        if mses_averaged:
            return field
        else:
            raise RuntimeError("No averaged MSes for ddf-pipeline found.")

    @task
    def prepare_ddf_subtract(field):
        mses_delay_corrected = run_prepare_ddf_subtract(field)
        if mses_delay_corrected:
            return field
        else:
            raise RuntimeError("No delay-corrected MSes for ddf subtract found.")

    @task
    def run_vlbi_image_intermediate(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_intermediate_img"] == PIPELINE_STATUS.finished:
            return field
        else:
            run_pilot_intermediate_image_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_subtract"] == PIPELINE_STATUS.finished:
            return field
        else:
            run_pilot_facet_subtract_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_imaging(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB)
        return field

    proceed = check_fields()
    get_field = get_unprocessed_target()
    field = download_field(get_field)
    result_cal1 = run_linc_calibrator1(field)
    result_cal2 = run_linc_calibrator2(field)
    best_cal = select_best_calibrator(result_cal1, result_cal2)
    result_targ = run_linc_target(best_cal)
    linc_is_valid = validate_linc_target(result_targ)
    result_vlbi_delay = run_vlbi_delay(linc_is_valid)

    proceed >> get_field
    await_approval_delay = PythonSensor(
        task_id="approve_delay",
        python_callable=get_approval,
        poke_interval=60,
        timeout=86400 * 7,
        mode="poke",
        op_args=[result_vlbi_delay, "vlbi_delay", NEEDS_MANUAL_APPROVAL_DELAY],
    )
    result_prepare_ddf = prepare_ddf(result_vlbi_delay)
    result_ddf = run_ddf_pipeline(result_prepare_ddf)
    result_prepare_ddf_subtract = prepare_ddf_subtract(result_ddf)
    result_ddf_subtract = run_ddf_subtract(result_prepare_ddf_subtract)
    result_vlbi_dd = run_vlbi_ddcal(result_ddf_subtract)
    result_vlbi_interm_img = run_vlbi_image_intermediate(result_vlbi_dd)
    result_vlbi_facet_subtract = run_vlbi_facet_subtract(result_vlbi_interm_img)
    _result_vlbi_facet_img = run_vlbi_facet_imaging(result_vlbi_facet_subtract)

    (
        await_approval_delay
        >> result_prepare_ddf
        >> result_ddf
        >> result_prepare_ddf_subtract
        >> result_ddf_subtract
        >> result_vlbi_dd
    )


pilot_widefield()
