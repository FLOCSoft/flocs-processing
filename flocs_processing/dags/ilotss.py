from enum import Enum
from flocs_processing.db_utils import PIPELINE_STATUS, FlocsDB
from flocs_processing.pipeline_runners import (
    check_target_subbands,
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
    launch_ddf_pipeline,
)

import configparser
import datetime
import getpass
import socket
import oracledb
import os
import pathlib
import random
import subprocess
import time

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk.exceptions import AirflowSkipException
from airflow.task.trigger_rule import TriggerRule

try:
    from flocs_lta.lta_search import ObservationStager
    from stager_access import (
        get_surls_requested,
        get_surls_online,
        reschedule,
        get_status,
    )
except oracledb.OperationalError as e:
    (error_obj,) = e.args
    if error_obj.full_code == "DPY-6005":
        print(
            "Cannot connect to the LTA. Staging related functionality is not available."
        )
from autoPILOT.ilotss.assess_calibrators import assess_and_compare

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
NEEDS_MANUAL_APPROVAL_DELAY = parser.getboolean(
    "DEFAULT", "NEEDS_MANUAL_APPROVAL_DELAY"
)

CWL_RUNNER_LINC_CALIBRATOR = "cwltool"
CWL_RUNNER_LINC_TARGET = "toil"
CWL_RUNNER_PILOT_DELAY = "toil"
CWL_RUNNER_PILOT_DDCAL = "toil"

CURRENT_DB = FlocsDB(DATABASE, TABLE_NAME)


class STAGING_PROGRESS(Enum):
    success = "C"
    partial_success = "I"
    failed = "E"
    aborted = "A"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        elif other.__class__ is self.__class__:
            return self.value == other.value
        else:
            raise NotImplementedError


class STAGING_STATUS(Enum):
    success = "success"
    partial_success = "partial success"
    failed = "failed"
    aborted = "aborted"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        elif other.__class__ is self.__class__:
            return self.value == other.value
        else:
            raise NotImplementedError


def get_approval(field, identifier, needs_approval):
    if not needs_approval or CURRENT_DB.get_step_status(
        field["field_name"], identifier, field["target_sas_id"]
    ) == PIPELINE_STATUS.finished.value:
        return dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])


def field_failed(context):
    run_id = f"{context['dag'].dag_id}:{context['dag_run'].run_id}"
    field = CURRENT_DB.get_run_field(run_id)
    if field:
        CURRENT_DB.set_field_failed(
            field["field_name"], field["target_sas_id"],
            str(context.get("exception") or "DAG run failed."), run_id=run_id,
        )


def step_finished(field, identifier):
    if CURRENT_DB.is_step_finished(field["field_name"], identifier, field["target_sas_id"]):
        return True
    if CURRENT_DB.is_step_processing(field["field_name"], identifier, field["target_sas_id"]):
        raise AirflowFailException(
            f"{identifier} may still be running; check the existing job before retrying."
        )
    return False


@dag(max_active_runs=4, on_failure_callback=field_failed)
def ilotss():
    @task
    def get_unprocessed_target():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        obsid = str(conf.get("target_sas_id") or "")
        run_id = f"{context['dag'].dag_id}:{context['dag_run'].run_id}"
        owned = CURRENT_DB.get_run_field(run_id)
        rows = [owned] if owned else CURRENT_DB.get_db_columns(obsid, intent="vlbi-widefield")
        for dbrow in rows:
            row = dict(dbrow)
            if conf.get("field_name") and row["field_name"] != conf["field_name"]:
                raise AirflowFailException("Requested field name and SAS ID do not match.")
            field = CURRENT_DB.claim_field(
                row["field_name"], row["target_sas_id"], run_id,
                pipeline="vlbi-widefield", cluster=parser["DEFAULT"].get("PROCESSING_CLUSTER"),
                user=getpass.getuser(), host=socket.gethostname(),
                input_location=row["input_location"] or os.path.join(DATA_DIR, row["field_name"]),
                work_directory=row["work_directory"] or PROCESSING_DIR,
                output_location=row["output_location"] or os.path.join(OUTPUT_DIR, row["field_name"]),
            )
            if field:
                return field
        if obsid:
            raise AirflowFailException(f"Requested target {obsid} is unavailable for this workflow.")
        raise AirflowSkipException("No unprocessed fields found.")

    @task.short_circuit
    def check_fields():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        obsid = str(conf.get("target_sas_id") or "")
        run_id = f"{context['dag'].dag_id}:{context['dag_run'].run_id}"
        fields = CURRENT_DB.get_run_field(run_id) or CURRENT_DB.get_db_columns(obsid, intent="vlbi-widefield")
        if obsid and not fields:
            raise AirflowFailException(f"Requested target {obsid} is not in the processing database.")
        return bool(fields)

    @task
    def download_field(field):
        ACCEPTED_ONLINE_FRACTION = 0.95
        MAX_RESCHEDULES_CALIBRATOR = 3
        MAX_RESCHEDULES_TARGET = 3
        minfreq = 120 if field["freq_min_mhz"] is None else field["freq_min_mhz"]
        maxfreq = 168 if field["freq_max_mhz"] is None else field["freq_max_mhz"]
        if minfreq >= maxfreq:
            raise AirflowFailException("freq_min_mhz must be less than freq_max_mhz.")
        if CURRENT_DB.is_step_finished(field["field_name"], "download", field["target_sas_id"]):
            check_target_subbands(field)
            return field
        else:
            CURRENT_DB.set_field_downloading(
                field["field_name"], field["target_sas_id"]
            )
            stage_calibrators = False
            num_downloaded_calib1 = 0
            num_downloaded_calib2 = 0
            num_staged_calib = 0
            num_staged_targ = 0
            if os.path.exists(f"srms_{field['target_sas_id']}_calibrators.txt"):
                print("Found srm file; counting calibrator SRMs.")
                out = subprocess.check_output(
                    f"wc -l srms_{field['target_sas_id']}_calibrators.txt | cut -f 1 -d ' '",
                    text=True,
                    shell=True,
                )
                num_staged_calib = int(out.strip())

            if os.path.exists(f"srms_{field['target_sas_id']}.txt"):
                print("Found srm file; counting target SRMs.")
                out = subprocess.check_output(
                    f"wc -l srms_{field['target_sas_id']}.txt | cut -f 1 -d ' '",
                    text=True,
                    shell=True,
                )
                num_staged_targ = int(out.strip())

            if ("target_sas_id" not in field) or (not field["target_sas_id"]):
                raise AirflowFailException(
                    f"No target SAS ID in database for field {field['field_name']}"
                )

            ms_folder = f"L{field['target_sas_id']}"
            calibrator_full_path = os.path.join(
                field["input_location"], "target", ms_folder
            )
            if os.path.exists(calibrator_full_path):
                if field["calibrator1_sas_id"]:
                    ms_folder = f"L{field['calibrator1_sas_id']}"
                    cal1_full_path = os.path.join(
                        field["input_location"], "calibrator", ms_folder
                    )
                    if os.path.exists(cal1_full_path):
                        num_downloaded_calib1 = len(
                            list(pathlib.Path(cal1_full_path).glob("*.MS"))
                        )
                    else:
                        stage_calibrators = True

                if field["calibrator2_sas_id"]:
                    ms_folder = f"L{field['calibrator2_sas_id']}"
                    cal2_full_path = os.path.join(
                        field["input_location"], "calibrator", ms_folder
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
                    calibrator_downloaded = True
                else:
                    print(
                        f"Number of staged calibrator MSes ({num_staged_calib}) does NOT equal number of downloaded MSes ({num_downloaded_calib}); restaging calibrators and resuming download."
                    )
                    stage_calibrators = True
                    calibrator_downloaded = False
            else:
                stage_calibrators = True
                calibrator_downloaded = False

            stage_target = False
            ms_folder = f"L{field['target_sas_id']}"
            target_full_path = os.path.join(
                field["input_location"], "target", ms_folder
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
                    target_downloaded = True
                else:
                    print(
                        f"Number of staged target MSes ({num_staged_targ}) does NOT equal number of downloaded MSes ({num_downloaded_targ}); staging target again and resuming download."
                    )
                    stage_target = True
                    target_downloaded = False
            else:
                stage_target = True
                target_downloaded = False

            if stage_calibrators or stage_target:
                print(f"Field {field['target_sas_id']} is not downloaded.")
                stager = ObservationStager(get_surls=True)
                stager.find_observation_by_sasid(
                    "ALL",
                    field["target_sas_id"],
                    None,
                    minfreq,
                    maxfreq,
                )
                expected = field["number_subbands"]
                if expected is not None and len(stager.target_uris) != expected:
                    raise AirflowFailException(
                        f"Expected {expected} target subbands, found {len(stager.target_uris)} in the selected frequency range."
                    )
                if stage_calibrators:
                    stager.find_nearest_calibrators(2, 120, 168)
                    stage_id_calibrators = stager.stage_calibrators()
                if stage_target:
                    stage_id_target = stager.stage_target()
            else:
                check_target_subbands(field)
                CURRENT_DB.set_field_downloaded(field["field_name"], field["target_sas_id"])
                return field

            calibrator_staged = False
            target_staged = False
            schedule_tries_cal = 0
            schedule_tries_tar = 0
            while True:
                if not calibrator_downloaded:
                    if (
                        get_status(stage_id_calibrators)
                        == STAGING_STATUS.partial_success
                    ):
                        if schedule_tries_cal < MAX_RESCHEDULES_CALIBRATOR:
                            print(
                                f"Partial success for calibrator; rescheduling {MAX_RESCHEDULES_CALIBRATOR - schedule_tries_cal} more times."
                            )
                            reschedule(stage_id_calibrators)
                            schedule_tries_cal += 1
                            continue
                    if len(get_surls_online(stage_id_calibrators)) >= int(
                        ACCEPTED_ONLINE_FRACTION
                        * len(get_surls_requested(stage_id_calibrators))
                    ):
                        calibrator_staged = True
                    if calibrator_staged and not calibrator_downloaded:
                        dl_path = os.path.join(
                            field["input_location"], "calibrator"
                        )
                        cmd = f"flocs-lta download --outdir {dl_path} {stage_id_calibrators}"
                        with (
                            open(
                                f"log_download_calibrators_{field['field_name']}.txt",
                                "w+",
                            ) as f_out,
                            open(
                                f"log_download_calibrators_{field['field_name']}_err.txt",
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
                    if get_status(stage_id_target) == STAGING_STATUS.partial_success:
                        if schedule_tries_tar < MAX_RESCHEDULES_TARGET:
                            print(
                                f"Partial success for target; rescheduling {MAX_RESCHEDULES_TARGET - schedule_tries_tar} more times."
                            )
                            reschedule(stage_id_target)
                            schedule_tries_tar += 1
                            continue
                    if len(get_surls_online(stage_id_target)) >= int(
                        ACCEPTED_ONLINE_FRACTION
                        * len(get_surls_requested(stage_id_target))
                    ):
                        target_staged = True
                    if target_staged and not target_downloaded:
                        dl_path = os.path.join(field["input_location"], "target")
                        cmd = f"flocs-lta download --outdir {dl_path} {stage_id_target}"
                        with (
                            open(
                                f"log_download_target_{field['field_name']}.txt",
                                "w+",
                            ) as f_out,
                            open(
                                f"log_download_target_{field['field_name']}_err.txt",
                                "w+",
                            ) as f_err,
                        ):
                            proc = subprocess.run(
                                cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                            )
                            if not proc.returncode:
                                target_downloaded = True
                            else:
                                raise RuntimeError
                if calibrator_downloaded and target_downloaded:
                    check_target_subbands(field)
                    CURRENT_DB.set_field_downloaded(field["field_name"], field["target_sas_id"])
                    CURRENT_DB.set_field_processing(
                        field["field_name"], field["target_sas_id"]
                    )
                    break
                time.sleep(60)
            return field

    @task
    def run_linc_calibrator1(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if not field["calibrator1_sas_id"]:
            raise AirflowSkipException("Calibrator 1 does not exist, skipping.")
        if step_finished(field, "calibrator1"):
            print(
                f"Flux density calibrator {field['calibrator1_sas_id']} for observation {field['field_name']} {field['target_sas_id']} already processed."
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
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if not field["calibrator2_sas_id"]:
            raise AirflowSkipException("Calibrator 2 does not exist, skipping.")
        if step_finished(field, "calibrator2"):
            print(
                f"Flux density calibrator {field['calibrator2_sas_id']} for observation {field['field_name']} {field['target_sas_id']} already processed."
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
    def select_best_calibrator(field, result1, result2):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        result1 = field if field["calibrator1_sas_id"] and CURRENT_DB.is_step_finished(
            field["field_name"], "calibrator1", field["target_sas_id"]
        ) else None
        result2 = field if field["calibrator2_sas_id"] and CURRENT_DB.is_step_finished(
            field["field_name"], "calibrator2", field["target_sas_id"]
        ) else None
        selected = field["selected_calibrator_sas_id"]
        if (result1 and selected == field["calibrator1_sas_id"]) or (
            result2 and selected == field["calibrator2_sas_id"]
        ):
            return field
        if result1 and (not result2):
            print("Only cal 1 succeeded, continuing with that")
            CURRENT_DB.set_final_calibrator(
                result1["field_name"],
                result1["target_sas_id"],
                result1["calibrator1_sas_id"],
            )
            return result1
        elif (not result1) and result2:
            print("Only cal 2 succeeded, continuing with that")
            CURRENT_DB.set_final_calibrator(
                result2["field_name"],
                result2["target_sas_id"],
                result2["calibrator2_sas_id"],
            )
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
                        result1["field_name"],
                        result1["target_sas_id"],
                        result1["calibrator1_sas_id"],
                    )
                    return result1
                elif cal == 2:
                    CURRENT_DB.set_final_calibrator(
                        result2["field_name"],
                        result2["target_sas_id"],
                        result2["calibrator2_sas_id"],
                    )
                    return result2
            else:
                outdir = result1["output_location"]
                calibrator1_path = get_most_recent_run(
                    outdir, result1["calibrator1_sas_id"], "LINC_calibrator"
                )
                calibrator1_solutions = (
                    calibrator1_path / "results_LINC_calibrator" / "cal_solutions.h5"
                )

                calibrator2_path = get_most_recent_run(
                    outdir, result2["calibrator2_sas_id"], "LINC_calibrator"
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
                            result1["field_name"],
                            result1["target_sas_id"],
                            result1["calibrator1_sas_id"],
                        )
                        return result1
                    case False:
                        print("Best score for calibrator2")
                        CURRENT_DB.set_final_calibrator(
                            result2["field_name"],
                            result2["target_sas_id"],
                            result2["calibrator2_sas_id"],
                        )
                        return result2
        else:
            raise AirflowFailException("No calibrators succeeded; stopping processing.")

    @task
    def run_linc_target(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "target"):
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
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_delay"):
            return field
        else:
            if CWL_RUNNER_PILOT_DELAY == "cwltool":
                run_pilot_delay_cwltool(field, CURRENT_DB, mode="widefield")
            elif CWL_RUNNER_PILOT_DELAY == "toil":
                run_pilot_delay_toil(field, CURRENT_DB, mode="widefield")
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def run_ddf_pipeline(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        logsdir = os.path.join(field["output_location"], "logs")
        if CURRENT_DB.is_step_processing(field["field_name"], "ddf", field["target_sas_id"]):
            print(
                f"ddf-pipeline for {field['field_name']} {field['target_sas_id']} should be running, attempting to resume polling..."
            )
            with open(
                os.path.join(
                    logsdir,
                    f"log_DDF-pipeline_{field['field_name']}_{field['target_sas_id']}.txt",
                ),
                "r",
            ) as f_out:
                jobid = None
                for line in f_out.readlines():
                    if "Submitted batch job" in line:
                        jobid = line.strip().split()[-1]
                        break
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
                            field["field_name"],
                            "ddf",
                            field["target_sas_id"],
                        )
                        return field
                    elif (
                        (status == "FAILED")
                        or ("TIMEOUT" in status)
                        or ("CANCELLED" in status)
                        or ("OUT_OF_MEM" in status)
                    ):
                        CURRENT_DB.set_status_failed(field["field_name"], "ddf", field["target_sas_id"])
                        raise RuntimeError(
                            f"DDF-pipeline for {field['field_name']} {field['target_sas_id']} failed."
                        )
        if step_finished(field, "ddf"):
            return field
        else:
            launch_ddf_pipeline(field, CURRENT_DB)
            return field

    @task
    def run_ddf_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_ddf_subtract"):
            return field
        else:
            run_pilot_process_ddf_toil(field, CURRENT_DB)
            return field

    @task
    def run_vlbi_ddcal(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_dd"):
            return field
        else:
            if CWL_RUNNER_PILOT_DDCAL == "cwltool":
                run_pilot_ddcal_cwltool(field, CURRENT_DB, mode="widefield")
            elif CWL_RUNNER_PILOT_DDCAL == "toil":
                run_pilot_ddcal_toil(field, CURRENT_DB, mode="widefield")
            else:
                raise RuntimeError("Invalid CWL runner specified.")
        return field

    @task
    def prepare_ddf(field):
        if CURRENT_DB.is_step_processing(field["field_name"], "prepare_ddf", field["target_sas_id"]):
            raise AirflowFailException("DDF preparation may still be running; check the existing jobs before retrying.")
        mses_averaged = run_prepare_ddf(field, CURRENT_DB)
        if mses_averaged:
            return field
        else:
            raise RuntimeError("No averaged MSes for ddf-pipeline found.")

    @task
    def prepare_ddf_subtract(field):
        if CURRENT_DB.is_step_processing(field["field_name"], "prepare_ddf_subtract", field["target_sas_id"]):
            raise AirflowFailException("DDF subtraction preparation may still be running; check the existing jobs before retrying.")
        mses_delay_corrected = run_prepare_ddf_subtract(field, CURRENT_DB)
        if mses_delay_corrected:
            return field
        else:
            raise RuntimeError("No delay-corrected MSes for ddf subtract found.")

    @task
    def run_vlbi_image_intermediate(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_intermediate_img"):
            return field
        else:
            run_pilot_intermediate_image_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_subtract"):
            return field
        else:
            run_pilot_facet_subtract_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_imaging_6p0(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_imaging_6.0"):
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=6.0)
        return field

    @task
    def run_vlbi_facet_imaging_3p0(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_imaging_3.0"):
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=3.0)
        return field

    @task
    def run_vlbi_facet_imaging_1p5(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_imaging_1.5"):
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=1.5)
        return field

    @task
    def run_vlbi_facet_imaging_0p6(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_imaging_0.6"):
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=0.6)
        return field

    @task
    def run_vlbi_facet_imaging_0p3(field):
        field = dict(CURRENT_DB.get_db_columns(field["target_sas_id"])[0])
        if step_finished(field, "vlbi_facet_imaging_0.3"):
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=0.3)
        return field

    @task
    def finish_field(field):
        for resolution in (6.0, 3.0, 1.5, 0.6, 0.3):
            if not CURRENT_DB.is_step_finished(
                field["field_name"], f"vlbi_facet_imaging_{resolution}", field["target_sas_id"]
            ):
                raise AirflowFailException(f"Imaging at {resolution} arcsec has not finished.")
        context = get_current_context()
        CURRENT_DB.set_field_finished(
            field["field_name"], field["target_sas_id"],
            run_id=f"{context['dag'].dag_id}:{context['dag_run'].run_id}",
        )
        return field

    proceed = check_fields()
    get_field = get_unprocessed_target()
    field = download_field(get_field)
    result_cal1 = run_linc_calibrator1(field)
    result_cal2 = run_linc_calibrator2(field)
    best_cal = select_best_calibrator(field, result_cal1, result_cal2)
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
    _result_vlbi_facet_img_6p0 = run_vlbi_facet_imaging_6p0(result_vlbi_facet_subtract)
    _result_vlbi_facet_img_3p0 = run_vlbi_facet_imaging_3p0(result_vlbi_facet_subtract)
    _result_vlbi_facet_img_1p5 = run_vlbi_facet_imaging_1p5(result_vlbi_facet_subtract)
    _result_vlbi_facet_img_0p6 = run_vlbi_facet_imaging_0p6(result_vlbi_facet_subtract)
    _result_vlbi_facet_img_0p3 = run_vlbi_facet_imaging_0p3(result_vlbi_facet_subtract)

    finished = finish_field(result_vlbi_facet_subtract)
    [
        _result_vlbi_facet_img_6p0, _result_vlbi_facet_img_3p0,
        _result_vlbi_facet_img_1p5, _result_vlbi_facet_img_0p6,
        _result_vlbi_facet_img_0p3,
    ] >> finished

    (
        await_approval_delay
        >> result_prepare_ddf
        >> result_ddf
        >> result_prepare_ddf_subtract
        >> result_ddf_subtract
        >> result_vlbi_dd
    )


ilotss()
