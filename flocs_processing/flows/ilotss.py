from enum import Enum
from flocs_processing.db_utils import PIPELINE_STATUS, FlocsDB
from flocs_processing.db_utils import FIELD_STATUS
from flocs_processing.config import load_config
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
    launch_ddf_pipeline,
)

import datetime
import os
import pathlib
import random
import subprocess
import time

from prefect import flow, task
from flocs_lta.lta_search import ObservationStager
from autoPILOT.ilotss.assess_calibrators import assess_and_compare
from stager_access import get_surls_requested, get_surls_online, reschedule, get_status

parser = load_config()

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


def wait_for_approval(field, identifier, needs_approval, timeout=86400 * 7):
    if not needs_approval:
        return field

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = CURRENT_DB.get_db_columns(field["sas_id_target"], intent="widefield")
        if rows and rows[0][f"status_{identifier}"] == PIPELINE_STATUS.finished.value:
            return dict(rows[0])
        time.sleep(60)
    raise TimeoutError(f"Approval for {identifier} was not received within {timeout} seconds.")


@flow
def ilotss(target_name: str | None = None, sas_id_target: str | None = None):
    @task
    def get_unprocessed_target(target_name=None, sas_id_target=None):
        field = None
        for dbrow in CURRENT_DB.get_db_columns(
            sas_id_target or "", intent="widefield"
        ):
            is_processing = False
            row = dict(dbrow)
            if target_name and row["target_name"] != target_name:
                continue
            if row["status"] == FIELD_STATUS.downloading.value:
                is_processing = True
                break
            status_keys = filter(lambda x: x.startswith("status_"), row.keys())
            for key in status_keys:
                if row[key] == PIPELINE_STATUS.processing.value:
                    is_processing = True
                    break
            if not is_processing:
                # Only select a field if nothing is processing it.
                field = row
                break
        if not field:
            return None
        print(field["target_name"])
        return field

    @task
    def check_fields():
        fields = CURRENT_DB.get_db_columns(intent="widefield")
        return bool(fields)

    @task
    def download_field(field):
        ACCEPTED_ONLINE_FRACTION = 0.95
        MAX_RESCHEDULES_CALIBRATOR = 3
        MAX_RESCHEDULES_TARGET = 3
        if field["downloaded"]:
            return field
        else:
            CURRENT_DB.set_field_downloading(
                field["target_name"], field["sas_id_target"]
            )
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

            if ("sas_id_target" not in field) or (not field["sas_id_target"]):
                raise RuntimeError(
                    f"No target SAS ID in database for field {field['target_name']}"
                )

            ms_folder = f"L{field['sas_id_target']}"
            calibrator_full_path = os.path.join(
                DATA_DIR, field["target_name"], "target", ms_folder
            )
            if os.path.exists(calibrator_full_path):
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
            calibrator_downloaded = False
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
                    CURRENT_DB.set_field_processing(
                        field["target_name"], field["sas_id_target"]
                    )
                    break
                time.sleep(60)
            return field

    @task
    def run_linc_calibrator1(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if not field["sas_id_calibrator1"]:
            print("Calibrator 1 does not exist, skipping.")
            return None
        if field["status_calibrator1"] == PIPELINE_STATUS.finished.value:
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
            print("Calibrator 2 does not exist, skipping.")
            return None
        if field["status_calibrator2"] == PIPELINE_STATUS.finished.value:
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

    @task
    def select_best_calibrator(result1, result2):
        if result1 and (not result2):
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
        else:
            raise RuntimeError("No calibrators succeeded; stopping processing.")

    @task
    def run_linc_target(field):
        if (field["status_target"] == PIPELINE_STATUS.finished.value) or (
            field["status_target"] == PIPELINE_STATUS.processing.value
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
        if field["status_vlbi_delay"] == PIPELINE_STATUS.finished.value:
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
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        logsdir = os.path.join(OUTPUT_DIR, field["target_name"], "logs")
        if field["status_ddf"] == PIPELINE_STATUS.processing.value:
            print(
                f"ddf-pipeline for {field['target_name']} {field['sas_id_target']} should be running, attempting to resume polling..."
            )
            with open(
                os.path.join(
                    logsdir,
                    f"log_DDF-pipeline_{field['target_name']}_{field['sas_id_target']}.txt",
                ),
                "r",
            ) as f_out:
                jobid = None
                for line in f_out.readlines():
                    if "Submitted batch job" in line:
                        jobid = line.strip().split()[-1]
                        break
                else:
                    raise RuntimeError("Failed to recover job id from log.")

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
                            field["target_name"],
                            "ddf",
                            field["sas_id_target"],
                        )
                        return field
                    elif (
                        (status == "FAILED")
                        or ("TIMEOUT" in status)
                        or ("CANCELELD" in status)
                        or ("OUT_OF_MEM" in status)
                    ):
                        raise RuntimeError(
                            f"DDF-pipeline for {field['target_name']} {field['sas_id_target']} failed."
                        )
        if field["status_ddf"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            launch_ddf_pipeline(field, CURRENT_DB)
            return field

    @task
    def run_ddf_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_ddf_subtract"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_process_ddf_toil(field, CURRENT_DB)
            return field

    @task
    def run_vlbi_ddcal(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_dd"] == PIPELINE_STATUS.finished.value) or (
            field["status_vlbi_dd"] == PIPELINE_STATUS.processing.value
        ):
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
        mses_averaged = run_prepare_ddf(field, CURRENT_DB)
        if mses_averaged:
            return field
        else:
            raise RuntimeError("No averaged MSes for ddf-pipeline found.")

    @task
    def prepare_ddf_subtract(field):
        mses_delay_corrected = run_prepare_ddf_subtract(field, CURRENT_DB)
        if mses_delay_corrected:
            return field
        else:
            raise RuntimeError("No delay-corrected MSes for ddf subtract found.")

    @task
    def run_vlbi_image_intermediate(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_intermediate_img"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_intermediate_image_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_subtract(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_subtract"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_subtract_toil(field, CURRENT_DB)
        return field

    @task
    def run_vlbi_facet_imaging_6p0(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=6.0)
        return field

    @task
    def run_vlbi_facet_imaging_3p0(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=3.0)
        return field

    @task
    def run_vlbi_facet_imaging_1p5(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=1.5)
        return field

    @task
    def run_vlbi_facet_imaging_0p6(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=0.6)
        return field

    @task
    def run_vlbi_facet_imaging_0p3(field):
        field = dict(CURRENT_DB.get_db_columns(field["sas_id_target"])[0])
        if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished.value:
            return field
        else:
            run_pilot_facet_imaging_toil(field, CURRENT_DB, resolution=0.3)
        return field

    if not check_fields():
        return None

    get_field = get_unprocessed_target(target_name, sas_id_target)
    if get_field is None:
        print("No unprocessed widefield observation found.")
        return None

    field = download_field(get_field)
    calibrator1_future = run_linc_calibrator1.submit(field)
    calibrator2_future = run_linc_calibrator2.submit(field)
    result_cal1 = None
    result_cal2 = None
    try:
        result_cal1 = calibrator1_future.result()
    except Exception as exc:
        print(f"Calibrator 1 processing failed: {exc}")
    try:
        result_cal2 = calibrator2_future.result()
    except Exception as exc:
        print(f"Calibrator 2 processing failed: {exc}")
    best_cal = select_best_calibrator(result_cal1, result_cal2)
    result_targ = run_linc_target(best_cal)
    linc_is_valid = validate_linc_target(result_targ)
    result_vlbi_delay = run_vlbi_delay(linc_is_valid)

    @task(name="wait-for-delay-approval")
    def approve_delay(field):
        return wait_for_approval(
            field, "vlbi_delay", NEEDS_MANUAL_APPROVAL_DELAY
        )

    approved_field = approve_delay(result_vlbi_delay)
    result_prepare_ddf = prepare_ddf(approved_field)
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

    return {
        "field": field["target_name"],
        "sas_id_target": field["sas_id_target"],
        "status": "completed",
    }


if __name__ == "__main__":
    ilotss()
