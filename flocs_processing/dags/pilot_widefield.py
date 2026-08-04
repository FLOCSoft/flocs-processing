from enum import Enum
import datetime
import functools
import os
import pathlib
import re
import sqlite3
import subprocess
import time

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.task.trigger_rule import TriggerRule
from flocs_lta.lta_search import ObservationStager
from losoto.h5parm import h5parm
from stager_access import get_surls_requested, get_surls_online

# Need to replace this with a config file
TABLE_NAME = ""
DATABASE = ""
SLURM_ACCOUNT = ""
SLURM_QUEUE = ""
DATA_DIR = ""
OUTPUT_DIR = ""
PROCESSING_DIR = ""
NN_MODEL_CACHE = ""
DDF_CONFIG = ""

NEEDS_MANUAL_APPROVAL_DELAY = True


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


def get_db_columns(obsid: str = None):
    with sqlite3.connect(DATABASE) as db:
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        columns = "target_name,priority,finished,downloaded,sas_id_calibrator1,sas_id_calibrator2,sas_id_calibrator_final,sas_id_target,status_calibrator1,status_calibrator2,status_target,status_vlbi_delay,status_vlbi_dd,status_ddf,status_vlbi_ddf_subtract"
        if obsid:
            field = cursor.execute(
                f"select {columns} from {TABLE_NAME} where sas_id_target=='{obsid}' and finished==0 order by priority desc"
            ).fetchall()
        else:
            field = cursor.execute(
                f"select {columns} from {TABLE_NAME} where finished==0 order by priority desc"
            ).fetchall()
        print(field)
    return field


def set_status_failed(name, identifier, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.error.value} where target_name=='{name}' and sas_id_target=='{target}'"
        )


def set_status_processing(name, identifier, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.processing.value} where target_name=='{name}' and sas_id_target=='{target}'"
        )


def set_status_await_approval(name, identifier, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.await_approval.value} where target_name=='{name}' and sas_id_target=='{target}'"
        )


def set_status_finished(name, identifier, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.finished.value} where target_name=='{name}' and sas_id_target=='{target}'"
        )


def set_status_downloaded(name, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set downloaded=1 where target_name=='{name}' and sas_id_target=='{target}'"
        )


def set_field_finished(name, target):
    query = f"update {TABLE_NAME} set finished=1 where target_name=='{name}' and sas_id_target=='{target}'"
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(query)


def set_final_calibrator(name, target, final_cal):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set sas_id_calibrator_final={final_cal} where target_name=='{name}' and sas_id_target=='{target}'"
        )


def get_most_recent_run(searchpath: str, sas_id: str, pipeline: str) -> pathlib.Path:
    rundirs = pathlib.Path(searchpath)
    rundirs_sorted = sorted(rundirs.iterdir())
    if pipeline:
        rundirs_sorted_filtered = [
            d
            for d in rundirs_sorted
            if ((sas_id in d.parts[-1]) and (pipeline in d.parts[-1])) and d.is_dir()
        ]
    else:
        rundirs_sorted_filtered = [d for d in rundirs_sorted if sas_id in d.parts[-1]]
    rundir_final = rundirs_sorted_filtered[-1].absolute()
    return rundir_final


@dag(max_active_runs=1)
def pilot_widefield():
    @task
    def get_unprocessed_target():
        field = dict(get_db_columns()[0])
        print(field["target_name"])
        return field

    @task.short_circuit
    def check_fields():
        fields = get_db_columns()
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
            if os.path.exists("srms_{field['sas_id_target']}_calibrators.txt"):
                print("Found srm file; counting calibrator SRMs.")
                out = subprocess.check_output(
                    "wc -l srms_{field['sas_id_target']}_calibrators.txt | cut -f 1 -d ' '",
                    text=True,
                )
                num_staged_calib = int(out.strip())

            if os.path.exists("srms_{field['sas_id_target']}.txt"):
                print("Found srm file; counting target SRMs.")
                out = subprocess.check_output(
                    "wc -l srms_{field['sas_id_target']}.txt | cut -f 1 -d ' '",
                    text=True,
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
                                set_status_downloaded(
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
        if (field["status_calibrator1"] == PIPELINE_STATUS.finished) or (
            field["status_calibrator1"] == PIPELINE_STATUS.processing
        ):
            print(
                f"Flux density calibrator {field['sas_id_calibrator1']} for observation {field['target_name']} {field['sas_id_target']} already processed."
            )
            return field
        else:
            print(
                f"Processing flux density calibrator {field['sas_id_calibrator1']} for observation {field['target_name']} {field['sas_id_target']}"
            )
            ms_folder = f"L{field['sas_id_calibrator1']}"
            set_status_processing(
                field["target_name"], "calibrator1", field["sas_id_target"]
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            cmd = f"flocs-run linc calibrator --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} {os.path.join(DATA_DIR, field['target_name'], 'calibrator', ms_folder)}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator1']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator1']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"], "calibrator1", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task
    def run_linc_calibrator2(field):
        if (field["status_calibrator2"] == PIPELINE_STATUS.finished) or (
            field["status_calibrator2"] == PIPELINE_STATUS.processing
        ):
            print(
                f"Flux density calibrator {field['sas_id_calibrator2']} for observation {field['target_name']} {field['sas_id_target']} already processed."
            )
            return field
        else:
            print(
                f"Processing flux density calibrator {field['sas_id_calibrator2']} for observation {field['target_name']} {field['sas_id_target']}"
            )
            ms_folder = f"L{field['sas_id_calibrator2']}"
            set_status_processing(
                field["target_name"], "calibrator2", field["sas_id_target"]
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            cmd = f"flocs-run linc calibrator --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} {os.path.join(DATA_DIR, field['target_name'], 'calibrator', ms_folder)}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator2']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator2']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"], "calibrator2", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task(trigger_rule=TriggerRule.ONE_DONE)
    def select_best_calibrator(result1, result2):
        if result1["sas_id_calibrator_final"]:
            return result1
        elif result2["sas_id_calibrator_final"]:
            return result2
        elif result1 and result2:
            print("Selecting between cal1 and cal2")
            # Need actual selection logic here
            set_final_calibrator(
                result1["target_name"],
                result1["sas_id_target"],
                result1["sas_id_calibrator1"],
            )
            return result1
        elif result1 and (not result2):
            print("Only cal 1 succeeded, continuing with that")
            set_final_calibrator(
                result1["target_name"],
                result1["sas_id_target"],
                result1["sas_id_calibrator1"],
            )
            return result1
        elif (not result1) and result2:
            print("Only cal 2 succeeded, continuing with that")
            set_final_calibrator(
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
            print(
                f"Processing target observation {field['target_name']} {field['sas_id_target']} with calibrator {field['sas_id_calibrator_final']}"
            )
            ms_folder = f"L{field['sas_id_target']}"
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            calibrator_path = get_most_recent_run(
                outdir, field["sas_id_calibrator_final"], "LINC_calibrator"
            )
            calibrator_solutions = (
                calibrator_path / "results_LINC_calibrator" / "cal_solutions.h5"
            )
            set_status_processing(
                field["target_name"], "target", field["sas_id_target"]
            )
            cmd = f"flocs-run linc target --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --cal-solutions {calibrator_solutions} {os.path.join(DATA_DIR, field['target_name'], 'target', ms_folder)}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_LINC_target_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_LINC_target_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"], "target", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task
    def validate_linc_target(field):
        return field

    @task(retries=0, retry_delay=datetime.timedelta(seconds=5))
    def run_vlbi_delay(field):
        if (field["status_vlbi_delay"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_delay"] == PIPELINE_STATUS.await_approval
        ):
            return field
        else:
            print(
                f"Processing delay calibration for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "LINC_target"
            )
            target_ms_path = target_path / "results_LINC_target" / "results"
            set_status_processing(
                field["target_name"], "vlbi_delay", field["sas_id_target"]
            )

            delay_cat = os.path.join(outdir, "delay_calibrators.csv")
            image_cat = os.path.join(outdir, "image_catalogue.csv")

            if not os.path.isfile(delay_cat):
                ms = list(target_ms_path.glob("*.dp3concat"))[0]
                cmd = f"lofar-vlbi-plot --MS {ms}"
                with (
                    open(
                        f"log_plot_field_{field['target_name']}_{field['sas_id_target']}.txt",
                        "w+",
                    ) as f_out,
                    open(
                        f"log_plot_field_{field['target_name']}_{field['sas_id_target']}_err.txt",
                        "w+",
                    ) as f_err,
                ):
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        raise RuntimeError(
                            "Failed to download necessary products for delay calibration."
                        )
            delay_cat = os.path.join(outdir, "delay_calibrators.csv")
            image_cat = os.path.join(outdir, "image_catalogue.csv")

            if not os.path.isfile(delay_cat):
                raise RuntimeError("Delay calibrator catalogue is missing or invalid.")
            if not os.path.isfile(image_cat):
                raise RuntimeError("Image source catalogue is missing or invalid.")

            proc = subprocess.run(
                "detect_bad_slurm_nodes.sh",
                shell=True,
                text=True,
                stdout=subprocess.PIPE,
            )
            bad_nodes = proc.stdout.strip()
            if bad_nodes:
                print(f"Excluding the following bad nodes from scheduling: {bad_nodes}")
                os.environ["TOIL_SLURM_ARGS"] = f"--exclude={bad_nodes}"

            context = get_current_context()
            if context["ti"].try_number == 1:
                cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} --apply-delay-solutions {target_ms_path}"
            else:
                # Extract the previous working directory
                flocs_workdir = ""
                print(
                    f"Scanning log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                )
                with open(
                    f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}.txt"
                ) as f_out:
                    for line in f_out.readlines():
                        print(line)
                        if "Running workflow with" in line:
                            flocs_workdir = line.split(" ")[-1].strip()
                            break
                if not flocs_workdir:
                    raise RuntimeError(
                        "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                    )
                print(f"Resuming failed PILOT run in {flocs_workdir}")
                cmd = f"flocs-run vlbi delay-calibration --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True

                if success:
                    if NEEDS_MANUAL_APPROVAL_DELAY:
                        set_status_await_approval(
                            field["target_name"], "vlbi_delay", field["sas_id_target"]
                        )
                    else:
                        set_status_finished(
                            field["target_name"], "vlbi_delay", field["sas_id_target"]
                        )
                else:
                    raise RuntimeError
        return field

    @task
    def run_ddf_pipeline(field):
        field = dict(get_db_columns(field["sas_id_target"])[0])
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
                        return field
                    elif (status == "FAILED") or ("TIMEOUT" in status):
                        raise RuntimeError(
                            f"DDF-pipeline for {field['target_name']} {field['sas_id_target']} failed."
                        )
        if field["status_ddf"] == PIPELINE_STATUS.finished:
            return field
        else:
            print(
                f"Starting ddf-pipeline for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_delay-calibration"
            )
            target_ms_path = target_path / "results_VLBI_delay-calibration"
            if not list(target_ms_path.glob("*pre-cal.ms")):
                target_path = get_most_recent_run(
                    outdir, field["sas_id_target"], "LINC_target"
                )
                target_ms_path = target_path / "results_LINC_target" / "results"

            cmd = f"flocs-run ddf-pipeline --scheduler slurm --slurm-time 72:00:00 --slurm-cores 32 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {OUTPUT_DIR} --config-file {DDF_CONFIG} {target_ms_path}"
            print(cmd)
            set_status_processing(field["target_name"], "ddf", field["sas_id_target"])
            with (
                open(
                    f"log_DDF-pipeline_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_DDF-pipeline_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                jobid = None
                if not proc.returncode:
                    f_out.seek(0)
                    for line in f_out.readlines():
                        if "Submitted batch job" in line:
                            jobid = line.strip().split()[-1]
                else:
                    raise RuntimeError("Failed to submit job.")

                if not jobid:
                    raise RuntimeError("Failed to retrieve job id")
                else:
                    while True:
                        print(f"Polling DDF-pipeine job {jobid}")
                        poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                        status = subprocess.run(
                            poll_cmd, shell=True, text=True, capture_output=True
                        ).stdout.strip()
                        if (status == "RUNNING") or (status == "PENDING"):
                            time.sleep(60)
                        elif status == "COMPLETED":
                            break
                        elif (
                            (status == "FAILED")
                            or ("TIMEOUT" in status)
                            or ("CANCELLED" in status)
                        ):
                            raise RuntimeError(
                                f"DDF-pipeline for {field['target_name']} {field['sas_id_target']} failed."
                            )
            return field

    @task
    def run_ddf_subtract(field):
        if (field["status_vlbi_ddf_subtract"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_ddf_subtract"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            print(
                f"Running ddf subtract for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_delay-calibration"
            )
            target_ms_path = target_path / "results_VLBI_delay-calibration"
            print(f"Using data at: {target_path}/*.dp3concat")

            ddf_path = get_most_recent_run(
                outdir, field["sas_id_target"], "DDF-pipeline"
            )
            ddf_sols_path = ddf_path / "SOLSDIR"
            print(f"Using DDF run at: {ddf_path}")

            context = get_current_context()
            if context["ti"].try_number == 1:
                set_status_processing(
                    field["target_name"], "ddf_subtract", field["sas_id_target"]
                )
                cmd = f"flocs-run vlbi process-ddf --runner toil --record-toil-stats --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --ms-suffix .dp3concat --ddf-rundir {ddf_path} --solsdir {ddf_sols_path} --do-subtraction {target_ms_path}"
            else:
                # Extract the previous working directory
                flocs_workdir = ""
                print(
                    f"Scanning log_VLBI_process-ddf_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                )
                with open(
                    f"log_VLBI_process-ddf_{field['target_name']}_{field['sas_id_target']}.txt"
                ) as f_out:
                    for line in f_out.readlines():
                        print(line)
                        if "Running workflow with" in line:
                            flocs_workdir = line.split(" ")[-1].strip()
                            break
                if not flocs_workdir:
                    raise RuntimeError(
                        "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                    )
                print(f"Resuming failed PILOT run in {flocs_workdir}")
                cmd = f"flocs-run vlbi process-ddf --runner toil --record-toil-stats --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --ms-suffix .dp3concat --ddf-rundir {ddf_path} --solsdir {ddf_sols_path} --do-subtraction {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_process-ddf_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_process-ddf_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"], "ddf_subtract", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
            return field

    @task
    def run_vlbi_ddcal(field):
        field = dict(get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_dd"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_dd"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            print(
                f"Processing ILT dd calibration for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            if "status_ddf" not in field:
                print("Not a widefield imaging run, checking LINC + delay calibration.")
                target_path = get_most_recent_run(
                    outdir, field["sas_id_target"], "LINC_target"
                )
                target_ms_path = target_path / "results_LINC_target" / "results"
                print(f"Using LINC target run: {target_path}")

                sols_path = get_most_recent_run(
                    outdir, field["sas_id_target"], "VLBI_delay"
                )
                sols_path = sols_path / "results_VLBI_delay-calibration"
                sols = list(
                    sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5")
                )[0]
                print(f"Using PILOT delay calibration solutions: {sols}")

                source_cat = os.path.join(
                    DATA_DIR, field["target_name"], "vlbi_target.csv"
                )
                if not os.path.isfile(source_cat):
                    raise AirflowFailException(f"{source_cat} not found.")

                set_status_processing(
                    field["target_name"], "vlbi_dd", field["sas_id_target"]
                )
                cmd = f"flocs-run vlbi dd-calibration --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --phasediff-score 10.0 --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
            else:
                print("Widefield imaging run, checking subtraction output.")
                target_path = get_most_recent_run(
                    outdir, field["sas_id_target"], "VLBI_process-ddf"
                )
                target_ms_path = target_path / "results_VLBI_process-ddf"
                print(f"Using subtracted data at: {target_path}")

                source_cat = os.path.join(
                    DATA_DIR, field["target_name"], "image_catalogue.csv"
                )
                if not os.path.isfile(source_cat):
                    raise AirflowFailException(f"{source_cat} not found.")

                set_status_processing(
                    field["target_name"], "vlbi_dd", field["sas_id_target"]
                )

                context = get_current_context()
                if context["ti"].try_number == 1:
                    cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
                else:
                    if field["status_vlbi_dd"] == PIPELINE_STATUS.downloaded:
                        # This way we can force a clean restart in the database.
                        cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
                    else:
                        # Extract the previous working directory
                        flocs_workdir = ""
                        print(
                            f"Scanning log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                        )
                        with open(
                            f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}.txt"
                        ) as f_out:
                            for line in f_out.readlines():
                                print(line)
                                if "Running workflow with" in line:
                                    flocs_workdir = line.split(" ")[-1].strip()
                                    break
                        if not flocs_workdir:
                            raise RuntimeError(
                                "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                            )
                        print(f"Resuming failed PILOT run in {flocs_workdir}")
                        cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"], "vlbi_dd", field["sas_id_target"]
                    )
                    set_field_finished(field["target_name"], field["sas_id_target"])
                else:
                    set_status_failed(
                        field["target_name"], "vlbi_dd", field["sas_id_target"]
                    )
                    raise RuntimeError
        return field

    @task
    def prepare_ddf(field):
        print(
            f"Preparing DDF input for {field['target_name']} {field['sas_id_target']}"
        )
        outdir = os.path.join(OUTPUT_DIR, field["target_name"])
        target_path = get_most_recent_run(outdir, field["sas_id_target"], "VLBI_delay")
        target_ms_path = target_path / "results_VLBI_delay-calibration"
        mses_unaveraged = list(target_ms_path.glob("*.dp3concat"))
        delay_sols = ""
        if not mses_unaveraged:
            print(
                "No MSes found in delay-calibration output, will apply delay solutions to LINC."
            )
            sols_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_delay"
            )
            sols_path = sols_path / "results_VLBI_delay-calibration"
            delay_sols = list(
                sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5")
            )[0]
            print(f"Using PILOT delay calibration solutions: {delay_sols}")

            linc_path = get_most_recent_run(
                outdir, field["sas_id_target"], "LINC_target"
            )
            print(f"Using LINC MSes at {linc_path}")
            linc_ms_path = linc_path / "results_LINC_target" / "results"
            mses_unaveraged = list(linc_ms_path.glob("*.dp3concat"))
        else:
            print("Found unaveraged data in delay calibration output.")
        mses_averaged = list(target_ms_path.glob("*_pre-cal.ms"))
        if not mses_unaveraged:
            raise RuntimeError(
                f"No unaveraged input MSes found at {linc_ms_path}/*.dp3concat"
            )
        if mses_unaveraged and (len(mses_averaged) == len(mses_unaveraged)):
            print("Appropriate input exists for ddf-pipeline.")
            return field

        jobids = []
        averaged_mses = []
        for ms in mses_unaveraged:
            out_ms = target_ms_path / f"{ms.stem}_pre-cal.ms"
            if out_ms.exists():
                print(f"Skipping {out_ms.name}, already exists.")
                averaged_mses.append(str(out_ms))
                continue
            if delay_sols:
                h5 = h5parm(str(delay_sols))
                ss = h5.getSolset("sol000")
                # We only expect there to be one direction: the delay calibrator.
                dirname = list(ss.getSou())[0]
                sourcedir = ss.getSou()[dirname]
                delaydir = f"[{sourcedir[0]},{sourcedir[1]}]"
                dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[average,applybeamdelay,applycal,applybeamtarget,filter] average.timeresolution=8 average.freqresolution=97.64kHz applybeamdelay.type=applybeam applybeamdelay.beammode=full applybeamdelay.updateweights=True applybeamdelay.direction={delaydir} applycal.parmdb={delay_sols} applycal.correction=fulljones applycal.soltab=[amplitude000,phase000] applybeamtarget.type=applybeam applybeamtarget.beammode=full applybeamtarget.updateweights=True filter.remove=True filter.baseline='[CR]S*&&'"
                print(dp3_cmd)
            else:
                dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[filter,average] average.timeresolution=8 average.freqresolution=97.64kHz filter.remove=True filter.baseline='[CR]S*&&'"
            submit_cmd = f'sbatch -A {SLURM_ACCOUNT} -p {SLURM_QUEUE} --time=02:00:00 -c 2 --job-name=dp3_avg_{ms.stem} --wrap="{dp3_cmd}"'
            print(f"Submitting: {submit_cmd}")
            proc = subprocess.run(
                submit_cmd, shell=True, text=True, capture_output=True
            )
            if proc.returncode:
                print(proc.stdout)
                print(proc.stderr)
                raise RuntimeError(f"Failed to submit SLURM job for {ms}")
            jobid = proc.stdout.strip().split()[-1]
            print(f"Submitted job {jobid} for {ms.name}")
            jobids.append((jobid, out_ms))
            averaged_mses.append(str(out_ms))

        while jobids:
            print(f"Polling {len(jobids)} SLURM jobs...")
            remaining = []
            for jobid, out_ms in jobids:
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if status == "COMPLETED":
                    print(f"Job {jobid} completed ({out_ms.name})")
                elif status == "FAILED":
                    raise RuntimeError(
                        f"DP3 averaging job {jobid} failed for {out_ms.name}"
                    )
                elif status in ("PENDING", "RUNNING"):
                    remaining.append((jobid, out_ms))
                else:
                    remaining.append((jobid, out_ms))
            jobids = remaining
            time.sleep(30)

        mses_averaged = list(target_ms_path.glob("*_pre-cal.ms"))
        if mses_averaged:
            return field
        else:
            raise RuntimeError("No averaged MSes for ddf-pipeline found.")

    @task
    def prepare_ddf_subtract(field):
        print(
            f"Preparing input for DDF subtract of {field['target_name']} {field['sas_id_target']}"
        )
        outdir = os.path.join(OUTPUT_DIR, field["target_name"])
        target_path = get_most_recent_run(outdir, field["sas_id_target"], "VLBI_delay")
        target_ms_path = target_path / "results_VLBI_delay-calibration"
        mses_unaveraged = list(target_ms_path.glob("*.dp3concat"))
        delay_sols = ""
        if not mses_unaveraged:
            print(
                "No MSes found in delay-calibration output, will apply delay solutions to LINC."
            )
            sols_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_delay"
            )
            sols_path = sols_path / "results_VLBI_delay-calibration"
            delay_sols = list(
                sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5")
            )[0]
            print(f"Using PILOT delay calibration solutions: {delay_sols}")

            linc_path = get_most_recent_run(
                outdir, field["sas_id_target"], "LINC_target"
            )
            print(f"Using LINC MSes at {linc_path}")
            linc_ms_path = linc_path / "results_LINC_target" / "results"
            mses_unaveraged = list(linc_ms_path.glob("*.dp3concat"))
        mses_averaged = list(target_ms_path.glob("*_pre-cal.ms"))
        mses_unaveraged_pilot = list(target_ms_path.glob("*.dp3concat"))
        if not mses_unaveraged:
            raise RuntimeError(
                f"No unaveraged input MSes found at {linc_ms_path}/*.dp3concat"
            )
        if mses_unaveraged_pilot and (
            len(mses_unaveraged_pilot) == len(mses_unaveraged)
        ):
            print("Appropriate input exists for ddf-pipeline.")
            return field

        jobids = []
        averaged_mses = []
        for ms in mses_unaveraged:
            out_ms = target_ms_path / f"{ms.stem}.dp3concat"
            if out_ms.exists():
                print(f"Skipping {out_ms.name}, already exists.")
                averaged_mses.append(str(out_ms))
                continue
            if delay_sols:
                h5 = h5parm(str(delay_sols))
                ss = h5.getSolset("sol000")
                # We only expect there to be one direction: the delay calibrator.
                dirname = list(ss.getSou())[0]
                sourcedir = ss.getSou()[dirname]
                delaydir = f"[{sourcedir[0]},{sourcedir[1]}]"
                dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[applybeamdelay,applycal,applybeamtarget] applybeamdelay.type=applybeam applybeamdelay.beammode=full applybeamdelay.updateweights=True applybeamdelay.direction={delaydir} applycal.parmdb={delay_sols} applycal.correction=fulljones applycal.soltab=[amplitude000,phase000] applybeamtarget.type=applybeam applybeamtarget.beammode=full applybeamtarget.updateweights=True"
                print(dp3_cmd)
            else:
                dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[]"
            submit_cmd = f'sbatch -A {SLURM_ACCOUNT} -p {SLURM_QUEUE} --time=08:00:00 -c 2 --job-name=dp3_avg_{ms.stem} --wrap="{dp3_cmd}"'
            print(f"Submitting: {submit_cmd}")
            proc = subprocess.run(
                submit_cmd, shell=True, text=True, capture_output=True
            )
            if proc.returncode:
                print(proc.stdout)
                print(proc.stderr)
                raise RuntimeError(f"Failed to submit SLURM job for {ms}")
            jobid = proc.stdout.strip().split()[-1]
            print(f"Submitted job {jobid} for {ms.name}")
            jobids.append((jobid, out_ms))
            averaged_mses.append(str(out_ms))

        while jobids:
            print(f"Polling {len(jobids)} SLURM jobs...")
            remaining = []
            for jobid, out_ms in jobids:
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if status == "COMPLETED":
                    print(f"Job {jobid} completed ({out_ms.name})")
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    raise RuntimeError(
                        f"DP3 averaging job {jobid} failed for {out_ms.name}"
                    )
                elif status in ("PENDING", "RUNNING"):
                    remaining.append((jobid, out_ms))
                else:
                    remaining.append((jobid, out_ms))
            jobids = remaining
            time.sleep(30)

        mses_averaged = list(target_ms_path.glob("*_pre-cal.ms"))
        if mses_averaged:
            return field
        else:
            raise RuntimeError("No averaged MSes for ddf-pipeline found.")

    @task
    def run_vlbi_image_intermediate(field):
        field = dict(get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_intermediate_img"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_intermediate_img"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            print(
                f"Processing ILT intermediate resolution imaging for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_process-ddf"
            )
            target_ms_path = target_path / "results_VLBI_process-ddf"
            print(f"Using subtracted data at: {target_path}")

            dd_sols_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_dd-calibration"
            )
            dd_sols = dd_sols_path / "results_VLBI_dd-calibration" / "merged.h5"
            print(f"Using dd solutions at: {target_path}")

            if not os.path.isfile(dd_sols):
                raise AirflowFailException(f"{dd_sols} not found.")

            set_status_processing(
                field["target_name"], "vlbi_intermediate_img", field["sas_id_target"]
            )

            context = get_current_context()
            if context["ti"].try_number == 1:
                cmd = f"flocs-run vlbi image-intermediate-resolution --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            else:
                if field["status_vlbi_intermediate_img"] == PIPELINE_STATUS.downloaded:
                    # This way we can force a clean restart in the database.
                    cmd = f"flocs-run vlbi image-intermediate-resolution --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
                else:
                    # Extract the previous working directory
                    flocs_workdir = ""
                    print(
                        f"Scanning log_VLBI_image_intermediate_resolution_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                    )
                    with open(
                        f"log_VLBI_image_intermediate_resolution_{field['target_name']}_{field['sas_id_target']}.txt"
                    ) as f_out:
                        for line in f_out.readlines():
                            print(line)
                            if "Running workflow with" in line:
                                flocs_workdir = line.split(" ")[-1].strip()
                                break
                    if not flocs_workdir:
                        raise RuntimeError(
                            "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                        )
                    print(f"Resuming failed PILOT run in {flocs_workdir}")
                    cmd = f"flocs-run vlbi image-intermediate-resolution --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --dd-solutions {dd_sols} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_image_intermediate_resolution_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_image_intermediate_resolution_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"],
                        "vlbi_intermediate_img",
                        field["sas_id_target"],
                    )
                    set_field_finished(field["target_name"], field["sas_id_target"])
                else:
                    set_status_failed(
                        field["target_name"],
                        "vlbi_intermediate_img",
                        field["sas_id_target"],
                    )
                    raise RuntimeError
        return field

    @task
    def run_vlbi_facet_subtract(field):
        field = dict(get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_facet_subtract"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_facet_subtract"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            print(
                f"Processing ILT facet subtraction for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_process-ddf"
            )
            target_ms_path = target_path / "results_VLBI_process-ddf"
            print(f"Using subtracted data at: {target_path}")

            dd_sols_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_dd-calibration"
            )
            dd_sols = dd_sols_path / "results_VLBI_dd-calibration" / "merged.h5"
            print(f"Using dd solutions at: {target_path}")

            if not os.path.isfile(dd_sols):
                raise AirflowFailException(f"{dd_sols} not found.")

            model_images_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_intermediate_resolution_imaging"
            )
            model_images = (
                model_images_path
                / "results_VLBI_intermediate_resolution_imaging"
                / "*-????-model-fpb.fits"
            )
            model_images = list(model_images_path.glob("*-????-model-fpb.fits"))
            print(f"Using model image at: {model_images_path}/*-????-model-fpb.fits")

            if not model_images:
                raise AirflowFailException(
                    "No suitable intermediate resolution model images found."
                )

            set_status_processing(
                field["target_name"], "vlbi_facet_subtract", field["sas_id_target"]
            )

            context = get_current_context()
            if context["ti"].try_number == 1:
                cmd = f"flocs-run vlbi facet-subtract --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --model-image-directory {model_images_path} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            else:
                if field["status_vlbi_facet_subtract"] == PIPELINE_STATUS.downloaded:
                    # This way we can force a clean restart in the database.
                    cmd = f"flocs-run vlbi facet-subtract --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --model-image-directory {model_images_path} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
                else:
                    # Extract the previous working directory
                    flocs_workdir = ""
                    print(
                        f"Scanning log_VLBI_facet_subtract_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                    )
                    with open(
                        f"log_VLBI_facet_subtract_{field['target_name']}_{field['sas_id_target']}.txt"
                    ) as f_out:
                        for line in f_out.readlines():
                            print(line)
                            if "Running workflow with" in line:
                                flocs_workdir = line.split(" ")[-1].strip()
                                break
                    if not flocs_workdir:
                        raise RuntimeError(
                            "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                        )
                    print(f"Resuming failed PILOT run in {flocs_workdir}")
                    cmd = f"flocs-run vlbi facet-subtract --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --dd-solutions {dd_sols} --model-image-directory {model_images_path} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_facet_subtract_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_facet_subtract_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"],
                        "vlbi_facet_subtract",
                        field["sas_id_target"],
                    )
                    set_field_finished(field["target_name"], field["sas_id_target"])
                else:
                    set_status_failed(
                        field["target_name"],
                        "vlbi_facet_subtract",
                        field["sas_id_target"],
                    )
                    raise RuntimeError
        return field

    @task
    def run_vlbi_facet_imaging(field):
        field = dict(get_db_columns(field["sas_id_target"])[0])
        if (field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.processing
        ):
            return field
        else:
            print(
                f"Processing ILT facet imaging for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_facet_subtract"
            )
            target_ms_path = target_path / "results_VLBI_facet_subtract"
            print(f"Using subtracted data at: {target_path}")

            facet_mses = list(target_ms_path.glob("facet*.ms"))

            if not facet_mses:
                raise AirflowFailException(
                    "No suitable intermediate resolution model images found."
                )

            set_status_processing(
                field["target_name"], "vlbi_facet_imaging", field["sas_id_target"]
            )

            context = get_current_context()
            if context["ti"].try_number == 1:
                cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --resolution 0.3asec --pixel-scale 0.1 --ms-suffix .ms {target_ms_path}"
            else:
                if field["status_vlbi_facet_imaging"] == PIPELINE_STATUS.downloaded:
                    # This way we can force a clean restart in the database.
                    cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --resolution 0.3asec --pixel-scale 0.1 --ms-suffix .ms {target_ms_path}"
                else:
                    # Extract the previous working directory
                    flocs_workdir = ""
                    print(
                        f"Scanning log_VLBI_facet_imaging_{field['target_name']}_{field['sas_id_target']}.txt for workdir."
                    )
                    with open(
                        f"log_VLBI_facet_imaging_{field['target_name']}_{field['sas_id_target']}.txt"
                    ) as f_out:
                        for line in f_out.readlines():
                            print(line)
                            if "Running workflow with" in line:
                                flocs_workdir = line.split(" ")[-1].strip()
                                break
                    if not flocs_workdir:
                        raise RuntimeError(
                            "Could not retrieve PILOT workdir. Flocs probably crashed before launching."
                        )
                    print(f"Resuming failed PILOT run in {flocs_workdir}")
                    cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --resolution 0.3asec --pixel-scale 0.1 --ms-suffix .ms {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with (
                open(
                    f"log_VLBI_facet_imaging_{field['target_name']}_{field['sas_id_target']}.txt",
                    "w+",
                ) as f_out,
                open(
                    f"log_VLBI_facet_imaging_{field['target_name']}_{field['sas_id_target']}_err.txt",
                    "w+",
                ) as f_err,
            ):
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                success = False
                pattern = re.compile(r"Workflow.* stopped. Success: True")
                if not proc.returncode:
                    f_err.seek(0)
                    if pattern.search(f_err.read()):
                        success = True
                if success:
                    set_status_finished(
                        field["target_name"],
                        "vlbi_facet_imaging",
                        field["sas_id_target"],
                    )
                    set_field_finished(field["target_name"], field["sas_id_target"])
                else:
                    set_status_failed(
                        field["target_name"],
                        "vlbi_facet_imaging",
                        field["sas_id_target"],
                    )
                    raise RuntimeError
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
