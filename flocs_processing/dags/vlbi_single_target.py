from enum import Enum
import functools
import os
import pathlib
import sqlite3
import subprocess
import time

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule
from flocs_lta.lta_search import ObservationStager
from stager_access import get_surls_requested, get_surls_online

# Need to replace this with a config file
TABLE_NAME = ""
DATABASE = ""
SLURM_ACCOUNT = ""
SLURM_QUEUE = ""
DATA_DIR = ""
OUTPUT_DIR = ""
PROCESSING_DIR = ""


@functools.total_ordering
class PIPELINE_STATUS(Enum):
    nothing = 0
    downloaded = 1
    finished = 2
    running = 3
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


def get_db_columns():
    with sqlite3.connect(DATABASE) as db:
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        columns = "target_name,priority,finished,downloaded,sas_id_calibrator1,sas_id_calibrator2,sas_id_calibrator_final,sas_id_target,status_calibrator1,status_calibrator2,status_target,status_vlbi_delay,status_vlbi_dd"
        field = cursor.execute(
            f"select {columns} from {TABLE_NAME} where finished==0 order by priority desc"
        ).fetchall()
        print(field)
    return field


def set_status_processing(name, identifier, target):
    with sqlite3.connect(DATABASE) as db:
        cursor = db.cursor()
        cursor.execute(
            f"update {TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.processing.value} where target_name=='{name}' and sas_id_target=='{target}'"
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
    # name = str(field_dict["target_name"])
    # target = str(field_dict["sas_id_target"])
    query = f"update {TABLE_NAME} set downloaded=1 where target_name=='{name}' and sas_id_target=='{target}'"
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
    rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
    if pipeline:
        rundirs_sorted_filtered = [
            d
            for d in rundirs_sorted
            if ((sas_id in d.parts[-1]) and (pipeline in d.parts[-1]))
        ]
    else:
        rundirs_sorted_filtered = [d for d in rundirs_sorted if sas_id in d.parts[-1]]
    rundir_final = rundirs_sorted_filtered[-1].absolute()
    return rundir_final


@dag
def single_target_vlbi(max_active_runs=1):
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
            has_cal1 = False
            stage_calibrators = False
            if field["sas_id_calibrator1"]:
                ms_folder = f"L{field['sas_id_calibrator1']}"
                cal1_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "calibrator", ms_folder
                )
                if os.path.exists(cal1_full_path):
                    has_cal1 = True
                else:
                    stage_calibrators = True

            has_cal2 = False
            if field["sas_id_calibrator2"]:
                ms_folder = f"L{field['sas_id_calibrator2']}"
                cal2_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "calibrator", ms_folder
                )
                if os.path.exists(cal2_full_path):
                    has_cal2 = True
                else:
                    stage_calibrators = True
            if field["sas_id_target"]:
                ms_folder = f"L{field['sas_id_target']}"
                target_full_path = os.path.join(
                    DATA_DIR, field["target_name"], "target", ms_folder
                )
                if os.path.exists(target_full_path):
                    stage_target = False
                else:
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
                    120e6,
                    168e6,
                )
                if stage_calibrators:
                    stager.find_nearest_calibrators(2, 120e6, 168e6)
                    stage_id_calibrators = stager.stage_calibrators()
                if stage_target:
                    stage_id_target = stager.stage_target()
            else:
                return field

            calibrator_staged = False
            target_staged = False
            calibrator_downloaded = has_cal1 or has_cal2
            target_downloaded = not stage_target
            while True:
                if len(get_surls_online(stage_id_calibrators)) == len(
                    get_surls_requested(stage_id_calibrators)
                ):
                    calibrator_staged = True
                if calibrator_staged and not calibrator_downloaded:
                    dl_path = os.path.join(DATA_DIR, field["target_name"], "calibrator")
                    cmd = (
                        f"flocs-lta download --outdir {dl_path} {stage_id_calibrators}"
                    )
                    with open(
                        f"log_download_calibrators_{field['target_name']}.txt",
                        "w",
                    ) as f_out, open(
                        f"log_download_calibrators_{field['target_name']}.txt",
                        "w",
                    ) as f_err:
                        proc = subprocess.run(
                            cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                        )
                        if not proc.returncode:
                            calibrator_downloaded = True
                        else:
                            raise RuntimeError

                if len(get_surls_online(stage_id_target)) == len(
                    get_surls_requested(stage_id_target)
                ):
                    calibrator_staged = True
                if target_staged and not target_downloaded:
                    dl_path = os.path.join(DATA_DIR, field["target_name"], "target")
                    cmd = f"flocs-lta download --outdir {dl_path} {stage_id_target}"
                    with open(
                        f"log_download_calibrators_{field['target_name']}.txt",
                        "w",
                    ) as f_out, open(
                        f"log_download_calibrators_{field['target_name']}.txt",
                        "w",
                    ) as f_err:
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
            field["status_calibrator1"] == PIPELINE_STATUS.running
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
            with open(
                f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator1']}.txt",
                "w",
            ) as f_out, open(
                f"log_LINC_calibrator_{field['target_name']}_{field['sas_id_calibrator1']}_err.txt",
                "w",
            ) as f_err:
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                if not proc.returncode:
                    set_status_finished(
                        field["target_name"], "calibrator1", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task
    def run_linc_calibrator2(field):
        raise AirflowSkipException()

    @task(trigger_rule=TriggerRule.ONE_DONE)
    def select_best_calibrator(result1, result2):
        if result1 and result2:
            print("Selecting between cal1 and cal2")
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
                result1["target_name"],
                result1["sas_id_target"],
                result1["sas_id_calibrator2"],
            )
            return result2
        else:
            raise AirflowFailException("No calibrators succeeded; stopping processing.")

    @task
    def run_linc_target(field):
        if (field["status_target"] == PIPELINE_STATUS.finished) or (
            field["status_target"] == PIPELINE_STATUS.running
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
            with open(
                f"log_LINC_target_{field['target_name']}_{field['sas_id_target']}.txt",
                "w",
            ) as f_out, open(
                f"log_LINC_target_{field['target_name']}_{field['sas_id_target']}_err.txt",
                "w",
            ) as f_err:
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                if not proc.returncode:
                    return True
                    set_status_finished(
                        field["target_name"], "target", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task
    def validate_linc_target(field):
        return field

    @task
    def run_vlbi_delay(field):
        if (field["status_vlbi_delay"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_delay"] == PIPELINE_STATUS.running
        ):
            return field
        else:
            print(
                f"Processing delay calibration for {field['target_name']} {field['sas_id_target']}"
            )
            ms_folder = f"L{field['sas_id_target']}"
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_calibrator_final"], "LINC_target"
            )
            target_ms_path = target_path / "results_LINC_target" / "results"
            set_status_processing(
                field["target_name"], "vlbi_delay", field["sas_id_target"]
            )
            cmd = f"flocs-run vlbi delay-calibration --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with open(
                f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}.txt",
                "w",
            ) as f_out, open(
                f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}_err.txt",
                "w",
            ) as f_err:
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                if not proc.returncode:
                    return True
                    set_status_finished(
                        field["target_name"], "vlbi_delay", field["sas_id_target"]
                    )
                else:
                    raise RuntimeError
        return field

    @task
    def run_vlbi_ddcal(field):
        if (field["status_vlbi_dd"] == PIPELINE_STATUS.finished) or (
            field["status_vlbi_dd"] == PIPELINE_STATUS.running
        ):
            return field
        else:
            print(
                f"Processing ILT dd calibration for {field['target_name']} {field['sas_id_target']}"
            )
            outdir = os.path.join(OUTPUT_DIR, field["target_name"])
            target_path = get_most_recent_run(
                outdir, field["sas_id_target"], "LINC_target"
            )
            target_ms_path = target_path / "results_LINC_target" / "results"
            print(f"Using LINC target run: {target_path}")

            sols_path = get_most_recent_run(
                outdir, field["sas_id_target"], "VLBI_delay"
            )
            sols_path = sols_path / "results_VLBI_delay-calibration"
            sols = sols_path.glob("merged_*_selfcalcycle???_linearfulljones*.h5")
            print(f"Using PILOT delay calibration solutions: {sols}")

            source_cat = os.path.join(DATA_DIR, field["target_name"], "vlbi_target.csv")
            if not os.path.isfile(source_cat):
                raise AirflowFailException(f"{source_cat} not found.")

            set_status_processing(
                field["target_name"], "vlbi_dd", field["sas_id_target"]
            )
            cmd = f"flocs-run vlbi dd-calibration --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --phasediff-score 10.0 --source-catalogue {source_cat} {target_ms_path}"
            if not os.path.isdir(outdir):
                os.mkdir(outdir)
            print(cmd)
            with open(
                f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}.txt",
                "w",
            ) as f_out, open(
                f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}_err.txt",
                "w",
            ) as f_err:
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                )
                if not proc.returncode:
                    return True
                    set_status_finished(
                        field["target_name"], "vlbi_dd", field["sas_id_target"]
                    )
                    set_field_finished(field["target_name"], field["sas_id_target"])
                else:
                    raise RuntimeError
        return field

    @task
    def run_ddf_pipeline(field):
        return True

    proceed = check_fields()
    get_field = get_unprocessed_target()
    field = download_field(get_field)
    result_cal1 = run_linc_calibrator1(field)
    result_cal2 = run_linc_calibrator2(field)
    best_cal = select_best_calibrator(result_cal1, result_cal2)
    result_targ = run_linc_target(best_cal)
    linc_is_valid = validate_linc_target(result_targ)
    result_vlbi_delay = run_vlbi_delay(linc_is_valid)
    result_vlbi_dd = run_vlbi_ddcal(result_vlbi_delay)
    # run_ddf_pipeline(vlbi_delay_is_valid)

    proceed >> get_field


single_target_vlbi()
