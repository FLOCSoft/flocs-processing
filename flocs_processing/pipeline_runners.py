import configparser
import os
import pathlib
import re
import subprocess
import time

from airflow.exceptions import AirflowFailException
from airflow.sdk import get_current_context

from flocs_processing.db_utils import PIPELINE_STATUS, FlocsDB

if "FLOCS_AIRFLOW_CONFIG" not in os.environ:
    raise RuntimeError(
        "FLOCS_AIRFLOW_CONFIG environment variable not set. Please point this to a valid configuration file."
    )

# Need to think of a way to centralise this and not read multiple times here and in the DAG
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


def run_linc_calibrator_cwltool(field, calibrator_field: int, db: FlocsDB):
    print(
        f"Processing flux density calibrator {field[f'sas_id_calibrator{calibrator_field}']} for observation {field['target_name']} {field['sas_id_target']}"
    )
    ms_folder = f"L{field[f'sas_id_calibrator{calibrator_field}']}"
    db.set_status_processing(
        field["target_name"], f"calibrator{calibrator_field}", field["sas_id_target"]
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    cmd = f"flocs-run linc calibrator --runner cwltool --scheduler slurm --slurm-cores 32 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} {os.path.join(DATA_DIR, field['target_name'], 'calibrator', ms_folder)}"
    with (
        open(
            f"log_LINC_calibrator_{field['target_name']}_{field[f'sas_id_calibrator{calibrator_field}']}.txt",
            "w+",
        ) as f_out,
        open(
            f"log_LINC_calibrator_{field['target_name']}_{field[f'sas_id_calibrator{calibrator_field}']}_err.txt",
            "w+",
        ) as f_err,
    ):
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
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
                print(f"Polling LINC calibrator job {jobid}")
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if (status == "RUNNING") or (status == "PENDING"):
                    time.sleep(60)
                elif status == "COMPLETED":
                    db.set_status_finished(
                        field["target_name"],
                        f"calibrator{calibrator_field}",
                        field["sas_id_target"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    raise RuntimeError(
                        f"LINC calibrator for {field['target_name']} {field['sas_id_target']} failed."
                    )


def run_linc_calibrator_toil(field, calibrator_field: int, db: FlocsDB):
    print(
        f"Processing flux density calibrator {field[f'sas_id_calibrator{calibrator_field}']} for observation {field['target_name']} {field['sas_id_target']}"
    )
    ms_folder = f"L{field[f'sas_id_calibrator{calibrator_field}']}"
    db.set_status_processing(
        field["target_name"], f"calibrator{calibrator_field}", field["sas_id_target"]
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    cmd = f"flocs-run linc calibrator --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} {os.path.join(DATA_DIR, field['target_name'], 'calibrator', ms_folder)}"
    if not os.path.isdir(outdir):
        os.mkdir(outdir)
    print(cmd)
    with (
        open(
            f"log_LINC_calibrator_{field['target_name']}_{field[f'sas_id_calibrator{calibrator_field}']}.txt",
            "w+",
        ) as f_out,
        open(
            f"log_LINC_calibrator_{field['target_name']}_{field[f'sas_id_calibrator{calibrator_field}']}_err.txt",
            "w+",
        ) as f_err,
    ):
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
        success = False
        pattern = re.compile(r"Workflow.* stopped. Success: True")
        if not proc.returncode:
            f_err.seek(0)
            if pattern.search(f_err.read()):
                success = True
        if success:
            db.set_status_finished(
                field["target_name"],
                f"calibrator{calibrator_field}",
                field["sas_id_target"],
            )
        else:
            raise RuntimeError


def run_linc_target_cwltool(field, db: FlocsDB):
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
    db.set_status_processing(field["target_name"], "target", field["sas_id_target"])
    cmd = f"flocs-run linc target --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --cal-solutions {calibrator_solutions} {os.path.join(DATA_DIR, field['target_name'], 'target', ms_folder)}"
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
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
                print(f"Polling LINC target job {jobid}")
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if (status == "RUNNING") or (status == "PENDING"):
                    time.sleep(60)
                elif status == "COMPLETED":
                    db.set_status_finished(
                        field["target_name"],
                        "target",
                        field["sas_id_target"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    raise RuntimeError(
                        f"LINC target for {field['target_name']} {field['sas_id_target']} failed."
                    )


def run_linc_target_toil(field, db: FlocsDB):
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
    db.set_status_processing(field["target_name"], "target", field["sas_id_target"])
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
        success = False
        pattern = re.compile(r"Workflow.* stopped. Success: True")
        if not proc.returncode:
            f_err.seek(0)
            if pattern.search(f_err.read()):
                success = True
        if success:
            db.set_status_finished(
                field["target_name"], "target", field["sas_id_target"]
            )
        else:
            raise RuntimeError


def run_pilot_delay_cwltool(field, db: FlocsDB):
    print(
        f"Processing delay calibration for {field['target_name']} {field['sas_id_target']}"
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    target_path = get_most_recent_run(outdir, field["sas_id_target"], "LINC_target")
    target_ms_path = target_path / "results_LINC_target" / "results"
    db.set_status_processing(field["target_name"], "vlbi_delay", field["sas_id_target"])

    delay_cat = os.path.join(outdir, "delay_calibrators.csv")
    image_cat = os.path.join(outdir, "image_catalogue.csv")

    if not os.path.isfile(delay_cat) or not os.path.isfile(image_cat):
        ms = list(target_ms_path.glob("*.dp3concat"))[0]
        cmd = f"lofar-vlbi-plot --force --output_dir {outdir} --MS {ms}"
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

    cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} --apply-delay-solutions {target_ms_path}"
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
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
                print(f"Polling LINC target job {jobid}")
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if (status == "RUNNING") or (status == "PENDING"):
                    time.sleep(60)
                elif status == "COMPLETED":
                    if NEEDS_MANUAL_APPROVAL_DELAY:
                        db.set_status_await_approval(
                            field["target_name"], "vlbi_delay", field["sas_id_target"]
                        )
                    else:
                        db.set_status_finished(
                            field["target_name"], "vlbi_delay", field["sas_id_target"]
                        )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    raise RuntimeError(
                        f"PILOT delay calibration for {field['target_name']} {field['sas_id_target']} failed."
                    )


def run_pilot_delay_toil(field, db: FlocsDB):
    print(
        f"Processing delay calibration for {field['target_name']} {field['sas_id_target']}"
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    target_path = get_most_recent_run(outdir, field["sas_id_target"], "LINC_target")
    target_ms_path = target_path / "results_LINC_target" / "results"
    db.set_status_processing(field["target_name"], "vlbi_delay", field["sas_id_target"])

    delay_cat = os.path.join(outdir, "delay_calibrators.csv")
    image_cat = os.path.join(outdir, "image_catalogue.csv")

    if not os.path.isfile(delay_cat) or not os.path.isfile(image_cat):
        ms = list(target_ms_path.glob("*.dp3concat"))[0]
        cmd = f"lofar-vlbi-plot --force --output_dir {outdir} --MS {ms}"
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
    if context["ti"].try_number == 1 or (
        not os.path.isfile(
            f"log_VLBI_delay-calibration_{field['target_name']}_{field['sas_id_target']}.txt"
        )
    ):
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
        success = False
        pattern = re.compile(r"Workflow.* stopped. Success: True")
        if not proc.returncode:
            f_err.seek(0)
            if pattern.search(f_err.read()):
                success = True

        if success:
            if NEEDS_MANUAL_APPROVAL_DELAY:
                db.set_status_await_approval(
                    field["target_name"], "vlbi_delay", field["sas_id_target"]
                )
            else:
                db.set_status_finished(
                    field["target_name"], "vlbi_delay", field["sas_id_target"]
                )
        else:
            raise RuntimeError


def run_pilot_ddcal_cwltool(field, db: FlocsDB):
    print(
        f"Processing ILT dd calibration for {field['target_name']} {field['sas_id_target']}"
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    if "status_ddf" not in field:
        print("Not a widefield imaging run, checking LINC + delay calibration.")
        target_path = get_most_recent_run(outdir, field["sas_id_target"], "LINC_target")
        target_ms_path = target_path / "results_LINC_target" / "results"
        print(f"Using LINC target run: {target_path}")

        sols_path = get_most_recent_run(outdir, field["sas_id_target"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[0]
        print(f"Using PILOT delay calibration solutions: {sols}")

        source_cat = os.path.join(DATA_DIR, field["target_name"], "vlbi_target.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["target_name"], "vlbi_dd", field["sas_id_target"]
        )
        cmd = f"flocs-run vlbi dd-calibration --runner cwltool --scheduler slurm --slurm-cores 32 --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --phasediff-score 10.0 --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
    else:
        print("Widefield imaging run, checking subtraction output.")
        target_path = get_most_recent_run(
            outdir, field["sas_id_target"], "VLBI_process-ddf"
        )
        target_ms_path = target_path / "results_VLBI_process-ddf"
        print(f"Using subtracted data at: {target_path}")

        source_cat = os.path.join(DATA_DIR, field["target_name"], "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["target_name"], "vlbi_dd", field["sas_id_target"]
        )

        cmd = f"flocs-run vlbi dd-calibration --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
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
                print(f"Polling LINC target job {jobid}")
                poll_cmd = f"sacct -X -j {jobid} --format=State --noheader"
                status = subprocess.run(
                    poll_cmd, shell=True, text=True, capture_output=True
                ).stdout.strip()
                if (status == "RUNNING") or (status == "PENDING"):
                    time.sleep(60)
                elif status == "COMPLETED":
                    db.set_status_finished(
                        field["target_name"], "vlbi_dd", field["sas_id_target"]
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    raise RuntimeError(
                        f"PILOT direction-dependent calibration for {field['target_name']} {field['sas_id_target']} failed."
                    )


def run_pilot_ddcal_toil(field, db: FlocsDB):
    print(
        f"Processing ILT dd calibration for {field['target_name']} {field['sas_id_target']}"
    )
    outdir = os.path.join(OUTPUT_DIR, field["target_name"])
    if "status_ddf" not in field:
        print("Not a widefield imaging run, checking LINC + delay calibration.")
        target_path = get_most_recent_run(outdir, field["sas_id_target"], "LINC_target")
        target_ms_path = target_path / "results_LINC_target" / "results"
        print(f"Using LINC target run: {target_path}")

        sols_path = get_most_recent_run(outdir, field["sas_id_target"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[0]
        print(f"Using PILOT delay calibration solutions: {sols}")

        source_cat = os.path.join(DATA_DIR, field["target_name"], "vlbi_target.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
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

        source_cat = os.path.join(DATA_DIR, field["target_name"], "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["target_name"], "vlbi_dd", field["sas_id_target"]
        )

        context = get_current_context()
        if context["ti"].try_number == 1 or (
            not os.path.isfile(
                f"log_VLBI_dd-calibration_{field['target_name']}_{field['sas_id_target']}.txt"
            )
        ):
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
        proc = subprocess.run(cmd, shell=True, text=True, stdout=f_out, stderr=f_err)
        success = False
        pattern = re.compile(r"Workflow.* stopped. Success: True")
        if not proc.returncode:
            f_err.seek(0)
            if pattern.search(f_err.read()):
                success = True
        if success:
            db.set_status_finished(
                field["target_name"], "vlbi_dd", field["sas_id_target"]
            )
            db.set_field_finished(field["target_name"], field["sas_id_target"])
        else:
            db.set_status_failed(
                field["target_name"], "vlbi_dd", field["sas_id_target"]
            )
            raise RuntimeError
