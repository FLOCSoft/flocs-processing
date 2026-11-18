import configparser
import os
import pathlib
import re
import shlex
import subprocess
import time

from airflow.exceptions import AirflowFailException
from airflow.sdk import get_current_context
from losoto.h5parm import h5parm

from flocs_processing.db_utils import PIPELINE_STATUS, FlocsDB

# Need to think of a way to centralise this and not read multiple times here and in the DAG
if "FLOCS_AIRFLOW_CONFIG" not in os.environ:
    if not pathlib.Path(os.path.expandvars("$HOME/.flocs_airflow.cfg")).is_file():
        raise RuntimeError(
            "FLOCS_AIRFLOW_CONFIG environment variable not set and no $HOME/.flocs_airflow.cfg exists. Please create a valid configuration file."
        )
    else:
        CONFIG_FILE = os.path.expandvars("$HOME/.flocs_airflow.cfg")
else:
    CONFIG_FILE = os.getenv("FLOCS_AIRFLOW_CONFIG") or ""

parser = configparser.ConfigParser()
parser.optionxform = str  # ty: ignore[invalid-assignment]
with open(CONFIG_FILE, "r") as config:
    parser.read_string("[DEFAULT]\n" + config.read())

print("Config summary:")
for k, v in parser["DEFAULT"].items():
    print(f"{k}: {v}")

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
    try:
        rundir_final = rundirs_sorted_filtered[-1].absolute()
        return rundir_final
    except IndexError:
        print(f"No {pipeline} run for {sas_id} found")
        raise RuntimeError(f"No {pipeline} run for {sas_id} found")


def check_target_subbands(field):
    expected = field.get("number_subbands")
    if expected is None:
        return
    input_root = field.get("input_location") or os.path.join(DATA_DIR, field["field_name"])
    target = pathlib.Path(input_root) / "target" / f"L{field['target_sas_id']}"
    actual = sum(ms.is_dir() for ms in target.glob("*.MS"))
    if actual != expected:
        raise AirflowFailException(
            f"Expected {expected} target subband MSes in {target}, found {actual}. "
            "Use an input directory containing only the selected subbands."
        )


def get_delay_solution(field, db, outdir):
    run = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay-calibration")
    solutions = list((run / "results_VLBI_delay-calibration").glob(
        "merged*selfcalcycle???_linearfulljones*.h5"
    ))
    if not solutions:
        raise AirflowFailException("Completed delay calibration has no merged solutions.")
    h5 = h5parm(str(solutions[0]), readonly=True)
    try:
        sources = h5.getSolset("sol000").getSou()
        source = next(iter(sources))
        if isinstance(source, bytes):
            source = source.decode()
        identifier = re.search(r"ILTJ\d{6}\.\d{2}[+\-]\d{6}\.\d{1}", solutions[0].name)
        if identifier:
            source = identifier.group(0)
        db.set_delay_calibrator(field["field_name"], field["target_sas_id"], source)
    finally:
        h5.close()
    return solutions[0]


def run_linc_calibrator_cwltool(field, calibrator_field: int, db: FlocsDB):
    print(
        f"Processing flux density calibrator {field[f'calibrator{calibrator_field}_sas_id']} for observation {field['field_name']} {field['target_sas_id']}"
    )
    ms_folder = f"L{field[f'calibrator{calibrator_field}_sas_id']}"
    db.set_status_processing(
        field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"]
    )
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    cmd = f"flocs-run linc calibrator --runner cwltool --scheduler slurm --slurm-cores 32 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} {os.path.join(field.get('input_location') or os.path.join(DATA_DIR, field['field_name']), 'calibrator', ms_folder)}"
    with (
        open(
            os.path.join(
                logsdir,
                f"log_LINC_calibrator_{field['field_name']}_{field[f'calibrator{calibrator_field}_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_LINC_calibrator_{field['field_name']}_{field[f'calibrator{calibrator_field}_sas_id']}_err.txt",
            ),
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
            db.set_status_failed(field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"])
            raise RuntimeError("Failed to submit job.")

        if not jobid:
            db.set_status_failed(field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"])
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
                        field["field_name"],
                        f"calibrator{calibrator_field}",
                        field["target_sas_id"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    db.set_status_failed(field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"])
                    raise RuntimeError(
                        f"LINC calibrator for {field['field_name']} {field['target_sas_id']} failed."
                    )


def run_linc_calibrator_toil(field, calibrator_field: int, db: FlocsDB):
    print(
        f"Processing flux density calibrator {field[f'calibrator{calibrator_field}_sas_id']} for observation {field['field_name']} {field['target_sas_id']}"
    )
    ms_folder = f"L{field[f'calibrator{calibrator_field}_sas_id']}"
    db.set_status_processing(
        field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"]
    )
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    cmd = f"flocs-run linc calibrator --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} {os.path.join(field.get('input_location') or os.path.join(DATA_DIR, field['field_name']), 'calibrator', ms_folder)}"
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_LINC_calibrator_{field['field_name']}_{field[f'calibrator{calibrator_field}_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_LINC_calibrator_{field['field_name']}_{field[f'calibrator{calibrator_field}_sas_id']}_err.txt",
            ),
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
                field["field_name"],
                f"calibrator{calibrator_field}",
                field["target_sas_id"],
            )
        else:
            db.set_status_failed(field["field_name"], f"calibrator{calibrator_field}", field["target_sas_id"])
            raise RuntimeError


def run_linc_target_cwltool(field, db: FlocsDB):
    check_target_subbands(field)
    print(
        f"Processing target observation {field['field_name']} {field['target_sas_id']} with calibrator {field['selected_calibrator_sas_id']}"
    )
    ms_folder = f"L{field['target_sas_id']}"
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    calibrator_path = get_most_recent_run(
        outdir, field["selected_calibrator_sas_id"], "LINC_calibrator"
    )
    calibrator_solutions = (
        calibrator_path / "results_LINC_calibrator" / "cal_solutions.h5"
    )
    db.set_status_processing(field["field_name"], "target", field["target_sas_id"])
    cmd = f"flocs-run linc target --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --output-fullres-data --cal-solutions {calibrator_solutions} {os.path.join(field.get('input_location') or os.path.join(DATA_DIR, field['field_name']), 'target', ms_folder)}"
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_LINC_target_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_LINC_target_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
            db.set_status_failed(field["field_name"], "target", field["target_sas_id"])
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
                        field["field_name"],
                        "target",
                        field["target_sas_id"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    db.set_status_failed(field["field_name"], "target", field["target_sas_id"])
                    raise RuntimeError(
                        f"LINC target for {field['field_name']} {field['target_sas_id']} failed."
                    )


def run_linc_target_toil(field, db: FlocsDB):
    check_target_subbands(field)
    print(
        f"Processing target observation {field['field_name']} {field['target_sas_id']} with calibrator {field['selected_calibrator_sas_id']}"
    )
    ms_folder = f"L{field['target_sas_id']}"
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    calibrator_path = get_most_recent_run(
        outdir, field["selected_calibrator_sas_id"], "LINC_calibrator"
    )
    calibrator_solutions = (
        calibrator_path / "results_LINC_calibrator" / "cal_solutions.h5"
    )
    db.set_status_processing(field["field_name"], "target", field["target_sas_id"])
    context = get_current_context()
    log_path = os.path.join(
        logsdir,
        f"log_LINC_target_{field['field_name']}_{field['target_sas_id']}.txt",
    )
    if context["ti"].try_number == 1 or not os.path.isfile(log_path):
        cmd = f"flocs-run linc target --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --output-fullres-data --cal-solutions {calibrator_solutions} {os.path.join(field.get('input_location') or os.path.join(DATA_DIR, field['field_name']), 'target', ms_folder)}"
    else:
        flocs_workdir = ""
        print(
            f"Scanning log_LINC_target_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
        )
        with open(log_path) as f_out:
            for line in f_out.readlines():
                print(line)
                if "Running workflow with" in line:
                    flocs_workdir = line.split(" ")[-1].strip()
                    break
        if not flocs_workdir:
            raise RuntimeError(
                "Could not retrieve LINC target workdir. Flocs probably crashed before launching."
            )
        print(f"Resuming failed LINC target run in {flocs_workdir}")
        cmd = f"flocs-run linc target --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --output-fullres-data --cal-solutions {calibrator_solutions} {os.path.join(field.get('input_location') or os.path.join(DATA_DIR, field['field_name']), 'target', ms_folder)}"
    print(cmd)
    with (
        open(
            log_path,
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_LINC_target_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"], "target", field["target_sas_id"],
            )
        else:
            db.set_status_failed(field["field_name"], "target", field["target_sas_id"])
            raise RuntimeError


def run_pilot_delay_cwltool(field, db: FlocsDB, mode="widefield"):
    print(
        f"Processing delay calibration for {field['field_name']} {field['target_sas_id']}"
    )
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
    target_ms_path = target_path / "results_LINC_target" / "results"
    db.set_status_processing(field["field_name"], "vlbi_delay", field["target_sas_id"])

    delay_cat = os.path.join(outdir, "delay_calibrators.csv")
    image_cat = os.path.join(outdir, "image_catalogue.csv")

    if not os.path.isfile(delay_cat) or not os.path.isfile(image_cat):
        ms = list(target_ms_path.glob("*.dp3concat"))[0]
        cmd = f"lofar-vlbi-plot --force --output_dir {outdir} --MS {ms}"
        with (
            open(
                os.path.join(
                    logsdir,
                    f"log_plot_field_{field['field_name']}_{field['target_sas_id']}.txt",
                ),
                "w+",
            ) as f_out,
            open(
                os.path.join(
                    logsdir,
                    f"log_plot_field_{field['field_name']}_{field['target_sas_id']}_err.txt",
                ),
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

    if mode == "widefield":
        cmd = f"flocs-run vlbi delay-calibration --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} {target_ms_path}"
    else:
        cmd = f"flocs-run vlbi delay-calibration --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} --apply-delay-solutions {target_ms_path}"
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
            db.set_status_failed(field["field_name"], "vlbi_delay", field["target_sas_id"])
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
                    get_delay_solution(field, db, outdir)
                    if NEEDS_MANUAL_APPROVAL_DELAY:
                        db.set_status_await_approval(
                            field["field_name"], "vlbi_delay", field["target_sas_id"],
                        )
                    else:
                        db.set_status_finished(
                            field["field_name"], "vlbi_delay", field["target_sas_id"],
                        )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    db.set_status_failed(field["field_name"], "vlbi_delay", field["target_sas_id"])
                    raise RuntimeError(
                        f"PILOT delay calibration for {field['field_name']} {field['target_sas_id']} failed."
                    )


def run_pilot_delay_toil(field, db: FlocsDB, mode="widefield"):
    print(
        f"Processing delay calibration for {field['field_name']} {field['target_sas_id']}"
    )
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
    target_ms_path = target_path / "results_LINC_target" / "results"
    db.set_status_processing(field["field_name"], "vlbi_delay", field["target_sas_id"])

    delay_cat = os.path.join(outdir, "delay_calibrators.csv")
    image_cat = os.path.join(outdir, "image_catalogue.csv")

    if not os.path.isfile(delay_cat) or not os.path.isfile(image_cat):
        ms = list(target_ms_path.glob("*.dp3concat"))[0]
        cmd = f"lofar-vlbi-plot --force --output_dir {outdir} --MS {ms}"
        with (
            open(
                os.path.join(
                    logsdir,
                    f"log_plot_field_{field['field_name']}_{field['target_sas_id']}.txt",
                ),
                "w+",
            ) as f_out,
            open(
                os.path.join(
                    logsdir,
                    f"log_plot_field_{field['field_name']}_{field['target_sas_id']}_err.txt",
                ),
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
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            )
        )
    ):
        if mode == "widefield":
            cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} {target_ms_path}"
        else:
            cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} --apply-delay-solutions {target_ms_path}"
    else:
        # Extract the previous working directory
        flocs_workdir = ""
        print(
            f"Scanning log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
        )
        with open(
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            )
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
        cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --ms-suffix dp3concat --delay-calibrator {delay_cat} --image-catalogue {image_cat} {target_ms_path}"
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_delay-calibration_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
            get_delay_solution(field, db, outdir)
            if NEEDS_MANUAL_APPROVAL_DELAY:
                db.set_status_await_approval(
                    field["field_name"], "vlbi_delay", field["target_sas_id"],
                )
            else:
                db.set_status_finished(
                    field["field_name"], "vlbi_delay", field["target_sas_id"],
                )
        else:
            db.set_status_failed(field["field_name"], "vlbi_delay", field["target_sas_id"])
            raise RuntimeError


def run_pilot_ddcal_cwltool(field, db: FlocsDB, mode="widefield"):
    print(
        f"Processing ILT dd calibration for {field['field_name']} {field['target_sas_id']}"
    )
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    if field["pipelines"] == "vlbi-single-target":
        print("Not a widefield imaging run, checking LINC + delay calibration.")
        target_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
        target_ms_path = target_path / "results_LINC_target" / "results"
        print(f"Using LINC target run: {target_path}")

        sols_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[0]
        print(f"Using PILOT delay calibration solutions: {sols}")

        source_cat = os.path.join(field.get("input_location") or os.path.join(DATA_DIR, field["field_name"]), "vlbi_target.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["field_name"], "vlbi_dd", field["target_sas_id"]
        )
        if mode == "widefield":
            cmd = f"flocs-run vlbi dd-calibration --runner cwltool --scheduler slurm --slurm-cores 32 --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
        elif mode == "single_target":
            cmd = f"flocs-run vlbi dd-calibration --runner cwltool --scheduler slurm --slurm-cores 32 --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --max-rejected-fraction 1.0 --keep-close-sources --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
    else:
        print("Widefield imaging run, checking subtraction output.")
        target_path = get_most_recent_run(
            outdir, field["target_sas_id"], "VLBI_process-ddf"
        )
        target_ms_path = target_path / "results_VLBI_process-ddf"
        print(f"Using subtracted data at: {target_path}")

        source_cat = os.path.join(field.get("input_location") or os.path.join(DATA_DIR, field["field_name"]), "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            source_cat = os.path.join(outdir, "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["field_name"], "vlbi_dd", field["target_sas_id"]
        )

        cmd = f"flocs-run vlbi dd-calibration --runner cwltool --scheduler slurm --slurm-cores 64 --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
            db.set_status_failed(field["field_name"], "vlbi_dd", field["target_sas_id"])
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
                        field["field_name"], "vlbi_dd", field["target_sas_id"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    db.set_status_failed(field["field_name"], "vlbi_dd", field["target_sas_id"])
                    raise RuntimeError(
                        f"PILOT direction-dependent calibration for {field['field_name']} {field['target_sas_id']} failed."
                    )


def run_pilot_ddcal_toil(field, db: FlocsDB, mode: str = "widefield"):
    print(
        f"Processing ILT dd calibration for {field['field_name']} {field['target_sas_id']}"
    )
    prior_status = db.get_step_status(field["field_name"], 'vlbi_dd', field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    if field["pipelines"] == "vlbi-single-target":
        print("Not a widefield imaging run, checking LINC + delay calibration.")
        target_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
        target_ms_path = target_path / "results_LINC_target" / "results"
        print(f"Using LINC target run: {target_path}")

        sols_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[0]
        print(f"Using PILOT delay calibration solutions: {sols}")

        source_cat = os.path.join(field.get("input_location") or os.path.join(DATA_DIR, field["field_name"]), "vlbi_target.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["field_name"], "vlbi_dd", field["target_sas_id"]
        )
        if mode == "widefield":
            cmd = f"flocs-run vlbi dd-calibration --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
        elif mode == "single_target":
            cmd = f"flocs-run vlbi dd-calibration --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --delay-solset {sols} --max-rejected-fraction 1.0 --peak-flux-cut 0.0 --keep-close-sources --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat {target_ms_path}"
    else:
        print("Widefield imaging run, checking subtraction output.")
        target_path = get_most_recent_run(
            outdir, field["target_sas_id"], "VLBI_process-ddf"
        )
        target_ms_path = target_path / "results_VLBI_process-ddf"
        print(f"Using subtracted data at: {target_path}")

        source_cat = os.path.join(field.get("input_location") or os.path.join(DATA_DIR, field["field_name"]), "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            source_cat = os.path.join(outdir, "image_catalogue.csv")
        if not os.path.isfile(source_cat):
            raise AirflowFailException(f"{source_cat} not found.")

        db.set_status_processing(
            field["field_name"], "vlbi_dd", field["target_sas_id"]
        )

        context = get_current_context()
        if context["ti"].try_number == 1 or (
            not os.path.isfile(
                os.path.join(
                    logsdir,
                    f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
                )
            )
        ):
            cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
        else:
            if prior_status == PIPELINE_STATUS.downloaded.value:
                # This way we can force a clean restart in the database.
                cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --runner toil --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --source-catalogue {source_cat} --model-cache {NN_MODEL_CACHE} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
            else:
                # Extract the previous working directory
                flocs_workdir = ""
                print(
                    f"Scanning log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
                )
                with open(
                    os.path.join(
                        logsdir,
                        f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
                    )
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
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_dd-calibration_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"], "vlbi_dd", field["target_sas_id"],
            )
        else:
            db.set_status_failed(
                field["field_name"], "vlbi_dd", field["target_sas_id"]
            )
            raise RuntimeError


def run_pilot_intermediate_image_toil(field, db: FlocsDB):
    print(
        f"Processing ILT intermediate resolution imaging for {field['field_name']} {field['target_sas_id']}"
    )
    prior_status = db.get_step_status(field["field_name"], 'vlbi_intermediate_img', field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_process-ddf"
    )
    target_ms_path = target_path / "results_VLBI_process-ddf"
    print(f"Using subtracted data at: {target_path}")

    dd_sols_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_dd-calibration"
    )
    dd_sols = dd_sols_path / "results_VLBI_dd-calibration" / "merged.h5"
    print(f"Using dd solutions: {dd_sols}")

    if not os.path.isfile(dd_sols):
        raise AirflowFailException(f"{dd_sols} not found.")

    db.set_status_processing(
        field["field_name"], "vlbi_intermediate_img", field["target_sas_id"]
    )

    context = get_current_context()
    if context["ti"].try_number == 1 or (
        not os.path.isfile(
            os.path.join(
                logsdir,
                f"log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}.txt",
            )
        )
    ):
        cmd = f"flocs-run vlbi image-intermediate-resolution --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
    else:
        if prior_status == PIPELINE_STATUS.downloaded.value:
            # This way we can force a clean restart in the database.
            cmd = f"flocs-run vlbi image-intermediate-resolution --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
        else:
            # Extract the previous working directory
            flocs_workdir = ""
            print(
                f"Scanning log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
            )
            with (
                open(
                    os.path.join(
                        logsdir,
                        f"log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}.txt",
                    )
                ) as f_out,
                open(
                    os.path.join(
                        logsdir,
                        f"log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}_err.txt",
                    )
                ) as f_err,
            ):
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
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_image_intermediate_resolution_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"],
                "vlbi_intermediate_img",
                field["target_sas_id"],
            )
        else:
            db.set_status_failed(
                field["field_name"],
                "vlbi_intermediate_img",
                field["target_sas_id"],
            )
            raise RuntimeError


def run_pilot_facet_subtract_toil(field, db: FlocsDB):
    print(
        f"Processing ILT facet subtraction for {field['field_name']} {field['target_sas_id']}"
    )
    prior_status = db.get_step_status(field["field_name"], 'vlbi_facet_subtract', field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_process-ddf"
    )
    target_ms_path = target_path / "results_VLBI_process-ddf"
    print(f"Using subtracted data at: {target_path}")

    dd_sols_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_dd-calibration"
    )
    dd_sols = dd_sols_path / "results_VLBI_dd-calibration" / "merged.h5"
    print(f"Using dd solutions at: {dd_sols}")

    if not os.path.isfile(dd_sols):
        raise AirflowFailException(f"{dd_sols} not found.")

    model_images_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_image_intermediate_resolution"
    ) / "results_VLBI_image_intermediate_resolution"
    model_images = list(model_images_path.glob("*-????-model-fpb.fits"))
    print(f"Using model images at: {model_images_path}")

    if not model_images:
        raise AirflowFailException(
            "No suitable intermediate resolution model images found."
        )

    db.set_status_processing(
        field["field_name"], "vlbi_facet_subtract", field["target_sas_id"]
    )

    context = get_current_context()
    if context["ti"].try_number == 1 or (
        not os.path.isfile(
            os.path.join(
                logsdir,
                f"log_VLBI_facet-subtract_{field['field_name']}_{field['target_sas_id']}.txt",
            )
        )
    ):
        cmd = f"flocs-run vlbi facet-subtract --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --model-image-directory {model_images_path} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
    else:
        if prior_status == PIPELINE_STATUS.downloaded.value:
            # This way we can force a clean restart in the database.
            cmd = f"flocs-run vlbi facet-subtract --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --dd-solutions {dd_sols} --model-image-directory {model_images_path} --ms-suffix .dp3concat.sub.ms {target_ms_path}"
        else:
            # Extract the previous working directory
            flocs_workdir = ""
            print(
                f"Scanning log_VLBI_facet-subtract_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
            )
            with open(
                os.path.join(
                    logsdir,
                    f"log_VLBI_facet-subtract_{field['field_name']}_{field['target_sas_id']}.txt",
                )
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
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_facet-subtract_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_facet-subtract_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"],
                "vlbi_facet_subtract",
                field["target_sas_id"],
            )
        else:
            db.set_status_failed(
                field["field_name"],
                "vlbi_facet_subtract",
                field["target_sas_id"],
            )
            raise RuntimeError


def run_pilot_facet_imaging_toil(field, db: FlocsDB, resolution: float = 6.0):
    print(
        f"Processing ILT facet imaging for {field['field_name']} {field['target_sas_id']}"
    )
    identifier = f"vlbi_facet_imaging_{resolution}"
    prior_status = db.get_step_status(field["field_name"], identifier, field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_facet_subtract"
    )
    target_ms_path = target_path / "results_VLBI_facet_subtract"
    print(f"Using subtracted data at: {target_path}")

    facet_mses = list(target_ms_path.glob("facet*.ms"))

    if not facet_mses:
        raise AirflowFailException(
            "No suitable intermediate resolution model images found."
        )

    db.set_status_processing(
        field["field_name"], identifier, field["target_sas_id"]
    )

    pixel_scale = resolution / 4
    context = get_current_context()
    if context["ti"].try_number == 1 or (
        not os.path.isfile(
            os.path.join(
                logsdir,
                f"log_VLBI_facet_imaging_{resolution}_{field['field_name']}_{field['target_sas_id']}.txt",
            )
        )
    ):
        cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --resolution {resolution}asec --pixel-scale {pixel_scale} --ms-suffix .ms {target_ms_path}"
    else:
        if prior_status == PIPELINE_STATUS.downloaded.value:
            # This way we can force a clean restart in the database.
            cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --resolution {resolution}asec --pixel-scale {pixel_scale} --ms-suffix .ms {target_ms_path}"
        else:
            # Extract the previous working directory
            flocs_workdir = ""
            print(
                f"Scanning log_VLBI_facet_imaging_{resolution}_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
            )
            with open(
                os.path.join(
                    logsdir,
                    f"log_VLBI_facet_imaging_{resolution}_{field['field_name']}_{field['target_sas_id']}.txt",
                )
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
            cmd = f"flocs-run vlbi facet-imaging --record-toil-stats --runner toil --scheduler slurm --slurm-time 72:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {flocs_workdir} --restart --outdir {outdir} --resolution {resolution}asec --pixel-scale {pixel_scale} --ms-suffix .ms {target_ms_path}"
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_facet_imaging_{resolution}_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_facet_imaging_{resolution}_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"],
                identifier,
                field["target_sas_id"],
            )
        else:
            db.set_status_failed(
                field["field_name"],
                identifier,
                field["target_sas_id"],
            )
            raise RuntimeError


def run_prepare_ddf_subtract(field, db: FlocsDB):
    print(
        f"Preparing input for DDF subtract of {field['field_name']} {field['target_sas_id']}"
    )
    db.set_status_processing(field["field_name"], "prepare_ddf_subtract", field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
    target_ms_path = target_path / "results_VLBI_delay-calibration"
    mses_unaveraged = list(target_ms_path.glob("*.dp3concat"))
    delay_sols = ""
    if not mses_unaveraged:
        print(
            "No MSes found in delay-calibration output, will apply delay solutions to LINC."
        )
        sols_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        delay_sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[
            0
        ]
        print(f"Using PILOT delay calibration solutions: {delay_sols}")

        linc_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
        print(f"Using LINC MSes at {linc_path}")
        linc_ms_path = linc_path / "results_LINC_target" / "results"
        mses_unaveraged = list(linc_ms_path.glob("*.dp3concat"))
    mses_unaveraged_pilot = list(target_ms_path.glob("*.dp3concat"))
    if not mses_unaveraged:
        db.set_status_failed(
            field["field_name"],
            "prepare_ddf_subtract",
            field["target_sas_id"],
        )
        raise RuntimeError(
            f"No unaveraged input MSes found at {linc_ms_path}/*.dp3concat"
        )
    if mses_unaveraged_pilot and (len(mses_unaveraged_pilot) == len(mses_unaveraged)):
        print("Appropriate input exists for ddf-pipeline.")
        db.set_status_finished(
            field["field_name"], "prepare_ddf_subtract", field["target_sas_id"],
        )
        return mses_unaveraged_pilot

    jobids = []
    delay_corrected_mses = []
    for ms in mses_unaveraged:
        out_ms = target_ms_path / f"{ms.stem}.dp3concat"
        if out_ms.exists():
            print(f"Skipping {out_ms.name}, already exists.")
            delay_corrected_mses.append(str(out_ms))
            continue
        if delay_sols:
            h5 = h5parm(str(delay_sols))
            ss = h5.getSolset("sol000")
            # We only expect there to be one direction: the delay calibrator.
            dirname = list(ss.getSou())[0]
            db.set_delay_calibrator(field["field_name"], field["target_sas_id"], dirname)
            sourcedir = ss.getSou()[dirname]
            delaydir = f"[{sourcedir[0]},{sourcedir[1]}]"
            dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[applybeamdelay,applycal,applybeamtarget] applybeamdelay.type=applybeam applybeamdelay.beammode=full applybeamdelay.updateweights=True applybeamdelay.direction={delaydir} applycal.parmdb={delay_sols} applycal.correction=fulljones applycal.soltab=[amplitude000,phase000] applybeamtarget.type=applybeam applybeamtarget.beammode=full applybeamtarget.updateweights=True"
            print(dp3_cmd)
        else:
            dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[]"
        submit_cmd = f'sbatch -A {SLURM_ACCOUNT} -p {SLURM_QUEUE} --time=08:00:00 -c 2 --job-name=dp3_avg_{ms.stem} --wrap="{dp3_cmd}"'
        print(f"Submitting: {submit_cmd}")
        proc = subprocess.run(submit_cmd, shell=True, text=True, capture_output=True)
        if proc.returncode:
            print(proc.stdout)
            print(proc.stderr)
            raise RuntimeError(f"Failed to submit SLURM job for {ms}")
        jobid = proc.stdout.strip().split()[-1]
        print(f"Submitted job {jobid} for {ms.name}")
        jobids.append((jobid, out_ms))
        delay_corrected_mses.append(str(out_ms))

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
                (status == "FAILED") or ("TIMEOUT" in status) or ("CANCELLED" in status)
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

    mses_delay_corrected = list(target_ms_path.glob("*.dp3concat"))
    db.set_status_finished(
        field["field_name"], "prepare_ddf_subtract", field["target_sas_id"],
    )
    return mses_delay_corrected


def run_prepare_ddf(field, db: FlocsDB):
    print(f"Preparing DDF input for {field['field_name']} {field['target_sas_id']}")
    db.set_status_processing(field["field_name"], "prepare_ddf", field["target_sas_id"])
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
    target_ms_path = target_path / "results_VLBI_delay-calibration"
    mses_unaveraged = list(target_ms_path.glob("*.dp3concat"))
    delay_sols = ""
    if not mses_unaveraged:
        print(
            "No MSes found in delay-calibration output, will apply delay solutions to LINC."
        )
        sols_path = get_most_recent_run(outdir, field["target_sas_id"], "VLBI_delay")
        sols_path = sols_path / "results_VLBI_delay-calibration"
        delay_sols = list(sols_path.glob("merged*selfcalcycle???_linearfulljones*.h5"))[
            0
        ]
        print(f"Using PILOT delay calibration solutions: {delay_sols}")

        linc_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
        print(f"Using LINC MSes at {linc_path}")
        linc_ms_path = linc_path / "results_LINC_target" / "results"
        mses_unaveraged = list(linc_ms_path.glob("*.dp3concat"))
    else:
        print("Found unaveraged data in delay calibration output.")
    mses_averaged = [target_ms_path / f"{ms.stem}_pre-cal.ms" for ms in mses_unaveraged]
    if not mses_unaveraged:
        db.set_status_failed(
            field["field_name"],
            "prepare_ddf",
            field["target_sas_id"],
        )
        raise RuntimeError(
            f"No unaveraged input MSes found at {linc_ms_path}/*.dp3concat"
        )
    for out_ms in mses_averaged:
        if out_ms.exists() and not (out_ms / ".flocs-prepared").is_file():
            raise RuntimeError(
                f"Cannot reuse {out_ms}: preparation completion is unverified. "
                "Check the previous job and move the output aside before retrying."
            )

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
            db.set_delay_calibrator(field["field_name"], field["target_sas_id"], dirname)
            sourcedir = ss.getSou()[dirname]
            delaydir = f"[{sourcedir[0]},{sourcedir[1]}]"
            dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[average,applybeamdelay,applycal,applybeamtarget,filter] average.timeresolution=8 average.freqresolution=97.64kHz applybeamdelay.type=applybeam applybeamdelay.beammode=full applybeamdelay.updateweights=True applybeamdelay.direction={delaydir} applycal.parmdb={delay_sols} applycal.correction=fulljones applycal.soltab=[amplitude000,phase000] applybeamtarget.type=applybeam applybeamtarget.beammode=full applybeamtarget.updateweights=True filter.remove=True filter.baseline='[CR]S*&&'"
            print(dp3_cmd)
        else:
            dp3_cmd = f"apptainer exec $CWL_SINGULARITY_CACHE/astronrd_linc_latest.sif DP3 numthreads=2 msin={ms} msout={out_ms} msout.uvwcompression=False  msout.antennacompression=False msout.scalarflags=False msout.storagemanager=Dysco steps=[filter,average] average.timeresolution=8 average.freqresolution=97.64kHz filter.remove=True filter.baseline='[CR]S*&&'"
        dp3_cmd += f" && touch {shlex.quote(str(out_ms / '.flocs-prepared'))}"
        submit_cmd = f"sbatch -A {SLURM_ACCOUNT} -p {SLURM_QUEUE} --time=02:00:00 -c 2 --job-name=dp3_avg_{ms.stem} --wrap={shlex.quote(dp3_cmd)}"
        print(f"Submitting: {submit_cmd}")
        proc = subprocess.run(submit_cmd, shell=True, text=True, capture_output=True)
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
                if not (out_ms / ".flocs-prepared").is_file():
                    raise RuntimeError(f"Job {jobid} completed without verified output: {out_ms}")
                print(f"Job {jobid} completed ({out_ms.name})")
            elif status.startswith(
                ("FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY", "NODE_FAIL",
                 "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED")
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

    db.set_status_finished(
        field["field_name"], "prepare_ddf", field["target_sas_id"],
    )
    return mses_averaged


def run_pilot_process_ddf_toil(field, db: FlocsDB):
    print(f"Running ddf subtract for {field['field_name']} {field['target_sas_id']}")
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_delay-calibration"
    )
    target_ms_path = target_path / "results_VLBI_delay-calibration"
    print(f"Using data at: {target_path}/*.dp3concat")

    ddf_path = get_most_recent_run(outdir, field["target_sas_id"], "DDF-pipeline")
    ddf_sols_path = ddf_path / "SOLSDIR"
    print(f"Using DDF run at: {ddf_path}")

    db.set_status_processing(field["field_name"], "vlbi_ddf_subtract", field["target_sas_id"])
    context = get_current_context()
    if context["ti"].try_number == 1 or (
        not os.path.isfile(
            os.path.join(
                logsdir,
                f"log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}.txt",
            )
        )
    ):
        cmd = f"flocs-run vlbi process-ddf --runner toil --record-toil-stats --scheduler slurm --slurm-time 24:00:00 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --ms-suffix .dp3concat --ddf-rundir {ddf_path} --solsdir {ddf_sols_path} --do-subtraction {target_ms_path}"
    else:
        # Extract the previous working directory
        flocs_workdir = ""
        print(
            f"Scanning log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}.txt for workdir."
        )
        with (
            open(
                os.path.join(
                    logsdir,
                    f"log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}.txt",
                )
            ) as f_out,
            open(
                os.path.join(
                    logsdir,
                    f"log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}_err.txt",
                )
            ) as f_err,
        ):
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
        os.makedirs(outdir, exist_ok=True)
    print(cmd)
    with (
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_VLBI_process-ddf_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
                field["field_name"], "vlbi_ddf_subtract", field["target_sas_id"],
            )
        else:
            db.set_status_failed(
                field["field_name"], "vlbi_ddf_subtract", field["target_sas_id"]
            )
            raise RuntimeError


def launch_ddf_pipeline(field, db: FlocsDB):
    print(f"Starting ddf-pipeline for {field['field_name']} {field['target_sas_id']}")
    outdir = field.get("output_location") or os.path.join(OUTPUT_DIR, field["field_name"])
    logsdir = os.path.join(outdir, "logs")
    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)
    if not os.path.isdir(logsdir):
        os.makedirs(logsdir, exist_ok=True)
    target_path = get_most_recent_run(
        outdir, field["target_sas_id"], "VLBI_delay-calibration"
    )
    target_ms_path = target_path / "results_VLBI_delay-calibration"
    if not list(target_ms_path.glob("*pre-cal.ms")):
        target_path = get_most_recent_run(outdir, field["target_sas_id"], "LINC_target")
        target_ms_path = target_path / "results_LINC_target" / "results"

    cmd = f"flocs-run ddf-pipeline --scheduler slurm --slurm-time 72:00:00 --slurm-cores 32 --slurm-account {SLURM_ACCOUNT} --slurm-queue {SLURM_QUEUE} --rundir {field.get('work_directory') or PROCESSING_DIR} --outdir {outdir} --config-file {DDF_CONFIG} {target_ms_path}"
    print(cmd)
    db.set_status_processing(field["field_name"], "ddf", field["target_sas_id"])
    with (
        open(
            os.path.join(
                logsdir,
                f"log_DDF-pipeline_{field['field_name']}_{field['target_sas_id']}.txt",
            ),
            "w+",
        ) as f_out,
        open(
            os.path.join(
                logsdir,
                f"log_DDF-pipeline_{field['field_name']}_{field['target_sas_id']}_err.txt",
            ),
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
            db.set_status_failed(field["field_name"], "ddf", field["target_sas_id"])
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
                    db.set_status_finished(
                        field["field_name"],
                        "ddf",
                        field["target_sas_id"],
                    )
                    break
                elif (
                    (status == "FAILED")
                    or ("TIMEOUT" in status)
                    or ("CANCELLED" in status)
                ):
                    db.set_status_failed(field["field_name"], "ddf", field["target_sas_id"])
                    raise RuntimeError(
                        f"DDF-pipeline for {field['field_name']} {field['target_sas_id']} failed."
                    )
