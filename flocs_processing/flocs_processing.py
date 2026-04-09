#!/usr/bin/env python
from astropy.table import Table
from concurrent.futures import ProcessPoolExecutor
from cyclopts import Parameter
from enum import Enum
from rich.console import Console
from typing import Annotated
import cyclopts
import functools
import glob
import os
import pathlib
import re
import subprocess
import sqlite3
import threading
import time

app = cyclopts.App()


@functools.total_ordering
class PIPELINE(Enum):
    download = 0
    linc_calibrator = 1
    linc_target = 2
    vlbi_delay = 3

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value == other.value

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value < other.value

    def __hash__(self):
        return hash(self.value)


PIPELINE_NAMES: dict[PIPELINE, str] = {
    PIPELINE.download: "not downloaded",
    PIPELINE.linc_calibrator: "LINC Calibrator",
    PIPELINE.linc_target: "LINC Target",
    PIPELINE.vlbi_delay: "PILOT delay calibration",
}


@functools.total_ordering
class PIPELINE_STATUS(Enum):
    nothing = 0
    downloaded = 1
    finished = 2
    running = 3
    processing = 98
    error = 99

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value == other.value

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value < other.value


@functools.total_ordering
class STAGING_STATUS(Enum):
    error = -1
    not_staged = 0
    in_progress = 1
    finished = 2

    def __eq__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value == other.value

    def __lt__(self, other):
        if self.__class__ is not other.__class__:
            raise NotImplementedError
        return self.value < other.value


@app.command()
def create_database(
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
    pipelines: Annotated[list[str], Parameter(help="", consume_multiple=True)] = [
        "linc"
    ],
):
    pipelines = list(map(str.lower, pipelines))
    dbstr = f"create table {table_name}(source_name text default NULL"

    if "linc" in pipelines:
        dbstr += ", sas_id_calibrator1 text default NULL, sas_id_calibrator2 text default NULL, sas_id_calibrator_final text default NULL, sas_id_target text primary key default NULL, status_calibrator1 smallint default 0, status_calibrator2 smallint default 0, status_target smallint default 0"
    if "ddf-pipeline" in pipelines:
        dbstr += ", status_ddf smallint default 0"
    if "vlbi-delay-widefield" in pipelines:
        dbstr += ", status_ddf smallint default 0"
        dbstr += ", status_delay smallint default 0"
    if "vlbi-delay-single-target" in pipelines:
        dbstr += ", status_delay smallint default 0"
    dbstr += ");"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Creating table via: {" ".join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to create table {table_name} in database {dbname}.")


@app.command()
def add_field(
    field_name: Annotated[str, Parameter(help="Name of the source/field to add.")],
    sas_id_calibrators: Annotated[
        list[str],
        Parameter(help="SAS IDs of the calibrators to add.", consume_multiple=True),
    ],
    sas_id_target: Annotated[
        str,
        Parameter(help="SAS ID of the target to add.", consume_multiple=True),
    ],
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
):
    dbstr = f"insert into {table_name} (source_name"
    if len(sas_id_calibrators) == 1:
        dbstr += ", sas_id_calibrator1"
    if len(sas_id_calibrators) == 2:
        dbstr += ", sas_id_calibrator1, sas_id_calibrator2"
    dbstr += f", sas_id_target) values ('{field_name}', "
    if len(sas_id_calibrators) == 1:
        dbstr += f"{sas_id_calibrators[0]}"
    if len(sas_id_calibrators) == 2:
        dbstr += f"{sas_id_calibrators[0]}, {sas_id_calibrators[1]}, "
    dbstr += f"{sas_id_target})"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Adding field {field_name} to {table_name} via: {" ".join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to update table {table_name} in database {dbname}.")


@app.command()
def process_from_database(
    dbname: Annotated[
        str, Parameter(help="Sqlite3 database from which processing will be done.")
    ],
    rundir: Annotated[
        str,
        Parameter(
            help="Directory where data is located and processing will take place."
        ),
    ],
    slurm_queues: Annotated[
        list[str], Parameter(help="Slurm queues that jobs can be submitted to.")
    ],
    slurm_account: Annotated[str, Parameter(help="Slurm account to submit under.")],
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
):
    fp = FlocsSlurmProcessor(
        database=dbname,
        slurm_queues=slurm_queues,
        slurm_account=slurm_account,
        table_name=table_name,
        rundir=rundir,
    )
    fp.start_processing_loop()


class FlocsSlurmProcessor:
    def __init__(
        self,
        database: str,
        slurm_queues: list,
        slurm_account: str,
        rundir: str,
        table_name: Annotated[
            str, Parameter(help="Database table to start processing in.")
        ] = "flocs_processing",
    ):
        self.DATABASE = database
        self.SLURM_QUEUES = ",".join(slurm_queues)
        self.SLURM_ACCOUNT = slurm_account
        self.TABLE_NAME = table_name
        self.RUNDIR = rundir

    def launch_calibrator(self, field_name, sas_id, restart: bool = False):
        if not restart:
            try:
                cmd = f"flocs-run linc calibrator --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/ --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 24:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --save-raw-solutions {self.RUNDIR}/{field_name}/calibrator/L{sas_id}"
                print(cmd)
                with open(
                    f"{field_name}/log_LINC_calibrator_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"{field_name}/log_LINC_calibrator_{field_name}_{sas_id}_err.txt",
                    "a",
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        return True
                    else:
                        return False
            except subprocess.CalledProcessError:
                print("something went wrong")
        else:
            rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
            rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
            rundirs_sorted_filtered = [
                d for d in rundirs_sorted if sas_id in d.parts[-1]
            ]
            # Last directory touched for this source
            rundir_final = rundirs_sorted_filtered[-1].parts[-1]
            try:
                cmd = f"flocs-run linc calibrator --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/{rundir_final} --outdir {self.RUNDIR}/{field_name} --restart --slurm-queue {self.SLURM_QUEUES} --slurm-time 24:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --save-raw-solutions {self.RUNDIR}/{field_name}/calibrator/L{sas_id}"
                print(cmd)
                with open(
                    f"{field_name}/log_LINC_calibrator_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"{field_name}/log_LINC_calibrator_{field_name}_{sas_id}_err.txt",
                    "a",
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        return True
                    else:
                        return False
            except subprocess.CalledProcessError:
                print("something went wrong")
        return False

    def launch_target(self, field_name, sas_id, sas_id_cal, restart: bool = False):
        if not restart:
            try:
                cal_sol_path = glob.glob(
                    f"{self.RUNDIR}/{field_name}/LINC_calibrator_L{sas_id_cal}*/results_LINC_calibrator/cal_solutions.h5"
                )[0]
                cmd = f"flocs-run linc target --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/ --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --output-fullres-data --min-unflagged-fraction 0.05 --cal-solutions {cal_sol_path} {self.RUNDIR}/{field_name}/target/L{sas_id}/"
                print(cmd)
                with open(
                    f"{field_name}/log_LINC_target_{field_name}_{sas_id}.txt", "w"
                ) as f_out, open(
                    f"{field_name}/log_LINC_target_{field_name}_{sas_id}_err.txt", "w"
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        pattern = re.compile(r"Workflow.* stopped. Success: False")
                        if pattern.search(proc.stderr):
                            return False
                        return True
                    else:
                        return False
            except subprocess.CalledProcessError:
                print("something went wrong")
        else:
            rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
            rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
            rundirs_sorted_filtered = [
                d for d in rundirs_sorted if sas_id in d.parts[-1]
            ]
            # Last directory touched for this source
            rundir_final = rundirs_sorted_filtered[-1].parts[-1]
            try:
                cal_sol_path = glob.glob(
                    f"{self.RUNDIR}/{field_name}/LINC_calibrator_L{sas_id_cal}*/results_LINC_calibrator/cal_solutions.h5"
                )[0]
                cmd = f"flocs-run linc target --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/{rundir_final} --restart --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --output-fullres-data --min-unflagged-fraction 0.05 --cal-solutions {cal_sol_path} {self.RUNDIR}/{field_name}/target/L{sas_id}/"
                print(cmd)
                with open(
                    f"{field_name}/log_LINC_target_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"{field_name}/log_LINC_target_{field_name}_{sas_id}_err.txt", "a"
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        return True
                    else:
                        return False
                return True
            except subprocess.CalledProcessError:
                print("something went wrong")
        return False

    def launch_vlbi_delay(self, field_name, sas_id, restart: bool = False):
        if not restart:
            print(f"Generating input catalogue(s) for {field_name}")
            rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/")
            rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
            rundirs_sorted_filtered = [
                d
                for d in rundirs_sorted
                if ((sas_id in d.parts[-1]) and ("arget" in d.parts[-1]))
            ]
            # Last LINC target reduction for this source
            linc_target_dir = rundirs_sorted_filtered[-1]
            first_ms = glob.glob(
                f"{linc_target_dir}/results_LINC_target/results/*.dp3concat"
            )[0]

            with open(
                f"{rundirs}/log_VLBI_delay-calibration_plot_field_{field_name}_{sas_id}.txt",
                "w",
            ) as f_out, open(
                f"{rundirs}/log_VLBI_delay-calibration_plot_field_{field_name}_{sas_id}_err.txt",
                "w",
            ) as f_err:
                cmd = f"lofar-vlbi-plot --output_dir {rundirs} --MS {first_ms} --continue_no_lotss --vlass"
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=subprocess.PIPE
                )
                if proc.returncode:
                    return False
            delay_csv = rundirs / "delay_calibrators.csv"
            if not os.path.isfile(delay_csv):
                print(f"Failed to find delay_calibrators.csv for {field_name}")
                return False
            dc = Table.read(delay_csv)
            model_image = rundirs / f"{dc[0]['Observation']}_vlass.fits"
            try:
                cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --scheduler slurm --rundir {rundirs/'rundir'} --outdir {rundirs} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --delay-calibrator {delay_csv} --model-image {model_image} --ms-suffix dp3concat {linc_target_dir/'results_LINC_target'/'results'}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"{rundirs}/log_VLBI_delay-calibration_{field_name}_{sas_id}.txt",
                    "w",
                ) as f_out, open(
                    f"{rundirs}/log_VLBI_delay-calibration_{field_name}_{sas_id}_err.txt",
                    "w",
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        pattern = re.compile(r"Workflow.* stopped. Success: False")
                        if pattern.search(proc.stderr):
                            return False
                        return True
                    else:
                        return False
            except subprocess.CalledProcessError:
                print("something went wrong")
                return False
        else:
            rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/")
            rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
            rundirs_sorted_filtered = [
                d
                for d in rundirs_sorted
                if ((sas_id in d.parts[-1]) and ("arget" in d.parts[-1]))
            ]
            # Last LINC target reduction for this source
            linc_target_dir = rundirs_sorted_filtered[-1]
            print(f"{linc_target_dir=}")

            vlbi_rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
            vlbi_rundirs_sorted = sorted(vlbi_rundirs.iterdir(), key=os.path.getctime)
            # vlbi_rundirs_sorted_filtered = [d for d in vlbi_rundirs_sorted if ((sas_id in d.parts[-1]) and ("delay" in d.parts[-1]))]
            vlbi_rundirs_sorted_filtered = [
                d for d in vlbi_rundirs_sorted if ("delay" in d.parts[-1])
            ]
            vlbi_dir = vlbi_rundirs_sorted_filtered[-1]
            print(f"{vlbi_dir=}")

            delay_csv = rundirs / "delay_calibrators.csv"
            try:
                cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --scheduler slurm --rundir {vlbi_dir} --restart --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account {self.SLURM_ACCOUNT} --runner toil --delay-calibrator {delay_csv} --ms-suffix dp3concat {linc_target_dir/'results_LINC_target/results'}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"{rundirs}/log_VLBI_delay-calibration_{field_name}_{sas_id}.txt",
                    "w",
                ) as f_out, open(
                    f"{rundirs}/log_VLBI_delay-calibration_{field_name}_{sas_id}_err.txt",
                    "w",
                ) as f_err:
                    proc = subprocess.run(
                        cmd, shell=True, text=True, stdout=f_out, stderr=f_err
                    )
                    if not proc.returncode:
                        pattern = re.compile(r"Workflow.* stopped. Success: False")
                        if pattern.search(proc.stderr):
                            return False
                        return True
                    else:
                        return False
            except subprocess.CalledProcessError:
                print("something went wrong")
        return False

    def summarise_status(self):
        console = Console(highlight=False)
        console.print(f"General statistics for {self.DATABASE}", style="bold")
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            not_started = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where (status_calibrator1=={PIPELINE_STATUS.nothing.value} or status_calibrator2=={PIPELINE_STATUS.nothing.value})"
            ).fetchall()[0][0]
            downloaded = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where (status_calibrator1=={PIPELINE_STATUS.downloaded.value} or status_calibrator2=={PIPELINE_STATUS.downloaded.value})"
            ).fetchall()[0][0]
            processing = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where (status_calibrator1=={PIPELINE_STATUS.processing.value} or status_calibrator2=={PIPELINE_STATUS.processing.value})"
            ).fetchall()[0][0]
            finished = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where (status_calibrator1=={PIPELINE_STATUS.finished.value} or status_calibrator2=={PIPELINE_STATUS.finished.value})"
            ).fetchall()[0][0]
            error = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where (status_calibrator1=={PIPELINE_STATUS.error.value} or status_calibrator2=={PIPELINE_STATUS.error.value})"
            ).fetchall()[0][0]
            console.print("Flux density calibrators", style="bold")
            console.print(f"= {not_started} calibrators not yet downloaded")
            console.print(f"= {downloaded} calibrators downloaded", style="yellow")
            console.print(f"= {processing} calibrators processing", style="cyan")
            console.print(f"= {finished} calibrators finished", style="green")
            console.print(f"= {error} calibrators failed", style="red")

            not_started = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where status_target=={PIPELINE_STATUS.nothing.value}"
            ).fetchall()[0][0]
            downloaded = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where status_target=={PIPELINE_STATUS.downloaded.value}"
            ).fetchall()[0][0]
            finished = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where status_target=={PIPELINE_STATUS.finished.value}"
            ).fetchall()[0][0]
            processing = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where status_target=={PIPELINE_STATUS.processing.value}"
            ).fetchall()[0][0]
            error = cursor.execute(
                f"select count(source_name) from {self.TABLE_NAME} where status_target=={PIPELINE_STATUS.error.value}"
            ).fetchall()[0][0]
            console.print("\nFields / science targets", style="bold")
            console.print(f"= {not_started} targets not yet downloaded")
            console.print(f"= {downloaded} targets downloaded", style="yellow")
            console.print(f"= {processing} targets processing", style="cyan")
            console.print(f"= {finished} targets finished", style="green")
            console.print(f"= {error} targets failed", style="red")

    def update_db_statuses(self, running_fields):
        if not running_fields:
            return
        print("== UPDATING DB STATUSES ==")
        console = Console(highlight=False)
        futures = running_fields.keys()
        to_delete = set()
        for future in futures:
            field = running_fields[future]
            console.print(f"[bold]Field: {field['name']}[/bold]")
            console.print(f"Target SAS ID: {field['sasid']}")
            console.print(f"Pipeline: {PIPELINE_NAMES[field['pipeline']]}")
            if future.cancelled():
                console.print("Status: [bold yellow]cancelled[/bold yellow]")
                del running_fields[future]
            elif future.done():
                if future.exception(timeout=10):
                    console.print("Status: [bold red]failed[/bold red]")
                    print(
                        f"Processing {field['identifier']} for {field['name']} failed."
                    )
                    print("Error was: ", future.exception(timeout=10))
                    with sqlite3.connect(self.DATABASE) as db:
                        cursor = db.cursor()
                        cursor.execute(
                            f"update {self.TABLE_NAME} set status_{field['identifier']}={PIPELINE_STATUS.error.value} where source_name=='{field['name']}'"
                        )
                else:
                    result = future.result()
                    print(f"Result was {result}")
                    with sqlite3.connect(self.DATABASE) as db:
                        cursor = db.cursor()
                        if result:
                            console.print("Status: [bold green]finished[/bold green]")
                            cursor.execute(
                                f"update {self.TABLE_NAME} set status_{field['identifier']}={PIPELINE_STATUS.finished.value} where source_name=='{field['name']}'"
                            )
                            if field["identifier"] == "target":
                                cursor.execute(
                                    f"update {self.TABLE_NAME} set status_delay={PIPELINE_STATUS.downloaded.value} where source_name=='{field['name']}'"
                                )
                        else:
                            console.print("Status: [bold red]failed[/bold red]")
                            cursor.execute(
                                f"update {self.TABLE_NAME} set status_{field['identifier']}={PIPELINE_STATUS.error.value} where source_name=='{field['name']}'"
                            )
                to_delete.add(future)
            else:
                console.print("Status: [bold cyan]running[/bold cyan]\n")
        for f in to_delete:
            print(f"Deleting future for {field['name']}")
            del running_fields[f]
        print("== UPDATING DB STATUSES FINISHED")

    def get_not_started(self, identifier: str):
        not_started = self.get_db_columns(identifier, PIPELINE_STATUS.downloaded)
        return not_started

    def get_failed(self, identifier: str):
        restart = self.get_db_columns(identifier, PIPELINE_STATUS.error)
        return restart

    def get_db_columns(self, identifier: str, status: PIPELINE_STATUS):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            if "calibrator" in identifier:
                columns = "source_name,sas_id_calibrator1,sas_id_calibrator2,sas_id_calibrator_final,sas_id_target"
            elif "target" in identifier:
                columns = "source_name,sas_id_calibrator_final,sas_id_target"
            elif "delay" in identifier:
                columns = "source_name,sas_id_target"
            else:
                columns = "*"
            restart = cursor.execute(
                f"select {columns} from {self.TABLE_NAME} where status_{identifier}=={status.value}"
            ).fetchall()
        return restart

    def is_processing(self, name, running_fields):
        return name in [v["name"] for f, v in running_fields.items()]

    def set_status_processing(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.processing.value} where source_name=='{name}' and sas_id_target=='{target}'"
            )

    def check_fields_linc_calibrator(self, running_fields, tpe):
        restart1 = self.get_failed("calibrator1")
        restart2 = self.get_failed("calibrator2")
        if restart1:
            for name, cal1, cal2, cal_final, target in restart1:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(
                        f"Re-starting LINC calibrator for calibrator 1 of field {name}"
                    )
                    future = tpe.submit(
                        self.launch_calibrator, name, cal1, restart=True
                    )
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_calibrator,
                        "identifier": "calibrator1",
                        "sasid": target,
                    }
                self.set_status_processing(name, "calibrator1", target)
        if restart2:
            for name, cal1, cal2, cal_final, target in restart2:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(
                        f"Re-starting LINC calibrator for calibrator 2 of field {name}"
                    )
                    future = tpe.submit(
                        self.launch_calibrator, name, cal2, restart=True
                    )
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_calibrator,
                        "identifier": "calibrator2",
                        "sasid": target,
                    }
                self.set_status_processing(name, "calibrator2", target)

        not_started1 = self.get_not_started("calibrator1")
        not_started2 = self.get_not_started("calibrator2")
        if not_started1:
            for name, cal1, cal2, cal_final, target in not_started1:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(
                        f"Re-starting LINC calibrator for calibrator 1 of field {name}"
                    )
                    future = tpe.submit(self.launch_calibrator, name, cal1)
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_calibrator,
                        "identifier": "calibrator1",
                        "sasid": target,
                    }
                self.set_status_processing(name, "calibrator1", target)
        if not_started2:
            for name, cal1, cal2, cal_final, target in not_started2:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(
                        f"Re-starting LINC calibrator for calibrator 2 of field {name}"
                    )
                    future = tpe.submit(self.launch_calibrator, name, cal2)
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_calibrator,
                        "identifier": "calibrator2",
                        "sasid": target,
                    }
                self.set_status_processing(name, "calibrator2", target)

    def check_fields_linc_target(self, running_fields, tpe):
        restart = self.get_failed("target")
        if restart:
            for name, cal_final, target in restart:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(f"Re-starting LINC target for field {name}")
                    future = tpe.submit(
                        self.launch_target,
                        name,
                        target,
                        cal_final,
                        restart=True,
                    )
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_target,
                        "sasid": target,
                        "identifier": "target",
                    }
                    self.set_status_processing(name, "target", target)
                    self.is_accepting_jobs = len(running_fields) < self.MAX_RUNNING
                    print(f"Launched {name}")

        not_started = self.get_not_started("target")
        if not_started:
            for name, cal_final, target in not_started:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(f"Starting LINC target for field {name}")
                    future = tpe.submit(self.launch_target, name, target, cal_final)
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.linc_target,
                        "sasid": target,
                        "identifier": "target",
                    }
                    self.set_status_processing(name, "target", target)
                    self.is_accepting_jobs = len(running_fields) < self.MAX_RUNNING
                    print(f"Launched {name}")

    def check_fields_vlbi_delay(self, running_fields, tpe):
        restart = self.get_failed("delay")
        if restart:
            for name, target in restart:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(f"Re-starting VLBI delay for field {name}")
                    future = tpe.submit(
                        self.launch_vlbi_delay,
                        name,
                        target,
                        restart=True,
                    )
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.vlbi_delay,
                        "sasid": target,
                        "identifier": "delay",
                    }
                    self.set_status_processing(name, "delay", target)
                    print(f"Launched {name}")

        not_started = self.get_not_started("delay")
        if not_started:
            for name, target in not_started:
                if (
                    not self.is_processing(name, running_fields)
                    and self.is_accepting_jobs
                ):
                    print(f"Starting VLBI delay for field {name}")
                    future = tpe.submit(self.launch_vlbi_delay, name, target)
                    running_fields[future] = {
                        "name": name,
                        "pipeline": PIPELINE.vlbi_delay,
                        "sasid": target,
                        "identifier": "delay",
                    }
                    self.set_status_processing(name, "delay", target)
                    print(f"Launched {name}")

    def start_processing_loop(self, allow_up_to=PIPELINE.linc_calibrator):
        print("Starting processing loop")
        allow_up_to = PIPELINE.vlbi_delay
        self.MAX_RUNNING = 3
        max_noqueue = 5
        noqueue = 0
        lock = threading.RLock()
        with ProcessPoolExecutor(max_workers=self.MAX_RUNNING + 1) as tpe:
            running_fields = {}

            while True:
                if len(running_fields) < 1:
                    noqueue += 1
                else:
                    noqueue = 0
                self.summarise_status()
                if noqueue >= max_noqueue:
                    print(
                        f"No new jobs added in queue for {max_noqueue * 60} s, quitting processing loop."
                    )
                    break
                self.is_accepting_jobs = len(running_fields) < self.MAX_RUNNING
                if allow_up_to >= PIPELINE.linc_calibrator:
                    with lock:
                        self.check_fields_linc_calibrator(running_fields, tpe)
                if allow_up_to >= PIPELINE.linc_target:
                    with lock:
                        self.check_fields_linc_target(running_fields, tpe)
                if allow_up_to >= PIPELINE.vlbi_delay:
                    with lock:
                        self.check_fields_vlbi_delay(running_fields, tpe)
                with lock:
                    self.update_db_statuses(running_fields)
                time.sleep(60)


def main():
    app()


if __name__ == "__main__":
    main()
