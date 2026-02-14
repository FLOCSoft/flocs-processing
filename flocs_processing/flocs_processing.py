#!/usr/bin/env python
from concurrent.futures import ProcessPoolExecutor
from cyclopts import Parameter
from enum import Enum
from rich.console import Console
from typing import Annotated, Iterable
import cyclopts
import functools
import glob
import os
import pathlib
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
    pipelines: Annotated[Iterable[str], Parameter(help="")] = ["linc"],
):
    pipelines = list(map(str.lower, pipelines))
    dbstr = f"create table {table_name}(source_name text default NULL"

    if "linc" in pipelines:
        dbstr += ", sas_id_calibrator1 text default NULL, sas_id_calibrator2 text default NULL, sas_id_calibrator_final text default NULL, sas_id_target text primary key default NULL, status_calibrator1 smallint default 0, status_calibrator2 smallint default 0, status_target smallint default 0"
    if "vlbi-delay" in pipelines:
        dbstr += ", status_delay smallint default 0"
    dbstr += ");"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Creating table via: {" ".join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to create table {table_name} in database {dbname}.")


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
    table_name: Annotated[
        str, Parameter(help="Database table that will be processed.")
    ] = "processing_flocs",
):
    fp = FlocsSlurmProcessor(
        database=dbname, slurm_queues=slurm_queues, table_name=table_name, rundir=rundir
    )
    fp.start_processing_loop()


class FlocsSlurmProcessor:
    def __init__(
        self,
        database: str,
        slurm_queues: list,
        rundir: str,
        table_name: Annotated[
            str, Parameter(help="Database table to start processing in.")
        ] = "flocs_processing",
    ):
        self.DATABASE = database
        self.SLURM_QUEUES = ",".join(slurm_queues)
        self.TABLE_NAME = table_name
        self.RUNDIR = rundir

    def launch_calibrator(self, field_name, sas_id, restart: bool = False):
        if not restart:
            try:
                cmd = f"flocs-run linc calibrator --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/ --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 24:00:00 --slurm-account lofarvlbi --runner toil --save-raw-solutions {self.RUNDIR}/{field_name}/calibrator/L{sas_id}"
                print(cmd)
                with open(
                    f"log_LINC_calibrator_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"log_LINC_calibrator_{field_name}_{sas_id}_err.txt", "a"
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
                cmd = f"flocs-run linc calibrator --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/{rundir_final} --outdir {self.RUNDIR}/{field_name} --restart --slurm-queue {self.SLURM_QUEUES} --slurm-time 24:00:00 --slurm-account lofarvlbi --runner toil --save-raw-solutions {self.RUNDIR}/{field_name}/calibrator/L{sas_id}"
                print(cmd)
                with open(
                    f"log_LINC_calibrator_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"log_LINC_calibrator_{field_name}_{sas_id}_err.txt", "a"
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
                cmd = f"flocs-run linc target --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/ --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --output-fullres-data --min-unflagged-fraction 0.05 --cal-solutions {cal_sol_path} {self.RUNDIR}/{field_name}/target/L{sas_id}/"
                print(cmd)
                with open(
                    f"log_LINC_target_{field_name}_{sas_id}.txt", "w"
                ) as f_out, open(
                    f"log_LINC_target_{field_name}_{sas_id}_err.txt", "w"
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
                cal_sol_path = glob.glob(
                    f"{self.RUNDIR}/{field_name}/LINC_calibrator_L{sas_id_cal}*/results_LINC_calibrator/cal_solutions.h5"
                )[0]
                cmd = f"flocs-run linc target --record-toil-stats --scheduler slurm --rundir {self.RUNDIR}/{field_name}/rundir/{rundir_final} --restart --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --output-fullres-data --min-unflagged-fraction 0.05 --cal-solutions {cal_sol_path} {self.RUNDIR}/{field_name}/target/L{sas_id}/"
                print(cmd)
                with open(f"log_LINC_target_{field_name}.txt", "a") as f_out, open(
                    f"log_LINC_target_{field_name}_err.txt", "a"
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
                f"log_VLBI_delay-calibration_plot_field_{field_name}_{sas_id}.txt", "w"
            ) as f_out, open(
                f"log_VLBI_delay-calibration_plot_field_{field_name}_{sas_id}_err.txt",
                "w",
            ) as f_err:
                cmd = f"lofar-vlbi-plot --output_dir {rundirs} --MS {first_ms} --continue_no_lotss"
                proc = subprocess.run(
                    cmd, shell=True, text=True, stdout=subprocess.PIPE
                )
                if proc.returncode:
                    return False
            delay_csv = rundirs / "delay_calibrators.csv"
            if not os.path.isfile(delay_csv):
                print(f"Failed to find delay_calibrators.csv for {field_name}")
                return False
            try:
                cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --scheduler slurm --rundir {rundirs/'rundir'} --outdir {rundirs} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --delay-calibrator {delay_csv} --ms-suffix dp3concat {linc_target_dir/'results_LINC_target'/'results'}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"log_VLBI_delay-calibration_{field_name}_{sas_id}.txt", "w"
                ) as f_out, open(
                    f"log_VLBI_delay-calibration_{field_name}_{sas_id}_err.txt", "w"
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
        else:
            rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/")
            rundirs_sorted = sorted(rundirs.iterdir(), key=os.path.getctime)
            rundirs_sorted_filtered = [
                d
                for d in rundirs_sorted
                if ((sas_id in d.parts[-1]) and ("arget" in d.parts[-1]))
            ]
            # Last LINC target reduction for this source
            linc_target_dir = rundirs_sorted_filtered[-1].parts[-1]

            vlbi_rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
            vlbi_rundirs_sorted = sorted(vlbi_rundirs.iterdir(), key=os.path.getctime)
            # vlbi_rundirs_sorted_filtered = [d for d in vlbi_rundirs_sorted if ((sas_id in d.parts[-1]) and ("delay" in d.parts[-1]))]
            vlbi_rundirs_sorted_filtered = [
                d for d in vlbi_rundirs_sorted if ("delay" in d.parts[-1])
            ]
            vlbi_dir = vlbi_rundirs_sorted_filtered[-1]

            delay_csv = rundirs / "delay_calibrators.csv"
            try:
                cmd = f"flocs-run vlbi delay-calibration --record-toil-stats --scheduler slurm --rundir {vlbi_dir} --restart --outdir {self.RUNDIR}/{field_name} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --delay-calibrator {self.RUNDIR}/{field_name}/{delay_csv} --ms-suffix dp3concat {linc_target_dir}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"log_VLBI_delay-calibration_{field_name}_{sas_id}.txt", "w"
                ) as f_out, open(
                    f"log_VLBI_delay-calibration_{field_name}_{sas_id}_err.txt", "w"
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

    def launch_vlbi_ddcal(self, field_name, sas_id, restart: bool = False):
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

        vlbi_rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
        vlbi_rundirs_sorted = sorted(vlbi_rundirs.iterdir(), key=os.path.getctime)
        vlbi_rundirs_sorted_filtered = [
            d for d in vlbi_rundirs_sorted if ("delay" in d.parts[-1])
        ]
        vlbi_dir = vlbi_rundirs_sorted_filtered[-1]

        target_csv = rundirs / "target.csv"
        if not os.path.isfile(target_csv):
            print(f"Failed to find target.csv for {field_name}")
            return False
        delay_solset = glob.glob(vlbi_dir / "results_VLBI_delay-calibration" / "*.h5")
        if not restart:
            try:
                cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --scheduler slurm --rundir {rundirs/'rundir'} --outdir {rundirs} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --delay-solset {delay_solset} --source-catalogue {target_csv} --ms-suffix dp3concat {linc_target_dir/'results_LINC_target'/'results'}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"log_VLBI_dd-calibration_{field_name}_{sas_id}.txt", "w"
                ) as f_out, open(
                    f"log_VLBI_dd-calibration_{field_name}_{sas_id}_err.txt", "w"
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
        else:
            vlbi_dd_rundirs = pathlib.Path(f"{self.RUNDIR}/{field_name}/rundir")
            vlbi_dd_rundirs_sorted = sorted(
                vlbi_rundirs.iterdir(), key=os.path.getctime
            )
            vlbi_dd_rundirs_sorted_filtered = [
                d for d in vlbi_rundirs_sorted if ("dd-calibration" in d.parts[-1])
            ]
            vlbi_dd_dir = vlbi_rundirs_sorted_filtered[-1]
            try:
                cmd = f"flocs-run vlbi dd-calibration --record-toil-stats --scheduler slurm --rundir {rundirs/'rundir'} --restart --outdir {rundirs} --slurm-queue {self.SLURM_QUEUES} --slurm-time 48:00:00 --slurm-account lofarvlbi --runner toil --delay-solset {delay_solset} --source-catalogue {target_csv} --ms-suffix dp3concat {linc_target_dir/'results_LINC_target'/'results'}"
                print(cmd)
                os.chdir(rundirs)
                with open(
                    f"log_VLBI_dd-calibration_{field_name}_{sas_id}.txt", "a"
                ) as f_out, open(
                    f"log_VLBI_dd-calibration_{field_name}_{sas_id}_err.txt", "a"
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

    def update_db_statuses(self):
        print("== UPDATING DB STATUSES ==")
        console = Console(highlight=False)
        futures = self.running_fields.keys()
        to_delete = set()
        for future in futures:
            console.print(f"Field: {future['field']}")
            console.print(f"= SAS ID: {future['sasid']}")
            console.print(f"= Pipeline: {PIPELINE_NAMES[future['pipeline']]}")
            field = self.running_fields[future]
            if future.cancelled():
                console.print("= Status: [bold yellow]cancelled[/bold yellow]")
                del self.running_fields[future]
            elif future.done():
                if future.exception(timeout=10):
                    console.print("= Status: [bold red]failed[/bold red]")
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
                    console.print("= Status: [bold green]finished[/bold green]")
                    result = future.result()
                    print(f"Result was {result}")
                    with sqlite3.connect(self.DATABASE) as db:
                        cursor = db.cursor()
                        if result:
                            cursor.execute(
                                f"update {self.TABLE_NAME} set status_{field['identifier']}={PIPELINE_STATUS.finished.value} where source_name=='{field['name']}'"
                            )
                            print(
                                f"Processing {field['identifier']} for {field['name']} succeeded."
                            )
                            if field["identifier"] == "target":
                                cursor.execute(
                                    f"update {self.TABLE_NAME} set status_delay={PIPELINE_STATUS.downloaded.value} where source_name=='{field['name']}'"
                                )
                        else:
                            cursor.execute(
                                f"update {self.TABLE_NAME} set status_{field['identifier']}={PIPELINE_STATUS.error.value} where source_name=='{field['name']}'"
                            )
                            print(
                                f"Processing {field['identifier']} for {field['name']} failed."
                            )
                to_delete.add(future)
            else:
                console.print("= Status: [bold cyan]running[/bold cyan]")
        for f in to_delete:
            self.running_fields.pop(f, None)
        print("== UPDATING DB STATUSES FINISHED")

    def get_not_started(self, identifier: str):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            not_started = cursor.execute(
                f"select * from {self.TABLE_NAME} where status_{identifier}=={PIPELINE_STATUS.downloaded.value}"
            ).fetchall()
        return not_started

    def get_failed(self, identifier: str):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            restart = cursor.execute(
                f"select * from {self.TABLE_NAME} where status_{identifier}=={PIPELINE_STATUS.error.value}"
            ).fetchall()
        return restart

    def is_processing(self, name):
        return name in [v["name"] for f, v in self.running_fields.items()]

    def set_status_processing(self, name, identifier, target):
        with sqlite3.connect(self.DATABASE) as db:
            cursor = db.cursor()
            cursor.execute(
                f"update {self.TABLE_NAME} set status_{identifier}={PIPELINE_STATUS.processing.value} where source_name=='{name}' and sas_id_target=='{target}'"
            )

    def start_processing_loop(self, allow_up_to=PIPELINE.linc_calibrator):
        print("Starting processing loop")
        allow_up_to = PIPELINE.linc_target
        MAX_RUNNING = 3
        max_noqueue = 5
        noqueue = 0
        with ProcessPoolExecutor(max_workers=MAX_RUNNING + 1) as tpe:
            self.running_fields = {}
            lock = threading.RLock()

            while True:
                if len(self.running_fields) < 1:
                    noqueue += 1
                else:
                    noqueue = 0
                self.summarise_status()
                if noqueue >= max_noqueue:
                    print(
                        f"No new jobs added in queue for {max_noqueue * 60} s, quitting processing loop."
                    )
                    break
                self.is_accepting_jobs = len(self.running_fields) < MAX_RUNNING
                if allow_up_to >= PIPELINE.linc_calibrator:
                    restart1 = self.get_failed("calibrator1")
                    restart2 = self.get_failed("calibrator2")
                    if restart1:
                        for name, cal1, cal2, cal_final, target, _, _, _, _ in restart1:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                with lock:
                                    print(
                                        f"Re-starting LINC calibrator for calibrator 1 of field {name}"
                                    )
                                    future = tpe.submit(
                                        self.launch_calibrator, name, cal1, restart=True
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_calibrator,
                                        "identifier": "calibrator1",
                                        "sasid": target,
                                    }
                                self.set_status_processing(name, "calibrator1", target)
                    if restart2:
                        for name, cal1, cal2, cal_final, target, _, _, _, _ in restart2:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                with lock:
                                    print(
                                        f"Re-starting LINC calibrator for calibrator 2 of field {name}"
                                    )
                                    future = tpe.submit(
                                        self.launch_calibrator, name, cal2, restart=True
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_calibrator,
                                        "identifier": "calibrator2",
                                        "sasid": target,
                                    }
                                self.set_status_processing(name, "calibrator2", target)

                    not_started1 = self.get_not_started("calibrator1")
                    not_started2 = self.get_not_started("calibrator2")
                    if not_started1:
                        for (
                            name,
                            cal1,
                            cal2,
                            cal_final,
                            target,
                            _,
                            _,
                            _,
                            _,
                        ) in not_started1:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                with lock:
                                    print(
                                        f"Re-starting LINC calibrator for calibrator 1 of field {name}"
                                    )
                                    future = tpe.submit(
                                        self.launch_calibrator, name, cal1
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_calibrator,
                                        "identifier": "calibrator1",
                                        "sasid": target,
                                    }
                                self.set_status_processing(name, "calibrator1", target)
                    if not_started2:
                        for (
                            name,
                            cal1,
                            cal2,
                            cal_final,
                            target,
                            _,
                            _,
                            _,
                            _,
                        ) in not_started2:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                with lock:
                                    print(
                                        f"Re-starting LINC calibrator for calibrator 2 of field {name}"
                                    )
                                    future = tpe.submit(
                                        self.launch_calibrator, name, cal2
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_calibrator,
                                        "identifier": "calibrator2",
                                        "sasid": target,
                                    }
                                self.set_status_processing(name, "calibrator2", target)
                if allow_up_to >= PIPELINE.linc_target:
                    restart = self.get_failed("target")
                    if restart:
                        for name, cal1, cal2, cal_final, target, _, _, _, _ in restart:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                print(f"Re-starting LINC target for field {name}")
                                with lock:
                                    future = tpe.submit(
                                        self.launch_target,
                                        name,
                                        target,
                                        cal_final,
                                        restart=True,
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_target,
                                        "sasid": target,
                                        "identifier": "target",
                                    }
                                self.set_status_processing(name, "target", target)
                                print(f"Launched {name}")

                    not_started = self.get_not_started("target")
                    if not_started:
                        for (
                            name,
                            _,
                            _,
                            cal_final,
                            target,
                            _,
                            _,
                            _,
                            _,
                        ) in not_started:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                print(f"Starting LINC target for field {name}")
                                with lock:
                                    future = tpe.submit(
                                        self.launch_target, name, target, cal_final
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.linc_target,
                                        "sasid": target,
                                        "identifier": "target",
                                    }
                                self.set_status_processing(name, "target", target)
                                print(f"Launched {name}")
                if allow_up_to >= PIPELINE.vlbi_delay:
                    restart = self.get_failed("delay")
                    if restart:
                        for name, _, _, _, target, _, _, _, _ in restart:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                print(f"Re-starting VLBI delay for field {name}")
                                with lock:
                                    future = tpe.submit(
                                        self.launch_vlbi_delay,
                                        name,
                                        target,
                                        restart=True,
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.vlbi_delay,
                                        "sasid": target,
                                        "identifier": "delay",
                                    }
                                self.set_status_processing(name, "delay", target)
                                print(f"Launched {name}")

                    not_started = self.get_not_started("delay")
                    if not_started:
                        for (
                            name,
                            _,
                            _,
                            _,
                            target,
                            _,
                            _,
                            _,
                            _,
                        ) in not_started:
                            if not self.is_processing(name) and self.is_accepting_jobs:
                                print(f"Starting VLBI delay for field {name}")
                                with lock:
                                    future = tpe.submit(
                                        self.launch_vlbi_delay, name, target
                                    )
                                    self.running_fields[future] = {
                                        "name": name,
                                        "pipeline": PIPELINE.vlbi_delay,
                                        "sasid": target,
                                        "identifier": "delay",
                                    }
                                self.set_status_processing(name, "delay", target)
                                print(f"Launched {name}")
                    else:
                        print("no new fields for VLBI delay")
                with lock:
                    self.update_db_statuses()
                time.sleep(60)


def main():
    app()


if __name__ == "__main__":
    main()
