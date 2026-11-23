"""Read resource usage for jobs submitted by a processing step."""

import logging
import re
import subprocess


logger = logging.getLogger(__name__)


def _cpu_seconds(value):
    if not re.fullmatch(r"(?:\d+-)?(?:\d+:)?\d+:\d+(?:\.\d+)?", value):
        return None
    days, separator, clock = value.partition("-")
    parts = clock.split(":") if separator else days.split(":")
    seconds = sum(float(part) * 60**i for i, part in enumerate(reversed(parts)))
    return seconds + (int(days) * 86400 if separator else 0)


def _rss_bytes(value):
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGTP]?)", value)
    if match is None:
        return None
    amount, unit = match.groups()
    return int(float(amount) * 1024 ** ("KMGTP".index(unit or "K") + 1))


def get_job_profiles(job_ids):
    """Return available Slurm accounting for the supplied allocation IDs."""
    ids = list(dict.fromkeys(str(job_id) for job_id in job_ids
                            if re.fullmatch(r"[0-9]+", str(job_id))))
    profiles = {job_id: {"job_id": job_id} for job_id in ids}
    step_cpu = {job_id: {} for job_id in ids}
    for start in range(0, len(ids), 100):
        batch = ids[start:start + 100]
        try:
            accounting = subprocess.run(
                ["sacct", "--jobs", ",".join(batch), "--noheader", "--parsable2",
                 "--units=K", "--format=JobIDRaw,State,ElapsedRaw,AllocCPUS,TotalCPU,MaxRSS"],
                capture_output=True, text=True, check=True, timeout=10,
            )
        except (OSError, subprocess.SubprocessError) as error:
            logger.warning("Slurm profiling unavailable: %s", error)
            break
        for line in accounting.stdout.splitlines():
            columns = [value.strip() for value in line.split("|")]
            if len(columns) != 6:
                continue
            row_id, state, elapsed, cpus, total_cpu, max_rss = columns
            job_id, _, step = row_id.partition(".")
            if job_id not in batch or step == "extern":
                continue
            profile = profiles[job_id]
            cpu = _cpu_seconds(total_cpu)
            rss = _rss_bytes(max_rss)
            if rss is not None:
                profile["peak_rss_bytes"] = max(rss, profile.get("peak_rss_bytes", 0))
            if step:
                if cpu is not None:
                    step_cpu[job_id][step] = cpu
                continue
            if cpu is not None:
                profile["cpu_seconds"] = cpu
            if elapsed.isdecimal():
                profile["elapsed_seconds"] = int(elapsed)
            if cpus.isdecimal():
                profile["allocated_cpus"] = int(cpus)
            if state:
                profile["state"] = state
    for job_id, profile in profiles.items():
        # Slurm may record CPU time only on the job steps.
        if not profile.get("cpu_seconds") and step_cpu[job_id]:
            profile["cpu_seconds"] = sum(step_cpu[job_id].values())
    return list(profiles.values())


def get_toil_job_profiles(*logs):
    """Find Slurm job IDs in the logs from a single Toil invocation."""
    pattern = re.compile(r"with batch system ID \d+ queued as job (\d+)\b")
    job_ids = []
    for stream in logs:
        try:
            stream.seek(0)
            job_ids.extend(match.group(1) for line in stream for match in pattern.finditer(line))
        except (OSError, UnicodeError) as error:
            logger.warning("Could not read Toil job IDs: %s", error)
    return get_job_profiles(job_ids)
