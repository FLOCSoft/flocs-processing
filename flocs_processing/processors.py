"""Helpers for registering Prefect deployments."""

import os
import shutil
import subprocess

from flocs_processing.config import load_config


def deploy_prefect(deployment: str | None = None):
    """Register deployments defined in prefect.yaml with the configured server."""
    if not os.getenv("PREFECT_API_URL"):
        raise RuntimeError(
            "PREFECT_API_URL must point to the Prefect API before deployments can be registered."
        )
    if shutil.which("prefect") is None:
        raise RuntimeError("The Prefect CLI is not installed in this environment.")

    # Fail early with a useful error if the flow environment is not configured.
    load_config()

    command = ["prefect", "deploy"]
    if deployment:
        command.extend(["--name", deployment])
    else:
        command.append("--all")
    subprocess.run(command, check=True)
