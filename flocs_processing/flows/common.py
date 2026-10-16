"""Shared helpers for Prefect flow orchestration."""

from flocs_processing.config import load_config
from flocs_processing.db_utils import FlocsDB
from prefect.deployments import run_deployment


def get_current_db() -> FlocsDB:
    config = load_config()["DEFAULT"]
    return FlocsDB(config["DATABASE"], config["TABLE_NAME"])


def launch_deployment(deployment_name: str, run_name: str, parameters: dict):
    flow_run = run_deployment(
        name=deployment_name,
        flow_run_name=run_name,
        parameters=parameters,
        timeout=None,
        poll_interval=60,
    )
    if flow_run.state is None or not flow_run.state.is_completed():
        state_name = flow_run.state.name if flow_run.state else "unknown"
        raise RuntimeError(
            f"Deployment {deployment_name} run {run_name} finished in state {state_name}."
        )
    return flow_run
