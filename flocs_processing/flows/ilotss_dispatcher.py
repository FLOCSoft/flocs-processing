"""Select and launch the next widefield observation."""

from datetime import datetime
import os

from prefect import flow, task

from flocs_processing.db_utils import FIELD_STATUS, PIPELINE_STATUS
from flocs_processing.flows.common import get_current_db, launch_deployment


ILOTSS_DEPLOYMENT = os.getenv("ILOTSS_DEPLOYMENT", "ilotss/ilotss")


@flow
def ilotss_dispatcher():
    @task
    def get_unprocessed_field():
        for dbrow in get_current_db().get_db_columns(intent="widefield"):
            field = dict(dbrow)
            if field["status"] == FIELD_STATUS.downloading.value:
                continue
            if any(
                value == PIPELINE_STATUS.processing.value
                for key, value in field.items()
                if key.startswith("status_")
            ):
                continue
            return field
        return None

    @task
    def launch_field(field):
        run_name = (
            f"ilotss__{field['target_name'].replace(' ', '_')}"
            f"__{field['sas_id_target']}__{datetime.now().isoformat()}"
        )
        return launch_deployment(
            ILOTSS_DEPLOYMENT,
            run_name,
            {
                "target_name": field["target_name"],
                "sas_id_target": field["sas_id_target"],
            },
        )

    field = get_unprocessed_field()
    if field is None:
        print("No unprocessed widefield observations found.")
        return None
    return launch_field(field)


if __name__ == "__main__":
    ilotss_dispatcher()
