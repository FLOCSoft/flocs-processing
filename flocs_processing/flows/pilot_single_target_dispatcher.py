from datetime import datetime
import os

from flocs_processing.db_utils import FIELD_STATUS, PIPELINE_STATUS
from flocs_processing.flows.common import get_current_db, launch_deployment

from prefect import flow, task


PILOT_SINGLE_TARGET_DEPLOYMENT = os.getenv(
    "PILOT_SINGLE_TARGET_DEPLOYMENT",
    "pilot_single_target/pilot-single-target",
)


@flow
def pilot_single_target_dispatcher():
    @task
    def check_fields():
        return bool(get_current_db().get_db_columns(intent="single-target"))

    @task
    def get_unprocessed_field():
        for dbrow in get_current_db().get_db_columns(intent="single-target"):
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
        date = datetime.now().isoformat()
        run_id = (
            f"pilot__single_target__{field['target_name'].replace(' ', '_')}"
            f"__{field['sas_id_target']}"
            f"__{date}"
        )
        return launch_deployment(
            PILOT_SINGLE_TARGET_DEPLOYMENT,
            run_id,
            {
                "target_name": field["target_name"],
                "sas_id_target": field["sas_id_target"],
            },
        )

    if not check_fields():
        return None

    field = get_unprocessed_field()
    if field is None:
        print("No unprocessed fields found.")
        return None

    return launch_field(field)


if __name__ == "__main__":
    pilot_single_target_dispatcher()
