from datetime import datetime
import configparser
import os

from airflow.providers.common.compat.sdk import DagRunTriggerException
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from flocs_processing.db_utils import FlocsDB
from flocs_processing.flocs_processing import FIELD_STATUS, PIPELINE_STATUS


if "FLOCS_AIRFLOW_CONFIG" not in os.environ:
    raise RuntimeError(
        "FLOCS_AIRFLOW_CONFIG environment variable not set. Please point this to a valid configuration file."
    )

CONFIG_FILE = os.environ["FLOCS_AIRFLOW_CONFIG"]
if not os.path.isfile(CONFIG_FILE):
    raise RuntimeError(f"{CONFIG_FILE} is not a valid file")

parser = configparser.ConfigParser()
parser.optionxform = str
with open(CONFIG_FILE, "r") as config:
    parser.read_string("[DEFAULT]\n" + config.read())

TABLE_NAME = parser["DEFAULT"]["TABLE_NAME"]
DATABASE = parser["DEFAULT"]["DATABASE"]
CURRENT_DB = FlocsDB(DATABASE, TABLE_NAME)


@dag(schedule="@continuous", max_active_runs=1, catchup=False)
def ilotss_dispatcher():
    @task.short_circuit
    def check_fields():
        return bool(CURRENT_DB.get_db_columns())

    @task
    def get_unprocessed_field():
        for dbrow in CURRENT_DB.get_db_columns():
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
        raise AirflowSkipException("No unprocessed fields found.")

    @task
    def launch_field(field):
        date = datetime.now().isoformat()
        run_id = (
            f"ilotss__{field['target_name'].replace(' ', '_')}"
            f"__{field['sas_id_target']}"
            f"__{date}"
        )
        raise DagRunTriggerException(
            trigger_dag_id="ilotss",
            dag_run_id=run_id,
            conf={
                "target_name": field["target_name"],
                "sas_id_target": field["sas_id_target"],
            },
            logical_date=None,
            reset_dag_run=False,
            skip_when_already_exists=False,
            wait_for_completion=True,
            allowed_states=["success"],
            failed_states=["failed"],
            poke_interval=60,
            deferrable=False,
        )

    proceed = check_fields()
    field = get_unprocessed_field()
    run_id = launch_field(field)

    proceed >> field >> run_id


ilotss_dispatcher()
