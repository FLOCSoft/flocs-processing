import configparser
import os

from airflow.api.common.trigger_dag import trigger_dag
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.utils.types import DagRunTriggeredByType

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
def pilot_widefield_dispatcher():
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
        run_id = (
            f"pilot_widefield_{field['target_name'].replace(' ', '_')}"
            f"_{field['sas_id_target']}"
        )
        trigger_dag(
            dag_id="pilot_widefield",
            triggered_by=DagRunTriggeredByType.OPERATOR,
            run_id=run_id,
            conf={
                "target_name": field["target_name"],
                "sas_id_target": field["sas_id_target"],
            },
        )
        return run_id

    proceed = check_fields()
    field = get_unprocessed_field()
    run_id = launch_field(field)

    proceed >> field >> run_id


pilot_widefield_dispatcher()
