import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Optional

import libtmux
import structlog

logger = structlog.getLogger()


class FlocsAirflowProcessor:
    def __init__(self, airflow_home: Optional[str], airflow_cores: int):
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PARALLELISM"] = str(airflow_cores)
        self.REQUIRED_AIRFLOW_VARS = [
            "AIRFLOW_HOME",
            "AIRFLOW__CORE__DAGS_FOLDER",
            "AIRFLOW__CORE__LOAD_EXAMPLES",
            "AIRFLOW__CORE__PARALLELISM",
            "AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY",
            "AIRFLOW__CORE__PLUGINS_FOLDER",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "AIRFLOW__LOGGING__BASE_LOG_FOLDER",
        ]
        if airflow_home:
            os.environ["AIRFLOW_HOME"] = airflow_home
            os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(
                airflow_home, "dags"
            )
            os.environ[
                "AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY"
            ] = os.path.join(airflow_home, "logs/dag_processor")
            os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = os.path.join(
                airflow_home, "plugins"
            )
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = (
                "sqlite:///$AIRFLOW_HOME/airflow.db"
            )
            os.environ["AIRFLOW__LOGGING__BASE_LOG_FOLDER"] = os.path.join(
                airflow_home, "logs"
            )

    def check_airflow_init(self):
        """Check if the Airflow instance is initialised already. If not, runs `airflow db migrate` to intitialise it."""
        af_home = os.environ["AIRFLOW_HOME"]
        if not os.path.isdir(af_home):
            logger.info(f"Airflow home {af_home} does not exist, creating it")
            os.makedirs(af_home)
        if not os.listdir(af_home):
            logger.info(
                f"Airflow home {af_home} is empty. Running `airflow db migrate` to initialise."
            )
            old_dir = os.getcwd()
            os.chdir(af_home)
            _ = subprocess.check_output(
                "airflow db migrate",
                text=True,
                shell=True,
            )
            os.chdir(old_dir)
        else:
            logger.info(
                f"Airflow home {af_home} is populated. Leaving it alone and assuming it is operational."
            )

    def check_environment(self):
        environment_ok = True
        if "FLOCS_AIRFLOW_CONFIG" not in os.environ:
            logger.warning(
                "FLOCS_AIRFLOW_CONFIG environment variable not set. Please point this to a valid configuration file."
            )
            environment_ok = False
        else:
            CONFIG_FILE = os.getenv("FLOCS_AIRFLOW_CONFIG") or ""
            if not os.path.isfile(CONFIG_FILE):
                logger.warning(f"{CONFIG_FILE} is not a valid file")
                environment_ok = False
            else:
                logger.info(f"Found config file {os.getenv('FLOCS_AIRFLOW_CONFIG')}.")

        if "AIRFLOW__API_AUTH__JWT_SECRET" not in os.environ:
            logger.critical("AIRFLOW__API_AUTH__JWT_SECRET not defined.")
            environment_ok = False

        for kw in self.REQUIRED_AIRFLOW_VARS:
            if kw not in os.environ:
                logger.warning(f"Required variable {kw} is not set.")
                environment_ok = False

        if environment_ok:
            logger.info("Flocs automated processing environment appears ok")
        else:
            logger.critical(
                "Flocs automated processing environment is not properly set up. Please see warnings."
            )
            sys.exit(1)

    def check_airflow_services(self):
        status = {
            "api-server": False,
            "dag-processor": False,
            "scheduler": False,
            "triggerer": False,
        }

        tmux_server = libtmux.Server()
        airflow_sessions = map(
            lambda x: x.name,
            filter(lambda x: "airflow" in x.name, tmux_server.sessions),
        )
        logger.info("Checking Airflow status:")
        for service in status.keys():
            if f"airflow-{service}" not in airflow_sessions:
                logger.info(f"-- {service}: not running")
                status[service] = False
            else:
                logger.info(f"-- {service}: running")
                status[service] = True
        return status

    def deploy_airflow_tmux(self):
        af_status = self.check_airflow_services()

        if any(not up for up in af_status.values()):
            source_file = os.path.abspath("source_flocs_airflow.sh")
            logger.info("Not all Airflow services are online.")
            tmux_server = libtmux.Server()
            for service, running in af_status.items():
                if not running:
                    logger.info(f"-- deploying {service}")
                    tmux_server.new_session(
                        session_name=f"airflow-{service}",
                        attach=False,
                        window_command=(
                            f"bash -c 'source {source_file} && airflow {service}'; bash -i"
                        ),
                    )

    def generate_jwt_secret(self):
        if os.path.isfile(os.path.expandvars("$HOME/.config/airflow/jwt_secret")):
            logger.info("JWT secret found, not (re)generating.")
        else:
            logger.info("No JWT secret found, generating one.")
            _ = subprocess.check_output(
                "mkdir -p $HOME/.config/airflow",
                text=True,
                shell=True,
            )

            _ = subprocess.check_output(
                "chmod 700 $HOME/.config/airflow",
                text=True,
                shell=True,
            )

            _ = subprocess.check_output(
                "openssl rand -hex 32 > $HOME/.config/airflow/jwt_secret",
                text=True,
                shell=True,
            )

            _ = subprocess.check_output(
                "chmod 600 $HOME/.config/airflow/jwt_secret",
                text=True,
                shell=True,
            )
            logger.info("JWT secret generated")
        secret = Path.home().joinpath(".config/airflow/jwt_secret").read_text().strip()
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = secret

    def write_source_file(self):
        if os.path.exists("source_flocs_airflow.sh"):
            logger.info("Found source_flocs_airflow.sh, not overwriting.")
        else:
            logger.info("Writing environment settings to source_flocs_airflow.sh")
            with open("source_flocs_airflow.sh", "w") as f:
                for kw in self.REQUIRED_AIRFLOW_VARS:
                    f.write(f"export {kw}={os.environ[kw]}\n")

                secret = (
                    Path.home()
                    .joinpath(".config/airflow/jwt_secret")
                    .read_text()
                    .strip()
                )
                f.write(f"export AIRFLOW__API_AUTH__JWT_SECRET={shlex.quote(secret)}\n")
            os.chmod("source_flocs_airflow.sh", 0o600)
