# End-to-end processing of ILT HBA data with flocs

This package aims to provide relatively simple end-to-end automatic processing of ILT HBA data. Where `flocs-runners` provides the interface to running pipelines, `flocs-processing` is the scaffolding to tie it together. Data reduction is coordinated via a dedicated SQLite database that holds information on which observations to process, which pipelines to run for them and all of the related statuses. Orchestration of all the pipelines is handled via Airflow through a DAG.

The autoPILOT package (https://github.com/LOFAR-VLBI/autoPILOT) needs to be on installed to enable the automatic calibrator assessment.

## Folder setup
Flocs-processing requires three folders to be setup:

* A processing folder -- this is where data is stored while processing
* A data folder -- this is where the input data is found
* An output folder -- this is where finished pipeline outputs are copied to, and searched for in steps that depend on it.

The expected naming directory structure for input data is `<data folder>/<field name>/{calibrator,target}`. Inside the calibrator and target folders, the observations should follow the usual `LXXXXXX` naming scheme. These **must** match the SAS IDs in the database for flocs to be able to find them.

## Database setup
A database for processing is created via `flocs-processing create-database`. This will create an empty database with the observation metadata, processing statuses and calibrator scores. The `--intent` option selects `PILOT-widefield` (the default) or `PILOT-single-target`. The `pipelines` column lists the component pipelines: `linc,ddf-pipeline,vlbi-delay-widefield` for widefield processing, or `linc,vlbi-delay-single-target` for single-target processing. These can be set with `--pipelines`. Datasets to process can be added via `flocs-processing add-field` or existing fields manipulated via `flocs-processing update-field-status` or `flocs-processing update-pipeline-status`.

By default, `add-field` reads observation metadata from the LTA, including the pointing coordinates in `pointing_ra_deg` and `pointing_dec_deg`, and station counts in `nr_core`, `nr_remote` and `nr_international`. The `--freq-min-mhz` and `--freq-max-mhz` options set the target frequency range, and `--number-subbands` selects that many subbands starting at the lowest frequency in the range. The selected count and frequency limits are stored for the DAG to use. Use `--no-fetch-metadata` to register local data with explicit selection values. Fields inherit the database defaults for intent and component pipelines unless these are set with `add-field`.

Each step records its status in a JSON file under `<dbname>.steps/<table>/<target SAS ID>/`, for example `vlbi_delay.json`. A missing file means the step has no recorded status. Keep the `<dbname>.steps` directory with the database when copying it. Use `update-pipeline-status` to change or clear a step status.

## Processing data
### Automatic setup via flocs
To start processing data, Airflow needs to be running. This can be setup automatically via `flocs-processing deploy-airflow`. See its help for options.

All that is left is to put the relevant dags in `<your airflow folder>/dags`. As a user you can trigger the DAGs manually, or use the dispatcher DAGs which will start processing automatically. On the "Dags" tab of the web UI you should now see the DAGs available if everything went well. To manually trigger one, click on the name and on the subsequent page use the "Trigger" button in the top right. This is not needed for the dispatchers.

### Manual setup
For manual setup of the Airflow part do the following:

1. Install airflow: `uv pip install apache-airflow`
2. Set up a folder that wil contain all of Airflow's own stuff and assign it to the `AIRFLOW_HOME` environment variable.
3. Run `airflow config list --defaults > "${AIRFLOW_HOME}/airflow.cfg"`
4. Define `AIRFLOW__CORE__DAGS_FOLDER` as `${AIRFLOW_HOME}/dags` and create the folder. Copy the DAGs inside `flocs_processing/dags` to this folder.
5. Define `AIRFLOW__CORE__LOAD_EXAMPLES` as `False`

Finally, define the following airflow variables:

```
export AIRFLOW_HOME=/path/to/some/folder/for/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_HOME/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=$AIRFLOW_HOME/logs/dag_processor
export AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW_HOME/plugins
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$AIRFLOW_HOME/logs
```

For a small test, you can run `airflow standalone` to start the Airflow instance for a small test.  For proper deployment, it is recommended by the Airflow docs to not use `standalone`. First we'll set up a persistent JWT secret for authentication purpose.

```
mkdir -p "$HOME/.config/airflow"
chmod 700 "$HOME/.config/airflow"
openssl rand -hex 32 > "$HOME/.config/airflow/jwt_secret"
chmod 600 "$HOME/.config/airflow/jwt_secret"
export AIRFLOW__API_AUTH__JWT_SECRET="$(cat "$HOME/.config/airflow/jwt_secret")"
```

Next, initialise Airflow's own database with

```
airflow db migrate
```

Finally, to start the necessary Airflow services, execute them like follows:

```
tmux new-session -d -s airflow-api-server "bash -c 'source $HOME/source_airflow.sh && airflow api-server; exec bash'"
tmux new-session -d -s airflow-triggerer "bash -c 'source $HOME/source_airflow.sh && airflow triggerer; exec bash'"
tmux new-session -d -s airflow-dag-processor "bash -c 'source $HOME/source_airflow.sh && airflow dag-processor; exec bash'"
tmux new-session -d -s airflow-scheduler "bash -c 'source $HOME/source_airflow.sh && airflow scheduler; exec bash'"
```

This should start four tmux sessions with these services running in the background. The credentials to log into e.g. the web interface will be stored in `${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated`. The Airflow instance will start on port 8080. You can access it via `localhost:8080` in your browser. If it is running on a remote cluster, you can set up a tunnel via e.g. `ssh -N -L 8080:localhost:8080 <remote>` to forward it to your local machine.

