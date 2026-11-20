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

The observation columns retain the names used on main, including `target_name`, `sas_id_target`, `sas_id_calibrator1`, `sas_id_calibrator2`, `sas_id_calibrator_final` and `status`.

Create a SQLite database with `flocs-processing create-database`. Use `--pipelines` to select `vlbi-widefield` (the default) or `vlbi-single-target`. Add observations with `flocs-processing add-field`. `freq_min_mhz` and `freq_max_mhz` set the target download range (120–168 MHz by default). For one-MS-per-subband inputs, `number_subbands` sets the expected input count.

Each observation has a `status` and a `status_*` column for each stage. The stage values are 0 (not started), 1 (downloaded), 2 (finished), 3 (awaiting approval), 98 (processing) and 99 (failed). Use `update-field-status` for the observation and `update-pipeline-status` for a stage. For example, after reviewing a delay solution, use the following command to approve it.

```sh
flocs-processing update-pipeline-status --dbname ILoTSS --field-name Example --sas-id-target 12345 --pipeline vlbi_delay --set-status success
```

The five `status_vlbi_facet_imaging_*` columns track each imaging resolution. The shared `status_vlbi_facet_imaging` reads as finished when all five are finished.

The selected delay calibrator has final `delay_calibrator_phase_score` and `delay_calibrator_amp_score` values. For DD calibration, `n_dd_calibrators` counts the directions in the final merged solution. The `dd_calibrator_phase_score_*` and `dd_calibrator_amp_score_*` columns give the minimum, maximum and median of the available final scores for those directions. Missing scores are NULL. The scores describe the last successfully read calibration products. Use the stage status to follow a retry.

To upgrade an existing database, stop writers using the old code, then run `flocs-processing migrate-database --dbname ILoTSS`. For an earlier version of this branch, keep `ILoTSS.steps/` beside the database during migration. The command imports those records once. It also renames the columns from earlier versions of this branch while preserving the stored values. The updated DAG reads and writes stage status in SQLite. Historical scores start as NULL and are populated when the relevant products are processed.

When migrating an upstream database, the command keeps the original table with a `_legacy` suffix. For observations without a unique pipeline route, specify `--legacy-pipeline vlbi-widefield` or `--legacy-pipeline vlbi-single-target`. Check any previously started facet imaging against its Airflow tasks or outputs before resuming those observations. The old shared flag does not identify which resolutions finished.

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

