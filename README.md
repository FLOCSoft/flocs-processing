# End-to-end processing of ILT HBA data with flocs

`flocs-processing` coordinates LOFAR processing pipelines using a SQLite database and Prefect flows. The `flocs-runners` package provides the pipeline execution interface.

The autoPILOT package (https://github.com/LOFAR-VLBI/autoPILOT) is required for automatic calibrator assessment.

## Folder setup

Flocs-processing uses three folders:

* A processing folder for intermediate data.
* A data folder containing input data.
* An output folder containing pipeline outputs used by later steps.

Input data is expected under `<data folder>/<field name>/{calibrator,target}`. Observation directories use the `LXXXXXX` naming scheme and must match SAS IDs in the database.

## Database setup

Create a database with `flocs-processing create-database`. Add observations with `flocs-processing add-field`; update field and pipeline status with `flocs-processing update-field-status` and `flocs-processing update-pipeline-status`.

## Prefect setup and deployments

Install the package and Prefect in the environment used by the Prefect worker. Configure the processing settings file using `FLOCS_CONFIG`; if unset, `~/.flocs.cfg` is used. The file contains the database path, table name, scheduler settings, and processing/data/output paths.

Point the Prefect CLI at your Prefect server and select a work pool:

```bash
export PREFECT_API_URL=http://<prefect-server>:4200/api
export FLOCS_CONFIG=/path/to/flocs.cfg
prefect work-pool ls
```

Edit `prefect.yaml` to set the work pool for your environment. It defines deployments for the processing flows and the two dispatchers. Dispatchers are scheduled every minute and process one observation at a time. The worker must have access to the project source, configuration file, SQLite database, input/output directories, and pipeline executables. For a process work pool, start a worker with:

```bash
prefect worker start --pool <work-pool-name>
```

Register all deployments from the project root:

```bash
flocs-processing deploy-prefect
```

To register one deployment only, pass its deployment name:

```bash
flocs-processing deploy-prefect --deployment pilot-single-target-dispatcher
```

The dispatchers start and wait for their corresponding processing deployments. Ensure the `pilot_single_target` and `ilotss` deployments are registered before enabling dispatcher schedules. Deployments can also be triggered manually from the Prefect UI or with `prefect deployment run <flow-name>/<deployment-name>`.

## Local Prefect server

For development, start a local server with `prefect server start`. Set `PREFECT_API_URL` to the API URL printed by Prefect, register deployments, and start a worker for the selected pool. Production server and worker setup should follow the Prefect deployment guidance for the chosen infrastructure and work-pool type.
