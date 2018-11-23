# etl-airflow

We're currently migrating from a cron-based system to Airflow. This repository stores:

- our Airflow configuration in `airflow.cfg`
- our DAGs in `/dags`
- eventually, custom plugins that contain Hooks and Operators

## airflow.cfg

Changes we've made to the default configuration:

- using a Postgres database for Airflow's metadata instead of the SQLite default
- using `LocalExecutor` instead of the default `SequentialExecutor` for local parallelization



