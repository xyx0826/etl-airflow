# etl-airflow

We're currently migrating from a cron-based system to Airflow. This repository stores:

- our Airflow configuration in `airflow.cfg`
- our DAGs in `/dags`
- eventually, custom plugins that contain Hooks and Operators

## airflow.cfg

Changes we've made to the default configuration:

- using a Postgres database for Airflow's metadata instead of the SQLite default
- using `LocalExecutor` instead of the default `SequentialExecutor` for local parallelization

## Setup

In order for airflow to run, the `scheduler` needs to be active. We run this in a `screen`.

The `scheduler` is important because it determines the current state of every DAG and DAG run every second (the "heartbeat").

It's also helpful to have the `webserver` running to inspect DAGs. This runs on `10.208.132.147:8080`.

You can forward this port to `localhost:8889` with `ssh -L 8889:localhost:8080 gisteam@10.208.132.147`.

## Structure

Given a discrete etl process named `example` which outputs datasets named `lorem` and `ipsum` we're structuring DAGs like so:

in the `dags/` folder: `example.py`

in the `example/` folder:
- `_sources.yml`
- `lorem.yml`
- `ipsum.yml`

More explanation of this to come.

### _sources.yml

This should be a YAML object:

```yml
<table name in the ETL database>:
  connection: <airflow Connection name>
  source_name: <source table name>
  fields: 
    - a
    - list
    - of
    - source column names
    - omitting this means we will "select *"
  where: <a WHERE clause if you want to restrict what we get>
<table>:
  ...
```

### More reading 

- [DAG Best Practices](https://www.astronomer.io/guides/dag-best-practices/)
- [Awesome Apache Airflow](https://github.com/jghoman/awesome-apache-airflow)