# etl-airflow

We're currently migrating from a cron-based system to [Airflow](https://airflow.apache.org/). This repository stores:

- our Airflow configuration in `airflow.cfg`
- our DAGs in `/dags`
- eventually, custom plugins that contain Hooks and Operators

## Setup

### Installation

On Linux or macOS, you can follow the [quick start](https://airflow.apache.org/start.html), then set these changes to the default configuration in `airflow.cfg`:

- using a Postgres database for Airflow's metadata instead of the SQLite default: `postgres+psycopg2://<connection string>`
- using `LocalExecutor` instead of the default `SequentialExecutor` for local parallelization with 4 workers
- set `catchup_by_default` to `False`; by default, new DAGs 'backfill' in time; disables this
- set `load_examples` to `False`; don't load the example DAGs

Airflow doesn't work on Windows, although you can run it on the Ubuntu subsystem with Windows 10. If you want to be able to edit your files from Windows, you need to set `AIRFLOW_HOME` as a folder in your Windows file directory: `/mnt/c/Users..`

### Running Airflow

In order for airflow to run, the `scheduler` needs to be active. In our current production environment, ee run this in a `screen`.

The `scheduler` is important because it determines the current state of every DAG and DAG run every second (the "heartbeat").

It's also helpful to have the `webserver` running to inspect DAGs. In production, this runs on `10.208.132.147:8080`.

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

```yaml
<table name in the ETL database>:
  connection: <airflow Connection name>
  source_name: <source table name>
  fields: 
    - <array of fields>
  where: <a where clause>
  method: <append or replace, which is the default>

# an example, running on a DAG named 'law'
# /dags/common/source.py will interpret this object

# this source table will become law.foia_requests
foia_requests:
  # there should be an existing Airflow connection named this
  connection: law_smartsheet
  # this is the Smartsheet's ID, identifying a unique table
  source_name: 5783942113585028
  # get four fields from the table
  fields: 
    - Requestor
    - Request
    - Granted
    - Response
    - Request_Date
  # a PostgreSQL where clause
  where: '"Request_Date" > now() - interval '1 week''
  # only include this key/value pair if you want to append; default is to replace
  method: append
```

### More reading 

- [DAG Best Practices](https://www.astronomer.io/guides/dag-best-practices/)
- [Awesome Apache Airflow](https://github.com/jghoman/awesome-apache-airflow)