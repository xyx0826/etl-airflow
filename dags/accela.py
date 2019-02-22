from airflow import DAG

import datetime as dt
import os
import re
import yaml
from sqlalchemy import create_engine
import pandas as pd

from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 2, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

# Hardcode base filenames we expect to find on ftp
tables = [
    'B1PERMIT',
    'B1_EXPIRATION',
    'B3ADDRES',
    'B3CONTACT',
    'B3CONTRA',
    'B3OWNERS',
    'B3PARCEL',
    'BCHCKBOX',
    'BPERMIT_DETAIL',
    'BWORKDES',
    'F4FEEITEM',
    'G6ACTION',
    'GPROCESS'
]

# Connect to FTP directory, get recent files and copy to local path
def get_ftp_files(**kwargs):
    conn = FTPHook('accela_ftp')
    d = conn.describe_directory('/opentext/TEST')

    for t in tables:
        r = get_recent_file(t, d)
        conn.retrieve_file(r['name'], f"/tmp/{t}.csv")

# Get most recently modified file for each expected filename
# First make a list of dicts from 'directory', then sort
def get_recent_file(name, directory):
    files = [{**directory[k], **{'name': k}} for k in directory.keys() if name in k]
    sorted_files = sorted(files, key=lambda k: int(k['modify'].split('.')[0]), reverse=True)
    return sorted_files[0]

# Make a Postgres table from a CSV using Pandas
def csv_to_pg(**kwargs):
    conn = PostgresHook.get_connection('etl_postgres')
    engine = create_engine(f"postgres://{conn.host}/{conn.schema}")
    df = pd.read_csv(f"/tmp/{kwargs['name']}.csv", delimiter='|', encoding='latin1', low_memory=False)
    df.to_sql(f"{kwargs['name'].lower().replace('_', '')}_update", con=engine, schema='accela', if_exists='replace')

with DAG('accela',
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    # Retrieve files from FTP
    opr_retrieve_files = PythonOperator(
        task_id='get_ftp_files',
        provide_context=True,
        python_callable=get_ftp_files,
    )

    opr_pause = BashOperator(
        task_id='post_extract_pause',
        bash_command="echo 'Stay cool.'"
    )

    # Create Postgres tables
    for t in tables:
        opr_make_pgtables = PythonOperator(
            task_id='csv_to_pg_'+t.lower().replace('_',''),
            provide_context=True,
            python_callable=csv_to_pg,
            op_kwargs={
                "name": t,
            }
        )

        opr_make_pgtables.set_upstream(opr_retrieve_files)
        opr_pause.set_upstream(opr_make_pgtables)
    

    open_datasets = [ f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}") if not f.startswith('_')]

    for od in open_datasets:
        od_name = od.split('.')[0]
        od_config = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/{od}"))

        # loop through the views
        for d, v in od_config['views'].items():

            v['name'] = f"{od_name}_{d}"
            v['dag'] = dag.dag_id

            # Make view & set downstream of opr_pause
            opr_make_view = PostgresOperator(
                task_id=f"make_view_{v['name']}",
                sql=f"create or replace view {v['dag'] + '.' + v['name']} as ({v['select']})",
                postgres_conn_id='etl_postgres'
            )
            opr_pause.set_downstream(opr_make_view)

            opr_csv_dump = PostgresOperator(
                task_id=f"dump_to_csv_{v['name']}",
                sql=f"COPY {v['dag'] + '.' + v['name']} TO '/tmp/{v['name']}.csv' WITH (FORMAT CSV, HEADER);",
                postgres_conn_id='etl_postgres'
            )

            opr_make_view.set_downstream(opr_csv_dump)

    # TBD Insert daily updates into master tables

    # TBD Create open data views

    # TBD Publish open data views