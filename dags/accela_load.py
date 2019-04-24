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

# Base filenames to Postgres table names
# Hardcode base filenames we expect to find on ftp
tables = {
    'B1PERMIT': 'b1permit' ,
    'B1_EXPIRATION': 'b1expiration',
    'B3ADDRES': 'b3addres',
    'B3CONTACT': 'b3contact',
    'B3CONTRA': 'b3contra',
    'B3OWNERS': 'b3owners',
    'B3PARCEL': 'b3parcel',
    'BCHCKBOX': 'bchckbox',
    'BPERMIT_DETAIL': 'bpermitdetail',
    'BWORKDES': 'bworkdes',
    'F4FEEITEM': 'f4feeitem',
    'G6ACTION': 'g6action',
    'GPROCESS': 'gprocess'
}

with DAG('accela_extract',
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    files = os.listdir("/tmp/accela_extract")

    for t in tables.keys():

        txtfile = [f for f in files if t in f]

        if len(txtfile) == 0:
            break

        opr_truncate_table = PostgresOperator(
            task_id=f"truncate_{tables[t]}_update",
            sql=f"truncate table accela.{tables[t]}_update",
            postgres_conn_id='etl_postgres'
        )

        opr_table_pause = BashOperator(
            task_id=f"pause_{tables[t]}",
            bash_command="echo 'Fake'"
        )

        for f in txtfile:
            opr_copy_table = PostgresOperator(
                task_id=f"copy_{tables[t]}_{f.split('_')[0]}",
                sql=[
                    f"""COPY accela.{tables[t]}_update FROM '/tmp/accela_extract/{f}' (FORMAT CSV, HEADER TRUE, QUOTE '"', DELIMITER '|', ENCODING 'latin-1')"""
                ],
                postgres_conn_id='etl_postgres'
            )

            opr_truncate_table.set_downstream(opr_copy_table)
            opr_copy_table.set_downstream(opr_table_pause)

        opr_insert_update = PostgresOperator(
            task_id=f"insert_{tables[t]}_{f.split('_')[0]}",
            sql=f"insert into accela.{tables[t]} select * from accela.{tables[t]}_update",
            postgres_conn_id='etl_postgres'
        )

        opr_table_pause.set_downstream(opr_insert_update)
































    # # Create Postgres tables
    # for t in tables:
    #     opr_make_pgtables = PythonOperator(
    #         task_id='csv_to_pg_'+t.lower().replace('_',''),
    #         provide_context=True,
    #         python_callable=csv_to_pg,
    #         op_kwargs={
    #             "name": t,
    #         }
    #     )

    #     opr_make_pgtables.set_upstream(opr_retrieve_files)
    #     opr_pause.set_upstream(opr_make_pgtables)
    

    # open_datasets = [ f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}") if not f.startswith('_')]

    # for od in open_datasets:
    #     od_name = od.split('.')[0]
    #     od_config = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/{od}"))

    #     # loop through the views
    #     for d, v in od_config['views'].items():

    #         v['name'] = f"{od_name}_{d}"
    #         v['dag'] = dag.dag_id

    #         # Make view & set downstream of opr_pause
    #         opr_make_view = PostgresOperator(
    #             task_id=f"make_view_{v['name']}",
    #             sql=["set search_path to accela", f"create or replace view {v['dag'] + '.' + v['name']} as ({v['select']})"],
    #             postgres_conn_id='etl_postgres'
    #         )
    #         opr_pause.set_downstream(opr_make_view)

    #         opr_csv_dump = PostgresOperator(
    #             task_id=f"dump_to_csv_{v['name']}",
    #             sql=f"COPY (select * from {v['dag'] + '.' + v['name']}) TO '/tmp/{v['name']}.csv' WITH (FORMAT CSV, HEADER);",
    #             postgres_conn_id='etl_postgres'
    #         )

    #         opr_make_view.set_downstream(opr_csv_dump)

    # TBD Insert daily updates into master tables

    # TBD Create open data views

    # TBD Publish open data views