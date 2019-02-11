from airflow import DAG
import datetime as dt
import os, re

from airflow.models import Variable

from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('assessor', 
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    opr_dummy1 = BashOperator(
        task_id='dummy',
        bash_command="echo 'fake'"
    )

    opr_dummy2 = BashOperator(
        task_id='dummy2',
        bash_command="echo 'fake is the new real'"
    )

    opr_dummy1.set_downstream(opr_dummy2)