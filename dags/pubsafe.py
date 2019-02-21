from airflow import DAG

import datetime as dt
import os
import re
import yaml

from common import sources
from common import destinations

from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('pubsafe', 
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    sources_to_extract = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/_sources.yml"))

    opr_pause = BashOperator(
        task_id='pause',
        bash_command="echo 'Paused for extraction.'"
    )

    for t, s in sources_to_extract.items():

        s['name'] = t
        s['dag'] = dag.dag_id

        opr_extract = PythonOperator(
            task_id=f"extract_{t}",
            python_callable=sources.extract_source,
            provide_context=True,
            op_kwargs=s
        )

        opr_extract.set_downstream(opr_pause)




