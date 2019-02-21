from airflow import DAG
import datetime as dt
import os, re, yaml
from common import sources
from common import destinations

from airflow.models import Variable

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 2, 21, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('health', 
    default_args=default_args,
    schedule_interval="0 0 1 * *") as dag:

    # wait for all extracts
    opr_wait = BashOperator(
        task_id='wait',
        bash_command="echo 'wait for all extracts to finish'"
    )

    sources_to_extract = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/_sources.yml"))

    # extract
    for t, s in sources_to_extract.items():

        s['name'] = t
        s['dag'] = dag.dag_id

        opr_extract = PythonOperator(
            task_id=f"extract_{t}",
            python_callable=sources.extract_source,
            provide_context=True,
            op_kwargs=s
        )

        opr_extract.set_downstream(opr_wait)