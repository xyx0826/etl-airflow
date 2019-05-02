from airflow import DAG
import datetime as dt
import os, re, yaml
from common import sources
from common import destinations

from airflow.models import Variable

from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 16, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('dlba',
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    # wait for all extracts to complete
    opr_pause = BashOperator(
        task_id='pause',
        bash_command="echo 'Deep breath.'"
    )

    # extracts
    sources_to_extract = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/_sources.yml"))

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

    # transforms & loads
    open_datasets = [f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}") if not f.startswith('_')]

    for od in open_datasets:
        od_name = od.split('.')[0]
        od_config = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/{od}"))

        # loop through the views
        for d, v in od_config['views'].items():
            v['name'] = f"{od_name}_{d}"
            v['dag'] = dag.dag_id

            # make views & set downstream of opr_pause
            opr_make_view = PostgresOperator(
                task_id=f"make_view_{v['name']}",
                sql=[f"drop view if exists {v['dag']}.{v['name']} cascade", f"create or replace view {v['dag'] + '.' + v['name']} as ({v['select']})"],
                postgres_conn_id='etl_postgres'
            )
            opr_pause.set_downstream(opr_make_view)

            # load by destination
            if 'id' in v.keys() and v['id'] != None and v['destination'] == "socrata":
                opr_socrata_upload = PythonOperator(
                    task_id=f"upload_{v['name']}",
                    python_callable=destinations.upload_to_socrata,
                    provide_context=True,
                    op_kwargs={
                        "id": v['id'],
                        "method": v['method'],
                        "table": v['dag'] + '.' + v['name']
                    }
                )
                opr_make_view.set_downstream(opr_socrata_upload)

            elif v['destination'] == 'ago':
                opr_dump_file = destinations.pg_to_file(v)
                opr_make_view.set_downstream(opr_dump_file)

                if v['export'] == 'shapefile':
                    filepath = f"/tmp/{v['name']}.zip"
                elif v['export'] == 'geojson':
                    filepath = f"/tmp/{v['name']}.json"

                opr_ago_upload = PythonOperator(
                    task_id=f"upload_{v['name']}",
                    python_callable=destinations.upload_to_ago,
                    op_kwargs={
                        "id": v['id'],
                        "filepath": filepath
                    }
                )
                opr_dump_file.set_downstream(opr_ago_upload)

            elif v['destination'] == 'graphql':
                pass
