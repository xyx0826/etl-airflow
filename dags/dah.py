from airflow import DAG
import datetime as dt
import os, re, yaml
from common import sources
from common import destinations
from common import spatial

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

with DAG('dah', 
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    # wait for all extracts
    opr_wait = BashOperator(
        task_id='wait',
        bash_command="echo 'wait for all extracts to finish'"
    )

    # run SQL transform
    opr_transform = BashOperator(
        task_id='transform',
        bash_command=f"psql -d etl -f {os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/blight_violations.sql"
    )

    opr_geocode = PythonOperator(
        task_id='geocode',
        python_callable=spatial.geocode_rows,
        op_kwargs = {
            "table": f"{dag.dag_id}.bvn",
            "address_column": "violation_address",
            "geometry_column": "geom",
            "parcel_column": "parcelno",
            "where": "parcelno is null",
            "geocoder": 'address'
        }
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

    # loop through open datasets to load
    open_datasets = [f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}") if f.endswith('.yml') and not f.startswith('_')]

    for od in open_datasets:
        od_name = od.split('.')[0]
        od_config = yaml.load(open(f"{os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/{od}"))

        # loop through the views
        for d, v in od_config['views'].items():

            v['name'] = f"{od_name}_{d}"
            v['dag'] = dag.dag_id

            # make view after geocoding & set downstream of dump_file
            opr_make_view = PostgresOperator(
                task_id=f"make_view_{v['name']}",
                sql=["set search_path to accela", f"create or replace view {v['dag'] + '.' + v['name']} as ({v['select']})"],
                postgres_conn_id='etl_postgres'
            )

            opr_transform.set_downstream(opr_geocode)
            opr_geocode.set_downstream(opr_make_view)

            # get appropriate Operator & set downstream of make_view
            opr_dump_file = destinations.pg_to_file(v)
            
            opr_make_view.set_downstream(opr_dump_file)

            if v['export'] == 'shapefile':
                filepath = f"/tmp/{v['name']}.zip"
            elif v['export'] == 'geojson':
                filepath = f"/tmp/{v['name']}.json"

            if v['id'] and len(v['id']) > 0:
                # upload to AGO and set downstream of dump_file
                opr_upload = PythonOperator(
                    task_id=f"upload_{v['name']}",
                    python_callable=destinations.upload_to_ago,
                    op_kwargs={
                        "id": v['id'],
                        "filepath": filepath
                    }
                )
            
                opr_dump_file.set_downstream(opr_upload)

opr_wait >> opr_transform