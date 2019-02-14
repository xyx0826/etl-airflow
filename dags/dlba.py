from airflow import DAG
import datetime as dt
import os, re, yaml

from airflow.models import Variable

from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import sources
from common import destinations

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 16, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('dlba',
  default_args=default_args,
  schedule_interval="0 1 * * *") as dag:

  opr_dummy = BashOperator(
      task_id='dummy',
      bash_command="echo 'fake'"
  )

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

    opr_insert_table = PostgresOperator(
        task_id = f"insert_table_{t}",
        sql=f"COPY {dag.dag_id}.{t} from '/tmp/{t}.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')",
        postgres_conn_id='etl_postgres'
    )

    opr_dummy.set_downstream(opr_extract)
    opr_extract.set_downstream(opr_insert_table)

#     opr_download_table = PythonOperator(
#       task_id=f"get_table_{t}",
#       python_callable=scrape_table,
#       provide_context=True,
#       op_kwargs = {
#         "table": t
#       }
#     )

#     opr_truncate_table = PostgresOperator(
#       task_id = f"truncate_table_{t}",
#       sql=f"truncate table {dag.dag_id}.{t}",
#       postgres_conn_id='etl_postgres'
#     )

#     opr_download_table.set_downstream(opr_truncate_table)  
#     opr_truncate_table.set_downstream(opr_insert_table)
#     opr_insert_table.set_downstream(opr_dummy)

#   # Transform & Load

#   homedir = "/home/gisteam/airflow/sql/"
#   sqls = [f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/sql/{dag.dag_id}") if f.endswith('.sql')]

#   open_datasets = {
#       "side_lots_sold": [
#           {"arcgis_online": "1528d20710624632bf155fd67b57b963"}
#       ],
#       "contracted_demos": [
#           {"arcgis_online": "e506c103f3a045a1aa53f7cd8e70dc1d"}
#       ],
#       "demo_pipeline": [
#           {"arcgis_online": "0d81898958304265ac45d2f59a7339f5"}
#       ],
#       "for_sale": [
#           {"arcgis_online": "dfb563f061b74f60b799c5eeae617fc8"}
#       ],
#       "own_it_now_sold": [
#           {"arcgis_online": "cc9cb6e697844796bda2fa74fb7614d9"}
#       ],
#       "all_ownership": [
#           {"arcgis_online": "04ba7b817d1d45ba89aab539af7ec438"}
#       ],
#       "auction_sold": [
#           {"arcgis_online": "183f901e76a1439ba6c5e04510d275d3"}
#       ],
#       "commercial_demos": [
#           {"arcgis_online": "9c2f2bfa12404e6481e1624700e34cce"}
#       ],
#       "completed_demos": [
#           {"arcgis_online": "5c5783282f11499ab82da107af532ac9"}
#       ],
#   }

#   for s in sqls:
#     name = s.replace('.sql', '')
#     opr_execute_sql = BashOperator(
#         task_id=f"execute_sql_{name}",
#         bash_command=f"psql -d etl < {os.environ['AIRFLOW_HOME']}/{dag.dag_id}/{s}"
#     )

#     opr_dump_geojson = BashOperator(
#         task_id=f"dump_geojson_{name}",
#         bash_command=f"ogr2ogr -f GeoJSON /tmp/{name}.json pg:dbname=etl {dag.dag_id}.{name}"
#     )

#     opr_execute_sql.set_downstream(opr_dump_geojson)

#     if name in open_datasets.keys():
#         opr_ago_upload = PythonOperator(
#             task_id=f"ago_upload_{name}",
#             python_callable=destinations.upload_to_ago,
#             op_kwargs = {
#                 "id": open_datasets[name][0]["arcgis_online"],
#                 "filepath": f"/tmp/{name}.json"
#             }
#         )
        
#         opr_dump_geojson.set_downstream(opr_ago_upload)
        
#     opr_dummy.set_downstream(opr_execute_sql)
