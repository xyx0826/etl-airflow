from airflow import DAG

import datetime as dt
import json
import arrow

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIOperator

from airflow.models import Variable

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook

from common import destinations

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 11, 28, 00, 00, 00),
    'concurrency': 3,
    'retries': 0
}

# Storing in a table like this:
# create table availability (
# 	oid serial primary key,
# 	vendor text,
# 	device_id text,
# 	timestamp timestamp,
# 	extra json
# 	)
	
# select addgeometrycolumn('public', 'availability', 'geom', 4326, 'POINT', 2)

pg = PostgresHook(
    postgres_conn_id='etl_postgres'
  )

with DAG('scooter_7a',
  default_args=default_args,
  schedule_interval="0 11 * * *") as dag:

  opr_dump_geojson = BashOperator(
    task_id = 'dump_geojson',
    bash_command = """ogr2ogr -f GeoJSON /tmp/scooter_7a.json -sql "SELECT a.*, cd.districts as district FROM scooters.availability a inner join base.council_districts cd on st_contains(st_transform(cd.wkb_geometry, 4326), a.geom) where timestamp = (select max(timestamp) from scooters.availability)" pg:dbname=etl scooters.availability"""
  )

  opr_upload_to_ago = PythonOperator(
    task_id='upload_to_ago',
    provide_context=True,
    python_callable=destinations.upload_to_ago,
    op_kwargs={
      "id": "424a9858887c4eadafbbe07b31cfeac3",
      "filepath": "/tmp/scooter_7a.json"
    }
  )

opr_dump_geojson >> opr_upload_to_ago