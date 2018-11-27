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
    postgres_conn_id='mobility_postgres'
  )

def upload_to_ago(**kwargs):
  from arcgis.gis import GIS
  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('ago_user'), Variable.get('ago_pass'))

  from arcgis.features import FeatureLayerCollection

  # this is the ID of the FeatureLayer, not the ID of the .json file
  item = gis.content.get(kwargs['id'])

  flc = FeatureLayerCollection.fromitem(item)

  flc.manager.overwrite(kwargs['filepath'])

with DAG('scooter_7a',
  default_args=default_args,
  schedule_interval="0 7 * * *") as dag:

  opr_dump_geojson = BashOperator(
    task_id = 'dump_geojson',
    bash_command = """rm /home/gisteam/scooter_7a.json && ogr2ogr -f GeoJSON /home/gisteam/scooter_7a.json -sql "SELECT * FROM availability where timestamp = (select max(timestamp) from availability)" pg:dbname=mobility public.availability"""
  )

  opr_upload_to_ago = PythonOperator(
    task_id='upload_to_ago',
    provide_context=True,
    python_callable=upload_to_ago,
    op_kwargs={
      "id": "",
      "filepath": "/home/gisteam/scooter_7a.json"
    }
  )

opr_dump_geojson >> opr_upload_to_ago