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
    postgres_conn_id='mobility_postgres'
  )

def get_bird(**kwargs):
  http = HttpHook('GET', http_conn_id='bird_mds')

  # authentication for Bird
  # headers = {
  #   'cookie': "__cfduid=d2f05c82286d876cc90162f808933a9951533661267",
  #   'app-version': "3.0.0",
  #   'authorization': "Bird eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJBVVRIIiwidXNlcl9pZCI6ImI3YmFlZTZjLTZhOGMtNDI1NC1hZjFhLTU3NDVjYzExMmFjZiIsImRldmljZV9pZCI6ImViYjMyZjJlLTg3OTktNGQ5My04OWQ4LTFjYThhZDgyYzg1MiIsImV4cCI6MTU2NDYyNTYwNn0.K_1gnC8KvwmB1cUuJWH0QJB0LuX3DEATk_-AmkckkSA"
  #   }

  # get availability endpoint with limit = 1000
  response = http.run(
    "/gbfs/detroit/free_bikes",
    {"limit": 1000},
  )

  birds = json.loads(response.text)

  for b in birds['data']['bikes']:
    # pop values to store extras in extra column
    device_id = b.pop('bike_id')
    lat = b.pop('lat')
    lon = b.pop('lon')

    insert = f"""
      insert into availability (
        vendor, 
        device_id, 
        timestamp,
        extra,
        geom
      ) values (
        'bird',
        '{device_id}',
        '{kwargs['execution_date']}',
        '{json.dumps(b)}',
        ST_SetSRID(ST_MakePoint({lon},{lat}), 4326)
      )
    """
    pg.run(insert)

  return response

def get_lime(**kwargs):
  http = HttpHook('GET', http_conn_id='lime_gbfs')

  # get availability endpoint with limit = 1000
  response = http.run(
    "/api/partners/v1/gbfs/detroit/free_bike_status"
  )

  limes = json.loads(response.text)

  for l in limes['data']['bikes']:
    device_id = l.pop('bike_id')
    lat = l.pop('lat')
    lon = l.pop('lon')

    insert = f"""
      insert into availability (
        vendor, 
        device_id, 
        timestamp,
        extra,
        geom
      ) values (
        'lime',
        '{device_id}',
        '{kwargs['execution_date']}',
        '{json.dumps(l)}',
        ST_SetSRID(ST_MakePoint({lon},{lat}), 4326)
      )
    """
    pg.run(insert)
  
  return response

def get_spin(**kwargs):
  http = HttpHook('GET', http_conn_id='spin_gbfs')

  response = http.run(
    "/api/gbfs/v1/detroit/free_bike_status"
  )

  spins = json.loads(response.text)

  for s in spins['data']['bikes']:
    device_id = s.pop('bike_id')
    lat = s.pop('lat')
    lon = s.pop('lon')

    insert = f"""
      insert into availability (
        vendor, 
        device_id, 
        timestamp,
        extra,
        geom
      ) values (
        'spin',
        '{device_id}',
        '{kwargs['execution_date']}',
        '{json.dumps(s)}',
        ST_SetSRID(ST_MakePoint({lon},{lat}), 4326)
      )
    """
    pg.run(insert)
  
  return response

with DAG('scooter_availability',
  default_args=default_args,
  schedule_interval="*/15 * * * *") as dag:

  # Call Bird & insert to Postgres
  opr_get_bird = PythonOperator(
    task_id='get_bird',
    provide_context=True,
    python_callable=get_bird,
  )

  opr_get_lime = PythonOperator(
    task_id='get_lime',
    provide_context=True,
    python_callable=get_lime,
  )

  opr_get_spin = PythonOperator(
    task_id='get_spin',
    provide_context=True,
    python_callable=get_spin,
  )

  opr_dump_geojson = BashOperator(
    task_id = 'dump_geojson',
    bash_command = """rm /home/gisteam/scooter_availability.json && ogr2ogr -f GeoJSON /home/gisteam/scooter_availability.json -sql "SELECT * FROM availability where timestamp = (select max(timestamp) from availability)" pg:dbname=mobility public.availability"""
  )

  opr_upload_to_ago = PythonOperator(
    task_id='upload_to_ago',
    provide_context=True,
    python_callable=destinations.upload_to_ago,
    op_kwargs={
      "id": "f5a877cdf4be4ea9a71ecbcbabd178d1",
      "filepath": "/home/gisteam/scooter_availability.json"
    }
  )

opr_get_bird >> opr_dump_geojson
opr_get_lime >> opr_dump_geojson
opr_get_spin >> opr_dump_geojson

opr_dump_geojson >> opr_upload_to_ago