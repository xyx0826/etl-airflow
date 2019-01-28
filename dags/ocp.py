from airflow import DAG

from airflow.models import Variable

from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 17, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

today = datetime.today().strftime('%m%d%Y')
file_name = 'Contracts_{}.csv'.format(today)
path = '/outgoing/' + file_name

# Connect to SFTP, get file dated today
def get_file(**kwargs):
    conn = SFTPHook('sftp_novatus')
    d = conn.describe_directory('/outgoing/')
    conn.retrieve_file(path, f"/tmp/{file_name}")

# Make a Postgres table from a CSV using Pandas
def csv_to_pg(**kwargs):
    conn = PostgresHook.get_connection('etl_postgres')
    engine = create_engine(f"postgres://{conn.host}/{conn.schema}")
    
    df = pd.read_csv(f"/tmp/{kwargs['name']}", low_memory=False)
    df.rename(columns=lambda x: clean_cols(x), inplace=True)
    df.to_sql(name='contracts', con=engine, schema='ocp', if_exists='append')

# Clean column names, remove special characters
def clean_cols(name):
  name = name.replace(":", "")
  name = name.replace("#", "")
  name = name.replace(" ", "_").lower().strip()
  return name

def upload_to_ago(**kwargs):
  from arcgis.gis import GIS
  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('ago_user'), Variable.get('ago_pass'))

  from arcgis.features import FeatureLayerCollection

  # this is the ID of the FeatureLayer, not the ID of the .json file
  item = gis.content.get(kwargs['id'])

  flc = FeatureLayerCollection.fromitem(item)

  flc.manager.overwrite(kwargs['filepath'])

with DAG('ocp',
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    # Empty yesterday's table first
    opr_truncate_table = BashOperator(
        task_id='psql_truncate',
        bash_command='psql -d etl -c "truncate ocp.contracts"'
    )

    # Retrieve CSV
    opr_retrieve_file = PythonOperator(
        task_id='get_file',
        provide_context=True,
        python_callable=get_file,
    )

    # Create Postgres table
    opr_make_pgtable = PythonOperator(
        task_id='csv_to_pg_'+file_name.lower(),
        provide_context=True,
        python_callable=csv_to_pg,
        op_kwargs={
            "name": file_name,
        }
    )

    # Create open data view
    opr_transform = BashOperator(
        task_id='transform',
        bash_command='psql -d etl -f /home/gisteam/airflow/sql/ocp/contracts.sql'
    )

    # Dump view to geojson
    opr_dump_geojson = BashOperator(
        task_id=f"dump_geojson",
        bash_command=f"ogr2ogr -f GeoJSON /tmp/contracts.json pg:dbname=etl ocp.contracts_socrata"
    )

    # Load geojson to AGO
    opr_ago_upload = PythonOperator(
        task_id=f"ago_upload",
        python_callable=upload_to_ago,
        op_kwargs = {
            "id": 'd62b67c9bbe647f980178be556e3d292',
            "filepath": f"/tmp/contracts.json"
        }
    )

opr_truncate_table >> opr_retrieve_file >> opr_make_pgtable >> opr_transform >> opr_dump_geojson >> opr_ago_upload
