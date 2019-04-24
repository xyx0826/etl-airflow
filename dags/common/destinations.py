from arcgis.gis import GIS

from airflow.models import Variable

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

# # #
# Function to upload to ArcGIS Online
# kwargs needed:
# 
# id: This is the AGO item ID #
# filepath: the local filepath on the ETL box
#
def upload_to_ago(**kwargs):
  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('AGO_USER'), Variable.get('AGO_PASS'))

  from arcgis.features import FeatureLayerCollection

  # this is the ID of the FeatureLayer, not the ID of the .json file
  item = gis.content.get(kwargs['id'])

  flc = FeatureLayerCollection.fromitem(item)

  flc.manager.overwrite(kwargs['filepath'])


# # #
# Function to fully replace in ArcGIS Online
# kwargs needed:
#
# id: This is the AGO item id # of the Feature Layer Collection
# table: this is the Postgres table/view that we want to send up. It should 
#
def replace_in_ago(**kwargs):
  from arcgis.features import FeatureLayer

  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('AGO_USER'), Variable.get('AGO_PASS'))
  item = gis.content.get(kwargs['id'])
  layer_url = f"{item.url}/0"

  layer = FeatureLayer(layer_url)

  hook = PostgresHook('etl_postgres')

  # Here's a query to make an ArcJSON item from each row in the table
  # Note: only works for point geometries right now.
  query = f"""
    SELECT jsonb_build_object(
        'geometry', jsonb_build_object(
          'x', ST_X(geom),
          'y', ST_Y(geom)
        ),
        'attributes', to_jsonb(row) - 'gid' - 'geom'
    ) FROM (SELECT * FROM {kwargs['table']}) row
  """

  res = hook.get_records(query)

  payload = [r[0] for r in res]

  # clear out all the rows in the table
  item.manager.truncate()

  # write all the rows in `res`
  chunk_size = 1000
  for i in range(0, len(payload), chunk_size):
    try:
      item.edit_features(adds=payload[i:i + chunk_size])
    except:
      print(f"Errored on {i}")
      item.edit_features(adds=payload=[i:i + chunk_size])

# # #
# Function to upload to Socrata
# kwargs needed:
# 
# table: name of the etl Postgres table/view to upload
# id: This is the Socrata 4x4
# method: one of "Replace" or "Upsert"
#
def upload_to_socrata(**kwargs):
  from sodapy import Socrata
  
  # make a sodapy Socrata client
  s = Socrata("data.detroitmi.gov", Variable.get("SOCRATA_TOKEN"), Variable.get("SOCRATA_USER"), Variable.get("SOCRATA_PASS"))

  # get the SQLAlchemy engine
  hook = PostgresHook('etl_postgres')
  eng = hook.get_sqlalchemy_engine()

  # get the payload
  result = eng.execute(f"select * from {kwargs['table']}")
  payload = [dict(row) for row in result]

  if kwargs['method'] == 'replace':
    job = s.replace( kwargs['id'], payload )
  else:
    chunk_size = 10000
    for i in range(0, len(payload), chunk_size):
      try:
        r = s.upsert( kwargs['id'], payload[i:i+chunk_size])
      except:
        print(f"Error on record {i}")
        r = s.upsert( kwargs['id'], payload[i:i+chunk_size])

def pg_to_file(view):
  full_view_name = view['dag'] + '.' + view['name']

  if view['export'] == 'shapefile':
    return BashOperator(
      task_id=f"dump_to_zipshp_{view['name']}",
      bash_command=f"pgsql2shp -f /tmp/{view['name']} etl {full_view_name} && zip /tmp/{view['name']}.zip /tmp/{view['name']}.*"
    )
  elif view['export'] == 'geojson':
    return BashOperator(
      task_id=f"dump_to_geojson_{view['name']}",
      bash_command=f"ogr2ogr -f GeoJSON /tmp/{view['name']}.json -t_srs epsg:4326 pg:dbname=etl {full_view_name}"
    )
  else:
    return None