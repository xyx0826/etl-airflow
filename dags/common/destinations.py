from arcgis.gis import GIS
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator

def upload_to_ago(**kwargs):
  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('ago_user'), Variable.get('ago_pass'))

  from arcgis.features import FeatureLayerCollection

  # this is the ID of the FeatureLayer, not the ID of the .json file
  item = gis.content.get(kwargs['id'])

  flc = FeatureLayerCollection.fromitem(item)

  flc.manager.overwrite(kwargs['filepath'])

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