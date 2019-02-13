from arcgis.gis import GIS
from airflow.models import Variable

def upload_to_ago(**kwargs):
  gis = GIS("https://detroitmi.maps.arcgis.com", Variable.get('ago_user'), Variable.get('ago_pass'))

  from arcgis.features import FeatureLayerCollection

  # this is the ID of the FeatureLayer, not the ID of the .json file
  item = gis.content.get(kwargs['id'])

  flc = FeatureLayerCollection.fromitem(item)

  flc.manager.overwrite(kwargs['filepath'])