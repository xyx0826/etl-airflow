from arcgis import gis, geocoding
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

ago = gis.GIS("https://detroitmi.maps.arcgis.com", Variable.get('AGO_USER'), Variable.get('AGO_PASS'))

geocoders = {
  'composite': geocoding.get_geocoders(ago)[0],
  'address': geocoding.get_geocoders(ago)[1],
  'centerline': geocoding.get_geocoders(ago)[2]
}

def geocode_rows(**kwargs):
  from arcgis import geocoding
# Expected **kwargs:
# table: name of table to geocode
# address_column: name of address column or expression
# geometry_column: name of geometry column
# parcel_column: name of parcel column
# where: a Postgres clause to restrict which rows get geocoded
# geocoder: enum ['composite', 'address', 'centerline']. default to composite

  hook = PostgresHook('etl_postgres')

  # default where clause
  where = kwargs['where'] if 'where' in kwargs.keys() else "1 = 1"
  geocoder = kwargs['geocoder'] if 'geocoder' in kwargs.keys() else 'composite'

  # generate an array of distinct addresses
  addresses = [r[0] for r in hook.get_records(f"select distinct {kwargs['address_column']} from {kwargs['table']} where {where}")]

  limit = 1000
  for i in range(0, len(addresses), limit):
    batch_addresses = addresses[i:i+limit]
    results = geocoding.batch_geocode(batch_addresses, out_sr=4326, geocoder=geocoders[geocoder])
    zipped = dict(zip(batch_addresses, results))

    for add, res in zipped.items():
      if 'parcel_column' in kwargs.keys():
        coords = res['location']
        pnum = res['attributes']['User_fld']
        query = f"update {kwargs['table']} set {kwargs['geometry_column']} = ST_SetSRID(ST_MakePoint({coords['x']}, {coords['y']}), 4326), {kwargs['parcel_column']} = '{pnum}' where {kwargs['address_column']} = '{add}'"
      elif res['location']['x'] != 'NaN':
        query = f"update {kwargs['table']} set {kwargs['geometry_column']} = ST_SetSRID(ST_MakePoint({coords['x']}, {coords['y']}), 4326) where {kwargs['address_column']} = '{add}'"
      else:
        pass



