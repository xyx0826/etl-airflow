# ArcGIS Python API requirements
from arcgis.geocoding import geocode
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import Point, filters

# Smartsheet & create a client using env var SMARTSHEET_API_TOKEN
import smartsheet
ss_client = smartsheet.Smartsheet()

# pull down the specific Smartsheet we want
sheet = ss_client.Sheets.get_sheet("2185096154376068", page_size=2)

layers = {
    "snf": {
        "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/DEGC_Bounds/FeatureServer/0",
        "field": "proj_name",
        "colId": 278909364266884
    },
    "nrsa": {
        "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/DEGC_Bounds/FeatureServer/1",
        "field": "name",
        "colId": 3656609084794756
    },
    "studyareas": {
        "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/DEGC_Bounds/FeatureServer/2",
        "field": "name",
        "colId": 5908408898480004
    },
    "districts": {
        "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/ArcGIS/rest/services/Council_Districts/FeatureServer/0",
        "field": "Name",
        "colId": 4782508991637380
    }
}

# we'll loop through all the rows in the sheet
sheet_rows = sheet.to_dict()['rows']

featureLayers = {k: FeatureLayer(layers[k]['url']) for k in layers.keys()}

new_rows = []
for r in sheet_rows:

    # get the address value by its index in r.cells; split on comma and get first chunk
    addressVal = r['cells'][1]['displayValue'].split(",")[0]
    # send that along to the geocoder
    g = geocode(addressVal, out_sr=4326)
    if g:
        # we make a new row object which we'll send back in the update.
        new_row = ss_client.models.Row()
        new_row.id = r['id']
        loc = g[0]['location']
        point = arcgis.geometry.Point(loc)
        for k in layers.keys():
            match = featureLayers[k].query(
                returnGeometry=False, 
                outFields='*',
                geometry_filter=arcgis.geometry.filters.intersects(point, sr=4326))
            value = match.value
            if match and len(match.features) > 0 and len(value['features']) > 0:
                new_cell = ss_client.models.Cell()
                new_cell.column_id = layers[k]['colId']
                new_cell.value = value['features'][0]['attributes'][layers[k]['field']]
                new_cell.strict = False
                new_row.cells.append(new_cell)
        print(f"Pushed new row: {addressVal}")
        print(new_row.id)
        res = ss_client.Sheets.update_rows(2185096154376068, [new_row])
    else:
        print(f"No match: {addressVal}")
        
    
    # write the latlng
    # throw latlng against FLs to get the value
    # write the row back to the file
    
print(new_rows)
