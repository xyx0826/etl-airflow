import pandas as pd

from airflow.models import Variable

import smartsheet
smartsheet = smartsheet.Smartsheet(Variable.get('SMARTSHEET_ACCESS_TOKEN'))

def get_sheet_as_df(sheet_id):
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=0)
    row_count = ss1.total_row_count
    ss1 = smartsheet.Sheets.get_sheet(sheet_id, page_size=row_count)
    df = get_values(ss1)
    s2 = get_columns(ss1)
    df.columns = s2
    return df

def get_columns(ss):
    cl = ss.get_columns()
    d3 = cl.to_dict()
    df = pd.DataFrame(d3['data'])
    df = df.set_index('id')
    return df.title

def get_values(ss):
    d = ss.to_dict()
    drows = d['rows']
    rownumber = [x['rowNumber'] for x in drows]
    rows = [x['cells'] for x in drows]
    values = [[x['displayValue'] or x['value'] for x in y] for y in rows]
    return pd.DataFrame(values)