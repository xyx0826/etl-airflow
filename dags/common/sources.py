from airflow.hooks.base_hook import BaseHook

import re

# import all the hooks
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.hooks.postgres_hook import PostgresHook

hooks = {
  'mssql': MsSqlHook,
  'salesforce': SalesforceHook
}

def flatten_record(rec):
    new_rec = {}
    for k,v in rec.items():
        # if it's a relationship...
        if k.endswith('__r'):
            try:
                # iterate through the OrderedDict we're given
                for a, b in v.items():
                    # ignore these keys
                    if a not in ['type', 'url', 'attributes']:
                        # construct a new key from the top-level key and the field name
                        key = re.sub("__r$", "", k) + '_' + re.sub("__c$", "", a)
                        # create it
                        new_rec[key] = b
            except AttributeError:
                pass
        # strip off '__c'
        elif k.endswith('__c'):
            new_rec[re.sub("__c$", "", k)] = v
        # no __c? just add it
        else:
            new_rec[k] = v
    return new_rec

def extract_source(**kwargs):
    conn = BaseHook(kwargs['connection']).get_connection(kwargs['connection'])
    conn_type = conn.conn_type

    # dummy list for records-to-insert
    recs = []

    # big case statement!
    if conn_type == 'mssql':
      hook = MsSqlHook(kwargs['connection'])
      fields = "*" if 'fields' not in kwargs.keys() else ", ".join(kwargs['fields'])
      where = "where 1 = 1" if 'where' not in kwargs.keys() else f"where {kwargs['where']}"
      statement = f"select {fields} from {kwargs['source_name']} {where}"
      recs = hook.get_records(statement)

    elif kwargs['connection'].endswith('_salesforce'):
      hook = SalesforceHook(kwargs['connection'])
      data = hook.get_object_from_salesforce(kwargs['source_name'], kwargs['fields'])
      hook.write_object_to_file([flatten_record(r) for r in data['records']], f"/tmp/{kwargs['source_name'].replace('__c','').lower()}.csv")
      
    else:
      pass

    # spin up a PostgresHook that we'll need to insert the data
    pg_hook = PostgresHook('etl_postgres')

    # Default is to truncate table
    if 'method' not in kwargs.keys():
      pg_hook.run(f"TRUNCATE TABLE {kwargs['dag']}.{kwargs['name']}")

    # Insert the records
    if conn_type == 'mssql':
      pg_hook.insert_rows(f"{kwargs['dag']}.{kwargs['name']}", recs[100:])
    
    elif kwargs['connection'].endswith('_salesforce'):
      pg_hook.run(f"COPY {kwargs['dag']}.{kwargs['name']} from '/tmp/{kwargs['source_name'].replace('__c','').lower()}.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

    else: 
      pass
