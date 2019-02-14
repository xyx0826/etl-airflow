from airflow.hooks.base_hook import BaseHook

import re

from common import helpers

# import all the hooks
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.hooks.postgres_hook import PostgresHook

hooks = {
  'mssql': MsSqlHook,
  'salesforce': SalesforceHook
}

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
      hook.write_object_to_file([helpers.flatten_salesforce_record(r) for r in data['records']], f"/tmp/{kwargs['source_name'].replace('__c','').lower()}.csv")
      
    else:
      pass

    # spin up a PostgresHook that we'll need to insert the data
    pg_hook = PostgresHook('etl_postgres')

    # Default is to truncate table
    if 'method' not in kwargs.keys():
      pg_hook.run(f"TRUNCATE TABLE {kwargs['dag']}.{kwargs['name']}")

    # Insert the records
    if conn_type == 'mssql':
      pg_hook.insert_rows(f"{kwargs['dag']}.{kwargs['name']}", recs)
    
    elif kwargs['connection'].endswith('_salesforce'):
      pg_hook.run(f"COPY {kwargs['dag']}.{kwargs['name']} from '/tmp/{kwargs['source_name'].replace('__c','').lower()}.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')")

    else: 
      pass
