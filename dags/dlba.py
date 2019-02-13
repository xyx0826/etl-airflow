from airflow import DAG
import datetime as dt
import os, re

from airflow.models import Variable

from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import destinations

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 16, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

# DLBA's Salesforce tables
tables = [
    {
        "sf_object": "Account",
        "fields": ["Council_District__c","Current_Status__c","Id","Latitude__c","Longitude__c","Name","Neighborhood__c","Property_Class__c","RecordTypeId","Related_Property_Case__r.Id","Property_Ownership__c","Parcel_ID__c","Inventory_Status_Socrata__c","Occupancy_Model_Probability_Pct__c"] 
    },
    {
        "sf_object": "Case",
        "fields": ["ACCT_Latitude__c", "ACCT_Longitude__c", "ASB_Abatement_Start_Date__c", "ASB_Abatement_Verification_Contractor__r.Name", "ASB_Document_URL__c", "ASB_Inspectors_Name__c", "ASB_Post_Abatement_Document_URL__c", "ASB_Post_Abatement_Insp_Date__c", "ASB_Post_Abatement_Notes__c", "ASB_Post_Abatement_Times_Failed__c", "ASB_Post_Abatement_Verification_Status__c", "ASB_Verifier_Name__c", "Abatement_Sub_Contractor__c", "Address__c", "BSEED_Final_Grade_Approved__c", "Council_District__c", "DEMO_ASB_Abatement_Contractor__r.Name", "DEMO_ASB_Abatement_Date__c", "DEMO_ASB_Survey_Contractor__r.Name", "DEMO_ASB_Survey_Status__c", "Demo_ASB_Post_Abatement_Failed_Date__c", "DEMO_Batch_Contractor_Name_del1__r.Name", "DEMO_Knock_Down_Date__c", "DEMO_NTP_Date__c", "DEMO_Planned_Knock_Down_Date__c", "DEMO_Pulled_Date__c", "Demo_Contractor_Proceed_Date__c", "Demo_Contractor_TEXT_ONLY__c", "Demo_Primarily_Funded_By__c", "Id", "Neighborhood__c", "Non_HHF_Commercial_Demo__c", "Parcel_ID__c", "Program__c", "Property__r.Latitude__c", "Property__r.Longitude__c", "Property__r.ZIP_Code__c", "RecordTypeId", "Socrata_Price_Type__c", "Socrata_Projected_Knocked_By_Date__c", "Socrata_Reported_Price__c", "Status"],
    },
    {
        "sf_object": "DLBA_Activity__c",
        "fields": ["Actual_Closing_Date__c", "Case__c", "DLBA_Activity_Type__c", "Id", "Listing_Date__c", "Name", "RecordTypeId", "Sale_Status__c" ]
    },
    {
        "sf_object": "Prospective_Buyer__c",
        "fields": ["Buyer_Status__c", "DLBA_Activity__c", "Final_Sale_Price__c", "Id", "Name", "Purchaser_Type__c"]
    },
    {
        "sf_object": "RecordType",
        "fields": ["Id", "Name", "Description"]
    },
    {
        "sf_object": "DBA_Commercial_Demo__c",
        "fields": ["ASB_Post_Abatement_Passed_Date__c", "BSEED_Final_Grade_Approved__c", "BSEED_Open_Hole_Approved__c", "BSEED_Winter_Grade_Approved__c", "Commercial_Demo_Status__c", "DBA_COM_Property__r.Council_District__c", "DBA_COM_Property__r.Latitude__c", "DBA_COM_Property__r.Longitude__c", "DBA_COM_Property__r.Name", "DBA_COM_Property__r.Neighborhood__c", "DBA_COM_Property__r.Parcel_ID__c", "Demo_Cost_Abatement__c", "Demo_Cost_Knock__c", "Demo_NtP_Dt__c", "Demo_Proj_Demo_Dt__c", "Demo_Pulled_Date__c", "Demolition_Contractor__r.Name", "ENV_Demo_Proceed_Dt__c", "ENV_Group_Number__c", "Final_Grade_Approved_Dt__c", "Knock_Start_Dt__c", "Name", "Related_Case__c", "Demo_Total_All_Costs__c"]
    }
]

def strip_sf_identifiers(name):
    """Remove __r and __c from a field name"""
    name = re.sub("__r\.", "_", name)
    name = re.sub("__c$", "", name)
    return name

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

def scrape_table(**kwargs):
  sf = SalesforceHook('dlba_salesforce')
  t = kwargs['table']
  data = sf.get_object_from_salesforce(t['sf_object'], t['fields'])
  sf.write_object_to_file([flatten_record(r) for r in data['records']], f"/tmp/{t['sf_object'].replace('__c','').lower()}.csv")

with DAG('dlba',
  default_args=default_args,
  schedule_interval="0 1 * * *") as dag:

  opr_dummy = BashOperator(
      task_id='dummy',
      bash_command="echo 'fake'"
  )

  # Extract
  for t in tables:

    tblname = t['sf_object'].replace('__c','').lower()

    opr_download_table = PythonOperator(
      task_id=f"get_table_{tblname}",
      python_callable=scrape_table,
      provide_context=True,
      op_kwargs = {
        "table": t
      }
    )

    opr_truncate_table = PostgresOperator(
      task_id = f"truncate_table_{tblname}",
      sql=f"truncate table dlba.{tblname}",
      postgres_conn_id='etl_postgres'
    )

    opr_insert_table = PostgresOperator(
      task_id = f"insert_table_{tblname}",
      sql=f"COPY dlba.{tblname} from '/tmp/{tblname}.csv' WITH (FORMAT CSV, HEADER, DELIMITER ',')",
      postgres_conn_id='etl_postgres'
    )

    opr_download_table.set_downstream(opr_truncate_table)  
    opr_truncate_table.set_downstream(opr_insert_table)
    opr_insert_table.set_downstream(opr_dummy)

  # Transform & Load

  homedir = "/home/gisteam/airflow/sql/"
  sqls = [f for f in os.listdir(f"{os.environ['AIRFLOW_HOME']}/sql/{dag.dag_id}") if f.endswith('.sql')]

  open_datasets = {
      "side_lots_sold": [
          {"arcgis_online": "1528d20710624632bf155fd67b57b963"}
      ],
      "contracted_demos": [
          {"arcgis_online": "e506c103f3a045a1aa53f7cd8e70dc1d"}
      ],
      "demo_pipeline": [
          {"arcgis_online": "0d81898958304265ac45d2f59a7339f5"}
      ],
      "for_sale": [
          {"arcgis_online": "dfb563f061b74f60b799c5eeae617fc8"}
      ],
      "own_it_now_sold": [
          {"arcgis_online": "cc9cb6e697844796bda2fa74fb7614d9"}
      ],
      "all_ownership": [
          {"arcgis_online": "04ba7b817d1d45ba89aab539af7ec438"}
      ],
      "auction_sold": [
          {"arcgis_online": "183f901e76a1439ba6c5e04510d275d3"}
      ],
      "commercial_demos": [
          {"arcgis_online": "9c2f2bfa12404e6481e1624700e34cce"}
      ],
      "completed_demos": [
          {"arcgis_online": "5c5783282f11499ab82da107af532ac9"}
      ],
  }

  for s in sqls:
    name = s.replace('.sql', '')
    opr_execute_sql = BashOperator(
        task_id=f"execute_sql_{name}",
        bash_command=f"psql -d etl < {os.environ['AIRFLOW_HOME']}/{dag.dag_id}/{s}"
    )

    opr_dump_geojson = BashOperator(
        task_id=f"dump_geojson_{name}",
        bash_command=f"ogr2ogr -f GeoJSON /tmp/{name}.json pg:dbname=etl dlba.{name}"
    )

    opr_execute_sql.set_downstream(opr_dump_geojson)

    if name in open_datasets.keys():
        opr_ago_upload = PythonOperator(
            task_id=f"ago_upload_{name}",
            python_callable=destinations.upload_to_ago,
            op_kwargs = {
                "id": open_datasets[name][0]["arcgis_online"],
                "filepath": f"/tmp/{name}.json"
            }
        )
        
        opr_dump_geojson.set_downstream(opr_ago_upload)
        
    opr_dummy.set_downstream(opr_execute_sql)



  