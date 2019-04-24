from airflow import DAG

import datetime
from sqlalchemy import create_engine

from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mssql_hook import MsSqlHook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2019, 3, 1, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

def sftp_to_pg(**kwargs):
  today = datetime.date.today().strftime('%y%m%d')
  conn = SFTPHook('sftp_cityftp')
  files = conn.describe_directory('/Home/IET/PNC')
  file_name = [fn for fn in files.keys() if fn.startswith(f"tls.cityofdetroit.out.{today}")][0]
  conn.retrieve_file(f"/Home/IET/PNC/{file_name}", f"/tmp/{file_name}")

  pg_conn = PostgresHook('etl_postgres')
  pg_conn.run("truncate table escrow.escrow")
  pg_conn.run(f"copy escrow.escrow from '/tmp/{file_name}' (FORMAT CSV, HEADER FALSE) ")

def mssql_backup(**kwargs):
    import sqlalchemy

    internal_connection_string = "mssql+pymssql://McbroomJ:v55DQsj@CODASQLWVP04.ds.detroitmi.gov/WTProperty"

    engine = sqlalchemy.create_engine(internal_connection_string)
    
    connection = engine.connect()

    # check for an empty escrowbalance; if it is, let's abort.
    count = connection.execute("select count(*) from property_data_escrowbalance")
    if count.fetchone()[0] == 0:
        return
    else:
        # clear out backup
        connection.execute("delete from WTProperty.dbo.property_data_escrowbalance_backup where 1=1")

        # Long insert statement
        # Insert all rows into the backup table
        insert = """
        insert into dbo.property_data_escrowbalance_backup 
        (master_account_num, master_account_name, sub_account_num, sub_account_name, short_name, account_status, group_num, item_num, original_balance, fed_withholding_tax_this_period, ytd_fed_withholding_tax, int_paid_this_period, ytd_int_paid, int_split_this_period, escrow_balance, escrow_begin_date, escrow_end_date) 
        select 
        master_account_num, master_account_name, sub_account_num, sub_account_name, short_name, account_status, group_num, item_num, original_balance, fed_withholding_tax_this_period, ytd_fed_withholding_tax, int_paid_this_period, ytd_int_paid, int_split_this_period, escrow_balance, escrow_begin_date, escrow_end_date 
        from WTProperty.dbo.property_data_escrowbalance
        """
        connection.execute(insert)

        # Clear out the original table
        connection.execute("delete from WTProperty.dbo.property_data_escrowbalance where 1=1")

def pg_to_mssql(**kwargs):
    pg_hook = PostgresHook('etl_postgres')
    mssql_hook = MsSqlHook('mssql_wtproperty')

    # The fields we want to work with
    target_fields = ['master_account_num', 'master_account_name', 'sub_account_num', 'sub_account_name', 'short_name', 'account_status', 'group_num', 'item_num', 'original_balance', 'fed_withholding_tax_this_period','ytd_fed_withholding_tax', 'int_paid_this_period', 'ytd_int_paid', 'int_split_this_period', 'escrow_balance']

    # pull records from postgres
    recs = pg_hook.get_records(f"select {', '.join(target_fields)}  from escrow.escrow")

    # insert records to mssql
    mssql_hook.insert_rows('property_data_escrowbalance', recs, target_fields)

with DAG('escrow',
    default_args=default_args,
    schedule_interval="0 11 * * *") as dag:

    # Retrieve CSV
    opr_retrieve_file = PythonOperator(
        task_id='get_file_and_insert_to_pg',
        provide_context=True,
        python_callable=sftp_to_pg,
    )

    # Do some stuff in the SQL Server DB - we have to make our own SQLAlchemy connection for some reason
    opr_do_backup = PythonOperator(
        task_id='perform_mssql_backup',
        provide_context=True,
        python_callable=mssql_backup
    )
    
    transforms = [
        "alter table escrow.escrow alter column original_balance type numeric using replace(trim(original_balance), ',', '')::numeric",
        "alter table escrow.escrow alter column fed_withholding_tax_this_period type numeric using fed_withholding_tax_this_period::numeric",
        "alter table escrow.escrow alter column ytd_fed_withholding_tax type numeric using ytd_fed_withholding_tax::numeric",
        "alter table escrow.escrow alter column int_paid_this_period type numeric using int_paid_this_period::numeric",
        "alter table escrow.escrow alter column ytd_int_paid type numeric using ytd_int_paid::numeric",
        "alter table escrow.escrow alter column int_split_this_period type numeric using int_split_this_period::numeric",
        "alter table escrow.escrow alter column escrow_balance type numeric using replace(trim(escrow_balance), ',', '')::numeric"
        "alter table escrow.escrow alter column master_account_name type varchar(128) using trim(master_account_name)",
        "alter table escrow.escrow alter column sub_account_name type varchar(128) using trim(sub_account_name)",
        "alter table escrow.escrow alter column short_name type varchar(128) using trim(short_name)",
        "alter table escrow.escrow alter column group_num type int4 using group_num::integer"
    ]

    opr_transform_local = PostgresOperator(
        task_id='transform_escrow',
        postgres_conn_id='etl_postgres',
        sql=transforms
    )

    opr_pg_to_mssql = PythonOperator(
        task_id='push_to_mssql',
        provide_context=True,
        python_callable=pg_to_mssql
    )

    opr_retrieve_file >> opr_do_backup

    opr_do_backup >> opr_transform_local

    opr_transform_local >> opr_pg_to_mssql
