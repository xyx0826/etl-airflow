from airflow import DAG
import datetime as dt
import os, re

from airflow.models import Variable

from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

# source tables we'll bring into postgres
tables = [
    {
        "mssql": "SWEETSpower.tblDAHPayments",
        "where": "",
        "psql": "dah.payments"
    },
    {
        "mssql": "SWEETSpower.tblZTickets",
        "where": """where (('2000-01-01' < "IssueDate" and "IssueDate" < '2025-01-01') or "IssueDate" is null)""",
        "psql": "dah.ztickets"
    },
    {
        "mssql": "SWEETSpower.tblDispAdjourn",
        "where": "",
        "psql": "dah.dispadjourn"
    },
    {
        "mssql": "SWEETSpower.tblDAHCityFines",
        "where": "",
        "psql": "dah.cityfines"
    },
    {
        "mssql": "SWEETSpower.tblDAHBlightTicketServiceCostTransactions",
        "where": "",
        "psql": "dah.blight_ticket_svc_cost"
    },
    {
        "mssql": "SWEETSpower.tblReSchedule",
        "where": """where (('2000-01-01' < "ProcessDate" and "ProcessDate" < '2025-01-01') or "ProcessDate" is null)""",
        "psql": "dah.reschedule"
    },
    {
        "mssql": "SWEETSpower.tblCourtTime",
        "where": "",
        "psql": "dah.courttime"
    },
    {
        "mssql": "SWEETSpower.tblAgency",
        "where": "",
        "psql": "dah.agency"
    },
    {
        "mssql": "SWEETSpower.tblDAHOrdinance",
        "where": "",
        "psql": "dah.ordinance"
    },
    {
        "mssql": "SWEETSpower.tblStreets",
        "where": "",
        "psql": "dah.streets"
    },
    {
        "mssql": "SWEETSpower.tblDAHViolatorAddress",
        "where": "",
        "psql": "dah.violator_address"
    },
    {
        "mssql": "SWEETSpower.tblState",
        "where": "",
        "psql": "dah.state"
    },
    {
        "mssql": "SWEETSpower.tblDAHViolatorInfo",
        "where": "",
        "psql": "dah.violator_info"
    },
    {
        "mssql": "SWEETSpower.tblSecurity",
        "where": "",
        "psql": "dah.security"
    },
    {
        "mssql": "SWEETSpower.tblDAHCountry",
        "where": "",
        "psql": "dah.country"
    },
    {
        "mssql": "SWEETSpower.tblDAHDispType",
        "where": "",
        "psql": "dah.disp_type"
    }
]

# Get records from MS SQLServer and insert into Postgres
def ms_to_pg(table):
    mshook = MsSqlHook('dah_database')
    records = mshook.get_records("select * from {} {}".format(table['mssql'], table['where']))

    pghook = PostgresHook('etl_postgres')
    pghook.insert_rows("{}".format(table['psql']), records)

with DAG('dah', 
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    opr_dummy = BashOperator(
        task_id='dummy',
        bash_command="echo 'fake'"
    )

    # Extract
    for t in tables:

        opr_truncate_table = PostgresOperator(
            task_id=f"truncate_{t['psql']}",
            sql=f"truncate table {t['psql']} cascade",
            postgres_conn_id='etl_postgres'
        )

        opr_extract_table = PythonOperator(
            task_id=f"get_{t['mssql']}",
            python_callable=ms_to_pg,
            provide_context=False,
            op_kwargs = {
                "table": t
            }
        )

        opr_dummy.set_downstream(opr_truncate_table)
        opr_truncate_table.set_downstream(opr_extract_table)
