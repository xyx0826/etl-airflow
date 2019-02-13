from airflow import DAG
import datetime as dt
import os
from common import destinations

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2019, 1, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

def pandas_to_csv(**kwargs):
    import pandas
    from slugify import slugify
    df = pandas.read_excel(kwargs['tmp_xlsx'], header=0, skiprows=1)
    df.columns = df.columns.map(lambda x: slugify(x).replace('-', '_'))

    # filter on lgu name
    det = df[df['current_lgu_lgu_name'] == 'DETROIT CITY'].fillna('')

    # get rid of everything after the comma in address
    det['address'].replace(',.{1,}', "", inplace=True, regex=True)

    # dump to csv
    det.to_csv(kwargs['tmp_csv'])

with DAG('liquor_licenses', 
    default_args=default_args,
    schedule_interval="0 1 * * *") as dag:

    tmp_xlsx = "/tmp/liquor_licenses.xlsx"
    tmp_csv = "/tmp/liquor_licenses.csv"

    opr_download = BashOperator(
        task_id='download_liquor_licenses',
        bash_command=f"curl -o {tmp_xlsx} https://www.michigan.gov/documents/lara/liclist_639292_7.xlsx"
    )

    opr_transform = PythonOperator(
        task_id='df_transform',
        python_callable=pandas_to_csv,
        op_kwargs = {
            "tmp_csv": tmp_csv,
            "tmp_xlsx": tmp_xlsx
        }
    )

    opr_load = PythonOperator(
        task_id='ago_upload_liquor_licenses',
        python_callable=destinations.upload_to_ago,
        op_kwargs={
            "id": "93edd2499ce74c56a73a2fefcd134280",
            "filepath": tmp_csv
        }
    )

    opr_load.set_upstream(opr_transform)
    opr_transform.set_upstream(opr_download)