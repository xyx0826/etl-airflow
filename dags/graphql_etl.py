from airflow import DAG

import datetime as dt

from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 11, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('graphql_etl',
  default_args=default_args,
  schedule_interval="0 1 * * *") as dag:

  # Create gql tables on etl prod (using .sql script)
  opr_create_gql_tables = BashOperator(
    task_id='create_gql_tables',
    bash_command='ssh gisteam@10.208.132.148 "psql -d etl < ~/create_gql_tables.sql"'
  )

  # Dump gql tables on etl prod (using pg_dump)
  opr_dump_gql_tables = BashOperator(
    task_id='dump_gql_tables',
    bash_command="""ssh gisteam@10.208.132.148 'pg_dump -d etl -t "gql.*" > gql_dump.sql'"""
  )

  # Send gql tables from etl prod to gql prod (using scp)
  opr_send_gql_tables = BashOperator(
    task_id='send_gql_tables',
    bash_command="""
      ssh gisteam@10.208.132.148 scp /home/gisteam/gql_dump.sql iotuser@10.208.37.176:/home/iotuser/gql_dump.sql
    """
  )

  # shut down Postgraphile
  opr_kill_postgraphile = BashOperator(
    task_id='kill_postgraphile',
    bash_command="""
      ssh iotuser@10.208.37.176 'kill `ps -ax | grep node | grep postgraphile | cut -c 1-5 | sed -n 1p`'
    """
  )

  # Create fresh gql db on gql prod (using shell script)
  opr_create_fresh_db = BashOperator(
    task_id='create_fresh_db',
    bash_command="""
      ssh iotuser@10.208.37.176 bash /home/iotuser/gql_new_prod_db.sh
    """
  )

  # restart Postgraphile
  opr_restart_postgraphile = BashOperator(
    task_id='restart_postgraphile',
    bash_command="""
      ssh iotuser@10.208.37.176 "nohup postgraphile -c postgres://graphql@localhost/graphql --watch --simple-collections both --schema gql --cors --append-plugins /home/iotuser/postgraphile-plugin-connection-filter/index.js &>/dev/null &"
    """
  )

opr_create_gql_tables >> opr_dump_gql_tables >> opr_send_gql_tables >> opr_kill_postgraphile >> opr_create_fresh_db >> opr_restart_postgraphile
