from airflow import DAG

import datetime as dt
import os, re, yaml

from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2018, 11, 28, 00, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('graphql',
  default_args=default_args,
  schedule_interval="0 1 * * *") as dag:

  # let's make the schema
  opr_make_graphql_schema = PostgresOperator(
    task_id='make_graphql_schema',
    sql=['drop schema if exists graphql cascade', 'create schema graphql'],
    postgres_conn_id='etl_postgres'
  )

  # let's make the parcels table
  parcel_sql = [
    "drop table if exists graphql.parcels cascade",
    "create table graphql.parcels as (select parcelnum as parcelno, geom as wkb_geometry, address from assessor.parcel_map_ago)",
    "alter table graphql.parcels add column uniq serial primary key",
    "delete from graphql.parcels p using graphql.parcels q where q.uniq < p.uniq and q.parcelno = p.parcelno",
    "alter table graphql.parcels add constraint parcels_uniq unique(parcelno)",
    "alter table graphql.parcels drop column uniq"
  ]

  opr_make_parcels = PostgresOperator(
    task_id='make_parcels',
    sql=parcel_sql,
    postgres_conn_id='etl_postgres'
  )

  opr_make_graphql_schema >> opr_make_parcels

  # create or replace the postgres functions
  opr_make_pgfunctions = BashOperator(
    task_id='make_functions',
    bash_command=f"psql -d etl -f {os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/functions.sql"
  )

  # dump the graphql schema (using pg_dump)
  opr_dump_schema = BashOperator(
    task_id='dump_schema',
    bash_command=f"""pg_dump -d etl -t "{dag.dag_id}.*" > /tmp/graphql_dump.sql"""
  )

  # transfer the dump from etl dev to gql prod (using secure copy)
  opr_transfer_schema = BashOperator(
    task_id='transfer_schema',
    bash_command="scp /tmp/graphql_dump.sql iotuser@10.208.37.176:/home/iotuser/graphql_dump.sql"
  )

  # transfer shell script to from etl dev to iot box (using scp)
  opr_transfer_script = BashOperator(
    task_id='transfer_script',
    bash_command=f"scp {os.environ['AIRFLOW_HOME']}/processes/{dag.dag_id}/create_db.sh iotuser@10.208.37.176:/home/iotuser/create_db.sh "
  )

  # on gql prod, shut down postgraphile (using grep)
  opr_kill_postgraphile = BashOperator(
    task_id='kill_postgraphile',
    bash_command="""
      ssh iotuser@10.208.37.176 'kill `ps -ax | grep node | grep postgraphile | cut -c 1-5 | sed -n 1p`'
    """
  )

  # on gql prod, import pg dump (using a shell script)
  opr_create_db = BashOperator(
    task_id='create_db',
    bash_command=f"ssh iotuser@10.208.37.176 bash /home/iotuser/create_db.sh "
  )

  # on gql prod, restart postgraphile
  opr_restart_postgraphile = BashOperator(
    task_id='restart_postgraphile',
    bash_command="""
      ssh iotuser@10.208.37.176 "nohup postgraphile -c postgres://graphql@localhost/graphqlnew -p 5000 --watch --simple-collections only --schema graphql --cors --append-plugins /home/iotuser/postgraphile-plugin-connection-filter/index.js --enhance-graphiql &>/dev/null & "
    """
  )
  
  # make the tables that reference parcels
  views = {
    "permits": [
      "drop table if exists graphql.permits",
      "create table graphql.permits as (select permit_no, site_address, parcel_no as parcelno, permit_issued, bld_permit_type, bld_permit_desc from tidemark.bldg_permits_socrata where parcel_no in (select parcelno from graphql.parcels) union select record_id, address, parcel_number as parcelno, permit_issue_date, permit_type, permit_description from accela.building_permits_socrata where parcel_number in (select parcelno from graphql.parcels))",
      "create index permits_pid_idx on graphql.permits using btree(parcelno)",
      "alter table graphql.permits add constraint permits_to_pid foreign key (parcelno) references graphql.parcels(parcelno)"
    ],
    "blight_violations": [
      "drop table if exists graphql.blight_violations",
      "create table graphql.blight_violations as (select * from dah.bvn where parcelno in (select parcelno from graphql.parcels))",
      "create index blight_violations_pid_idx on graphql.blight_violations using btree(parcelno)",
      "alter table graphql.blight_violations add constraint blight_violations_to_pid foreign key (parcelno) references graphql.parcels(parcelno)"
    ],
    "demolitions": [
      "drop table if exists graphql.demos",
      "create table graphql.demos as (select * from dlba.all_demos_graphql d where d.parcel_id in (select parcelno from graphql.parcels))",
      "alter table graphql.demos rename column parcel_id to parcelno",
      "create index demos_pid_idx on graphql.demos using btree(parcelno)",
      "alter table graphql.demos add constraint demos_to_pid foreign key (parcelno) references graphql.parcels(parcelno)"
    ],
    "property_sales": [
      "drop table if exists graphql.sales cascade",
      "create table graphql.sales as (select * from assessor.sales s where s.pnum in (select parcelno from graphql.parcels))",
      "alter table graphql.sales rename column pnum to parcelno",
      "create index sales_pid_idx on graphql.sales using btree(parcelno)",
      "alter table graphql.sales add constraint sales_to_pid foreign key (parcelno) references graphql.parcels(parcelno)"
    ],
    "ownership": [
      "drop table if exists graphql.ownership cascade",
      "create table graphql.ownership as (select * from assessor.parcelmaster pm where pm.pnum in (select parcelno from graphql.parcels))",
      "alter table graphql.ownership rename column pnum to parcelno",
      "create index ownership_pid_idx on graphql.ownership using btree(parcelno)",
      "alter table graphql.ownership add constraint ownership_to_pid foreign key (parcelno) references graphql.parcels(parcelno)"
    ]
  }

  for key in views:
    value = views[key]

    opr_make_views = PostgresOperator(
      task_id=f"make_{key}",
      sql=value,
      postgres_conn_id='etl_postgres'
    )

    opr_make_parcels.set_downstream(opr_make_views)
    opr_make_views.set_downstream(opr_make_pgfunctions)

opr_make_pgfunctions >> opr_dump_schema >> opr_transfer_schema >> opr_transfer_script >> opr_kill_postgraphile >> opr_create_db >> opr_restart_postgraphile
