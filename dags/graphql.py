from airflow import DAG

import datetime as dt

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

#   # Create gql tables on etl prod (using .sql script)
#   opr_create_gql_tables = BashOperator(
#     task_id='create_gql_tables',
#     bash_command='ssh gisteam@10.208.132.148 "psql -d etl < ~/create_gql_tables.sql"'
#   )

#   # Dump gql tables on etl prod (using pg_dump)
#   opr_dump_gql_tables = BashOperator(
#     task_id='dump_gql_tables',
#     bash_command="""ssh gisteam@10.208.132.148 'pg_dump -d etl -t "gql.*" > gql_dump.sql'"""
#   )

#   # Send gql tables from etl prod to gql prod (using scp)
#   opr_send_gql_tables = BashOperator(
#     task_id='send_gql_tables',
#     bash_command="""
#       ssh gisteam@10.208.132.148 scp /home/gisteam/gql_dump.sql iotuser@10.208.37.176:/home/iotuser/gql_dump.sql
#     """
#   )

#   # shut down Postgraphile
#   opr_kill_postgraphile = BashOperator(
#     task_id='kill_postgraphile',
#     bash_command="""
#       ssh iotuser@10.208.37.176 'kill `ps -ax | grep node | grep postgraphile | cut -c 1-5 | sed -n 1p`'
#     """
#   )

#   # Create fresh gql db on gql prod (using shell script)
#   opr_create_fresh_db = BashOperator(
#     task_id='create_fresh_db',
#     bash_command="""
#       ssh iotuser@10.208.37.176 bash /home/iotuser/gql_new_prod_db.sh
#     """
#   )

#   # restart Postgraphile
#   opr_restart_postgraphile = BashOperator(
#     task_id='restart_postgraphile',
#     bash_command="""
#       ssh iotuser@10.208.37.176 "nohup postgraphile -c postgres://graphql@localhost/graphql --watch --simple-collections both --schema gql --cors --append-plugins /home/iotuser/postgraphile-plugin-connection-filter/index.js &>/dev/null &"
#     """
#   )

# opr_create_gql_tables >> opr_dump_gql_tables >> opr_send_gql_tables >> opr_kill_postgraphile >> opr_create_fresh_db >> opr_restart_postgraphile
