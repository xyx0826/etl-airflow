# dropdb graphqlnew --if-exists
# createdb graphqlnew
# psql -d graphqlnew -c 'create extension postgis;'
psql -d graphql -U graphql -c 'drop schema if exists graphql; create schema graphql;'
psql -d graphql -U graphql < /home/iotuser/graphql_dump.sql
rm /home/iotuser/graphql_dump.sql
