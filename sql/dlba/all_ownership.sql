create or replace view dlba.all_ownership as (
select
  name,
  parcel_id,
  inventory_status_socrata,
  neighborhood,
  council_district,
  st_setsrid(st_makepoint(longitude::numeric, latitude::numeric), 4326) as geom
from dlba.account
where recordtypeid = '012j0000000xKnSAAU'
  and parcel_id is not null
  and property_ownership = 'DLBA Owned'
  and name not like '%Fake St%')