create or replace view dlba.completed_demos as (
  select
        address,
        parcel_id,
        demo_contractor_text_only as contractor_name,
        socrata_reported_price as price,
        demo_primarily_funded_by as funding_source,
        demo_knock_down_date as demolition_date,
        non_hhf_commercial_demo as commercial,
        council_district,
        neighborhood,
        st_setsrid(st_makepoint(property_longitude::double precision, property_latitude::double precision), 4326) as geom
      from dlba.case
      where demo_knock_down_date::timestamp >= date '2014-01-01'
        and socrata_reported_price::numeric > 0
        and demo_contractor_text_only != ''
)