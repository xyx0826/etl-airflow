create or replace view dlba.contracted_demos as (
  select
        address,
        case 
          when demo_planned_knock_down_date is null 
          then socrata_projected_knocked_by_date::timestamp
          else demo_planned_knock_down_date::timestamp
        end as demolish_by_date,
        socrata_reported_price as price,
        parcel_id,
        demo_contractor_text_only as contractor_name,
        council_district,
        non_hhf_commercial_demo as commercial_building, 
        neighborhood,
				st_setsrid(st_makepoint(acct_longitude::numeric, acct_latitude::numeric), 4326) as geom
      from dlba.case
      where socrata_reported_price::numeric > 0
        and demo_contractor_text_only <> ''
        and status = 'Demo Contracted'
)