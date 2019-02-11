create or replace view dlba.commercial_demos as (
  select
        dba_com_property_name as address,
        dba_com_property_parcel_id as parcel_id,
        commercial_demo_status as status,
        demo_cost_abatement as abatement_cost,
        demo_cost_knock as demo_cost,
        demo_ntp_dt::timestamp as demo_proceed_date,
        demo_proj_demo_dt::timestamp as projected_demo_date,
        env_group_number,
        knock_start_dt::timestamp as demo_date,
        demolition_contractor_name as demolition_contractor,
        dba_com_property_neighborhood as neighborhood,
        dba_com_property_council_district as council_district,
        bseed_final_grade_approved::timestamp as final_grade_date,
        bseed_open_hole_approved::timestamp as open_hole_date,
        bseed_winter_grade_approved::timestamp as winter_grade_date,
        demo_total_all_costs as total_demo_cost,
				st_setsrid(st_makepoint(dba_com_property_longitude::numeric, dba_com_property_latitude::numeric), 4326) as geom
      from dlba.dba_commercial_demo
      where (knock_start_dt::timestamp >= date '2014-01-01' or knock_start_dt is null)
        and commercial_demo_status in ('Demo Contracted', 'Demolished', 'Demo Pipeline') 
        and demo_pulled_date is null
        and address not like '%Fake St%'
)