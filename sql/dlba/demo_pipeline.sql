create or replace view dlba.demo_pipeline as (
  select
		c.address,
		c.parcel_id,
		c.council_district,
		c.non_hhf_commercial_demo as commercial_building,
		c.neighborhood,
		st_setsrid(st_makepoint(c.acct_longitude::numeric, c.acct_latitude::numeric), 4326) as geom
	from dlba.case c
		inner join dlba.account a on a.related_property_case_id = c.id
	where c.asb_document_url is not null
		and c.demo_contractor_proceed_date is null
		and c.demo_asb_survey_status = 'Completed'
		and c.demo_knock_down_date is null
		and c.demo_pulled_date is null
		and c.recordtypeid in ('012j0000000xtGbAAI', '012j0000000zM27AAE', '012j0000000xtGcAAI')
)