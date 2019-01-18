create or replace view dlba.for_sale as (
  select 
		c.address,
		c.parcel_id,
		c.program,
		a.listing_date::timestamp as listing_date,
		c.neighborhood,
		c.council_district,
		st_setsrid(st_makepoint(c.acct_longitude::numeric, c.acct_latitude::numeric), 4326) as geom
	from dlba.dlba_activity a
		inner join dlba.case c on a.case = c.id
	where (a.recordtypeid = '012j0000000xtGoAAI'
		or a.dlba_activity_type in ('Demo Pull Sale', 'Demo Pull for Demo Sale', 'Renovation Sale', 'Own It Now', 'Own It Now - Bundled Property', 'Auction - Bundled Property'))
		and a.sale_status = 'For Sale On Site'
		and c.status = 'For Sale'
		and c.address not like '%Fake St%'
)