create or replace view dlba.auction_sold as (
  select
        a.actual_closing_date::timestamp as actual_closing_date,
        a.sale_status,
        c.address,
        c.parcel_id,
        'Auction'::text as program,
        c.neighborhood,
        c.council_district,
        pb.buyer_status,
        pb.final_sale_price,
        pb.purchaser_type,
				st_setsrid(st_makepoint(c.acct_longitude::numeric, c.acct_latitude::numeric), 4326) as geom
      from dlba.dlba_activity a
        inner join dlba.case c on a.case = c.id
        inner join dlba.prospective_buyer pb on pb.dlba_activity = a.id
      where a.recordtypeid = '012j0000000xtGoAAI' 
        and a.sale_status = 'Closed'
        and pb.buyer_status = 'Selected'
        and c.address not like '%Fake St%'
)