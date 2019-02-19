drop table if exists dah.bvn cascade;
-- create initial master table
create table dah.bvn ( 
  ticket_id bigint,
  ticket_number text,
  agency_name text,
  inspector_name text,
  violator_name text,
  violation_street_number text,
  violation_street_name text,
  violation_zip_code text,
  violator_id bigint,
  mailing_address_str_number text,
  mailing_address_str_name text,
  city text,
  state text,
  zip_code text,
  non_us_str_code text,
  country text,
  violation_date timestamp,
  ticket_issued_time text,
  hearing_date timestamp,
  hearing_time text,
  judgment_date timestamp,
  violation_code text,
  violation_description text,
  disposition text,
  fine_amount double precision,
  admin_fee double precision,
  state_fee double precision,
  late_fee double precision,
  discount_amount double precision,
  clean_up_cost double precision,
  judgment_amount double precision,
  payment_amount double precision,
  balance_due double precision,
  payment_date timestamp,
  payment_status text,
  collection_status text,
  violation_address text,
  parcelno text
);

-- add indices on helper tables. speeds up ze joins
create index if not exists dah_bvn_zticket_idx on dah.bvn using btree(ticket_id);
create index if not exists dah_ztickets_zticketid_idx on dah.ztickets using btree("ZTicketID");
create index if not exists dah_violator_info_idx on dah.violator_info using btree("ZTicketID");
create index if not exists dah_dispadjourn_idx on dah.dispadjourn using btree("ZTicketID");
create index if not exists dah_violator_address_idx on dah.violator_address using btree("ViolatorID");
create index if not exists dah_payment_id_idx on dah.payments using btree("ZticketID");

-- start joins
insert into dah.bvn (
    ticket_id,
    ticket_number,
    agency_name,
    inspector_name,
    violation_street_number,
    violation_street_name,
    violation_zip_code,
    violator_id,
    violation_date,
    ticket_issued_time,
    hearing_date,
    hearing_time,
    judgment_date,
    violation_code,
    violation_description,
    fine_amount,
    late_fee,
    admin_fee,
    state_fee,
    discount_amount,
    clean_up_cost
  )
  select
    z."ZTicketID" as ticket_id,
    z."TicketNumber" as ticket_number,
    ag."AgencyName" as agency_name,
    concat_ws(' ', se."UserFirstName", se."UserLastName") as inspector_name,
    z."ViolStreetNumber" as violation_street_number,
    st."StreetName" as violation_street_name,
    z."ViolZipCode" as violation_zip_code,
    vi."FileID" as violator_id,
    z."IssueDate"::timestamp as violation_date,
    z."IssueTime" as ticket_issued_time,
    (CASE
      WHEN (z."ZTicketID" in (select "ZTicketID" from dah.reschedule)) 
      THEN (select "ReScheduleDate" from dah.reschedule r where z."ZTicketID" = r."ZTicketID" order by "ReScheduleDate" desc limit 1)
      ELSE z."CourtDate"
    END) as hearing_date,
    (CASE 
      WHEN z."CourtTime" = 1.0 THEN '9:00 AM'
      WHEN z."CourtTime" = 2.0 THEN '10:30 AM'
      WHEN z."CourtTime" = 3.0 THEN '1:30 PM'
      WHEN z."CourtTime" = 4.0 THEN '3:00 PM'
      WHEN z."CourtTime" = 5.0 THEN '6:00 PM'
      ELSE null
    END) as hearing_time,
    da."EntryDt" as judgment_date,
    od."OrdLaw" as violation_code,
    od."OrdDescription" as violation_description,
    cf."OffFineAmt" as fine_amount,
    z."LateFee" as late_fee,
    z."AdminFee" as admin_fee,
    z."JSA" as state_fee,
    z."DiscAmt" as discount_amount,
    (select sum("ServiceCost") from dah.blight_ticket_svc_cost bts where z."ZTicketID" = bts."ZTicketID" and bts."ServiceType" = 6) as clean_up_cost
  from dah.ztickets z
    left outer join dah.agency ag on ag."AgencyID" = z."AgencyID"
    left outer join dah.security se on se."SecurityID" = z."ZTicketUserID"
    left outer join dah.streets st on st."StreetID" = z."ViolStreetName"
    left outer join dah.violator_info vi on vi."ZTicketID" = z."ZTicketID"
    left outer join dah.ordinance od on od."OrdID" = z."ViolDescID"
    left outer join dah.cityfines cf on cf."OffFinID" = z."OrigFineAmt"
    left outer join dah.dispadjourn da on da."ZTicketID" = z."ZTicketID"
  where z."VoidTicket" = 0;

-- new fine amount
update dah.bvn j 
  set fine_amount = z."NewFineAmt" 
  from dah.ztickets z 
  where j.ticket_id = z."ZTicketID" and z."NewFineAmt" > 0;

-- join violator info
update dah.bvn j 
  set
    violator_name = 
      CASE
        WHEN i."BusinessName" is not null THEN i."BusinessName"
        ELSE concat_ws(' ', i."FirstName", i."LastName")
      END
  from dah.violator_info i 
  where j.ticket_id = i."ZTicketID";

-- join violator address
update dah.bvn j 
  set
    mailing_address_str_number = a."StreetNumber",
    mailing_address_str_name = a."StreetName",
    city = a."City",
    state = (select "StateAbrev" from dah.state s where s."StateID" = a."StateID"),
    zip_code = a."ZipCode",
    non_us_str_code = a."NonUsCity",
    country = (select "CountryDesc" from dah.country c where c."CountryID"::text = a."CountryID"::text)
  from dah.violator_address a 
  where a."ViolatorID" = j.violator_id;

-- join dah payments
update dah.bvn j 
  set 
    admin_fee = p."AdminFee",
    state_fee = p."StateFee"
  from dah.payments p 
  where j.ticket_id = p."ZticketID";

-- compute judgment amount: sum OrigFineAmt, StateFee, LateFee, RemediationCost, minus DiscAmt
update dah.bvn j
  set judgment_amount = (
    COALESCE(fine_amount, 0) + 
    COALESCE(admin_fee, 0) + 
    COALESCE(state_fee, 0) + 
    COALESCE(late_fee, 0) + 
    COALESCE(clean_up_cost, 0) -
    COALESCE(discount_amount, 0)
  );

-- compute payment amount: sum all rows with PaymentAmt for a unique ZTicketID
update dah.bvn j 
  set payment_amount = (select sum("PaymentAmt") 
  from dah.payments 
  where "ZticketID" = j.ticket_id and "PayAction" = 1 and "Void" != 1);
update dah.bvn j 
  set payment_amount = judgment_amount
  where j.ticket_id in (select "ZticketID" from dah.payments where "PymtKnd" = 2);

-- compute balance due
update dah.bvn j
  set balance_due = (judgment_amount - COALESCE(payment_amount, 0));

-- compute payment date
update dah.bvn j 
  set payment_date = (select max("PaymentDt") 
  from dah.payments 
  where "ZticketID" = j.ticket_id and dah.payments."PymtKnd" != 5);

-- create disposition string
update dah.bvn j 
  set disposition =
    case 
      when da."SetAside" != 'NaN' THEN 'PENDING'
      else
        (select concat_ws(' ',
          (select dt."Distype" from dah.disp_type dt where da."RespType" = dt."DispositionID"), 
          'by',
          (select dt."Distype" from dah.disp_type dt where da."DispositionID" = dt."DispositionID")
          )
        )
      end 
  from (select * from dah.dispadjourn da where "SetAside" != 1 order by "EntryDt" desc) da where da."ZTicketID" = j.ticket_id;

-- if disposition is "not responsible", set payment and judgment to $0 (bc might be refunded)
update dah.bvn j 
  set 
    payment_amount = 0, 
    judgment_amount = 0, 
    admin_fee = 0, 
    state_fee = 0,
    balance_due = 0
  where disposition like 'Not resp%';

-- create payment status
update dah.bvn j
  set payment_status = (
    CASE 
      WHEN 
      (j.disposition LIKE 'Not responsible%' OR
        j.disposition LIKE 'Responsible (Fine Waived)%') THEN 'NO PAYMENT DUE'
      WHEN j.payment_amount = 0 THEN 'NO PAYMENT APPLIED'
      WHEN j.payment_amount > 0 and j.balance_due > 0 THEN 'PARTIAL PAYMENT APPLIED'
      WHEN j.ticket_id in (select "ZticketID" from dah.payments where "PymtKnd" = 2) THEN 'PAID IN FULL'
      ELSE null
    END);

-- zero out balance due where no payment due
update dah.bvn j 
  set balance_due = 0 
  where j.payment_status = 'NO PAYMENT DUE';

-- create collection status
update dah.bvn j
  set collection_status = 'In collections' 
  where j.ticket_id in (select "ZTicketID" from dah.dispadjourn where "CollectionFlag" != 'NaN');

-- create violation address
update dah.bvn j 
  set violation_address = concat_ws(' ', violation_street_number, violation_street_name);

-- join on parcel address (geocode in future bc dah addresses often lack direction)
create index if not exists dah_bvn_violation_address_idx on dah.bvn using btree(violation_address);

-- if disposition is like "Not responsible", then judgment amount should display $0 (eg 17002283DAH)
update dah.bvn 
  set judgment_amount = 0 
  where disposition like 'Not %';

-- if court date is in the future, then judgment amount and balance due should display $0 (eg 17026937DAH)
update dah.bvn 
  set 
    judgment_amount = 0, 
    balance_due = 0 
  where hearing_date > NOW();

-- create view for AGO
-- drop view if exists dah.bvn_ago cascade;
-- create view dah.bvn_ago as select * from dah.bvn;
