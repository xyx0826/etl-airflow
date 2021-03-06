drop view if exists accela.building_permits;
create view accela.building_permits as 
select
concat_ws('-', per."B1_PER_ID1", per."B1_PER_ID2", per."B1_PER_ID3"),
per."B1_ALT_ID" as record_id,
regexp_replace(concat_ws(' ', addr."B1_HSE_NBR_START", addr."B1_STR_DIR", addr."B1_STR_NAME", addr."B1_STR_SUFFIX"), '\s{1,}', ' ') as address,
to_date(per."REC_DATE", 'dd-MON-YY') as permit_issue_date,
'' as permit_type, 
'' as current_legal_use,
'' as type_of_construction,
chck."B1_CHECKLIST_COMMENT" as estimate,
des."B1_WORK_DESC" as permit_description,
det."TOTAL_FEE" as fee_amount,
det."BALANCE" as amount_due,
det."TOTAL_PAY" as amount_paid,
own."B1_OWNER_FULL_NAME" as owner_name,
regexp_replace(concat_ws(' ', own."B1_MAIL_ADDRESS1", own."B1_MAIL_ADDRESS2", own."B1_MAIL_ADDRESS3", own."B1_MAIL_CITY", own."B1_MAIL_STATE", own."B1_MAIL_ZIP", own."B1_MAIL_COUNTRY"), '/s{1,4}', '/s', 'g') as owner_address,
own."B1_PHONE" as owner_telephone,
own."B1_EMAIL" as owner_email,
contr."B1_BUS_NAME" as contractor_name,
regexp_replace(concat_ws(' ', contr."B1_ADDRESS1", contr."B1_ADDRESS2", contr."B1_ADDRESS3", contr."B1_CITY", contr."B1_STATE", contr."B1_ZIP"), '/s{1,4}', '/s', 'g') as contractor_address,
contr."B1_PHONE1" as contractor_phone,
proc."SD_APP_DES" as status,
proc."SD_PRO_DES" as task,
par."B1_COUNCIL_DISTRICT" as council,
'' as date_completed,
'' as date_expiration,
par."B1_PARCEL_NBR" as parcel_number
'' as area_parcel
from b1permit per
left outer join b3addres addr on addr."B1_PER_ID1" = per."B1_PER_ID1" and addr."B1_PER_ID3" = per."B1_PER_ID3"
left outer join b3parcel par on par."B1_PER_ID1" = per."B1_PER_ID1" and par."B1_PER_ID3" = per."B1_PER_ID3"
left outer join b3owners own on own."B1_PER_ID1" = per."B1_PER_ID1" and own."B1_PER_ID3" = per."B1_PER_ID3"
left outer join bworkdes des on des."B1_PER_ID1" = per."B1_PER_ID1" and des."B1_PER_ID3" = per."B1_PER_ID3"
left outer join bpermitdetail det on det."B1_PER_ID1" = per."B1_PER_ID1" and det."B1_PER_ID3" = per."B1_PER_ID3"
left outer join bchckbox chck on chck."B1_PER_ID1" = per."B1_PER_ID1" and chck."B1_PER_ID3" = per."B1_PER_ID3"
left outer join b3contra contr on contr."B1_PER_ID1" = per."B1_PER_ID1" and contr."B1_PER_ID3" = per."B1_PER_ID3"
left outer join gprocess proc on proc."B1_PER_ID1" = per."B1_PER_ID1" and proc."B1_PER_ID3" = per."B1_PER_ID3"
where 
to_date(per."REC_DATE", 'dd-MON-YY') >= '2018-12-08'
and per."B1_PER_GROUP" = 'Permits'
and per."B1_PER_TYPE" in ('Building', 'Demolition', 'TempUse', 'SignAwning')
and per."B1_APPL_STATUS" = 'Issued'
and own."B1_PRIMARY_OWNER" = 'Y'
and addr."B1_PRIMARY_ADDR_FLG" = 'Y'
and chck."B1_CHECKBOX_DESC_ALT" = 'Estimated Cost of Construction By Contractor/Homeowner'
and proc."SD_PRO_DES" = 'Permit Issuance'
order by record_id;
