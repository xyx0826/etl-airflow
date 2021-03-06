select 
per."B1_ALT_ID" as record_id,
proc."SD_PRO_DES" as task,
regexp_replace(concat_ws(' ', addr."B1_HSE_NBR_START", addr."B1_STR_DIR", addr."B1_STR_NAME", addr."B1_STR_SUFFIX"), '\s{1,}', ' ') as address,
addr."B1_SITUS_ZIP" as zip_code,
own."B1_OWNER_FULL_NAME" as primary_name,
to_date(per."REC_DATE", 'dd-MON-YY') as record_status_date,
'' as expiration_date
from b1permit per
left outer join b3addres addr on addr."B1_PER_ID1" = per."B1_PER_ID1" and addr."B1_PER_ID3" = per."B1_PER_ID3"
left outer join b3owners own on own."B1_PER_ID1" = per."B1_PER_ID1" and own."B1_PER_ID3" = per."B1_PER_ID3"
left outer join gprocess proc on proc."B1_PER_ID1" = per."B1_PER_ID1" and proc."B1_PER_ID3" = per."B1_PER_ID3"
where to_date(per."REC_DATE", 'dd-MON-YY') >= '2018-12-15'
and per."B1_PER_GROUP" = 'CodeEnforcement'
and per."B1_PER_TYPE" = 'Inspections'
and per."B1_PER_SUB_TYPE" = 'Vacant'
and addr."B1_PRIMARY_ADDR_FLG" = 'Y'
and own."B1_PRIMARY_OWNER" = 'Y'
and proc."SD_PRO_DES" = 'Issue Registration'
and own."B1_PRIMARY_OWNER" = 'Y';
