drop view if exists accela.business_licenses;
create view accela.business_licenses as
SELECT
    per."B1_ALT_ID" as case_a,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) as address,
    P."B1_PARCEL_NBR" as parcelnum,
    per."REC_DATE" as permit_issued,
    b."B1_BUSINESS_NAME" as bus_name,
    o."B1_OWNER_FULL_NAME" as owner_name,
    per."B1_PER_CATEGORY" as bus_own, -- license type
    per."B1_PER_SUB_TYPE" as license_description,
    '' as business_description -- values should be like "corporation", etc
FROM accela.b3contact b
    LEFT OUTER JOIN accela.b1permit per ON b."B1_PER_ID1" = per."B1_PER_ID1" 
		  AND b."B1_PER_ID3" = per."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3owners o ON b."B1_PER_ID1" = o."B1_PER_ID1" 
		  AND b."B1_PER_ID3" = o."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3addres ADD ON b."B1_PER_ID1" = ADD."B1_PER_ID1" 
		  AND b."B1_PER_ID3" = ADD."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3parcel P ON b."B1_PER_ID1" = P."B1_PER_ID1" 
		  AND b."B1_PER_ID3" = P."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'BusinessLicense'
    AND per."B1_PER_CATEGORY" in ('License', 'Renewal')