drop view if exists accela.business_licenses;
create view accela.business_licenses as
SELECT
    per."B1_ALT_ID" as case_number,
    concat_ws(' ', addr."B1_HSE_NBR_START", addr."B1_STREET_NAME_START", addr."B1_STR_NAME", addr."B1_STR_SUFFIX") as business_address,
    par."B1_PARCEL_NBR" as parcel_num,
    con."B1_BUSINESS_NAME" as business_name,
    own."B1_OWNER_FULL_NAME" as owner_name,
	to_date(per."REC_DATE", 'dd-MON-YY') as license_issued,
    per."B1_PER_CATEGORY" as license_type,
    per."B1_PER_SUB_TYPE" as license_description,
    chk."B1_CHECKLIST_COMMENT" as business_description -- values should be like "corporation", etc
FROM accela.b3contact con
    LEFT OUTER JOIN accela.b1permit per ON con."B1_PER_ID1" = per."B1_PER_ID1" 
		AND con."B1_PER_ID3" = per."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3owners own ON con."B1_PER_ID1" = own."B1_PER_ID1" 
		AND con."B1_PER_ID3" = own."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3addres addr ON con."B1_PER_ID1" = addr."B1_PER_ID1" 
		AND con."B1_PER_ID3" = addr."B1_PER_ID3"
    LEFT OUTER JOIN accela.b3parcel par ON con."B1_PER_ID1" = par."B1_PER_ID1" 
		AND con."B1_PER_ID3" = par."B1_PER_ID3"
	LEFT OUTER JOIN accela.bchckbox chk ON con."B1_PER_ID1" = chk."B1_PER_ID1" 
		AND con."B1_PER_ID3" = chk."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'BusinessLicense'
    AND per."B1_PER_CATEGORY" in ('License', 'Renewal') -- where did "New Owner", "NLNL", "NLOL" go?
	AND con."B1_CONTACT_TYPE_FLAG" = 'organization'
ORDER BY to_date(per."REC_DATE", 'dd-MON-YY') desc
