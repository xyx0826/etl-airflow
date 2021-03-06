drop view if exists accela.rental_registrations;
create view accela.rental_registrations as 
SELECT
    distinct on (per."B1_ALT_ID")
    per."B1_ALT_ID" as record_id,
		g."SD_PRO_DES" as task,
		regexp_replace(concat_ws(' ', addr."B1_HSE_NBR_START", addr."B1_STR_DIR", addr."B1_STR_NAME", addr."B1_STR_SUFFIX"), '\s{1,}', ' ') as rental_address,
		p."B1_PARCEL_NBR" as parcel_number,
		addr."B1_SITUS_ZIP" as rental_zip,
		'' as record_type,
		'' as units,
		own."B1_OWNER_FULL_NAME" as owner_name,
		regexp_replace(concat_ws(' ', own."B1_MAIL_ADDRESS1", own."B1_MAIL_ADDRESS2", own."B1_MAIL_ADDRESS3", own."B1_MAIL_CITY", own."B1_MAIL_STATE", own."B1_MAIL_ZIP", own."B1_MAIL_COUNTRY"), '/s{1,4}', '/s', 'g') as owner_address,
		to_date(per."REC_DATE", 'dd-MON-YY') as issued_date
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3addres addr ON per."B1_PER_ID1" = addr."B1_PER_ID1" 
		AND per."B1_PER_ID3" = addr."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel p ON per."B1_PER_ID1" = p."B1_PER_ID1" 
		AND per."B1_PER_ID3" = p."B1_PER_ID3"
	LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
	LEFT OUTER JOIN b3owners own ON own."B1_PER_ID1" = per."B1_PER_ID1" 
        AND own."B1_PER_ID3" = per."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'CodeEnforcement'
    AND per."B1_PER_TYPE" = 'Inspections'
    AND per."B1_PER_SUB_TYPE" = 'Rental'
    AND g."SD_PRO_DES" = 'Issue Registration'
	AND to_date(per."REC_DATE", 'dd-MON-YY') > date '2018-12-07'
