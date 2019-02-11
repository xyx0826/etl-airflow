drop view if exists accela.contractors;
create view accela.contractors as 
SELECT
		per."B1_ALT_ID" as case_number,
		co."B1_LICENSE_TYPE" as license_type,
		'' as license_description,
		'' as master_name,
		b."B1_BUSINESS_NAME" as contractor_company,
		coalesce(b."B1_PHONE1", b."B1_PHONE2") as contractor_contact,
		exp."EXPIRATION_DATE" as expiration_date
FROM accela.b3contact b
    LEFT OUTER JOIN accela.b1permit per ON b."B1_PER_ID1" = per."B1_PER_ID1" 
		AND b."B1_PER_ID3" = per."B1_PER_ID3"  
		LEFT OUTER JOIN accela.b3contra co ON co."B1_PER_ID1" = per."B1_PER_ID1" 
		AND co."B1_PER_ID3" = per."B1_PER_ID3"   
		LEFT OUTER JOIN accela.b1expiration exp ON exp."B1_PER_ID1" = per."B1_PER_ID1" 
		AND exp."B1_PER_ID3" = per."B1_PER_ID3"
where per."B1_PER_CATEGORY" = 'License' and per."B1_PER_GROUP" = 'Licenses'