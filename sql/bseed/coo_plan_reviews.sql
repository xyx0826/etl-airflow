drop view if exists accela.coo_plan_reviews;
create view accela.coo_plan_reviews as
SELECT
    per."B1_ALT_ID" as case_no,
    per."REC_DATE" as date,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) as address,
    P."B1_PARCEL_NBR" as parcel_no,
    per."B1_APP_TYPE_ALIAS" as permit_type,
    g."SD_PRO_DES" as action_description_gprocess,
    '' as legal_use,
    '' as est_cost,
    d."B1_WORK_DESC" as bld_permit_desc,
    '' as bld_type_use,
    '' as bld_type_use_code,
    '' as bld_type_use_calculated,
    '' as bld_type_const,
    f."GF_FEE" as amt_due, -- many fees per PR, figure this out later
    ADD."B1_SITUS_ZIP" as prc_zip_code
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3addres ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
		AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
		AND per."B1_PER_ID3" = P."B1_PER_ID3"
	LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
    LEFT OUTER JOIN accela.g6action a ON per."B1_PER_ID1" = a."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = a."B1_PER_ID3"
    LEFT OUTER JOIN bworkdes d ON per."B1_PER_ID1" = d."B1_PER_ID1" 
        AND per."B1_PER_ID3" = d."B1_PER_ID3"
    LEFT OUTER JOIN f4feeitem f ON per."B1_PER_ID1" = f."B1_PER_ID1" 
        AND per."B1_PER_ID3" = f."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'Planning' 
    AND per."B1_PER_TYPE" = 'Plan Review'
    AND g."SD_PRO_DES" = 'CofO'