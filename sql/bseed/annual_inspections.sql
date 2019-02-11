drop view if exists accela.annual_inspections;
create view accela.annual_inspections as
SELECT
    per."B1_ALT_ID" as case_number,
    P."B1_PARCEL_NBR" as parcel_assess,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) AS address,
    o."B1_OWNER_FULL_NAME" as owner,
    '' as project_name,
    per."REC_DATE" as action_date,
    g."SD_PRO_DES" as action_description_gprocess, -- which one of these is the right action description??? 
    a."G6_ACT_TYP" as action_description_g6action,
    a."INSP_RESULT_TYPE" as result,
    '' as cert_compliance_date,
    '' as date_referred_to_dah,
    '' as legal_use
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3addres ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = P."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3owners o ON per."B1_PER_ID1" = o."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = o."B1_PER_ID3"
    LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
    LEFT OUTER JOIN accela.g6action a on per."B1_PER_ID1" = a."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = a."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'CodeEnforcement' 
    AND per."B1_PER_TYPE" = 'Inspections'
    AND per."B1_PER_SUB_TYPE" = 'Commercial'
    AND a."G6_ACT_TYP" in ('Annual Called Inspection', 'Annual Inspection', 'Annual Called Re-Inspection', 'Annual Re-Inspection')
    AND a."G6_DOC_DES" = 'Insp Completed'
