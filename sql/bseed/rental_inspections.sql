drop view if exists accela.rental_inspections;
create view accela.rental_inspections as 
SELECT
    distinct on (per."B1_ALT_ID")
    per."B1_ALT_ID" as csm_caseno,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) as address,
    P."B1_PARCEL_NBR" as parcelnum,
    '' as parcel_geom,
    g."SD_PRO_DES" as action_description_gprocess,
    a."G6_ACT_DES" as action_description_g6action, -- this one actually seems to have closer values to current open data
    '' as actn_menu_id, -- drop?
    '' as actn_code, -- drop?
    '' as csa_creation_date, -- drop?
    '' as csa_date1, -- drop?
    '' as csa_date2, -- drop?
    per."REC_DATE" as csa_date3,
    '' as csa_disp,
    '' as case_typ
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3addres ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
		AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
		AND per."B1_PER_ID3" = P."B1_PER_ID3"
	LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
    LEFT OUTER JOIN accela.g6action a on per."B1_PER_ID1" = a."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = a."B1_PER_ID3"
WHERE per."B1_PER_GROUP" = 'CodeEnforcement'
    AND per."B1_PER_TYPE" = 'Inspections'
    AND per."B1_PER_SUB_TYPE" = 'Rental'