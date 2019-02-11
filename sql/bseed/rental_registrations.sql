drop view if exists accela.rental_registrations;
create view accela.rental_registrations as 
SELECT
    distinct on (per."B1_ALT_ID")
    per."B1_ALT_ID" as csm_caseno,
    '' as csm_name_first,
    '' as csm_name_last,
    '' as csm_name_mi,
    '' as csm_recd_by,
    '' as csm_recd_date,
    P."B1_PARCEL_NBR" as parcelnum,
    '' as parcel_geom,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) as address,
    g."SD_PRO_DES" as action_description,
    '' as csa_creation_date,
    '' as csa_date1,
    '' as csa_date2,
    '' as csa_date3,
    '' as csa_disp,
    '' as pmb_dwelling_units,
    '' as pmb_type_use,
    '' as csm_status,
    '' as case_type
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3addres ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
		AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
		AND per."B1_PER_ID3" = P."B1_PER_ID3"
	LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
WHERE
    WHERE per."B1_PER_GROUP" = 'CodeEnforcement'
    AND per."B1_PER_TYPE" = 'Inspections'
    AND per."B1_PER_SUB_TYPE" = 'Rental'
    AND g."SD_PRO_DES" = 'Issue Registration'
