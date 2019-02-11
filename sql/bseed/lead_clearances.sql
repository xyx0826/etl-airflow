drop view if exists accela.lead_clearances;
create view accela.lead_clearances as
SELECT
    distinct on (per."B1_ALT_ID")
    per."B1_ALT_ID" as csm_caseno,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) as address,
    P."B1_PARCEL_NBR" as parcelnum,
    g."SD_PRO_DES" as action_description,
    `` as actn_menu_id,
    `` as actn_code,
    `` as csa_creation_date,
    `` as csa_date1,
    `` as csa_date2,
    `` as csa_date3,
    `` as csa_disp,
    `` as case_type
FROM accela.b1permit per
	LEFT OUTER JOIN accela.b3address ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
		AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
	LEFT OUTER JOIN accela.b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
		AND per."B1_PER_ID3" = P."B1_PER_ID3"
	LEFT OUTER JOIN accela.gprocess g ON per."B1_PER_ID1" = g."B1_PER_ID1" 
	    AND per."B1_PER_ID3" = g."B1_PER_ID3"
WHERE `` in ('PMB', 'VPO')
    and `` = 'A'
    and `` = '01'
