select 
	per."B1_ALT_ID" as csm_caseno,
	o."B1_OWNER_FULL_NAME" as owner_name,
  pdt."B1_CREATED_BY" as csm_recd_by,
	per."REC_DATE" as csm_recd_date,
	pa."B1_PARCEL_NBR" as parcelnum,
	concat_ws(' ', a."B1_HSE_NBR_START", a."B1_STR_NAME") as address,
	ac."G6_ACT_DES" as action_description,
	'' as csa_creation_date,
	'' as csa_date1,
	'' as csa_date2,
	'' as csa_date3,
	ac."G6_STATUS" as csa_disp,
	-- '' as pmb_dwelling_units,
	-- '' as pmb_type_use,
	'' as csm_status
-- SPATIAL JOIN	'' as prc_zip_code
from b1permit per
    LEFT OUTER JOIN bpermitdetail pdt ON per."B1_PER_ID1" = pdt."B1_PER_ID1" 
    AND per."B1_PER_ID3" = pdt."B1_PER_ID3"
-- 		LEFT OUTER JOIN b1expiration e ON per."B1_PER_ID1" = e."B1_PER_ID1" 
--     AND per."B1_PER_ID3" = e."B1_PER_ID3"
-- 		LEFT OUTER JOIN gprocess gp ON per."B1_PER_ID1" = gp."B1_PER_ID1" 
--     AND per."B1_PER_ID3" = gp."B1_PER_ID3"
		LEFT OUTER JOIN b3addres a ON per."B1_PER_ID1" = a."B1_PER_ID1" 
    AND per."B1_PER_ID3" = a."B1_PER_ID3"
		LEFT OUTER JOIN b3parcel pa ON per."B1_PER_ID1" = pa."B1_PER_ID1" 
    AND per."B1_PER_ID3" = pa."B1_PER_ID3"
-- 		LEFT OUTER JOIN bworkdes wd ON per."B1_PER_ID1" = wd."B1_PER_ID1" 
--     AND per."B1_PER_ID3" = wd."B1_PER_ID3"
-- 		LEFT OUTER JOIN f4feeitem f ON per."B1_PER_ID1" = f."B1_PER_ID1" 
--     AND per."B1_PER_ID3" = f."B1_PER_ID3"
		LEFT OUTER JOIN b3owners o ON per."B1_PER_ID1" = o."B1_PER_ID1" 
    AND per."B1_PER_ID3" = o."B1_PER_ID3"
		left outer JOIN g6action ac ON per."B1_PER_ID1" = ac."B1_PER_ID1" 
    AND per."B1_PER_ID3" = ac."B1_PER_ID3"
		LEFT OUTER JOIN b3contra c ON per."B1_PER_ID1" = c."B1_PER_ID1" 
    AND per."B1_PER_ID3" = c."B1_PER_ID3"
		LEFT OUTER JOIN bchckbox cb ON per."B1_PER_ID1" = cb."B1_PER_ID1" 
    AND per."B1_PER_ID3" = cb."B1_PER_ID3"
where 
per."B1_PER_GROUP" = 'CodeEnforcement' and per."B1_PER_TYPE" = 'Inspections' and per."B1_PER_SUB_TYPE" = 'Vacant'
--and ac."G6_DOC_DES" = 'Insp Completed'