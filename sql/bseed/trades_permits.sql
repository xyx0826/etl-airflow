select 
per."B1_ALT_ID" as permit_no,
per."REC_DATE" as permit_issued,
e."EXPIRATION_DATE" as permit_expire,
pdt."B1_CLOSED_DATE" as permit_completed,
gp."SD_APP_DES" as permit_status,
concat_ws(' ', a."B1_HSE_NBR_START", a."B1_STR_NAME") as site_address,
-- BLANK as between1,
pa."B1_PARCEL_NBR" as parcel_no,
pa."B1_LOT" as prc_lot,
pa."B1_SUBDIVISION" as prc_subdiv,
per."B1_PER_TYPE" as case_type,
wd."B1_WORK_DESC" as case_description,
-- BLANK as legal_use,
-- BLANK as estimated_cost,
pa."B1_PARCEL_AREA" as parcel_size,
pa."B1_SUPERVISOR_DISTRICT" as parcel_cluster_sector,
-- BLANK as stories,
pa."B1_PLAN_AREA" as parcel_floor_area,
-- BLANK as parcel_ground_area,
-- BLANK as prc_aka_address,
-- !! ASI?? '' as elv_type_of_work,
-- '' as permit_description,
f."GF_DES" as fdicn_description,
-- !! ASI?? '' as elv_type_of_use,
--'' as residential,
--'' as description,
--'' as elv_type_const_cod,
--'' as elv_zoning_dist,
--'' as elv_use_group,
--'' as elv_basement,
f."GF_COD" as fee_type,
per."B1_ALT_ID" as csm_caseno,
pdt."B1_CREATED_BY" as csf_created_by,
'' as seq_no,
-- !! Where do we find this?? '' as pcf_amt_due,
o."B1_OWNER_FULL_NAME" as owner_name,
o."B1_MAIL_ADDRESS1" as owner_address1,
c."B1_CAE_LNAME" as contractor_last_name,
c."B1_ADDRESS1" as contractor_address1
from b1permit per
    LEFT OUTER JOIN bpermitdetail pdt ON per."B1_PER_ID1" = pdt."B1_PER_ID1" 
    AND per."B1_PER_ID3" = pdt."B1_PER_ID3"
		LEFT OUTER JOIN b1expiration e ON per."B1_PER_ID1" = e."B1_PER_ID1" 
    AND per."B1_PER_ID3" = e."B1_PER_ID3"
		LEFT OUTER JOIN gprocess gp ON per."B1_PER_ID1" = gp."B1_PER_ID1" 
    AND per."B1_PER_ID3" = gp."B1_PER_ID3"
		LEFT OUTER JOIN b3addres a ON per."B1_PER_ID1" = a."B1_PER_ID1" 
    AND per."B1_PER_ID3" = a."B1_PER_ID3"
		LEFT OUTER JOIN b3parcel pa ON per."B1_PER_ID1" = pa."B1_PER_ID1" 
    AND per."B1_PER_ID3" = pa."B1_PER_ID3"
		LEFT OUTER JOIN bworkdes wd ON per."B1_PER_ID1" = wd."B1_PER_ID1" 
    AND per."B1_PER_ID3" = wd."B1_PER_ID3"
		LEFT OUTER JOIN f4feeitem f ON per."B1_PER_ID1" = f."B1_PER_ID1" 
    AND per."B1_PER_ID3" = f."B1_PER_ID3"
		LEFT OUTER JOIN b3owners o ON per."B1_PER_ID1" = o."B1_PER_ID1" 
    AND per."B1_PER_ID3" = o."B1_PER_ID3"
		LEFT OUTER JOIN b3contra c ON per."B1_PER_ID1" = c."B1_PER_ID1" 
    AND per."B1_PER_ID3" = c."B1_PER_ID3"
where 
"B1_PER_GROUP" = 'Permits'
and 
"B1_PER_TYPE" in ('Electrical', 'Mechanical', 'BoilerPV', 'Elevator', 'Plumbing')
-- added this to make some sense of everything...
and gp."SD_APP_DES" = 'Issued'