drop view if exists accela.building_permits;
create view accela.building_permits as 
SELECT
    per."B1_PER_ID1",
    per."B1_PER_ID3",
    per."B1_ALT_ID" AS permit_no,
    per."REC_DATE" AS permit_issued,
    EXP."EXPIRATION_DATE" AS permit_expire,
    pdt."B1_CLOSED_DATE" AS permit_completed,
    '' as permit_status,
    concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) AS site_address,
    P."B1_PARCEL_NBR" AS parcel_no,
    '' as lot_number,
    '' as subdivision,
    '' as case_type,
    d."B1_WORK_DESC" AS case_description,
    '' as legal_use,
    '' as estimated_cost,
    '' as parcel_size,
    '' as parcel_cluster_sector,
    '' as stories,
    '' as parcel_floor_area,
    '' as parcel_ground_area,
    f."GF_DES" AS fdicn_description,
    f."GF_COD" AS fee_type,
    o."B1_OWNER_FULL_NAME" AS owner_name,
    C."B1_CAE_LNAME" AS contractor_last_name,
    C."B1_CAE_FNAME" AS contractor_first_name,
    c."B1_ADDRESS1" as contractor_address1,
    c."B1_ADDRESS2" as contractor_address2,
    c."B1_CITY" as contractor_city,
    c."B1_STATE" as contractor_stat,
    c."B1_ZIP" as contractor_zip,
    f."FEEITEM_SEQ_NBR" AS seq_no 
FROM
    b1permit per
    LEFT OUTER JOIN b1expiration EXP ON per."B1_PER_ID1" = EXP."B1_PER_ID1" 
    AND per."B1_PER_ID3" = EXP."B1_PER_ID3"
    LEFT OUTER JOIN b1expiration EXP ON per."B1_PER_ID1" = EXP."B1_PER_ID1" 
    AND per."B1_PER_ID3" = EXP."B1_PER_ID3"
    LEFT OUTER JOIN bpermitdetail pdt ON per."B1_PER_ID1" = pdt."B1_PER_ID1" 
    AND per."B1_PER_ID3" = pdt."B1_PER_ID3"
    LEFT OUTER JOIN b3addres ADD ON per."B1_PER_ID1" = ADD."B1_PER_ID1" 
    AND per."B1_PER_ID3" = ADD."B1_PER_ID3"
    LEFT OUTER JOIN b3parcel P ON per."B1_PER_ID1" = P."B1_PER_ID1" 
    AND per."B1_PER_ID3" = P."B1_PER_ID3"
    LEFT OUTER JOIN bworkdes d ON per."B1_PER_ID1" = d."B1_PER_ID1" 
    AND per."B1_PER_ID3" = d."B1_PER_ID3"
    LEFT OUTER JOIN f4feeitem f ON per."B1_PER_ID1" = f."B1_PER_ID1" 
    AND per."B1_PER_ID3" = f."B1_PER_ID3"
    LEFT OUTER JOIN b3owners o ON per."B1_PER_ID1" = o."B1_PER_ID1" 
    AND per."B1_PER_ID3" = o."B1_PER_ID3"
    LEFT OUTER JOIN b3contra C ON per."B1_PER_ID1" = C."B1_PER_ID1" 
    AND per."B1_PER_ID3" = C."B1_PER_ID3" 
    where f."GF_COD" = 'BLD_PMT'
ORDER BY
    per."REC_DATE" DESC