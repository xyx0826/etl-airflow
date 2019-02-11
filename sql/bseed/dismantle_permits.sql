drop view if exists accela.dismantle_permits;
create view accela.dismantle_permits as
SELECT
	a."G6_ACT_DES" as action_description,
	p."B1_PARCEL_NBR" as parcel_no,
	concat_ws ( ' ', ADD."B1_HSE_NBR_START", ADD."B1_STREET_NAME_START", ADD."B1_STR_NAME", ADD."B1_STR_SUFFIX" ) AS address
FROM 
    accela.g6action A 
    left outer join accela.b3parcel P ON a."B1_PER_ID1" = P."B1_PER_ID1" 
	    AND a."B1_PER_ID3" = P."B1_PER_ID3"
    left outer join accela.b3address add ON a."B1_PER_ID1" = add."B1_PER_ID1" 
	    AND a."B1_PER_ID3" = add."B1_PER_ID3"
WHERE
	A."G6_ACT_DES" IN (
		'Final Grade Inspection',
		'Open Hole Demo Inspection ',
		'Open Hole Demo Inspection',
	    'Winter Grade Inspection');