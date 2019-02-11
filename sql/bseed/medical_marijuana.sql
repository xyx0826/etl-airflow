delete from accela.mmcc where action is null;
-- geocode accela.mmcc
drop view if exists accela.medical_marijuana;
create view accela.medical_marijuana as 
SELECT
    trim(mmcc_business_name) as name,
    location as address,
    action,
    geom
FROM accela.mmcc
WHERE geom is not null;