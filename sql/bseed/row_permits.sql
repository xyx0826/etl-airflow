drop view if exists accela.row_permits;
create view accela.row_permits as 
SELECT
    `` as case_number,
    `` as permit_type,
    `` as project_name,
    `` as description,
    `` as work_done_by,
    `` as contractor,
    `` as contractor_address,
    `` as contractor_city,
    `` as contractor_state,
    `` as contractor_zip,
    `` as issued_date,
    `` as start_date,
    `` as end_date,
    `` as permit_address,
    `` as location
FROM
WHERE cm.case_type = 'ENG'