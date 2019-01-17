drop view if exists ocp.contracts_socrata;
create view ocp.contracts_socrata as 
    select
        case 
          when contract_nigp_code = 'NaN' then 'No code'
          else contract_nigp_code
        end as contract_nigp_code,
        contract_contract_id,
        upper(replace(rtrim(trim(company_company_name), '.'), ',', '')) as company_company_name,
        case 
          when company_city = 'NaN' then null
          else company_city
        end as company_city,
        case 
          when trim(company_state) = 'MICHIGAN' then 'MI'
          else upper(company_state)
        end as company_state,
        upper(contract_contract_purpose) as contract_contract_purpose,
        case
          when contract_value = 'NaN' then null
          else ltrim(contract_value, '$')::float
        end as contract_value,
        case 
          when contract_effective_date = 'NaN' then null
          else (select makeSocrataDate(contract_effective_date::timestamp))
        end as contract_effective_date,
        case 
          when contract_contract_expiration_date = 'NaN' then null
          else (select makeSocrataDate(contract_contract_expiration_date::timestamp))
        end as contract_contract_expiration_date,
        case
          when contract_document = 'NaN' then 'No document'
          else contract_document
        end as contract_document,
        contract_status,
        contract_contract_agreement_type
    from ocp.contracts 
    order by contract_nigp_code, contract_value asc;
