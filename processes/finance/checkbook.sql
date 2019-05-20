-- open checkbook includes data about vendor spending; it does not include payroll

-- in the `finance` schema, there are these tables:
---- input: accounts_payable_fy1718
---- lookups: agency, approp, cc, fund, object
---- output: checkbook_fy1718

-- make a clean table
drop table checkbook_fy1718 cascade;
create table checkbook_fy1718 as 
select
	"PAYMENT_HIST_DIST_ID",
	"PAYMENT_METHOD_CODE",
	"CHECK_NUMBER",
	"CHECK_DATE",
	"VOID_DATE",
	"CHECK_AMOUNT",
	"PAYMENT_TYPE_FLAG",
	"CHECKRUN_NAME",
	"STATUS_LOOKUP_CODE",
	"VENDOR_TYPE",
	"VENDOR_NAME",
	"VENDOR_SITE_CODE",
	"PAYMENT_NUM",
	"PERIOD_NAME",
	"ACCOUNTING_DATE",
	"INVOICE_PAYMENT_AMOUNT",
	"INV_PMT_DIST_AMT",
	"INVOICE_DIST_AMOUNT",
	"REVERSAL_FLAG",
	"InvoiceSource",
	"INVOICE_NUM",
	"INVOICE_AMOUNT",
	"INVOICE_DATE",
	split_part("ExpFund",'-',1) as fund_number,
	substring("ExpFund",6) as fund_desc,
	costcenter_firsttwo as dept_number,
	(select a.dept_name from agency a where costcenter_firsttwo = a.dept_number) as dept_desc,
	split_part("ExpAppropriation",'-',1) as approp_number,
	substring("ExpAppropriation",7) as approp_desc,
	split_part("ExpenseCostCenter",'-',1) as cost_center_number,
	substring("ExpenseCostCenter",8) as cost_center_desc,
	split_part("ExpenseObject",'-',1) as object_number,
	substring("ExpenseObject",8) as object_desc,
	(select o.shorthand_desc from object o where (split_part("ExpenseObject",'-',1))::text = o.object::text) as object_desc_shorthand
from accounts_payable_fy1718
where "STATUS_LOOKUP_CODE" != 'VOIDED';

-- summary stat: total spending by dept
select 
	sum(replace("INV_PMT_DIST_AMT", ',', '')::numeric) as spending_sum, 
	dept_desc
from checkbook_fy1718 
group by dept_desc
order by sum(replace("INV_PMT_DIST_AMT", ',', '')::numeric) desc;

-- summary stat: top 10 vendors
select 
	sum(replace("INV_PMT_DIST_AMT", ',', '')::numeric) as spending_sum, 
	"VENDOR_NAME"
from checkbook_fy1718 
group by "VENDOR_NAME"
order by sum(replace("INV_PMT_DIST_AMT", ',', '')::numeric) desc
limit 10;
