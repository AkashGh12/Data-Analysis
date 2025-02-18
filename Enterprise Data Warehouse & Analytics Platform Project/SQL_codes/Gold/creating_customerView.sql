
-- there is some situations where cst_gebder and gen is different and we have to keep the only one.
-- as per source cst_gndr is more accurate than gen 
-- so we will use cst_gndr as primary but when there is a n/a but gen has that value then we will take that.
-- we are genarating surrogate key using row_number() to uniquely identify every rows or records

create view gold.dim_customers as
select * from 
(select
ROW_NUMBER() over(order by cst_id ) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when cst_gndr != 'n/a' then ci.cst_gndr
	 else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid) as x
where customer_key!=1


select * from gold.dim_customers