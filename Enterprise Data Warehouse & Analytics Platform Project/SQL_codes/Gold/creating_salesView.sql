
-- we want to join the product_number with this sales table and remove the existing product_key
-- same for the customer_id. We want to connect the fact sales table with dim products and dim customers

create view gold.fact_sales as
select 
sls_ord_num as order_number,
pr.product_key,
cs.customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products as pr 
on sd.sls_prd_key= pr.product_number
left join gold.dim_customers as cs
on sd.sls_cust_id= cs.customer_id


select * from gold.fact_sales