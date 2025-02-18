------------------ for [silver].[crm_cust_info]--------------------------------------------------------

-- check for null or duplicates in primary key
-- expectetion in no null or duplicate 

select cst_id, COUNT(*) from bronze.crm_cust_info
group by cst_id
having COUNT(*)>1;

--- check for unwanted spaces

select cst_firstname from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_lastname from bronze.crm_cust_info
where cst_firstname != TRIM(cst_lastname)

-- Data standarization and consistancy 

select distinct(cst_gndr) from bronze.crm_cust_info

select distinct(cst_marital_status) from bronze.crm_cust_info

------------------------------ for [silver].[crm_prd_info] ---------------------------------------------

select prd_id, COUNT(*) from bronze.crm_prd_info
group by prd_id
having COUNT(*)>1;

select prd_nm from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- checking negetive or null values occurance in cost

select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is NULL

--- checking if end date is lower rhan the start date 

select * from bronze.crm_prd_info
where prd_start_dt>prd_end_dt

--- testing the part 

select prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost,
case when UPPER(trim(prd_line)) = 'M' then 'Mountain'
	 when UPPER(trim(prd_line)) = 'T' then 'Touring'
	 when UPPER(trim(prd_line)) = 'S' then 'Other Sales'
	 when UPPER(trim(prd_line)) = 'R' then 'Road'
	 else 'n/a' 
end as prd_line,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as test_end_date
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

------------------------------ for [silver].[crm_sales_details]---------------------------------------------

-- checking if there is any date which is less than 0 or the size is less that 8 coz ddmmyyyy

select * from bronze.crm_sales_details
where sls_order_dt <=0 or LEN(sls_order_dt)<8

-- checkinh if any wrong date or not 

select * from bronze.crm_sales_details
where sls_order_dt>20500101 or  sls_order_dt<19000101

-- checking if order date is smaller  than the shipping date or not

select * from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt

-- correcting the value of sales and quantity and price

select sls_sales as old_sls,
sls_quantity,
sls_price as old_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity* ABS(sls_price)
	then sls_quantity * ABS(sls_price) 
	else sls_sales
end as sls_sales, 
case when sls_price is null or sls_price <= 0
	 then sls_sales/nullif(sls_quantity,0) 
	 else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity* sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0  or sls_price <=0 

------------------------------ for [silver].[erp_cust_az12] ---------------------------------------------

--- checking bday range 

select * from bronze.erp_cust_az12
where bdate>GETDATE() or bdate<'1920-01-01'

select distinct(gen),
case when upper(trim(gen))in ('M' , 'Male') then 'Male'
	 when upper(trim(gen)) in ('F','Female') then 'Female'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12

------------------------------ for [silver].[erp_loc_a101] ---------------------------------------------

select distinct(cntry) from bronze.erp_loc_a101

------------------------------ for [silver].[erp_px_cat_g1v2] ---------------------------------------------

select cat_id from silver.crm_prd_info

--- checking for unwanted spaces

select cat from bronze.erp_px_cat_g1v2
where TRIM(cat) != cat

select subcat from bronze.erp_px_cat_g1v2
where TRIM(subcat) != subcat

select distinct (subcat) from bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2
