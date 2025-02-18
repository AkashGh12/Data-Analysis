
--- for all tables we will truncate and then we will insert in those tables
--- that we will remove the chance of getting duplicate data

------------------ for [silver].[crm_cust_info]--------------------------------------------------------
--- In cleaning part where 2 or 3 same data is present or duplicated.
--- we will take the most recent value and remove others coz most recent value is most fresh
-- getting only fresh data 

create or alter procedure silver.load_silver as
begin 
	print '>> Trancating Table: silver.crm_cust_info '
	truncate table silver.crm_cust_info;
	print '>> Inserting Data into: silver.crm_cust_info'
	insert into [silver].[crm_cust_info] (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)

	select cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	case when upper(trim(cst_marital_status))='M' then 'Married' 
		 when upper(trim(cst_marital_status))='S' then 'Single'
		 else 'n/a'
	end cst_marital_status,
	case when upper(trim(cst_gndr))='M' then 'Male' 
		 when upper(trim(cst_gndr))='F' then 'Female'
		 else 'n/a'
	end cst_gndr,
	cst_create_date
	from 
	(select *,
	ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as rn
	from bronze.crm_cust_info) as x
	where rn = 1;


	------------------------------ for [silver].[crm_prd_info] ---------------------------------------------

	--- We are extracting and cat_id and transforming it coz we need to match it with 
	--- another table later on 
	--- cat_id with erp_px_cat_g1v2
	--- prd_key with crm_sales_details

	--- where ever the end date is smaller than start date, we are transforming end date by 
	--- its previous day of next start date using lead() function

	--- we need to update the tables datatypes also to repair the ddl of this table


	print '>> Trancating Table: silver.crm_prd_info '
	truncate table silver.crm_prd_info;
	print '>> Inserting Data into: silver.crm_prd_info'

	if OBJECT_ID('silver.crm_prd_info','u') is not null 
		drop table silver.crm_prd_info;

	CREATE TABLE silver.crm_prd_info (
		prd_id       INT,
		cat_id       NVARCHAR(50),
		prd_key      NVARCHAR(50),
		prd_nm       NVARCHAR(50),
		prd_cost     INT,
		prd_line     NVARCHAR(50),
		prd_start_dt DATE,
		prd_end_dt   DATE,
		dwh_create_date datetime2 default getdate()
	);

	insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	select prd_id,
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
	cast(prd_start_dt as date) as prd_start_dt ,
	CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
	from bronze.crm_prd_info;

	------------------------------ for [silver].[crm_sales_details] ---------------------------------------------

	print '>> Trancating Table: silver.crm_sales_details '
	truncate table silver.crm_sales_details;
	print '>> Inserting Data into: silver.crm_sales_details'

	-- updating the ddl command as per new data 

	if OBJECT_ID('silver.crm_sales_details','u') is not null 
		drop table silver.crm_sales_details;

	CREATE TABLE silver.crm_sales_details (
		sls_ord_num  NVARCHAR(50),
		sls_prd_key  NVARCHAR(50),
		sls_cust_id  INT,
		sls_order_dt date,
		sls_ship_dt  date,
		sls_due_dt   date,
		sls_sales    INT,
		sls_quantity INT,
		sls_price    INT,
		dwh_create_date datetime2 default getdate()
	);

	insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or LEN(sls_order_dt)!=8 then null
		 else cast(cast(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	case when sls_ship_dt = 0 or LEN(sls_ship_dt)!=8 then null
		 else cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case when sls_due_dt = 0 or LEN(sls_due_dt)!=8 then null
		 else cast(cast(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity* ABS(sls_price)
		then sls_quantity * ABS(sls_price) 
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <= 0
		 then sls_sales/nullif(sls_quantity,0) 
		 else sls_price
	end as sls_price
	from bronze.crm_sales_details;


	------------------------------ for [silver].[erp_cust_az12] ---------------------------------------------

	--- we need to connect cid with silver.cust_info 
	-- thats why we need to update it to make it similar. for that we need to remove NAS from 1st 
	-- handle some futuristic birtdates with null
	-- handle male and female with proper order

	print '>> Trancating Table: silver.erp_cust_az12 '
	truncate table silver.erp_cust_az12;
	print '>> Inserting Data into: silver.erp_cust_az12'

	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)

	select
	case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
		 else cid
	end as cid,
	case when bdate>GETDATE() then null
		 else bdate
	end as bdate,
	case when upper(trim(gen))in ('M' , 'Male') then 'Male'
		 when upper(trim(gen)) in ('F','Female') then 'Female'
		 else 'n/a'
	end as gen
	from bronze.erp_cust_az12;

	------------------------------ for [silver].[erp_loc_a101] ---------------------------------------------

	-- we need to connect cid with [silver].[crm_cust_info] cst_key
	-- thats why updating it 
	-- handling countries

	print '>> Trancating Table: silver.erp_loc_a101 '
	truncate table silver.erp_loc_a101;
	print '>> Inserting Data into: silver.erp_loc_a101'

	insert into silver.erp_loc_a101(
	cid,cntry
	)
	select 
	replace (cid,'-','') as cid,
	case when trim(cntry) = 'DE' then 'Germany'
		 when trim(cntry) in ('United States','US','USA') then 'United States'
		 when cntry is null or cntry = '' then 'n/a' 
		 else TRIM(cntry)
	end as cntry
	from bronze.erp_loc_a101;


	------------------------------ for [silver].[erp_px_cat_g1v2] ---------------------------------------------

	print '>> Trancating Table: silver.erp_px_cat_g1v2 '
	truncate table silver.erp_px_cat_g1v2;
	print '>> Inserting Data into: silver.erp_px_cat_g1v2 '

	-- we will connect id with cat_id of silver.crm_prd_info

	insert into silver.erp_px_cat_g1v2(
	id,
	cat, 
	subcat,
	maintenance
	)

	select id,
	cat, 
	subcat,
	maintenance
	from [bronze].[erp_px_cat_g1v2];
end 

