/* 
============================================================
DDL Script:  BRONZE LAYER TABLES
    ---------------------------------------------------------
Script Purpose:
    This script create tables in the 'bronze; schema, dropping 
    the existing tables if they already exits.
    Run the script to re-define the DDL structure of
    'bronze' Tables
============================================================
*/


-- This command used for finding the exting tables in the date base 
-- If tables is finded then 'Drop them' and 'Recreate the New Tables'
If Object_id ('bronze.crm_cust_info', 'U') Is not Null 
	drop table bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id               Int,
	cst_key              nvarchar(50),
	cst_firstname        nvarchar(50),
	cst_lastname         nvarchar(50),
	cst_material_status  nvarchar(50),
	cst_gndr             nvarchar(50),
	cst_create_data      Date
);

If Object_id ('bronze.crm_prd_info', 'U') Is not Null 
	drop table bronze.crm_prd_info;
Create Table bronze.crm_prd_info (
	prd_id        Int,
	prd_key       nvarchar(50),
	prd_nm        nvarchar(50),
	prd_cost      Int,
	prd_line      nvarchar(50),
	prd_start_dt  DATETIME,
	prd_end_dt    DATETIME 
);

If Object_id ('bronze.crm_sales_details', 'U') Is not Null 
	drop table bronze.crm_sales_details;
Create Table bronze.crm_sales_details (
	sls_ord_num     nvarchar(50),
	sls_prd_key     nvarchar(50),
	sls_cust_id     Int,
	sls_order_dt    INT,
	sls_ship_dt     INT,
	sls_due_dt      INT,
	sls_sales       INT,
	sls_quantity    INT,
	sls_price       Int
);

If Object_id ('bronze.erp_loc_a101', 'U') Is not Null 
	drop table bronze.erp_loc_a101;
Create table bronze.erp_loc_a101 (
	cid   nvarchar(50),
	cntry nvarchar(50)
);

If Object_id ('bronze.erp_cust_az12', 'U') Is not Null 
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	cid   nvarchar(50),
	bdate DATE,
	gen   nvarchar(50)
);
If Object_id ('bronze.erp_px_cat_g1v2', 'U') Is not Null 
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
	id        nvarchar(50),
	cat       nvarchar(50),
	subcat    nvarchar(50),
	maintance nvarchar(50)
);

