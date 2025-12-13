/*
===========================================================================
DDL script: Creating Gold Layer
===========================================================================
Script Pupose:
    This script craete view for all the gold layer in the warehouse.
    This 'Gold Layer' Represent the final dimensions and fact tables
    (Star Schema)

    Each view perfromed transformations and combines data from the 
    'Silver Layer' to produce a clean, enriched, and bussiness-ready 
    dataset.

Usage: 
    - These views can be quried directly for the analytics and reporting.
============================================================================
*/ 


--=====================================================================================
-- Create Dimension: gold.dim_customers
--=====================================================================================
IF OBJECT_ID ('gold.dim_customers' , 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO

CREATE VIEW gold.dim_customers AS
select 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,	-- This is the 'Surrogate Key'
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_material_status as marital_status,
	--  This is the 'Data Integration'
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master 'gender' table Info
		ELSE COALESCE(ca.gen, 'n/a')
	END as gender,
	ca.bdate AS birth_date,
	ci.cst_create_data AS create_data
	
from silver.crm_cust_info as ci
left Join silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
left Join silver.erp_loc_a101 as la
ON ci.cst_key = la.cid

GO

--------------------------------------------------------------------------------------------------------

--=====================================================================================
-- Create Dimension: gold.dim_products
--=====================================================================================
IF OBJECT_ID ('gold.dim_products' , 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- This is the 'Surrogate Key'
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS sub_category,
	pc.maintance AS maintaince,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_dt
	
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
where pn.prd_end_dt is null   -- Filter out all Historical Data

GO
-------------------------------------------------------------------------------------------
--=====================================================================================
-- Create FACT: gold.fact_sales
--=====================================================================================

IF OBJECT_ID ('gold.fact_sales' , 'V') IS NOT NULL
	DROP VIEW gold.fact_sales

GO
  
CREATE VIEW gold.fact_sales AS
select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price

from silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers as cu
ON sd.sls_cust_id = cu.customer_id

GO
