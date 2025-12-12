/* 
==================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==================================================================================
Script Purpose:
    This Stored Procedure performed the ETL (Extract, Transformation, Load) Process
    to populate the 'Silver' Schema tables from 'bronze' tables schema 
  Action Performed:
      - Truncate Silver Tables
      - insert Transformed and clean Data from Bronze to Silver.

Parameters:
    None.
    This stored Procedure is not accept any parameters or return any values.

Usages Example:
    - Call the Stored procedure with This query
    EXEC silver.load_silver;
*/

-- Now we can Create 'Stored Procedure'
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	-- This is used for the 'Calculating the Time'
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- The '@batch_start_time DATETIME, @batch_end_time DATETIME' they are used to calculate the whole BRONZE layer time
	BEGIN TRY
		SET @batch_start_time = GETDATE(); -- The batch layer are start with this section
		PRINT '=================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=================================================================';

		PRINT '--------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------'
	
		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data into: silver.crm_cust_info';
		-- Now we can Insered into the 'bronze.crm_cust_info' 
		INSERT INTO silver.crm_cust_info (cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_material_status,
				cst_gndr,
				cst_create_data
				)

		-- This Query used to Data Transformation and Data Clenging 
		select 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
				case
				When UPPER(TRIM(cst_material_status)) ='S' then 'Single'
				When UPPER(TRIM(cst_material_status)) = 'M' then 'Married' 
			ELSE
				'N/A'
				end as cst_material_status, -- Normalize marital status values to readable format
			case
				When UPPER(TRIM(cst_gndr)) ='M' then 'Male'
				When UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
			ELSE
				'N/A'
				end as  cst_gndr, -- Normalization the Gender Value for the readable format
			cst_create_data
		from
		(
		select *,
				ROW_NUMBER() OVER (Partition BY cst_id Order By cst_create_data Desc) as flag_list
		from bronze.crm_cust_info
		where  cst_id is not null )t 
		where flag_list = 1    -- Select most recent record per customers

	
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		--=====================================================================================================
		-- Loading silver.crm_prd_info

		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,      
			prd_line,      
			prd_start_dt,  
			prd_end_dt    
		)


		-- This is used for the Trnasormation the DATA in the silver.crm_prd_info
		Select 
			prd_id,

			REPLACE(SUBSTRING(prd_key,1,5),'-', '_') as cat_id, -- Extarct Category ID

			-- len() is used to dynamic setting in the prd_key
			SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- Extarct the product key
	
			prd_nm,

			ISNULL(prd_cost, 0) as prd_cost,

		CASE
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Roads'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			ELSE 'n/a' 
		END as prd_line,  -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE)as prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) -1 AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date

		from bronze.crm_prd_info
		-- This Process is the "Data Enrichment"

		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		--=====================================================================================================
		--Loading silver.crm_sales_details

		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into: silver.crm_sales_details';
		-- Inserting the Data Into the 'silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
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


		-- This is the Transformation of the "silver.crm_sales_details"
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		-- This is for the "sls_order_dt"
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NUll
			ELSE CAST(CAST(sls_order_dt as NVARCHAR) AS DATE)
		END as sls_order_dt,

		-- This is for the "sls_ship_dt"
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NUll
			ELSE CAST(CAST(sls_ship_dt as NVARCHAR) AS DATE)
		END as sls_ship_dt,

		-- This is for the "sls_due_dt"
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_due_dt) != 8 THEN NUll
			ELSE CAST(CAST(sls_due_dt as NVARCHAR) AS DATE)
		END as sls_due_dt,

		-- This is for the "sls_sales"
		CASE 
			WHEN sls_sales is NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing and incorrect

		sls_quantity,

		-- This is fro the "sls_price"
		CASE 
			WHEN sls_price <= 0 OR sls_price is null 
			THEN sls_sales / NuLLIF(sls_quantity,0)
			ELSE sls_price
		END sls_price  -- Derived price if original value is invalid

		from bronze.crm_sales_details;

		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'


		--=====================================================================================================
		PRINT '--------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------'
		-- Loading silver.erp_cust_az12

		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into: silver.erp_cust_az12';
		-- Now Inseting the value in silver.erp_cust_az12
		INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
		)



		-- Tranforming the DATA in the 'silver.erp_cust_az12'
		select 
		-- This is for "cid"
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,len(cid))
			ELSE cid
		END AS cid,              --Remove 'NAS' prefix if present

		-- This is for "bdate"
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		end as bdate,            -- Set Future birthdate is NULL
		-- This is for 'gen'

		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F' ,'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
			ELSE 'n/a'
		END as gen                -- Normalize gender value for handle unknown cases
		from bronze.erp_cust_az12;

		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		--=====================================================================================================
		-- Loading silver.erp_loc_a101

		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into: silver.erp_loc_a101';
		-- Now Inserting the value 
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)



		-- This is used for transfomed the "silver.erp_loc_a101"
		select 
		Replace(cid, '-', '') as cid,

		-- This is DATA STANDARIZATION and CONSISTENCY
		CASE 
			WHEN TRIM(cntry) is null OR TRIM(cntry) = '' THEN 'n/a'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			when TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			ELSE TRIM(cntry)
		END as cntry     -- Normalization and handle missing or blank country codes
		from bronze.erp_loc_a101;

		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		--=====================================================================================================
		-- loading silver.px_cat_g1v2

		SET @start_time = GETDATE();
		PRINT ' TRUNCATE Table: silver.px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
		-- Insert the value in the 'silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintance
		)

		select 
		id,
		cat,
		subcat,
		maintance
		from bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		SET @batch_end_time = GETDATE();
		PRINT '==================================================='
		PRINT 'Loading Sliver Layer is Completed'
		PRINT '  -  Total Load Duration: ' + CAST(DATEDIFF(second , @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'
		PRINT '==================================================='

	END TRY
	BEGIN Catch
		print '===================================================='
		print 'Error Occured During Loading Silver Layer';
		print 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		print '====================================================='

	END Catch
END
