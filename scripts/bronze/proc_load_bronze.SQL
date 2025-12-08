/* 
==========================================================================================
    🔄 LOAD: SOURCE → BRONZE LAYER
------------------------------------------------------------------------------------------
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It Performs the following Actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from the csv Files to bronze Tables

Parameters:
    - None.
    - This stored procedure does not accept any parameters or return any values.

Usages:
    Uses the command to access the bronze Layer to store in 'Stored procedure'
    - EXEC bronze.load_bronze;
=============================================================================================
*/


-- Now Create a "Stored Procedure" for storing the data and executed the every days for the new data 
-- Frequently Used SQL Code in the "Stored Procedure" on the Database
  
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- This is used for the 'Calculating the Time'
	DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	-- The '@batch_start_time DATETIME, @batch_end_time DATETIME' they are used to calculate the whole BRONZE layer time
	BEGIN TRY
		SET @batch_start_time = GETDATE(); -- The batch layer are start with this section
		PRINT '=================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================================';

		PRINT '--------------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------'


		SET @start_time = GETDATE(); -- They have to gives the "Exact Time" when loading the Table
		print '>> Truncate Table: bronze.crm_cust_info'
		TRUNCATE Table bronze.crm_cust_info;  -- Using this is not be the duplicate the row

		print '>> Inserting Data into: bronze.crm_cust_info'
		-- Load massive amount of data at time
		BUlK INSERT bronze.crm_cust_info
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'


		------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();
		print '>> Truncate Table: bronze.crm_prd_info'
		TRUNCATE Table bronze.crm_prd_info;  -- Using this is not be the duplicate the row
		print '>> Inserting Data into: bronze.crm_prd_info'
		-- Load massive amount of data at time
		BUlK INSERT bronze.crm_prd_info
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		---------------------------------------------------------------------------------------------------------
		
		SET @start_time = GETDATE();
		print '>> Truncate Table: bronze.crm_sales_details'
		TRUNCATE Table bronze.crm_sales_details;  -- Using this is not be the duplicate the row
		print '>> Truncate Table: bronze.crm_sales_details'
		-- Load massive amount of data at time
		BUlK INSERT bronze.crm_sales_details
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'


		--========================================================================================
		-- This is the start from the New Tables Like : "ERP" csv Files
		--========================================================================================
		PRINT '--------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------'

		SET @start_time = GETDATE();
		print '>> Truncate Table: bronze.erp_cust_az12'
		TRUNCATE Table bronze.erp_cust_az12;  -- Using this is not be the duplicate the row

		print '>> Inserting Data into: bronze.erp_cust_az12'
		-- Load massive amount of data at time
		BUlK INSERT bronze.erp_cust_az12
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		-------------------------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		print '>> Truncate Table: bronze.erp_loc_a101'
		TRUNCATE Table bronze.erp_loc_a101;  -- Using this is not be the duplicate the row

		print '>> Inserting Data into: bronze.erp_loc_a101'
		-- Load massive amount of data at time
		BUlK INSERT bronze.erp_loc_a101
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		---------------------------------------------------------------------------------------------------------

		SET @start_time = GETDATE();
		print '>> Truncate Table: bronze.erp_px_cat_g1v2'
		TRUNCATE Table bronze.erp_px_cat_g1v2;  -- Using this is not be the duplicate the row

		print '>> Inserting Data into: bronze.erp_px_cat_g1v2'
		-- Load massive amount of data at time
		BUlK INSERT bronze.erp_px_cat_g1v2
		-- Path of the folder in your files
		FROM 'C:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,  -- We can say row start from 'second row'
			FIELDTERMINATOR = ',',  -- used for what they are Separate the row eg '# , $ ! etc'
			TABLOCK      -- used for make the good performance
		);
		SET @end_time = GETDATE(); -- They are used to calculate a "END Time" of the loading
		PRINT '>> Load Duration: ' + CAST (DATEDIFF (second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		-- This statement finding the diffrences of the "Seconds" between the '@start_time' and '@end_time'
		Print '>> -------------'

		SET @batch_end_time = GETDATE();
		PRINT '==================================================='
		PRINT 'Loading BRONZE Layer is Completed'
		PRINT '  -  Total Load Duration: ' + CAST(DATEDIFF(second , @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds'
		PRINT '==================================================='

	END Try
	BEGIN Catch
	print '===================================================='
	print 'Error Occured During Loading Bronze Layer';
	print 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
	print '====================================================='

	END Catch
END
