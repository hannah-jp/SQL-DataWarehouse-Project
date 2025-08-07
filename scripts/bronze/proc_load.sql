/*
	- This script truncates all data in the tables and inserts all the info in bulk to make sure all changes are inserted to the table
	- This process is repeated for the 6 tables under the bronze schema
Procedure:
-- Each time this script is run, we are refreshing this table, so if there are any changes in the file, they are loaded to the table
-- We want to insert the data, not row-by-row, but all at once, in a bluk insert
-- And then we can add comments to PRINT before each insert so we know where the process is at
-- Now Add TRY, CATCH to ensure error handling, data integrity, and issue logging for easier debigging
-- TRY and CATCH - SQL runs the TRY block and if it fails, it runs the CATCH clock to handle the error
-- Track ETL Duration - helps identify bottlenecks, optimize performance, monitor trends, detect issues
-- DECLAREd the variables so we can use them
-- GETDATE gives us the exact time the process starts, DATEDIFF gives difference between two dates, returns days, months, years
-- Get duration of loading the whole Bronze layer or whole batch
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '=======================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=======================================';

		PRINT '---------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'

		/* Above, FIRSTROW is telling SQL that the data starts from the second row and first row is header
		   FIELDTERMINATOR is the delimiter, comma in CSV files
		   TABLOCK locks the data in the table as it is inserted

		   NOW WE CHECK THE QUALITY OF THE TABLE, especailly when working with files
		   Check if data shifted because it is a common problem when source are CSV files
		   Check the Count of the rows - one less row because of header
		*/

		--SELECT * FROM bronze.crm_cust_info
		--SELECT COUNT (*) FROM bronze.crm_cust_info

		--
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'

		--
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'

		--
		PRINT '---------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '---------------------------------------';
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\hanna\OneDrive\Documents\SQL\SQL-DWing-Analytics\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'
		--
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\hanna\OneDrive\Documents\SQL\SQL-DWing-Analytics\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'
		--
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\hanna\OneDrive\Documents\SQL\SQL-DWing-Analytics\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'

		SET @batch_end_time = GETDATE()
		PRINT 'Loading Bronze Layer completed'
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) + 'seconds'
		PRINT '-------------------'

	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================='
	END CATCH
END
