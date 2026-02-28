/*
==================================================
Stored Procedure:Load Bronze layer(source-> bronze)
===================================================
Script Purpose:
  This stored procedure loads data into the bronze schema from the external csv files.
  It performs the following actions:
    -Truncates the bronze tables before loading data.
    -uses the BULK INSERT commmand to load data from csv files to bronze tables.
Parameters:
  This Stored procedure does not accept any parameters or return any values.
Usage Example:
  EXEC bronze.load_bronze;
====================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		PRINT'========================================================'
		PRINT'LOADING BRONZE LAYER'
		PRINT'========================================================'
	
		PRINT'--------------------------------------------------------'
		PRINT'LOADING CRM TABLES'
		PRINT'--------------------------------------------------------'
		SET @batch_start_time=GETDATE()
		print'TRUNCATING AND INSERTING DATA INTO  bronze.crm_cust_info'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		print'TRUNCATING AND INSERTING DATA INTO  bronze.crm_prd_info'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		print'TRUNCATING AND INSERTING DATA INTO  bronze.crm_sales_details'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINT'--------------------------------------------------------';
		PRINT'LOADING ERP TABLES';
		PRINT'--------------------------------------------------------';
		print'TRUNCATING AND INSERTING DATA INTO  bronze.erp_cust_az12'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		print'TRUNCATING AND INSERTING DATA INTO  bronze.erp_loc_a101'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		print'TRUNCATING AND INSERTING DATA INTO  bronze.erp_px_cat_g1v2'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\hp\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		print'>>load Duration:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds'
		SET @batch_end_time=GETDATE()
		PRINT'>>LOAD DURATION FOR ENTIRE BATCH OF TABLES IS '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) +' SECONDS'
	END TRY
	BEGIN CATCH
		PRINT'================================='
		PRINT'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT'Error Messaege'+ERROR_MESSAGE();
		PRINT'Error Messaege'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'================================='
	END CATCH
END

