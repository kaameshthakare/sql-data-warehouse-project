/*
=========================================================================================================
Store Procedure: Load Bronze Layer (Source ->)
=========================================================================================================
Script Purpose:
   This is store procedure loads data into the 'bronze'schema from external CSV files.
   It performs the following action:
   - Truncates the bronze tables before loading the data.
   - Uses the 'BULK INSERT' command toload data from csv Files to bronze tables

Parameters:
   None.
   This stored procedure does not accept any parameters or return any values.

Usage Example:
   EXEC bronze.load_bronze;
=========================================================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
	    set @batch_start_time =GETDATE();
		PRINT '=======================================================';
		PRINT 'Loading Bronze Tables';
		PRINT '=======================================================';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------';

		SET @start_time =GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>>Inserting Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);
		SET @end_time =GETDATE();

		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		SET @start_time =GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);

		SET @end_time =GETDATE();
		 
		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';
		PRINT '>>Truncating Table: bronze.crm_sales_details';

		SET @start_time =GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>>Inserting Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		PRINT '-------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------';

		SET @start_time =GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';

		SET @start_time =GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		);

		SET @end_time =GETDATE();
		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';


		SET @start_time =GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Vaibhav\Desktop\kamesh\ETL-Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		   FIRSTROW =2,
		   FIELDTERMINATOR = ',',
		   TABLOCK
		); 
		SET @end_time =GETDATE();
		PRINT '>> Load Duration: '+ cast (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------';
		set @batch_end_time =GETDATE();
		PRINT '========================================================================';
		PRINT 'Loading Bronze layer is completed';
		PRINT '  - Total Load Duration: ' + cast (DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '========================================================================';

	END TRY
	BEGIN CATCH
	   PRINT '=======================================================================================';
	   PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	   PRINT 'Error Message' + ERROR_MESSAGE();
	   PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	   PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	   PRINT '=======================================================================================';
	END CATCH
END
