/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files. 
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
None. 
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS  -- load all the data from the source csv file into the bronze table
BEGIN    
    DECLARE @start_date DATETIME,@start_date_all DATETIME, @end_date DATETIME, @end_date_all DATETIME;
    BEGIN TRY
        SET @start_date_all = GETDATE();
        Print '================================================='
        Print 'Loading CRM Tables';
        Print '================================================='
        PRINT ''

        SET @start_date = GETDATE();
        IF (SELECT Count(*) FROM bronze.crm_cust_info) IS NOT NULL
            PRINT '>> Truncating Table: bronze.crm_cust_info';
            TRUNCATE TABLE bronze.crm_cust_info
		
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK
        INSERT
            bronze.crm_cust_info
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_crm\cust_info.csv'
        With (
                -- specify the format of the source file
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + 'seconds';


        Print '*************************************************'
        SET @start_date = GETDATE();
        IF (SELECT Count(*) FROM bronze.crm_prd_info) IS NOT NULL
            PRINT '>> Truncating Table: bronze.crm_prd_info';
            TRUNCATE TABLE bronze.crm_prd_info
        
   		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK
        INSERT
            bronze.crm_prd_info
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_crm\prd_info.csv'
        With (
                -- specify the format of the source file
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + 'seconds';



        Print '*************************************************'
        SET @start_date = GETDATE();
        IF (SELECT Count(*) FROM bronze.crm_sales_details) IS NOT NULL
            PRINT '>> Truncating Table: bronze.crm_sales_datails';
            TRUNCATE TABLE bronze.crm_sales_details

   		PRINT '>> Inserting Data Into: bronze.crm_sales_datails';
        BULK
        INSERT
            bronze.crm_sales_details
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_crm\sales_details.csv'
        With (
                -- specify the format of the source file
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + 'seconds';
        Print ''
        Print '================================================='
        Print 'Loading ERP Tables';
        Print '================================================='
        
        SET @start_date = GETDATE();
        PRINT ''
        IF (SELECT Count(*) FROM bronze.erp_cust_az12) IS NOT NULL
            PRINT '>> Truncating Table: bronze.erp_cust_az12';
            TRUNCATE TABLE bronze.erp_cust_az12

   		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK
        INSERT
            bronze.erp_cust_az12
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_erp\CUST_AZ12.csv'
        With (
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + 'seconds';
        Print '*************************************************'
        SET @start_date = GETDATE();
        IF (SELECT Count(*) FROM bronze.erp_loc_a101) IS NOT NULL
            PRINT '>> Truncating Table: bronze.erp_loc_a101';
            TRUNCATE TABLE bronze.erp_loc_a101

   		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK
        INSERT
            bronze.erp_loc_a101
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_erp\LOC_A101.csv'
        With (
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + 'seconds';
        Print '*************************************************'
        SET @start_date = GETDATE();
        IF (SELECT Count(*) FROM bronze.erp_px_cat_g1v2) IS NOT NULL
            PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
            TRUNCATE TABLE bronze.erp_px_cat_g1v2

   		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK
        INSERT
            bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Oussama Pro\Downloads\DE Path\My DE Projects\SQL_Data_Warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
        With (
                Firstrow = 2,
                Fieldterminator = ',',
                Tablock
            );
        SET @end_date = GETDATE();
        PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR) + ' seconds';
        SET @end_date_all = GETDATE();
        PRINT 'LOAD DURATION OF ALL THE PROCESS :' + CAST(DATEDIFF(millisecond, @start_date_all, @end_date_all)*0.001 AS NVARCHAR) + ' seconds';

    END TRY
    BEGIN CATCH
    PRINT '========================';
    PRINT 'ERROR OCCURED DURING LOADING !!';
    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
    PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'ERROR STATE' + CAST(ERROR_STATE() AS NVARCHAR );
    -- we can also add here insert to a log table
    PRINT '========================';

    END CATCH 

END;