/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None. 
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;
===============================================================================
*/

/* Some Methods Definitions:
>> ROW_NUMBER() OVER * REQUIRED: Sort the data, then assign row numbers + OPTIONALLY: restarting per group.
>> The CASE expression is used to define different results based on specified conditions in an SQL statement.
>> LEAD() lets you access data from the next row (a future row) without doing a self-join [REQUIRED: ORDER BY defines the sequence of rows in a result set.]
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    BEGIN TRY
        Print '================================================='
        -- Cleaning bronze.crm_cust_info \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        PRINT '>> Truncating Table: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Cleaning and Inserting Data Into: silver.crm_cust_info';

        INSERT INTO
            silver.crm_cust_info (
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                cst_marital_status,
                cst_gndr,
                cst_create_date
            )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                ELSE 'Unknown'
            END cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'Unknown'
            END cst_gndr,
            cst_create_date
        FROM (
                -- CLEANING DUPLICATES (Let the flags equals 1)
                SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY
                            cst_id
                        ORDER BY cst_create_date DESC
                    ) as flag
                from bronze.crm_cust_info
            ) t
        Where
            flag = 1
            AND cst_id IS NOT NULL;
        Print '=================================================' 

        -- Cleaning bronze.crm_prd_info \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        PRINT '>> -------------';


        -- Cleaning bronze.crm_sales_details \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        
        
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
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
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        PRINT '>> -------------';

        -- Cleaning bronze.erp_loc_a101 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        Print '================================================='
        PRINT '>> Truncating Table: silver.erp_loc_a101';

        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Cleaning and Inserting Data Into: silver.erp_loc_a101';

        INSERT INTO
            silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') as cid,
            Case
                When Trim(cntry) is null
                or Upper(Trim(cntry)) like '' then 'Unknown'
                when Upper(Trim(cntry)) in ('US', 'USA') then 'United States'
                when Upper(Trim(cntry)) = 'DE' then 'Germany'
                else cntry
            end cntry
        From bronze.erp_loc_a101
        Print '================================================='

        -- Cleaning bronze.erp_px_cat_g1v2 \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        Print '================================================='
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Cleaning and Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO
            silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT *
        FROM bronze.erp_px_cat_g1v2
        Print '================================================='
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