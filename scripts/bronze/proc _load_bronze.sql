-- Procedure to load all Bronze layer tables from flat CSV files
-- Handles CRM and ERP source files using BULK INSERT
-- Includes logging, timing, and error handling

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- Declare variables to track processing time
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        -- Start logging
        PRINT '===============================================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===============================================================================';

        -- Start of CRM Tables loading
        PRINT '-------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-------------------------------------------------------------------------------';

        -- Load bronze.crm_cust_info
        SET @start_time = GETDATE(); -- Record start time
        PRINT '>>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info; -- Clear old data
        PRINT 'Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2, -- Skip header
            FIELDTERMINATOR = ',', -- CSV delimiter
            TABLOCK -- Lock table for performance
        );
        SET @end_time = GETDATE(); -- Record end time
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- Load bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT 'Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- Load bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT 'Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- Start of ERP Tables loading
        PRINT '-------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-------------------------------------------------------------------------------';

        -- Load bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT 'Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- Load bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT 'Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- Load bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT 'Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Sql\dwl_project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> --------------------';

        -- End of batch loading
        SET @batch_end_time = GETDATE();
        PRINT '=================================================';
        PRINT 'Loading bronze layer is completed';
        PRINT '>>-Total Load  Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=================================================';

    END TRY

    BEGIN CATCH
        -- Error handler for any failure during BULK INSERT
        PRINT '================================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================================';
    END CATCH
END;
