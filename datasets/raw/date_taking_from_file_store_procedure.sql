CREATE OR ALTER PROCEDURE pro_healthconnect_raw_1
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME,
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME,
            @ROW_COUNT INT,
            @sql NVARCHAR(MAX),
            @yesterday VARCHAR(8),
            @basePath NVARCHAR(500),
            @encountersFile NVARCHAR(500),
            @claimsFile NVARCHAR(500),
            @proceduresFile NVARCHAR(500),
            @providersFile NVARCHAR(500),
            @medicationsFile NVARCHAR(500),
            @patientsFile NVARCHAR(500),
            @payersFile NVARCHAR(500),
            @diagnosesFile NVARCHAR(500);

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        -- Get yesterday's date as YYYYMMDD
        SET @yesterday = CONVERT(VARCHAR(8), DATEADD(DAY, -1, GETDATE()), 112);

        -- Base folder path (use double backslash for network/shared path)
        SET @basePath = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\';

        -- File paths
        SET @encountersFile   = @basePath + 'encounters_'   + @yesterday + '.csv';
        SET @claimsFile       = @basePath + 'claims_'      + @yesterday + '.csv';
        SET @proceduresFile   = @basePath + 'procedures_'  + @yesterday + '.csv';
        SET @providersFile    = @basePath + 'providers_'   + @yesterday + '.csv';
        SET @medicationsFile  = @basePath + 'medications_' + @yesterday + '.csv';
        SET @patientsFile     = @basePath + 'patients_'    + @yesterday + '.csv';
        SET @payersFile       = @basePath + 'payers_'      + @yesterday + '.csv';
        SET @diagnosesFile    = @basePath + 'diagnoses_'   + @yesterday + '.csv';

        PRINT '=================================================================';
        PRINT 'RAW TABLES LOAD STARTED';
        PRINT '=================================================================';

        -----------------------------------------------------------------------------------------
        -- RAW_CLAIMS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_claims;
        SET @sql = N'BULK INSERT raw_claims FROM ''' + @claimsFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_claims';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_DIAGNOSES
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_diagnoses;
        SET @sql = N'BULK INSERT raw_diagnoses FROM ''' + @diagnosesFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_diagnoses';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_ENCOUNTERS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_encounters;
        SET @sql = N'BULK INSERT raw_encounters FROM ''' + @encountersFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_encounters';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_MEDICATIONS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_medications;
        SET @sql = N'BULK INSERT raw_medications FROM ''' + @medicationsFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_medications';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_PATIENTS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_patients;
        SET @sql = N'BULK INSERT raw_patients FROM ''' + @patientsFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_patients';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_PAYERS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_payers;
        SET @sql = N'BULK INSERT raw_payers FROM ''' + @payersFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_payers';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_PROCEDURES
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_procedures;
        SET @sql = N'BULK INSERT raw_procedures FROM ''' + @proceduresFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_procedures';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        -- RAW_PROVIDERS
        SET @start_time = GETDATE();
        TRUNCATE TABLE raw_providers;
        SET @sql = N'BULK INSERT raw_providers FROM ''' + @providersFile + ''' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK);';
        EXEC sp_executesql @sql;
        SET @ROW_COUNT = @@ROWCOUNT;
        PRINT 'SOURCE ----> RAW_LAYER: ' + CAST(@ROW_COUNT AS VARCHAR) + ' ROWS LOADED INTO raw_providers';
        SET @end_time = GETDATE();
        PRINT 'LOADING TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' SECONDS';
        PRINT '-----------------------------------------------------------------------------------------';

        -----------------------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT 'TOTAL BATCH LOADING TIME: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' SECONDS';
        PRINT '=================================================================';
        PRINT 'RAW TABLES LOAD COMPLETED';
        PRINT '=================================================================';

    END TRY
    BEGIN CATCH
        PRINT '=====================================================';
        PRINT ' ERROR OCCURRED DURING PRO_HEALTHCONNECT_RAW LOAD';
        PRINT '=====================================================';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '=====================================================';
    END CATCH
END;
