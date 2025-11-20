CREATE OR ALTER PROCEDURE TruncateData(@DatabaseName NVARCHAR(128), @SchemaName NVARCHAR(128), @TableName NVARCHAR(128))
AS
BEGIN
    DECLARE @Sql NVARCHAR(MAX)
    SET @Sql = 'TRUNCATE TABLE ' + QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)
    EXEC sp_executesql @Sql
END



CREATE OR ALTER PROCEDURE SP_InsertRAW
    @DatabaseName NVARCHAR(255),
    @SchemaName NVARCHAR(255),
    @TableName NVARCHAR(255),
    @FilePath VARCHAR(255),
    @TruncateBeforeInsert BIT = 1 
AS
BEGIN
	PRINT 'Table ' + @TableName + ' has been truncated before inserting new data.';
	print('Loading data in : '+@TableName)

    SET NOCOUNT ON;

    DECLARE @BulkInsertSQL NVARCHAR(MAX);
    DECLARE @RowCount INT;
    DECLARE @FileName NVARCHAR(255);
    DECLARE @FullTableName NVARCHAR(255);

    BEGIN TRY
	
        -- Extract just the file name from full path (for logging)
        SET @FileName = RIGHT(@FilePath, CHARINDEX('\', REVERSE(@FilePath)) - 1);

        -- Combine database, schema, and table name into a full table name
        SET @FullTableName = QUOTENAME(@DatabaseName) + '.' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

        -- Optionally truncate the table before bulk insert
        IF @TruncateBeforeInsert = 1
        BEGIN
            EXEC TruncateData @DatabaseName, @SchemaName, @TableName;
        END

        -- Build dynamic bulk insert query
        SET @BulkInsertSQL = '
            BULK INSERT ' + @FullTableName +
            ' FROM ' + QUOTENAME(@FilePath, '''') + '
            WITH (
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''\n'',
                FIRSTROW = 2
            )';

        -- Execute bulk insert
        EXEC sp_executesql @BulkInsertSQL;

        -- Get inserted row count
        SET @RowCount = @@ROWCOUNT;
        PRINT 'File: ' + @FileName + ' --> ' + CAST(@RowCount AS VARCHAR(20)) + ' rows inserted.';
		print(' ')
    END TRY
    BEGIN CATCH
        PRINT 'Error while inserting data from file.';
        PRINT 'File: ' + ISNULL(@FileName, @FilePath);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
    END CATCH
END;

CREATE OR ALTER PROCEDURE SP_ProcHealthConnect_RawLoad
AS
BEGIN
    SET NOCOUNT ON;

    -- Get yesterday's date in YYYYMMDD format
    DECLARE @DatePart VARCHAR(20) = CONVERT(VARCHAR(10), DATEADD(DAY, -1, GETDATE()), 112);

    -- Declare file paths with dynamic date
    DECLARE 
        @claimsFile       VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\claims_' + @DatePart + '.csv',
        @diagnosesFile    VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\diagnoses_' + @DatePart + '.csv',
        @encountersFile   VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\encounters_' + @DatePart + '.csv',
        @medicationsFile  VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\medications_' + @DatePart + '.csv',
        @patientsFile     VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\patients_' + @DatePart + '.csv',
        @payersFile       VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\payers_' + @DatePart + '.csv',
        @proceduresFile   VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\procedures_' + @DatePart + '.csv',
        @providersFile    VARCHAR(255) = 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\dataset1\providers_' + @DatePart + '.csv';

    -- Execute SP_InsertRAW for each file
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_claims',      @FilePath=@claimsFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_diagnoses',   @FilePath=@diagnosesFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_encounters',  @FilePath=@encountersFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_medications', @FilePath=@medicationsFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_patients',    @FilePath=@patientsFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_payers',      @FilePath=@payersFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_procedures',  @FilePath=@proceduresFile;
    EXEC SP_InsertRAW @DatabaseName='dev_HealthConnect_raw', @SchemaName='dbo', @TableName='raw_providers',   @FilePath=@providersFile;

    PRINT '';
    PRINT 'Raw layer processed successfully.';
END;


