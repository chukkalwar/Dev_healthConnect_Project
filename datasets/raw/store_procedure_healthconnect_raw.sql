

CREATE OR ALTER PROCEDURE pro_healthconnect_raw AS
BEGIN
  SET NOCOUNT ON;   
		DECLARE @start_time Datetime, @end_time datetime, @batch_start_time DATETIME, @batch_end_time DATETIME
		DECLARE @ROW_COUNT INT
		BEGIN TRY
				SET @batch_start_time = GETDATE();
--==========================================================================================================
				
				PRINT '=================================================================';
				PRINT 'RAW TABLES';
				PRINT '=================================================================';
						
				--PRINT '=====================================================';
				-- PRINT 'RAW_CLAIMS TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_CLAIMS TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_claims;
						
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO RAW_CLAIMS TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_claims
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\claims.csv'
						with (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						);

						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN raw_claims TABLES'

						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';

						PRINT '-----------------------------------------------------------------------------------------';
						--PRINT'=================================================================================================';
--========================================================================================================================
				--PRINT '=====================================================';
				-- PRINT 'RAW_DIAGNOSES TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_DIAGNOSES TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_diagnoses;
				--PRINT '=====================================================';
				-- PRINT 'INSERT DATA DATE INTO RAW_DIAGNOSES TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_diagnoses
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\diagnoses.csv'
						with (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_diagnoses TABLES'

						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
						--PRINT'=================================================================================================';
--========================================================================================================================
				--PRINT '=====================================================';
				-- PRINT 'RAW_ENCOUNTERS TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_ENCOUNTERS TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_encounters;
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO RAW_ENCOUNTERS TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_encounters
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\encounters.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_encounters TABLES'

						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
						
						--PRINT'=================================================================================================';
--========================================================================================================================
				--PRINT '=====================================================';
				-- PRINT 'RAW_MEDICATIONS TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_MEDICATIONS TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_medications;
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO RAW_MEDICATIONS TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_medications
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\medications.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_medications TABLES'
						
						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS'
						PRINT '-----------------------------------------------------------------------------------------';
						
						--PRINT'=================================================================================================';
--========================================================================================================================
				--PRINT '=====================================================';
				-- PRINT 'RAW_PATIENTS TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_PATIENTS TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_patients;
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO RAW_PATIENTS TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_patients
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\patients.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_patients TABLES'

						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
						
						--PRINT'=================================================================================================';
						
--========================================================================================================================

				--PRINT '=====================================================';
				-- PRINT 'RAW_PAYERS TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE RAW_PAYERS TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_payers;
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO RAW_PAYERS TABLE';
				--PRINT '=====================================================';
						BULK INSERT raw_payers
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\payers.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_payers TABLES'
						
						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
					
					--	PRINT'=================================================================================================';

--========================================================================================================================

				--PRINT '=====================================================';
				-- PRINT 'RAW_procedures TABLE';
				--PRINT '=====================================================';
						SET @start_time  = GETDATE();
				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE raw_procedures TABLE';
				--PRINT '=====================================================';
						TRUNCATE TABLE raw_procedures;
						
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO raw_procedures TABLE';
				--PRINT '=====================================================';

						BULK INSERT raw_procedures
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\procedures.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_procedures TABLES'

						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
						
						--PRINT'=================================================================================================';

--========================================================================================================================

				--PRINT '=====================================================';
				-- PRINT 'RAW_providers TABLE';
				--PRINT '=====================================================';
						SET @start_time = GETDATE();

				--PRINT '=====================================================';
				-- PRINT 'TRUNCATE raw_providers TABLE';
				--PRINT '=====================================================';

						TRUNCATE TABLE raw_providers;
						
				--PRINT '=====================================================';
			-- -- PRINT 'INSERT DATA DATA INTO raw_providers TABLE';
				--PRINT '=====================================================';

						BULK INSERT raw_providers
						FROM 'C:\Users\ASUS\OneDrive\Documents\HealthCareProject\datasets\providers.csv'
						WITH (
							FIRSTROW = 2,
							FIELDTERMINATOR = ',',
							TABLOCK
						)

						
						SET @ROW_COUNT = @@ROWCOUNT
						PRINT 'SOURCE ----> RAW_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN raw_providers TABLES'
						
						SET @end_time = GETDATE();
						PRINT '>>> LOADING TIME: '+CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR)+ 'SECONDS';
						
						PRINT '-----------------------------------------------------------------------------------------';
						
						--PRINT'=================================================================================================';
--========================================================================================================================
				SET @batch_end_time = GETDATE();
				PRINT '>>> LOADING BATCH PROCESSING TIME: '+CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR)+'SECONDS';
			END TRY
			BEGIN CATCH
				PRINT '=====================================================';
				PRINT ' ERROR OCCURED DURING THE PRO_HEALTHCONNECT_RAW_LOAD'
				PRINT '=====================================================';
				PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
				PRINT 'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS VARCHAR);
				PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS VARCHAR);
				PRINT '=====================================================';

			END CATCH
END