CREATE OR ALTER PROCEDURE pro_healthconnect_cleansed AS
BEGIN
      SET NOCOUNT ON;
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		DECLARE @ROW_COUNT INT
		BEGIN TRY
			SET @batch_start_time = GETDATE();
--==========================================================================================================
				PRINT '=============================================================================';
				PRINT 'CLEANSED TABLES';
				PRINT '=============================================================================';
					--=========================================delete data from the tables=============================================
				----PRINT '=====================================================';
				PRINT 'TRUNCATE TABLES';
				----PRINT '=====================================================';
					 TRUNCATE TABLE  cleansed_medications;  PRINT 'TRUNCATED: cleansed_medications';
					 TRUNCATE TABLE  cleansed_diagnoses;  PRINT 'TRUNCATED: cleansed_diagnoses';
					 TRUNCATE TABLE  cleansed_claims; PRINT 'TRUNCATED: cleansed_claims';
					 TRUNCATE TABLE  cleansed_procedures; PRINT 'TRUNCATED: cleansed_procedures';
					 TRUNCATE TABLE cleansed_encounters; PRINT 'TRUNCATED: cleansed_encounters';
					 TRUNCATE TABLE cleansed_patients; PRINT 'TRUNCATED: cleansed_patients';
					 TRUNCATE TABLE cleansed_providers; PRINT 'TRUNCATED: cleansed_providers';
					 TRUNCATE TABLE cleansed_payers; PRINT 'TRUNCATED: cleansed_payers';

			PRINT '-----------------------------------------------------------------------'
--======================parents_table=============================================
			
				--PRINT '=====================================================';
				--PRINT 'PAYERS TABLE';
				--PRINT '=====================================================';
				SET @start_time = GETDATE();
					Insert into cleansed_payers(
							payer_id,
							Payer_name
					)select 
						   payer_id,
						   UPPER(TRIM(payer_name)) AS payer_name
					 from dev_healthconnect_raw.dbo.raw_payers
				
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_payers TABLE'

				SET @end_time = GETDATE();
				
				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
--------------------------------------------------------------------------------------------------------------------
					
				--PRINT '=====================================================';
				--PRINT 'PROVIDERS TABLE';
				--PRINT '=====================================================';

				SET @start_time = GETDATE();

					INSERT INTO cleansed_providers(
							provider_id,
							first_name,
							last_name,
							specialty,
							npi
					 )SELECT 
							provider_id,
							UPPER(TRIM(first_name)) AS first_name,
							UPPER(TRIM(last_name)) AS last_name,
							UPPER(TRIM(specialty)) AS specialty,
							npi
					  FROM dev_healthconnect_raw.dbo.raw_providers
					  
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_providers TABLE'


				SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
-----------------------------------------------------------------------------------------------------------------
					
				--PRINT '=====================================================';
				--PRINT 'PATIENTS TABLE';
				--PRINT '=====================================================';
					
				SET @start_time = GETDATE();

					INSERT INTO cleansed_patients (
						patient_id,
						first_name,
						last_name,
						gender,
						date_of_birth,
						state_code,
						city,
						phone
					)SELECT
						patient_id,
						UPPER(TRIM(first_name)) AS first_name,
						UPPER(TRIM(last_name)) AS last_name,
						CASE 
							WHEN UPPER(TRIM(gender)) = 'M' THEN 'Male'
							WHEN UPPER(TRIM(gender)) = 'F' THEN 'Female'
							WHEN UPPER(TRIM(gender)) = 'O' THEN 'Other'
							ELSE 'N/A'
						END AS gender,
						CONVERT(DATE, date_of_birth, 105) AS date_of_birth, 
						UPPER(TRIM(state_code)) AS state_code,
						UPPER(TRIM(city)) AS city,
						phone
					FROM dev_healthconnect_raw.dbo.raw_patients;

					
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_patients TABLE'

				
				SET @end_time = GETDATE();
				
				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
--------------------------------------------------------------------------------------------------------------------
 
				--PRINT '=====================================================';
				--PRINT 'PROCEDURES TABLES';
				--PRINT '=====================================================';

				SET @start_time = GETDATE();

					INSERT INTO cleansed_procedures(
							procedure_id,
							encounter_id,
							procedure_description
					)SELECT
							CAST(TRIM(CAST(procedure_id AS VARCHAR)) AS INT),
							CAST(TRIM(CAST(encounter_id AS VARCHAR)) AS INT),
							UPPER(TRIM(procedure_description)) AS procedure_description
					FROM dev_healthconnect_raw.dbo.raw_procedures;

					
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_procedures TABLE'

				
				SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
----------------------------------------------------------------------------------------------------------
--===================================CHILD TABLES===========================================================					
					
				--PRINT '=====================================================';
				--PRINT 'ENCOUNTERS TABLE';
				--PRINT '=====================================================';
				
				SET @start_time = GETDATE();

					INSERT INTO cleansed_encounters(
						encounter_id,
						patient_id,
						provider_id,
						encounter_type,
						encounter_start,
						encounter_end,
						height_cm,
						weight_kg,
						systolic_bp,
						diastolic_bp
					)SELECT 
						encounter_id,
						patient_id,
						provider_id,
						UPPER(TRIM(encounter_type)) AS encounter_type,
						encounter_start,
						encounter_end,
						height_cm,
						weight_kg,
						systolic_bp,
						diastolic_bp
					 FROM dev_healthconnect_raw.dbo.raw_encounters;

					 
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_encounters TABLE'

				SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
-------------------------------------------------------------------------------------------------------------------
					
				--PRINT '=====================================================';
				--PRINT 'CLAIMS TABLE';
				--PRINT '=====================================================';
					
				SET @start_time = GETDATE();

					INSERT INTO cleansed_claims(
							claim_id,
							encounter_id,
							payer_id,
							admit_date,
							discharge_date,
							total_billed_amount,
							total_allowed_amount,
							total_paid_amount,
							claim_status
					)SELECT
							claim_id,
							encounter_id,
							payer_id,
							FORMAT(CONVERT(DATEtime, admit_date, 105), 'yyyy-MM-dd') as admit_date,
							FORMAT(CONVERT(DATEtime, discharge_date, 105), 'yyyy-MM-dd') as discharge_date,							
							total_billed_amount,
							total_allowed_amount,
							total_paid_amount,
							UPPER(TRIM(claim_status))AS claim_status
					FROM dev_healthconnect_raw.dbo.raw_claims;

					
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_claims TABLE'


				SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
-------------------------------------------------------------------------------------------------------

					
				--PRINT '=====================================================';
				--PRINT 'DIAGNOSES TABLE';
				--PRINT '=====================================================';

				SET @start_time = GETDATE();

					INSERT INTO cleansed_diagnoses( 
						 diagnosis_id,
						 encounter_id,
						 diagnosis_description,
						 is_primary
					)SELECT 
						 diagnosis_id,
						 encounter_id,
						 UPPER(TRIM(diagnosis_description)) AS diagnosis_description,
						 is_primary
					 FROM dev_healthconnect_raw.dbo.raw_diagnoses;

					 
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_diagnoses TABLE'

				
				SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '------------------------------------------------------------------------------------'
----------------------------------------------------------------------------------------------------------------------------
					
				--PRINT '=====================================================';
				--PRINT 'MEDICATIONS TABLE';
				--PRINT '=====================================================';
				
				SET @start_time = GETDATE();

					INSERT INTO cleansed_medications(
						   medication_id,
						   encounter_id,
						   drug_name,
						   route,
						   dose,
						   frequency,
						   days_supply
					)SELECT
						   medication_id,
						   encounter_id,
						   UPPER(TRIM(drug_name)) AS drug_name,
						   UPPER(TRIM(route)) AS route,
						   UPPER(TRIM(dose)) AS dose,
						   frequency,
						   days_supply
					FROM dev_healthconnect_raw.dbo.raw_medications;

					
				SET @ROW_COUNT = @@ROWCOUNT
				PRINT 'RAW_LAYER -----> CLEANSED_LAYER '+ CAST(@ROW_COUNT AS VARCHAR) + ' ROWS TRANSFERED IN cleansed_medications TABLE'


					SET @end_time = GETDATE();

				PRINT '>>> LOAD_TIME: '+CAST(DATEDIFF(SECOND, @start_time, @end_time)AS VARCHAR)+ 'seconds'
				PRINT '----------------------------------------------------------------------------------'


			SET @batch_end_time = GETDATE();

			PRINT '>>> BATCH_LOAD_TIME: '+CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time)AS VARCHAR)+ 'seconds'


		END TRY
		BEGIN CATCH
		
				--PRINT '=====================================================';
				PRINT 'ERROR OCCURED IN TABLES';
				--PRINT '=====================================================';

				PRINT 'ERROR_MESSAGE: '+ ERROR_MESSAGE();
				PRINT 'ERROR_NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
				PRINT 'ERROR_STATE: ' + CAST(ERROR_STATE() AS VARCHAR);


		END CATCH
END