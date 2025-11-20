
CREATE OR ALTER PROCEDURE pro_healthconnect_refined_2 AS
BEGIN
  SET NOCOUNT ON;   
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME
	DECLARE @ROW_COUNT INT
		BEGIN TRY
		SET @batch_start_time = GETDATE()

		PRINT '====================================================================================';
		PRINT 'REFINED TABLES';
		PRINT '====================================================================================';
--==========================================================================================================
				--refined_DW.refined_ claimns table
--==========================================================================================================

				MERGE refined_DW.refined_claims AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_claims AS S
				ON T.claim_id = S.claim_id
				WHEN MATCHED AND (
					T.total_billed_amount <> S.total_billed_amount OR
					T.total_allowed_amount <> S.total_allowed_amount OR
					T.total_paid_amount <> S.total_paid_amount OR
					T.claim_status <> S.claim_status
				)
				THEN
					UPDATE SET
						T.total_billed_amount = S.total_billed_amount,
						T.total_allowed_amount = S.total_allowed_amount,
						T.total_paid_amount = S.total_paid_amount,
						T.claim_status = S.claim_status,
						T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
					INSERT (
						claim_id, 
						encounter_id, 
						payer_id, 
						admit_date, 
						discharge_date,
						total_billed_amount,
						total_allowed_amount,
						total_paid_amount,
						claim_status,
						load_date,
						record_updated_date
					)
					VALUES (
						S.claim_id, 
						S.encounter_id, 
						S.payer_id, 
						S.admit_date, 
						S.discharge_date,
						S.total_billed_amount,
						S.total_allowed_amount,
						S.total_paid_amount,
						S.claim_status,
						S.loadDate,
						GETDATE()
					);

					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_claims TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';
--==========================================================================================================
				--refined diagnoses table
--==========================================================================================================
				Merge refined_DW.refined_diagnoses AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_diagnoses AS S
				ON T.diagnosis_id = S.diagnosis_id
				WHEN MATChED AND(
						T.diagnosis_description <> S.diagnosis_description or
						T.is_primary <> S.is_primary
				)
				THEN UPDATE SET
							T.diagnosis_description = S.diagnosis_description,
							T.is_primary = S.is_primary,
							T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT (
						diagnosis_id, 
						encounter_id,
						diagnosis_description,
						is_primary,
						load_date,
						record_updated_date
				)
				VALUES(
						S.diagnosis_id, 
						S.encounter_id,
						S.diagnosis_description,
						S.is_primary,
						S.LoadDate,
						GETDATE()
				);
				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_diagnoses TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

				
--====================================================================================================================
				--refined encounter tables
--====================================================================================================================

				MERGE refined_DW.refined_encounters AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_encounters AS S
				ON T.encounter_id = S.encounter_id
				WHEN MATCHED 
							AND(
								T.weight_kg <> S.weight_kg OR
								T.systolic_bp <> S.systolic_bp OR
								T.diastolic_bp <> S.diastolic_bp
							)
							THEN UPDATE SET
											T.weight_kg = S.weight_kg,
											T.systolic_bp = S.systolic_bp,
											T.diastolic_bp = S.diastolic_bp,
											T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT(
						encounter_id,
						patient_id,
						provider_id,
						encounter_type,
						encounter_start,
						encounter_end,
						height_cm,
						weight_kg,
						systolic_bp,
						diastolic_bp,
						load_date,
						record_updated_date
				)
				VALUES(
						S.encounter_id,
						S.patient_id,
						S.provider_id,
						S.encounter_type,
						S.encounter_start,
						S.encounter_end,
						S.height_cm,
						S.weight_kg,
						S.systolic_bp,
						S.diastolic_bp,
						S.loaddate,
						GETDATE()
				);
				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_encounters TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

			
--======================================================================================================
				-- REFINED MEDICATIONS TABLE
--======================================================================================================
				MERGE refined_DW.refined_medications AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_medications AS S
				ON T.medication_id = S.medication_id
				WHEN MATCHED 
							AND(
								T.drug_name <> S.drug_name OR
								T.route <> S.route OR
								T.dose <> S.dose OR
								T.frequency <> S.frequency OR
								T.days_supply <> S.days_supply
							) THEN UPDATE SET
								T.drug_name = S.drug_name,
								T.route = S.route,
								T.dose = S.dose,
								T.frequency = S.frequency,
								T.days_supply = S.days_supply,
								T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT (
						medication_id,
						encounter_id,
						drug_name,
						route,
						dose,
						frequency,
						days_supply,
						load_date,
						record_updated_date
				)
				VALUES(
						S.medication_id,
						S.encounter_id,
						S.drug_name,
						S.route,
						S.dose,
						S.frequency,
						S.days_supply,
						S.loadDate,
						GETDATE()
				);
				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_medications TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

--=====================================================================================================
				--refined patients table
--=====================================================================================================
				Merge refined_DW.refined_patients AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_patients AS S
				ON T.patient_id = S.patient_id
				WHEN MATCHED 
							AND(
								T.last_name <> S.last_name OR
								T.state_code <> S.state_code OR
								T.city <> S.city OR
								T.phone <> S.phone
							) THEN UPDATE SET	
								T.last_name = S.last_name,
								T.state_code = S.state_code,
								T.city = S.city,
								T.phone = S.phone,
								T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT(
						patient_id,
						first_name,
						last_name,
						gender,
						date_of_birth,
						state_code,
						city,
						phone,
						load_date,
						record_updated_date
				)
				VALUES(
						S.patient_id,
						S.first_name,
						S.last_name,
						S.gender,
						S.date_of_birth,
						S.state_code,
						S.city,
						S.phone,
						S.loaddate,
						GETDATE()
				);

				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_patients TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

--=======================================================================================================
				--REFINED PAYERS TABLE
--=======================================================================================================
				MERGE refined_DW.refined_payers AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_payers AS S
				ON T.payer_id = S.payer_id
				WHEN MATCHED 
							AND(
							T.payer_name <> S.payer_name 
							)THEN UPDATE SET
											T.payer_name = S.payer_name,
											T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT (
						payer_id,
						payer_name,
						load_date,
						record_updated_date
				)
				VALUES(
						S.payer_id,
						S.payer_name,
						S.loaddate,
						GETDATE()
				);

				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_payers TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

--=======================================================================================================
				--REFINED PROCEDURES TABLE
--=======================================================================================================
				MERGE refined_DW.refined_procedures AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_procedures AS S
				ON T.procedure_id = S.procedure_id
				WHEN MATCHED 
							AND(
								T.procedure_description <> S.procedure_description
							) THEN UPDATE SET
								T.procedure_description = S.procedure_description,
								T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT (
						procedure_id,
						encounter_id,
						procedure_description,
						load_date,
						record_updated_date
				)
				VALUES(
						S.procedure_id,
						S.encounter_id,
						S.procedure_description,
						S.loaddate,
						GETDATE()
				);

				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_procedures TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';
--======================================================================================================
				--REFINED PROVIDERS TABLE
--======================================================================================================
				MERGE refined_DW.refined_providers AS T
				USING dev_healthconnect_cleansed.dbo.cleansed_providers AS S
				ON T.provider_id = S.provider_id
				WHEN MATCHED 
							AND(
								T.last_name <> S.last_name OR
								T.specialty <> S.specialty OR
								T.npi <> S.npi
							) THEN UPDATE SET
								T.last_name = S.last_name,
								T.specialty = S.specialty,
								T.npi = S.npi,
								T.record_updated_date = GETDATE()
				WHEN NOT MATCHED BY TARGET THEN
				INSERT(
						provider_id,
						first_name,
						last_name,
						specialty,
						npi,
						load_date,
						record_updated_date
				)
				VALUES(
						provider_id,
						first_name,
						last_name,
						specialty,
						npi,
						loaddate,
						GETDATE()
				);

				
					SET @ROW_COUNT = @@ROWCOUNT
PRINT 'CLEANSED_LAYER -----> refined_DW.refined_LAYER '+ CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN refined_DW.refined_providers TABLE'
					PRINT '-------------------------------------------------------------------------------------------------------------';

--===================================================================================
			SET @batch_end_time = GETDATE()
			PRINT '>>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR)+ 'SECONDS';

		END TRY
		BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING'
		PRINT 'ERROR_MESSAGE: '+ ERROR_MESSAGE();
		PRINT 'ERROR_MESSAGE: '+ CAST(ERROR_NUMBER()AS VARCHAR);
		PRINT 'ERROR_MESSAGE: '+ CAST(ERROR_STATE() AS VARCHAR);
		END CATCH
END
