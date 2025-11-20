CREATE OR ALTER PROCEDURE pro_healthconnect_refined_1 AS
BEGIN
    SET NOCOUNT ON;   
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '====================================================================================';
        PRINT 'REFINED TABLES';
        PRINT '====================================================================================';

        ------------------------------------------------------------------
        -- Refined Claims Table
        ------------------------------------------------------------------
        DECLARE @Claims_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_claims AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_claims AS S
        ON T.claim_id = S.claim_id
        WHEN MATCHED AND (
            T.total_billed_amount <> S.total_billed_amount OR
            T.total_allowed_amount <> S.total_allowed_amount OR
            T.total_paid_amount <> S.total_paid_amount OR
            T.claim_status <> S.claim_status
        )
        THEN UPDATE SET
            T.total_billed_amount = S.total_billed_amount,
            T.total_allowed_amount = S.total_allowed_amount,
            T.total_paid_amount = S.total_paid_amount,
            T.claim_status = S.claim_status,
            T.record_updated_date = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                claim_id, encounter_id, payer_id, admit_date, discharge_date,
                total_billed_amount, total_allowed_amount, total_paid_amount,
                claim_status, load_date, record_updated_date
            )
            VALUES (
                S.claim_id, S.encounter_id, S.payer_id, S.admit_date, S.discharge_date,
                S.total_billed_amount, S.total_allowed_amount, S.total_paid_amount,
                S.claim_status, S.LoadDate, GETDATE()
            )
        OUTPUT $action INTO @Claims_Actions;

        DECLARE @Claims_Insert INT = (SELECT COUNT(*) FROM @Claims_Actions WHERE ActionType = 'INSERT');
        DECLARE @Claims_Update INT = (SELECT COUNT(*) FROM @Claims_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_claims - INSERT: ' + CAST(@Claims_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Claims_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';

        ------------------------------------------------------------------
        -- Refined Diagnoses Table
        ------------------------------------------------------------------
        DECLARE @Diagnoses_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_diagnoses AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_diagnoses AS S
        ON T.diagnosis_id = S.diagnosis_id
        WHEN MATCHED AND (
            T.diagnosis_description <> S.diagnosis_description OR
            T.is_primary <> S.is_primary
        )
        THEN UPDATE SET
            T.diagnosis_description = S.diagnosis_description,
            T.is_primary = S.is_primary,
            T.record_updated_date = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (diagnosis_id, encounter_id, diagnosis_description, is_primary, load_date, record_updated_date)
            VALUES (S.diagnosis_id, S.encounter_id, S.diagnosis_description, S.is_primary, S.LoadDate, GETDATE())
        OUTPUT $action INTO @Diagnoses_Actions;

        DECLARE @Diagnoses_Insert INT = (SELECT COUNT(*) FROM @Diagnoses_Actions WHERE ActionType = 'INSERT');
        DECLARE @Diagnoses_Update INT = (SELECT COUNT(*) FROM @Diagnoses_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_diagnoses - INSERT: ' + CAST(@Diagnoses_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Diagnoses_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';

        ------------------------------------------------------------------
        -- Refined Encounters Table
        ------------------------------------------------------------------
        DECLARE @Encounters_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_encounters AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_encounters AS S
        ON T.encounter_id = S.encounter_id
        WHEN MATCHED AND (
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
            INSERT (encounter_id, patient_id, provider_id, encounter_type, encounter_start, encounter_end,
                    height_cm, weight_kg, systolic_bp, diastolic_bp, load_date, record_updated_date)
            VALUES (S.encounter_id, S.patient_id, S.provider_id, S.encounter_type, S.encounter_start, S.encounter_end,
                    S.height_cm, S.weight_kg, S.systolic_bp, S.diastolic_bp, S.LoadDate, GETDATE())
        OUTPUT $action INTO @Encounters_Actions;

        DECLARE @Encounters_Insert INT = (SELECT COUNT(*) FROM @Encounters_Actions WHERE ActionType = 'INSERT');
        DECLARE @Encounters_Update INT = (SELECT COUNT(*) FROM @Encounters_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_encounters - INSERT: ' + CAST(@Encounters_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Encounters_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';

        ------------------------------------------------------------------
        -- Refined Medications Table
        ------------------------------------------------------------------
        DECLARE @Medications_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_medications AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_medications AS S
        ON T.medication_id = S.medication_id
        WHEN MATCHED AND (
            T.drug_name <> S.drug_name OR
            T.route <> S.route OR
            T.dose <> S.dose OR
            T.frequency <> S.frequency OR
            T.days_supply <> S.days_supply
        )
        THEN UPDATE SET
            T.drug_name = S.drug_name,
            T.route = S.route,
            T.dose = S.dose,
            T.frequency = S.frequency,
            T.days_supply = S.days_supply,
            T.record_updated_date = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (medication_id, encounter_id, drug_name, route, dose, frequency, days_supply, load_date, record_updated_date)
            VALUES (S.medication_id, S.encounter_id, S.drug_name, S.route, S.dose, S.frequency, S.days_supply, S.LoadDate, GETDATE())
        OUTPUT $action INTO @Medications_Actions;

        DECLARE @Medications_Insert INT = (SELECT COUNT(*) FROM @Medications_Actions WHERE ActionType = 'INSERT');
        DECLARE @Medications_Update INT = (SELECT COUNT(*) FROM @Medications_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_medications - INSERT: ' + CAST(@Medications_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Medications_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';

        ------------------------------------------------------------------
        -- Refined Patients Table
        ------------------------------------------------------------------
        DECLARE @Patients_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_patients AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_patients AS S
        ON T.patient_id = S.patient_id
        WHEN MATCHED AND (
            T.last_name <> S.last_name OR
            T.state_code <> S.state_code OR
            T.city <> S.city OR
            T.phone <> S.phone
        )
        THEN UPDATE SET
            T.last_name = S.last_name,
            T.state_code = S.state_code,
            T.city = S.city,
            T.phone = S.phone,
            T.record_updated_date = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (patient_id, first_name, last_name, gender, date_of_birth, state_code, city, phone, load_date, record_updated_date)
            VALUES (S.patient_id, S.first_name, S.last_name, S.gender, S.date_of_birth, S.state_code, S.city, S.phone, S.LoadDate, GETDATE())
        OUTPUT $action INTO @Patients_Actions;

        DECLARE @Patients_Insert INT = (SELECT COUNT(*) FROM @Patients_Actions WHERE ActionType = 'INSERT');
        DECLARE @Patients_Update INT = (SELECT COUNT(*) FROM @Patients_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_patients - INSERT: ' + CAST(@Patients_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Patients_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';

        ------------------------------------------------------------------
        -- Refined Providers Table
        ------------------------------------------------------------------
        DECLARE @Providers_Actions TABLE (ActionType VARCHAR(20));
        MERGE refined_providers AS T
        USING dev_healthconnect_cleansed.dbo.cleansed_providers AS S
        ON T.provider_id = S.provider_id
        WHEN MATCHED AND (
            T.last_name <> S.last_name OR
            T.specialty <> S.specialty OR
            T.npi <> S.npi
        )
        THEN UPDATE SET
            T.last_name = S.last_name,
            T.specialty = S.specialty,
            T.npi = S.npi,
            T.record_updated_date = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (provider_id, first_name, last_name, specialty, npi, load_date, record_updated_date)
            VALUES (S.provider_id, S.first_name, S.last_name, S.specialty, S.npi, S.LoadDate, GETDATE())
        OUTPUT $action INTO @Providers_Actions;

        DECLARE @Providers_Insert INT = (SELECT COUNT(*) FROM @Providers_Actions WHERE ActionType = 'INSERT');
        DECLARE @Providers_Update INT = (SELECT COUNT(*) FROM @Providers_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_providers - INSERT: ' + CAST(@Providers_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Providers_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';
		
        ------------------------------------------------------------------
        -- Refined Payers Table
        ------------------------------------------------------------------
		DECLARE @Payers_Actions TABLE (ActionType VARCHAR(20));
			MERGE refined_payers AS T
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
				)
		        OUTPUT $action INTO @Payers_Actions;

        DECLARE @Payers_Insert INT = (SELECT COUNT(*) FROM @Payers_Actions WHERE ActionType = 'INSERT');
        DECLARE @Payers_Update INT = (SELECT COUNT(*) FROM @Payers_Actions WHERE ActionType = 'UPDATE');
        PRINT 'refined_providers - INSERT: ' + CAST(@Payers_Insert AS VARCHAR) + ', UPDATE: ' + CAST(@Payers_Update AS VARCHAR);
        PRINT '-------------------------------------------------------------------------------------------------------------';
		
        ------------------------------------------------------------------
        -- Refined Procedures Table
        ------------------------------------------------------------------
		Declare @Procedures_Actions Table (ActionType varchar(20))

		MERGE refined_procedures AS T
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
				)
				OUTPUT $action INTO @Procedures_Actions;

				DECLARE @Procedure_Insert INT = (SELECT count(*) FROM @Procedures_Actions WHERE ActionType = 'INSERT');
				DECLARE @Procedure_Update INT = (SELECT count(*) FROM @Procedures_Actions WHERE ActionType = 'UPDATE');

			PRINT 'refined_Procedure - INSERT: '+ CAST(@Procedure_Insert as varchar) + ', UPDATE: '+ CAST(@Procedure_Update as Varchar);
        ------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '>>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR)+ ' SECONDS';

    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED DURING LOADING';
        PRINT 'ERROR_MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR_NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'ERROR_STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
    END CATCH
END
