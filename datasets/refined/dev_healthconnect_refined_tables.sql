

IF OBJECT_ID('refined_DW.refined_claims', 'U') IS NOT NULL
	DROP TABLE refined_DW.refined_claims;

CREATE TABLE refined_DW.refined_claims(
		claim_id INT,
		encounter_id INT,
		payer_id INT,
		admit_date Date,
		discharge_date DATE,
		total_billed_amount DECIMAL(18,2),
		total_allowed_amount DECIMAL(18,2),
		total_paid_amount DECIMAL(18,2),
		claim_status Varchar(50),
		load_date Datetime,
		record_updated_date DATETIME
);


IF OBJECT_ID('refined_DW.refined_diagnoses', 'U') IS NOT NULL
	DROP TABLE refined_DW.refined_diagnoses;

CREATE TABLE refined_DW.refined_diagnoses(
		diagnosis_id INT,
		encounter_id INT,
		diagnosis_description VARCHAR(50),
		is_primary INT,
		load_date Datetime,
		record_updated_date DATETIME
);



IF OBJECT_ID('refined_DW.refined_encounters', 'U') IS NOT NULL
		DROP TABLE refined_DW.refined_encounters;

CREATE TABLE refined_DW.refined_encounters(
		encounter_id INT,
		patient_id INT,
		provider_id INT,
		encounter_type varchar(50),
		encounter_start datetime,
		encounter_end datetime,
		height_cm INT,
		weight_kg DECIMAL(10,2),
		systolic_bp INT,
		diastolic_bp INT,
		load_date Datetime,
		record_updated_date DATETIME
);



IF OBJECT_ID('refined_DW.refined_medications', 'U') IS NOT NULL
		DROP TABLE refined_DW.refined_medications;

CREATE TABLE refined_DW.refined_medications(
			medication_id INT,
			encounter_id INT,
			drug_name VARCHAR(50),
			route Varchar(50),
			dose varchar(50),
			frequency varchar(50),
			days_supply INT,
		    load_date Datetime,
		    record_updated_date DATETIME
);



IF OBJECT_ID('refined_DW.refined_patients','U') IS NOT NULL 
		DROP TABLE refined_DW.refined_patients;

CREATE TABLE refined_DW.refined_patients(
			patient_id INT,
			first_name Varchar(50),
			last_name Varchar(50),
			gender Varchar(10),
			date_of_birth varchar(50),
			state_code Varchar(50),
			city varchar(50),
			phone varchar(50),
		    load_date Datetime ,
		    record_updated_date DATETIME
);



IF OBJECT_ID('refined_DW.refined_payers', 'U') IS NOT NULL
		DROP TABLE refined_DW.refined_payers;

CREATE TABLE refined_DW.refined_payers(
		payer_id INT,
		Payer_name Varchar(50),
		load_date Datetime,
		record_updated_date DATETIME
);




IF OBJECT_ID('refined_DW.refined_procedures', 'U') IS NOT NULL
		DROP TABLE refined_DW.refined_procedures;

CREATE TABLE refined_DW.refined_procedures(
		procedure_id INT,
		encounter_id INT,
		procedure_description Varchar(50),
		load_date Datetime,
		record_updated_date DATETIME
)


IF OBJECT_ID('refined_DW.refined_providers', 'U') IS NOT NULL
		DROP TABLE refined_DW.refined_providers;

CREATE TABLE refined_DW.refined_providers(
		provider_id INT,
		first_name Varchar(50),
		last_name Varchar(50),
		specialty Varchar(50),
		npi Varchar(50),
		load_date Datetime,
		record_updated_date DATETIME
)
