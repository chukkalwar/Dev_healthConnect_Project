
IF OBJECT_ID('raw_claims', 'U') IS NOT NULL
	DROP TABLE raw_claims;

CREATE TABLE raw_claims(
		claim_id INT,
		encounter_id INT,
		payer_id INT,
		admit_date date,
		discharge_date date,
		total_billed_amount DECIMAL(18,2),
		total_allowed_amount DECIMAL(18,2),
		total_paid_amount DECIMAL(18,2),
		claim_status Varchar(50)
);


IF OBJECT_ID('raw_diagnoses', 'U') IS NOT NULL
	DROP TABLE raw_diagnoses;

CREATE TABLE raw_diagnoses(
		diagnosis_id INT,
		encounter_id INT,
		diagnosis_description VARCHAR(50),
		is_primary INT
);



IF OBJECT_ID('raw_encounters', 'U') IS NOT NULL
		DROP TABLE raw_encounters;

CREATE TABLE raw_encounters(
		encounter_id INT,
		patient_id INT,
		provider_id INT,
		encounter_type varchar(50),
		encounter_start Date,
		encounter_end Date,
		height_cm INT,
		weight_kg DECIMAL(10,2),
		systolic_bp INT,
		diastolic_bp INT
);



IF OBJECT_ID('raw_medications', 'U') IS NOT NULL
		DROP TABLE raw_medications;

CREATE TABLE raw_medications(
			medication_id INT,
			encounter_id INT,
			drug_name VARCHAR(50),
			route Varchar(50),
			dose varchar(50),
			frequency varchar(50),
			days_supply INT
);



IF OBJECT_ID('raw_patients','U') IS NOT NULL 
		DROP TABLE raw_patients;

CREATE TABLE raw_patients(
			patient_id INT,
			first_name Varchar(50),
			last_name Varchar(50),
			gender Varchar(10),
			date_of_birth varchar(50),
			state_code Varchar(50),
			city varchar(50),
			phone varchar(50)
);



IF OBJECT_ID('raw_payers', 'U') IS NOT NULL
		DROP TABLE raw_payers;

CREATE TABLE raw_payers(
		payer_id INT,
		Payer_name Varchar(50)
);




IF OBJECT_ID('raw_procedures', 'U') IS NOT NULL
		DROP TABLE raw_procedures;

CREATE TABLE raw_procedures(
		procedure_id INT,
		encounter_id INT,
		procedure_description Varchar(50)
)


IF OBJECT_ID('raw_providers', 'U') IS NOT NULL
		DROP TABLE raw_providers;

CREATE TABLE raw_providers(
		provider_id INT,
		first_name Varchar(50),
		last_name Varchar(50),
		specialty Varchar(50),
		npi Varchar(50)
)
