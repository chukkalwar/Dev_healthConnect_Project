
--======================================================================================================================
--parents table
--======================================================================================================================

IF OBJECT_ID('cleansed_providers', 'U') IS NOT NULL
		DROP TABLE cleansed_providers;

CREATE TABLE cleansed_providers(
		provider_id INT primary key,
		first_name Varchar(50)NOT NULL,
		last_name Varchar(50)NOT NULL,
		specialty Varchar(50)NOT NULL,
		npi Varchar(50)NOT NULL UNIQUE,
		LoadDate DATETIME NOT NULL DEFAULT GETDATE()

)
------------------------------------------------------------------------------------------------------------------------


IF OBJECT_ID('cleansed_payers', 'U') IS NOT NULL
		DROP TABLE cleansed_payers;

CREATE TABLE cleansed_payers(
		payer_id INT primary key,
		Payer_name Varchar(50)NOT NULL,
		LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);


----------------------------------------------------------------------------------------------------------------------


IF OBJECT_ID('cleansed_patients','U') IS NOT NULL 
		DROP TABLE cleansed_patients;

CREATE TABLE cleansed_patients(
			patient_id INT primary key,
			first_name Varchar(50)NOT NULL,
			last_name Varchar(50)NOT NULL,
			gender Varchar(10),
			date_of_birth date NOT NULL,
			state_code Varchar(50)NOT NULL,
			city varchar(50)NOT NULL,
			phone varchar(50)NOT NULL,
		    LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);


--======================================================================================================================
--child table
--======================================================================================================================



IF OBJECT_ID('cleansed_encounters', 'U') IS NOT NULL
		DROP TABLE cleansed_encounters;

CREATE TABLE cleansed_encounters(
		encounter_id INT primary key,
		patient_id INT not null,
		provider_id INT not null,
		encounter_type varchar(50)NOT NULL,
		encounter_start Date NOT NULL,
		encounter_end Date NOT NULL,
		height_cm INT,
		weight_kg FLOAT NOT NULL,
		systolic_bp INT not null,
		diastolic_bp INT not null,
		LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);

--------------------------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('cleansed_medications', 'U') IS NOT NULL
		DROP TABLE cleansed_medications;

CREATE TABLE cleansed_medications(
			medication_id INT primary key,
			encounter_id INT not null,
			drug_name VARCHAR(50) NOT NULL,
			route Varchar(50),
			dose varchar(50) NOT NULL,
			frequency varchar(50)NOT NULL,
			days_supply INT,
     		LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);

--------------------------------------------------------------------------------------------------------------------------------------------------------------


IF OBJECT_ID('cleansed_procedures', 'U') IS NOT NULL
		DROP TABLE cleansed_procedures;

CREATE TABLE cleansed_procedures(
		procedure_id INT primary key,
		encounter_id INT,
		procedure_description Varchar(50) NOT NULL,
     	LoadDate DATETIME NOT NULL DEFAULT GETDATE()
)

--------------------------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('cleansed_diagnoses', 'U') IS NOT NULL
	DROP TABLE cleansed_diagnoses;

CREATE TABLE cleansed_diagnoses(
		diagnosis_id INT primary key,
		encounter_id INT not null,
		diagnosis_description VARCHAR(50) NOT NULL,
		is_primary INT,
     	LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);


--------------------------------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('cleansed_claims', 'U') IS NOT NULL
	DROP TABLE cleansed_claims;

CREATE TABLE cleansed_claims(
		claim_id INT primary key,
		encounter_id INT not null,
		payer_id INT,
		admit_date Date not null,
		discharge_date DATE NOT NULL,
		total_billed_amount DECIMAL(18,2)NOT NULL,
		total_allowed_amount DECIMAL(18,2)NOT NULL,
		total_paid_amount DECIMAL(18,2)NOT NULL,
		claim_status Varchar(50),
     	LoadDate DATETIME NOT NULL DEFAULT GETDATE()
);

--------------------------------------------------------------------------------------------------------------------------------------------------------------




