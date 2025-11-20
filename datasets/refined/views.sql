--1 what is yearly trend of total claim payouts, number of claims processed,and average claim amount paid  


CREATE OR ALTER VIEW VW_Yearly_Claim_Trend AS
select 
	year(admit_date)  as claim_year,
	count(claim_id)as number_of_claims,
	round(avg(total_paid_amount),2) as avg_Claim_amount
from REFINED_DW.refined_claims
group by year(admit_date) 

--=================================================================================================================


--2 what is yearly trend of total claim payouts, number of claims processed,and average claim amount paid by insurance companies.

CREATE OR ALTER VIEW VW_Yearly_Claim_By_Payer AS
select 
	year(admit_date) as claim_year,
	payer_name,
	count(claim_id)as number_of_claims,
	round(avg(total_paid_amount),2) as avg_Claim_amount
from [REFINED_DW].[refined_claims] as a
left join [REFINED_DW].[refined_payers] as b
on a.payer_id = b.payer_id
group by year(admit_date),payer_name

--==================================================================================================================


--3 what is the total claim amount for the current week, month, and year to date.(YTD,MTD,WTD,DTD)

CREATE OR ALTER VIEW VW_Total_Claim_Amount_WMYD_To_Date AS
WITH MaxDate AS (
    SELECT CAST(MAX(discharge_date) AS DATE) AS latest_discharge_date
    FROM REFINED_DW.Refined_Claims
)
SELECT
    -- Day-to-Date Total
    SUM(
        CASE 
            WHEN CAST(r.discharge_date AS DATE) = m.latest_discharge_date 
            THEN r.total_allowed_amount 
            ELSE 0 
        END
    ) AS DTD_Total_Claim_Amount,

    -- Week-to-Date Total
    SUM(
        CASE 
            WHEN DATEPART(WEEK, r.discharge_date) = DATEPART(WEEK, m.latest_discharge_date)
                 AND YEAR(r.discharge_date) = YEAR(m.latest_discharge_date)
            THEN r.total_allowed_amount 
            ELSE 0 
        END
    ) AS WTD_Total_Claim_Amount,

    -- Month-to-Date Total
    SUM(
        CASE 
            WHEN MONTH(r.discharge_date) = MONTH(m.latest_discharge_date)
                 AND YEAR(r.discharge_date) = YEAR(m.latest_discharge_date)
            THEN r.total_allowed_amount 
            ELSE 0 
        END
    ) AS MTD_Total_Claim_Amount,

    -- Year-to-Date Total
    SUM(
        CASE 
            WHEN YEAR(r.discharge_date) = YEAR(m.latest_discharge_date)
            THEN r.total_allowed_amount 
            ELSE 0 
        END
    ) AS YTD_Total_Claim_Amount

FROM REFINED_DW.Refined_Claims r
CROSS JOIN MaxDate m
WHERE r.discharge_date IS NOT NULL;
--=================================================================================================================

--4 which provider have the  highets number of patient encounters each year

CREATE OR ALTER VIEW VW_Top_Providers_By_Encounters AS
select top(10)
	year(encounter_start) as encounter_year,
	a.provider_id,
	CONCAT(first_name,' ',last_name) as provider_name ,
	count(patient_id) as num_of_patient
from [REFINED_DW].[refined_providers] as a 
left join [REFINED_DW].[refined_encounters] as b
on a.provider_id = b.provider_id
group by year(encounter_start),a.provider_id,CONCAT(first_name,' ',last_name)
--===========================================================================================================

--5 which diagnosis contribute most to total healthcare claim cost.

CREATE OR ALTER VIEW VW_Top_Diagnosis_Claim_Cost AS
with xyz as(
select 
a.encounter_id,b.diagnosis_id,diagnosis_description
from [REFINED_DW].[refined_encounters] as a
left join [REFINED_DW].[refined_diagnoses] as b
on a.encounter_id = b.encounter_id
)
select top (10)
diagnosis_id,
diagnosis_description,
sum(total_paid_amount) as claim_cost
from xyz
left join [REFINED_DW].[refined_claims] as b
on xyz.encounter_id = b.encounter_id
group by diagnosis_id,diagnosis_description

--============================================================================================================

--6 what is the total number of patient visit ,total claim cost and average cost per patient visit per year.

CREATE OR ALTER VIEW VW_Patient_Visit_Stats AS
with xyz as(
select 
a.encounter_id,a.patient_id,b.claim_id,admit_date,discharge_date,total_paid_amount
from [REFINED_DW].[refined_encounters] as a
left join [REFINED_DW].[refined_claims] as b
on a.encounter_id = b.encounter_id
)
select 
b.patient_id,
	CONCAT(first_name,' ',last_name) as patient_name,
	year(admit_date) as year,
	count(a.patient_id) as number_of_patient_visit,
	sum(total_paid_amount) as total_claim_cost,
	sum(total_paid_amount)/count(a.patient_id) as avg_cost
from xyz as a
left join [REFINED_DW].[refined_patients] as b
on a.patient_id = b.patient_id
group by b.patient_id,CONCAT(first_name,' ',last_name),year(admit_date)
--==================================================================================================================

--7 what are the total claim amount, total number of claims and avg claims amount by gender for each year, including yearly and overall total.

CREATE OR ALTER VIEW VW_Claim_Stats_By_Gender AS
with xyz as(
select 
claim_id,b.encounter_id,admit_date,total_paid_amount,c.patient_id,gender,claim_status
from [REFINED_DW].[refined_claims] as a
left join [REFINED_DW].[refined_encounters]as b
on a.encounter_id = b.encounter_id
left join [REFINED_DW].[refined_patients] as c
on b.patient_id = c.patient_id
where claim_status = 'paid'
),
xyz2 as(
select 
year(admit_date) as year,
gender,
count(claim_id) as number_of_claims,
sum(total_paid_amount) as total_claim_amount,
avg(total_paid_amount) avg_claim_amount
from xyz
group by year(admit_date),gender
)
select *,
sum(total_claim_amount) over(partition by year order by year) as yearly_total,
sum(total_claim_amount) over() as overall_total
from xyz2

--===================================================================================================================

--8 which medication prescribed most frequently aross all patients visit?

CREATE OR ALTER VIEW VW_Top_Prescribed_Medications AS
select drug_name,
      count(drug_name) as most_frequently_used_drg
from [REFINED_DW].[refined_medications]
group by drug_name;

--==================================================================================================================

--9 how was the inpatient stay duration (submission to discharged) varied year over year, and what is the percentage increase or decrease compared to previous year

CREATE OR ALTER VIEW VW_Inpatient_Stay_YoY AS
WITH StayDurations AS (
    SELECT 
        YEAR(admit_date) AS admit_year,
        DATEDIFF(DAY, admit_date, discharge_date) AS stay_days
    FROM [REFINED_DW].[refined_claims]
    WHERE admit_date IS NOT NULL 
      AND discharge_date IS NOT NULL
),
YearlyAvg AS (
    SELECT 
        admit_year,
        AVG(stay_days * 1.0) AS avg_stay_duration
    FROM StayDurations
    GROUP BY admit_year
)
SELECT 
    admit_year,
    avg_stay_duration,
    LAG(avg_stay_duration) OVER (ORDER BY admit_year) AS prev_year_avg,
    CASE 
        WHEN LAG(avg_stay_duration) OVER (ORDER BY admit_year) IS NULL 
             THEN NULL
        ELSE ROUND(
            ((avg_stay_duration - LAG(avg_stay_duration) OVER (ORDER BY admit_year)) 
              / LAG(avg_stay_duration) OVER (ORDER BY admit_year)) * 100, 2
        )
    END AS percentage
FROM YearlyAvg

--==================================================================================================================

--10  how much have total paid claim amount changed compared to the previous year, and 
--what is the year over year percentage growth in insurance claim payments 

CREATE OR ALTER VIEW VW_Claim_Payout_YoY AS
WITH YearlyTotals AS (
    SELECT 
        YEAR(admit_date) AS claim_year,
        SUM(total_paid_amount) AS total_paid
    FROM REFINED_DW.refined_claims
    WHERE admit_date IS NOT NULL 
    GROUP BY YEAR(admit_date)
)
SELECT 
    claim_year,
    total_paid,
    LAG(total_paid) OVER (ORDER BY claim_year) AS prev_year_total,
    CASE 
        WHEN LAG(total_paid) OVER (ORDER BY claim_year) IS NULL 
             THEN NULL
        ELSE ROUND(
            ((total_paid - LAG(total_paid) OVER (ORDER BY claim_year)) 
              / LAG(total_paid) OVER (ORDER BY claim_year)) * 100, 2
        )
    END AS percentage
FROM YearlyTotals;
