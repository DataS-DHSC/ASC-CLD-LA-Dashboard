---------------------------
--  Create master table  --
---------------------------

-- This code creates a master dataset from which each event type can then be counted
-- It adds on all the required derived fields to one table for the purposes of the dashboard only
 
/* Step process:
1. Takes the latest submission for each LA covering the specified reporting period
2. Filters records to within the period of interest (Q1, Q2 etc), accounting for date of death when populated
3. Derives the age at event end date using birth month and year (or reporting end date for services where end date is null), then converts this to age bands
4. Creates new ID field based on NHS number unless null then LA ID
5. Creates planned and unplanned categories for reviews
6. Creates categories for event outcome by grouping NFAs together
7. Creates categories for services (long, short, carer)
8. Calculates the number of weeks a service was open for
9. Creates a new cleaned event outcome field for the purposes of joining onto the event outcome hierarchy table for sorting at a later stage
10. Joins on the rank number to event outcome, required when sorting for determining unique events
11. Outputs the master table which is then used in subsequent scripts for determining unique events */


-----------------------------------------------------
-- Filter to the specified reporting period --
-----------------------------------------------------

DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01' 
DECLARE @ReportingPeriodEndDate AS DATE = '2023-12-31' 
DECLARE @SubmissionsAsOfDate AS DATE = GETDATE()

-- Create a temporary table to store the list of relevant submissions
DROP TABLE IF EXISTS #Submissions;

CREATE TABLE #Submissions (Der_Load_Filename VARCHAR(256));

-- Execute the stored procedure which outputs the latest file for each LA covering the specified reporting period as of a given date
-- Results are inserted into the temporary table
INSERT INTO #Submissions 
EXEC ASC_Sandbox.GetMandatorySubmissions 
  @ReportingPeriodStartDate = @ReportingPeriodStartDate,
  @ReportingPeriodEndDate = @ReportingPeriodEndDate,
  @SubmissionsAsOfDate = @SubmissionsAsOfDate


-- Filter the full dataset to the file list created above

DROP TABLE IF EXISTS #CLD_Reporting_Period;

SELECT t2.* 
INTO #CLD_Reporting_Period
FROM #Submissions t1
LEFT JOIN DHSC_ASC.CLD_R1_Raw t2
  ON t1.Der_Load_Filename = t2.Der_Load_Filename ;

 
-----------------------------------------------------
-- TEMP TABLE 1--
-- Filter to events which fall within the period of interest, accounting for date of death
-- Create age at event end date and derived age bands
-----------------------------------------------------

DROP TABLE IF EXISTS #Temp1_Ages;


SELECT *,
  CASE
    WHEN Der_Latest_Age <18 THEN '<18'
    WHEN Der_Latest_Age BETWEEN 18 AND 19 THEN '18-19' --age bands
    WHEN Der_Latest_Age BETWEEN 20 AND 29 THEN '20-29'
    WHEN Der_Latest_Age BETWEEN 30 AND 39 THEN '30-39'
    WHEN Der_Latest_Age BETWEEN 40 AND 49 THEN '40-49'
    WHEN Der_Latest_Age BETWEEN 50 AND 59 THEN '50-59'
    WHEN Der_Latest_Age BETWEEN 60 AND 69 THEN '60-69'
    WHEN Der_Latest_Age BETWEEN 70 AND 79 THEN '70-79'
    WHEN Der_Latest_Age BETWEEN 80 AND 89 THEN '80-89'
    WHEN Der_Latest_Age >=90 THEN '90+'
    ELSE 'unknown'
  END AS Der_Age_Band,
  CASE
    WHEN Der_Latest_Age <18 THEN '<18'  --working age bands
    WHEN Der_Latest_Age <= 64 AND Der_Latest_Age >=18 THEN '18-64' 
    WHEN Der_Latest_Age >= 65 THEN '65+'
    ELSE 'unknown'
  END AS der_working_age_band INTO #temp1_ages
FROM (
  SELECT *, -- Derive latest age at the end of an event (or reporting period if a service without an end date)
    CASE
      -- requests, assessments and reviews use event end date
      WHEN der_birth_year IS NOT NULL AND event_type not like '%service%' 
        THEN floor((datediff (DAY, (cast(concat(der_birth_year, '-', der_birth_month, '-', '01') AS date)), event_end_date))/365.25)
      
      -- services use event end date when not null
      WHEN der_birth_year IS NOT NULL AND event_type like '%service%' AND event_end_date IS NOT NULL 
        THEN floor((datediff (DAY, (cast(concat(der_birth_year, '-', der_birth_month, '-', '01') AS date)), event_end_date))/365.25) --use end date for services if it isn't null
      
      -- services use reporting period end date when event end date is null
      WHEN der_birth_year IS NOT NULL AND event_type like '%service%' AND event_end_date IS NULL 
        THEN floor((datediff (DAY, (cast(concat(der_birth_year, '-', der_birth_month, '-', '01') AS date)), reporting_period_end_date))/365.25) --use reporting end date for services with null end date
      ELSE NULL 
    END AS [Der_Latest_Age] 
  FROM (
    SELECT *, 
    --Check for date of death - when date of death is between event start and end dates or when event end date is null, 
    --replace event end date with date of death
      CASE 
        WHEN (Date_of_Death BETWEEN Event_Start_Date AND Event_End_Date)
             OR (Date_of_Death > Event_Start_Date
             AND Event_End_Date IS NULL) THEN Date_of_Death 
        ELSE Event_End_Date 
      END AS Der_Event_End_Date 
    FROM #CLD_Reporting_Period
    ) a
  ) b 
--select requests, assessments, reviews which start before the end of the period and end withing the reporting period
WHERE ((Event_Type not like '%service%' 
  AND Der_Event_End_Date BETWEEN @ReportingPeriodStartDate AND @ReportingPeriodEndDate 
  AND Event_Start_Date <= @ReportingPeriodEndDate ) --any events with null start dates are by default excluded
  OR

-- selects services which start before the reporting period end and end must be after the start or null
  (Event_Type like '%service%' AND (Der_Event_End_Date >= @ReportingPeriodStartDate OR Der_Event_End_Date IS NULL)
  AND Event_Start_Date <= @ReportingPeriodEndDate) )

-- select records where date of death is null or greater than the reporting period start and event start dates
  AND (Date_of_Death IS NULL
  OR (Date_of_Death >= @ReportingPeriodStartDate
  AND Date_of_Death >= Event_Start_Date));

-----------------------------------------------------
-- TEMP TABLE 2--
-- Create a new person ID field which is the NHS number where populated, if not then the LA person ID
-- Includes check as to whether the LA ID is associated with an NHS number in the latest person data table
-- If a row doesn't have an LA ID or a NHS number then it will be excluded here 
-----------------------------------------------------
--select rows which don't have an NHS Number but do have an LA ID

DROP TABLE IF EXISTS #Missing_NHS;

SELECT * 
INTO #Missing_NHS
FROM #Temp1_Ages
WHERE Der_NHS_Number_Traced_Pseudo IS NULL AND Der_LA_Person_Unique_Identifier_Pseudo IS NOT NULL;

--Select rows which do have an NHS number
DROP TABLE IF EXISTS #Provided_NHS;

SELECT *,
  Der_NHS_Number_Traced_Pseudo AS Der_Person_ID,
  'NHS' AS ID_Source 
INTO #Provided_NHS
FROM #Temp1_Ages
WHERE Der_NHS_Number_Traced_Pseudo IS NOT NULL;

--Create table of distinct LA IDs with the latest reported NHS number traced
--where LA IDs have multiple associated NHS numbers it takes the latest based on Der_Latest_Import_Flag,
--      Reporting period start and end dates (descending) and then the File record id as a last resort
--as of 27/07/23 there were 91 LA IDs with multiple NHS nos, out of 1.2m <0.01%

DROP TABLE IF EXISTS #Person_ID_Lookup;

SELECT * 
INTO #Person_ID_Lookup
FROM (
  SELECT 
    Der_NHS_Number_Traced_Pseudo,
    Der_LA_Person_Unique_Identifier_Pseudo,
    LA_Code,
    LatestRecord = row_number() OVER (   
      PARTITION BY isnull(LA_Code, ''),    --only include LA ID & LA code as we only want 1 NHS number per LA ID
                   isnull(Der_LA_Person_Unique_Identifier_Pseudo, '')
      ORDER BY Der_Latest_Import_Flag DESC, 
               Reporting_Period_End_Date DESC, 
               Reporting_Period_Start_Date DESC, 
               Der_File_Record_ID DESC)
  FROM DHSC_ASC.CLD_R1_Latest_Person_Data
  --Keep the latest record for each LA ID and LA Code, and only where LA ID isn't blank
  WHERE Der_NHS_Number_Traced_Pseudo IS NOT NULL 
  ) a 
WHERE LatestRecord = 1 AND Der_LA_Person_Unique_Identifier_Pseudo IS NOT NULL;

--Join those with a missing NHS number by LA ID onto the person table created above to see if an NHS number exists

DROP TABLE IF EXISTS #Missing_Fixed_NHS;

SELECT *,
  CASE
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND t2_Der_NHS_Number_Traced_Pseudo IS NULL 
      THEN Der_LA_Person_Unique_Identifier_Pseudo -- Use LA ID when it hasn't found an NHS no.
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND t2_Der_NHS_Number_Traced_Pseudo IS NOT NULL 
      THEN t2_Der_NHS_Number_Traced_Pseudo --NHS no. where it's found a match
  END AS Der_Person_ID,
  --New field to identify the source of the person ID
  CASE
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND t2_Der_NHS_Number_Traced_Pseudo IS NULL THEN 'LA'
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND t2_Der_NHS_Number_Traced_Pseudo IS NOT NULL THEN 'NHS'
  END AS ID_Source
INTO #Missing_Fixed_NHS
FROM (
  SELECT 
    t1.*,
    t2.Der_LA_Person_Unique_Identifier_Pseudo AS t2_Der_LA_Person_Unique_Identifier_Pseudo,
    t2.LA_Code AS t2_LA_Code,
    t2.Der_NHS_Number_Traced_Pseudo AS t2_Der_NHS_Number_Traced_Pseudo
  FROM #Missing_NHS AS t1
  LEFT JOIN #Person_ID_Lookup AS t2 
  ON t1.Der_LA_Person_Unique_Identifier_Pseudo = t2.Der_LA_Person_Unique_Identifier_Pseudo
  AND t1.LA_Code = t2.LA_Code
  ) a;

--Remove unnecessary columns prior to combining tables back together

ALTER TABLE #Missing_Fixed_NHS
DROP COLUMN t2_Der_LA_Person_Unique_Identifier_Pseudo,
  t2_Der_NHS_Number_Traced_Pseudo,
  t2_LA_Code;

--Join the 2 tables back together (those with NHS number and those without)

DROP TABLE IF EXISTS #Temp2_IDs;

SELECT * 
INTO #Temp2_IDs
FROM #provided_nhs
UNION
SELECT *
FROM #Missing_Fixed_NHS;

-----------------------------------------------------
-- TEMP TABLE 3--
-- Creates high level categories for reviews (planned and unplanned)
-- Creates high level categories for services (short, long, carer)
-- Converts some blanks to nulls
-- Creates a cleaned and stripped version of event outcome which is needed for joining to the
--     hierarchy for ordering in a later step
-----------------------------------------------------

DROP TABLE IF EXISTS #Temp3_Derived_Fields;


SELECT *,
  CASE
    WHEN Review_Reason LIKE '%unplanned%' AND Event_Type = 'Review' THEN 'Unplanned'
    WHEN Review_Reason LIKE 'planned%' AND Event_Type = 'Review' THEN 'Planned'
    WHEN (Review_Reason IS NULL oR Review_Reason ='') AND Event_type = 'Review' THEN NULL
    WHEN Event_Type NOT LIKE '%Review%' THEN NULL
    ELSE 'Review Type Unknown'
  END AS Review_Type,
  CASE
    WHEN Service_Type LIKE '%long%' THEN 'Long Term'
    WHEN Service_Type LIKE '%short%' THEN 'Short Term'
    WHEN Service_Type LIKE '%Carer%' THEN 'Carer Support'
    WHEN Event_Type NOT LIKE '%service%' THEN NULL
    ELSE 'Unknown'
  END AS Service_Type_Grouped,
  --group all NFA reasons into one
  CASE
    WHEN Event_Outcome LIKE '%NFA%' THEN 'NFA' 
    ELSE Event_Outcome
  END AS Event_Outcome_Grouped,
  --for the sankey chart, blanks need to become nulls
  nullif(Event_Outcome, '') AS Event_Outcome_Nulled, 
  nullif(Request_Route_Of_Access, '') AS Request_Route_of_Access_Nulled,
  nullif(Assessment_Type, '') AS Assessment_Type_Nulled,
  --strip punctuation from event outcome for matching on the hierarchy in next step
  nullif(Review_Outcomes_Achieved, '') AS Review_Outcomes_Achieved_Nulled, 
  CASE 
    WHEN Event_Outcome = '' OR Event_Outcome IS NULL THEN NULL
    ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
      Event_Outcome, ' ', ''), '.', ''), '-', ''), '(', ''), ')', ''), '_', ''), '%', ''), ',', ''), ';', ''), '/', ''), '\', ''),':', ''), '!', ''), '&', '') 
    END
  AS Event_Outcome_Raw_Stripped 
INTO #Temp3_Derived_Fields
FROM #Temp2_IDs;

-----------------------------------------------------
-- TEMP TABLE 4--
-- Merges on the event outcome ranks from a hierarchy lookup table
-- This required in a later stage when determining unique events
-----------------------------------------------------

--Create event outcome hierarchy table
DROP TABLE IF EXISTS ASC_Sandbox.Event_Outcome_Hierarchy;
DROP TABLE IF EXISTS #EO_Hierarchy;

CREATE TABLE 
 #EO_Hierarchy (
	Event_Outcome_Hierarchy INT,
	Event_Outcome_Spec VARCHAR(100)
	);

INSERT INTO #EO_Hierarchy VALUES
  ('1','Progress to Reablement/ST-Max'),
  ('2','Progress to Assessment'),
  ('3','Admitted to hospital'),
  ('4','Progress to Re-assessment / Unplanned Review'),
  ('5','Progress to End of Life Care'),
  ('6','No change in package'),
  ('7','Service ended as planned'),
  ('8','Progress to financial assessment '),
  ('9','Provision of service'),
  ('10','NFA - Deceased'),
  ('11','NFA - Moved to another LA'),
  ('12','NFA - 100% NHS funded care'),
  ('13','NFA - Information & Advice / Signposting only'),
  ('14','NFA - Self-funded client (Inc. 12wk disregard)'),
  ('15','NFA - Support declined'),
  ('16','NFA - Support ended: Other reason'),
  ('17','NFA - No services offered: Other reason'),
  ('18','NFA- Other')
;

SELECT *,
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Event_Outcome_Spec, ' ', ''), '.', ''),'-', ''),'(', ''),')', ''),'_', ''),'%', ''), ',', ''), ';', ''),'/', ''),'\', ''),':', ''), '!', ''), '&', '') as Event_Outcome_Stripped
INTO ASC_Sandbox.Event_Outcome_Hierarchy
FROM #EO_Hierarchy;

DROP TABLE IF EXISTS #Temp4_EO_Hierarchy;

SELECT 
  t1.*,
  CASE WHEN t2.Event_Outcome_Hierarchy IS NULL THEN 999
    ELSE t2.Event_Outcome_Hierarchy 
  END
  AS Event_Outcome_Hierarchy,
  t2.Event_Outcome_Spec	
INTO #Temp4_EO_Hierarchy
FROM #Temp3_Derived_Fields AS t1
LEFT JOIN ASC_Sandbox.Event_Outcome_Hierarchy AS t2
--wildcards allow matching when strings before or after the differ to the spec
  ON (t1.Event_Outcome_Raw_Stripped LIKE '%' + t2.Event_Outcome_Stripped + '%' OR
  t2.Event_Outcome_Stripped LIKE '%' + t1.Event_Outcome_Raw_Stripped + '%')
--Manual fix for now where event outcome = other is matching to multiple event outcomes (e.g. moved to another LA, NFA - Other)
-- This only selects the event outcome 'other' where it has matched to 18 (NFA - Other) to prevent introducing duplicates
WHERE ISNULL(t1.Event_Outcome, '') != 'Other' OR (Event_Outcome = 'Other' AND Event_Outcome_Hierarchy = 18);


-----------------------------------------------------
------ FINAL TABLE ---------
-- select the fields to retain
-----------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Master_Table;

SELECT 
  LA_Code,
  LA_Name,
  Reporting_Period_Start_Date,
  Reporting_Period_End_Date,
  Client_Type,
  Gender,
  Ethnicity,
  Der_Age_Event_Start_Date,
  [Der_Latest_Age],
  Der_Age_Band,
  Der_Working_Age_Band,
  Date_of_Death,
  Primary_Support_Reason,
  Event_Type,
  Event_Reference,
  Event_Start_Date,
  Der_Event_End_Date AS Event_End_Date,
  Event_Description,
  Event_Outcome_Nulled AS Event_Outcome,
  Event_Outcome_Hierarchy,
  Event_Outcome_Grouped,
  Request_Route_of_Access_Nulled AS Request_Route_of_Access,
  Assessment_Type_Nulled AS Assessment_Type,
  Eligible_Needs_Identified,
  Method_of_assessment,
  Review_Reason,
  Review_Type,
  Review_Outcomes_Achieved_Nulled AS Review_Outcomes_Achieved,
  Method_of_Review,
  Service_Type,
  Service_Type_Grouped,
  Service_Component,
  Delivery_Mechanism,
  Unit_Cost,
  Cost_Frequency_Unit_Type,
  Planned_units_per_week,
  Provider_CQC_Location_ID,
  Provider_CQC_Location_Name,
  Der_Person_ID,
  Der_File_Record_ID ,
  Der_Unique_Record_ID,
  Der_Unique_Event_Flag,
  Der_Conversation,
  --Following fields are kept solely for running the DQ checks procedure
  Der_NHS_Number_Pseudo,
  Der_LA_Person_Unique_Identifier_Pseudo,
  GP_Practice_Code,
  GP_Practice_Name,
  Der_Birth_Year,
  Der_Postcode_Sector,
  Accommodation_Status,
  Der_Age_Reporting_Period_End_Date,
  Employment_Status,
  Has_Unpaid_Carer,
  Autism_Spectrum_Disorder_ASD,
  Visual_Impairment,
  Hearing_Impairment,
  Dementia,
  Client_Funding_Status,
  Total_Hrs_Caring_per_week,
  No_of_adults_being_cared_for,
  Der_Adult_1_Linked_Person_ID_Pseudo,
  Der_Adult_2_Linked_Person_ID_Pseudo,
  Der_Adult_3_Linked_Person_ID_Pseudo,
  Der_Duplicates_Flag,
  ImportDate,
  Der_Load_Filename,
  Der_Latest_Import_Flag
INTO ASC_Sandbox.LA_PBI_Master_Table
FROM #Temp4_EO_Hierarchy;

