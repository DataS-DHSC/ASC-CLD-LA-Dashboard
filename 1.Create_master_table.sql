---------------------------
--  Create master table  --
---------------------------

-- This code creates a master dataset from which each event type can then be counted
-- It adds on all the required derived fields to one table for the purposes of the LA dashboard
 
/* Step process:
1. Takes the latest submission for each LA covering the specified reporting period
2. Filters records to within the period of interest (Q1, Q2 etc), accounting for date of death when populated
3. Derives the age at event end date using birth month and year (or reporting end date for services where end date is null), then converts this to age bands
4. Creates new person ID field based on NHS number unless null then LA person ID
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
DECLARE @ReportingPeriodEndDate AS DATE = '2024-03-31' 
DECLARE @SubmissionsAsOfDate AS DATE = GETDATE() --usually left as today's date unless trying to replicate figures as of a given date

-- Create a temporary table to store the list of relevant submissions
DROP TABLE IF EXISTS #Submissions;

CREATE TABLE #Submissions 
  (LA_Name VARCHAR(256),
  ImportDate DATETIME);

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
  ON t1.LA_Name = t2.LA_Name AND t1.ImportDate = t2.ImportDate ;


-----------------------------------------------------
-- TEMP TABLE 1--
-- Filter to events which fall within the period of interest, accounting for date of death
-- Create age at event end date and derived age bands
-----------------------------------------------------

DROP TABLE IF EXISTS #Temp1_Ages;

SELECT *,
  CASE
    WHEN Der_Latest_Age < 18 THEN 'Under 18'
    WHEN Der_Latest_Age BETWEEN 18 AND 24 THEN '18 to 24'
    WHEN Der_Latest_Age BETWEEN 25 AND 44 THEN '25 to 44'
    WHEN Der_Latest_Age BETWEEN 45 AND 64 THEN '45 to 64'
    WHEN Der_Latest_Age BETWEEN 65 AND 74 THEN '65 to 74'
    WHEN Der_Latest_Age BETWEEN 75 AND 84 THEN '75 to 84'
    WHEN Der_Latest_Age BETWEEN 85 AND 94 THEN '85 to 94'
    WHEN Der_Latest_Age >= 95 THEN '95 and above'
    ELSE 'Unknown'
  END AS Der_Age_Band,
  CASE
    WHEN Der_Latest_Age <18 THEN 'Under 18'  --working age bands
    WHEN Der_Latest_Age <= 64 AND Der_Latest_Age >=18 THEN '18 to 64' 
    WHEN Der_Latest_Age >= 65 THEN '65 and above'
    ELSE 'Unknown'
  END AS Der_Working_Age_Band
INTO #Temp1_Ages
FROM (

  SELECT *, -- Derive latest age at the end of an event (or reporting period if a service without an end date)
    CASE
      -- requests, assessments and reviews use event end date
      WHEN Der_Birth_Year IS NOT NULL AND Event_Type NOT LIKE '%service%' 
        THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25)
      
      -- services use event end date when not null
      WHEN Der_Birth_Year IS NOT NULL AND Event_Type LIKE '%service%' AND Der_Event_End_Date IS NOT NULL 
        THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25) --use end date for services if it isn't null
      
      -- services use reporting period end date when event end date is null
      WHEN Der_Birth_Year IS NOT NULL AND Event_Type LIKE '%service%' AND Der_Event_End_Date IS NULL 
        THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Reporting_Period_End_Date))/365.25) --use reporting end date for services with null end date
      ELSE NULL 
    END AS Der_Latest_Age,

    --Clean event type
    CASE 
      WHEN Event_Type LIKE '%Service%' THEN 'Service' 
      WHEN Event_Type LIKE '%Assessment%' THEN 'Assessment' 
      WHEN Event_Type LIKE '%Request%' THEN 'Request'
      WHEN Event_Type LIKE '%Review%' THEN 'Review'
      ELSE Event_Type 
    END AS Event_Type_Clean

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
WHERE ((Event_Type_Clean <> 'Service' 
  AND Der_Event_End_Date BETWEEN @ReportingPeriodStartDate AND @ReportingPeriodEndDate 
  AND Event_Start_Date <= @ReportingPeriodEndDate ) --any events with null start dates are by default excluded
  OR

-- selects services which start before the reporting period end and end must be after the start or null
  (Event_Type_Clean = 'Service' AND (Der_Event_End_Date >= @ReportingPeriodStartDate OR Der_Event_End_Date IS NULL)
  AND Event_Start_Date <= @ReportingPeriodEndDate) )

-- select records where date of death is null or greater than the reporting period start and event start dates
  AND (Date_of_Death IS NULL
  OR (Date_of_Death >= @ReportingPeriodStartDate
  AND Date_of_Death >= Event_Start_Date));

-----------------------------------------------------
-- TEMP TABLE 2--
-- Create a new person ID field which is the LA provided NHS number, if null then the Traced NHS number
-- If both NHS numbers are null then the LA person ID is used
-- If a row doesn't have an LA ID or a NHS number then it will be excluded here 
-----------------------------------------------------

DROP TABLE IF EXISTS #Temp2_IDs;

SELECT *,
  CASE
    WHEN Der_NHS_Number_Traced_Pseudo IS NOT NULL THEN Der_NHS_Number_Traced_Pseudo
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NOT NULL THEN Der_NHS_Number_Pseudo
    WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NULL THEN Der_LA_Person_Unique_Identifier_Pseudo
  END AS 'Der_NHS_LA_Combined_Person_ID'
INTO #Temp2_IDs
FROM #Temp1_Ages


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
    WHEN Review_Reason LIKE '%unplanned%' AND Event_Type_Clean = 'Review' THEN 'Unplanned'
    WHEN Review_Reason LIKE 'planned%' AND Event_Type_Clean = 'Review' THEN 'Planned'
    WHEN (Review_Reason IS NULL OR Review_Reason ='') AND Event_Type_Clean = 'Review' THEN NULL
    WHEN Event_Type_Clean <> 'Review' THEN NULL
    ELSE 'Review Type Unknown'
  END AS Review_Type,
  CASE
    WHEN Service_Type LIKE '%long%' THEN 'Long Term'
    WHEN Service_Type LIKE '%short%' THEN 'Short Term'
    WHEN Service_Type LIKE '%Carer%' THEN 'Carer Support'
    WHEN Event_Type_Clean <> 'Service' THEN NULL
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
  Der_Latest_Age,
  Der_Age_Band,
  Der_Working_Age_Band,
  Date_of_Death,
  Primary_Support_Reason,
  Event_Type_Clean AS Event_Type,
  Event_Start_Date,
  Der_Event_End_Date AS Event_End_Date,
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
  Der_NHS_LA_Combined_Person_ID,
  Der_Conversation,
  Der_Conversation_1,
  Der_Unique_Record_ID
INTO ASC_Sandbox.LA_PBI_Master_Table
FROM #Temp4_EO_Hierarchy;