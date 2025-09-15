------------------------------------------
/*  ASCOF 2D
The proportion of people aged 65 and over discharged from hospital into reablement and who remained in the community within 12 weeks of discharge.

Part 1 (outcomes) - the proportion of people who remained in the community within 12 weeks of hospital discharge, 
of those aged 65 and over who received reablement support after discharge.

Part 2 (provision) - proportion of people who were provided with reablement services following discharge from hospital, of those aged 65 and over

This metric uses CLD linked to SUS (secondary uses services data containing information on hospital episodes) to identify who received 
reablement after hospital and to measure their outcomes.

The code for ASCOF 2D follows these stages:
1.	Create initial CLD tables, including a table of long term support services 
(to identify those in long term support when admitted to hospital) and a table of reablement services.
2.	Create a table of hospital discharges from SUS (part 2 denominator).
3.	Create a linked table of discharges into reablement (part 2 numerator which is also part 1 denominator).
4.	Identify outcomes for those discharged into reablement (part 1 numerator).
5.  Create the aggregated outputs 

*/

--======================================================================================================================
--================================== CREATING A STATIC CUT FOR HOSPITAL SUS DATA =======================================
/*
To ensure the figures are reproducible a static cut of SUS data is required (as it updates daily). 

Uncomment this code block if you wish to refresh the SUS base data, otherwise skip this step. Remember to edit 
the date prefix on the table name to reflect the date of the refresh
*/

--DROP TABLE IF EXISTS ASC_Sandbox.SUS_250908;  --update date
--SELECT *
--INTO ASC_Sandbox.SUS_250908  --update date
--FROM DHSC_SUS.APCE
--WHERE Discharge_Date BETWEEN '2023-04-01' AND CAST(GETDATE() AS DATE);

--====================================================================================================================
--================================== STAGE 0: SET REPORTING PERIOD OF INTEREST =======================================

-- Define parameter values
DECLARE @ReportingPeriodStartDate DATE = '2024-04-01';
DECLARE @ReportingPeriodEndDate DATE = '2025-03-31';

--=======================================================================================================================
--================================== STAGE 1: CREATE INITIAL CLD TABLES =================================================

----------------------------------------------------------------------------------------------
-- Create clean version of the latest CLD main table and join with latest person details --
----------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #CLD_Latest_Person_Details

SELECT * 
INTO #CLD_Latest_Person_Details
FROM ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_CLD_Cleaned
SELECT 
  a.*,
  b.Date_of_Death as DOD_Latest
INTO ASC_Sandbox.ASCOF_2D_CLD_Cleaned
FROM ASC_Sandbox.CLD_230401_250630_JoinedSubmissions a
LEFT JOIN #CLD_Latest_Person_Details b
ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
AND a.LA_Code = b.LA_Code
WHERE 
  Client_Type_Cleaned = 'Service User'
  -- Exclude people who died before the start of the reporting period
  AND (b.Date_of_Death >= @ReportingPeriodStartDate OR b.Date_of_Death IS NULL) 
  -- Event must either have a valid end date after start or be ongoing (null end date)
  AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) 
  AND a.Der_NHS_LA_Combined_Person_ID IS NOT NULL

--Set null end dates to future date for ease of processing the clustering
UPDATE ASC_Sandbox.ASCOF_2D_CLD_Cleaned
SET Der_Event_End_Date = 
  CASE 
    WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' 
    ELSE Der_Event_End_Date
END;

--------------------------------------------------------------
-- Identify and create table of long term support events --
--------------------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_LTS
SELECT 
	DISTINCT *
INTO ASC_Sandbox.ASCOF_2D_LTS
FROM ASC_Sandbox.ASCOF_2D_CLD_Cleaned
WHERE Service_Type_Cleaned LIKE 'Long%' 

-------------------------------------------------------
-- Identify and create table of reablement events --
-------------------------------------------------------
DROP TABLE IF EXISTS #Reablement_events
SELECT *
INTO #Reablement_events
FROM ASC_Sandbox.ASCOF_2D_CLD_Cleaned
WHERE Event_Type = 'service'
  AND Service_Type_Cleaned = 'Short Term Support: ST-Max'
	AND Service_Component_Cleaned LIKE 'Reablement'

----------------------------------------
-- Cluster reablement events together --
----------------------------------------
-- 1. Order the data based on the fields listed, assign row numbers and the previous event end date
DROP TABLE IF EXISTS #ST_Max_Grouped

SELECT  
  LA_Code,
  LA_Name,
  ROW_NUMBER () OVER (ORDER BY 
                        LA_Code,
                        LA_Name,
                        Der_NHS_LA_Combined_Person_ID, 
                        Event_Start_Date, 
                        Der_Event_End_Date, 
                        COALESCE(Event_Outcome_Hierarchy, 999) ASC, --Ensures null event outcomes are lowest ranked
                        Der_unique_record_id DESC) AS RN, 
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  Service_Type_Cleaned,
  Service_Component_Cleaned,
  Event_Outcome_Cleaned,
  Der_unique_record_id,
  Event_Outcome_Hierarchy,
  Der_Working_Age_Band,
  DOD_Latest,
  MAX(Der_Event_End_Date) OVER (PARTITION BY 
                              LA_Code, 
                              LA_Name,
                              Der_NHS_LA_Combined_Person_ID 
                            ORDER BY 
                              Event_Start_Date, 
                              Der_Event_End_Date,
                              COALESCE(Event_Outcome_Hierarchy, 999) ASC,
                              Der_unique_record_id DESC 
                              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Date                                 
INTO #ST_Max_Grouped
FROM #Reablement_events


--2. Determine whether events are consecutive (max 7 day apart) or concurrent (overlapping), then assign cluster id 
--   (note: the choice of 7 days ensures that there cannot be another ST-Max event within 7 days of an ST-Max cluster. 
--    This means that an ST-Max can never be the sequel of an ST-Max cluster, 
--    because sequels are determined by looking only in the 7 days following an ST-Max cluster.)

DROP TABLE IF EXISTS #ST_Max_Clusters_Assigned

SELECT 
  *,
  DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) AS Day_Diff,
  CASE 
    WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 7 THEN 0 
    ELSE 1 
  END AS ClusterStartInd,
  SUM (CASE 
          WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 7 THEN 0 
          ELSE 1 END) 
  OVER (ORDER BY LA_Code, RN) AS ST_Max_Cluster_ID
INTO #ST_Max_Clusters_Assigned
FROM #ST_Max_Grouped


--3. Create a table of one line per cluster
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_Reablement

SELECT DISTINCT
  LA_Code AS Reablement_LA_Code,
  LA_Name AS Reablement_LA_Name,
  Der_NHS_LA_Combined_Person_ID, 
  ST_Max_Cluster_ID,
  MIN (Event_Start_Date) AS ST_Max_Cluster_Start,
  MAX(Der_Event_End_Date) AS ST_Max_Cluster_End,
  MAX(DOD_Latest) AS Date_of_Death
INTO ASC_Sandbox.ASCOF_2D_Reablement
FROM #ST_Max_Clusters_Assigned
GROUP BY 
  LA_Code, 
  LA_Name, 
  Der_NHS_LA_Combined_Person_ID, 
  ST_Max_Cluster_ID

--==============================================================================================================================
--================================== STAGE 2: CREATE TABLE OF ALL DISCHARGES (PART 2 DENOMINATOR) ==============================
--------------------------------------
-- SUS All Discharges (Denominator) --
--------------------------------------
DROP TABLE IF EXISTS #SUS_All_Discharges;
SELECT DISTINCT
	Der_DHSC_Pseudo_NHS_Number,
	Admission_Date,
	Discharge_Date,
	APCS_Ident,
	Der_Postcode_LSOA_2011_Code,
	CASE 
		WHEN Month_of_Birth_SUS IS NOT NULL AND Year_of_Birth_SUS IS NOT NULL
		THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Year_of_Birth_SUS, '-', Month_of_Birth_SUS, '-', '01') AS DATE)), Discharge_Date))/365.25)
		ELSE NULL
	END AS Der_Latest_Age_SUS
INTO #SUS_All_Discharges
FROM ASC_Sandbox.SUS_250908 A
WHERE 
	CASE 
		WHEN Month_of_Birth_SUS IS NOT NULL AND Year_of_Birth_SUS IS NOT NULL 
		THEN FLOOR((DATEDIFF(DAY, CAST(CONCAT(Year_of_Birth_SUS, '-', Month_of_Birth_SUS, '-01') AS DATE), Discharge_Date)) / 365.25)
		ELSE NULL 
	END >= 65 -- ensures ages 65+ only is included
	AND Admission_Method NOT IN ('31', '32', '82', '83') -- excludes selected method of admission
	AND Discharge_Method NOT IN ('4', '5') -- excludes patients who died in hospital and stillbirth
	AND Discharge_Destination NOT IN ('49', '50', '51', '52', '79', '87', '88') -- excludes listed discharge destinations

	-- filtering to specific acute
	AND Treatment_Function_Code NOT IN (
		  '199', '223', '290', '291', '331', '344', '345', '346', -- Others
		  '424', -- Well Babies
		  '499', '501', '504', '560', -- Maternity codes
		  '660', '661', '662', '840', '920') -- Others
	AND Treatment_Function_Code NOT LIKE '65%'  -- Other exclusions
	AND Treatment_Function_Code NOT LIKE '7%'   -- Mental health & learning disabilities
	
  --filter on dates
  AND Admission_Date >= '2023-04-01' --Cut off date is CLD start (would only be included in the reporting period anyway if hospital stay & reablement was 1+ years)
	AND Admission_Date < Discharge_Date -- DQ check to ensure admission occurs before discharge
  AND Discharge_Date BETWEEN @ReportingPeriodStartDate AND @ReportingPeriodEndDate  --Updated definition counting based on discharges which occured in the year period

  --Filter to discharge epsiode
  AND APCS_Last_Ep_Ind = 1 -- Includes the last hospital episode

  -- Exclude those not having unique identifier
  AND A.Der_DHSC_Pseudo_NHS_Number IS NOT NULL

	-- Exclude patients in listed long-term support types at time of admission
	AND NOT EXISTS (SELECT 1 FROM ASC_Sandbox.ASCOF_2D_LTS B --replace with #All_LTS_Cohort when agreed
		WHERE 
			A.Der_DHSC_Pseudo_NHS_Number = B.Der_NHS_LA_Combined_Person_ID
			AND B.Service_Type_Cleaned IN ('Long Term Support: Nursing Care','Long Term Support: Residential Care','Long Term Support: Prison')
			AND (A.Admission_Date BETWEEN B.Event_Start_Date AND B.Der_Event_End_Date OR (A.Admission_Date >= B.Event_Start_Date AND B.Der_Event_End_Date IS NULL)));

------------------------------------------
-- Map LSOA on SUS Discharge to UTLA -----
------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_All_Discharges;  --Will become the part 2 denominator
SELECT DISTINCT
    a.*,
	b.UTLA_Name_2023 AS UTLA_Name,

    -- Determine cohort type based on presence in LTS
    CASE 
        WHEN EXISTS (SELECT 1 FROM ASC_Sandbox.ASCOF_2D_LTS c
            WHERE c.Der_NHS_LA_Combined_Person_ID = a.Der_DHSC_Pseudo_NHS_Number
                AND c.Service_Type_Cleaned = 'Long Term Support: Community'
                AND (a.Admission_Date BETWEEN c.Event_Start_Date AND c.Der_Event_End_Date
                    OR (a.Admission_Date >= c.Event_Start_Date AND c.Der_Event_End_Date IS NULL))) 
		THEN 'Those in LT community support'
        ELSE 'Those not in any LTS' --since we excluded LTS (prison, nursing and residential) in process 1 already
    END AS Client_Type
INTO ASC_Sandbox.ASCOF_2D_All_Discharges
FROM #SUS_All_Discharges a
LEFT JOIN ASC_Sandbox.REF_ONS_LSOA11_UTLA23_Lookup b
    ON a.Der_Postcode_LSOA_2011_Code = b.LSOA_Code_2011
WHERE b.UTLA_Code_2023 LIKE 'E%';  --England only

--==================================================================================================================================
--================== STAGE 3: LINK REABLEMENT TO SUS HOSPITAL DISCHARGES (PART 1 DENOMINATOR AND PART 2 NUMERATOR) =================
----------------------------------------------------
-- Link SUS data with reablement events data ---
----------------------------------------------------
--This simply joins the two datasets together and not yet filters on related events
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_STMax_Linked_CLD_SUS;
SELECT 
	a.*,
	b.*
INTO ASC_Sandbox.ASCOF2D_STMax_Linked_CLD_SUS
FROM ASC_Sandbox.ASCOF_2D_All_Discharges a
LEFT JOIN ASC_Sandbox.ASCOF_2D_Reablement b
ON A.Der_DHSC_Pseudo_NHS_Number = B.Der_NHS_LA_Combined_Person_ID;

--------------------------------------------------------------------------------------------------
-- Filter for the latest discharge date within the reablement/discharge date overlapping period --
--------------------------------------------------------------------------------------------------
-- Hospital discharge date can occur up to 7 days prior to reablement starting and up to 3 days after. 
-- There may be multiple hospital discharges within this period so we take the latest discharge.

DROP TABLE IF EXISTS #Max_Date
SELECT DISTINCT 
	ST_Max_Cluster_ID,
	MAX(Discharge_Date) AS Max_Discharge_Date
INTO #Max_Date
FROM ASC_Sandbox.ASCOF2D_STMax_Linked_CLD_SUS
WHERE Discharge_Date >= DATEADD(dd, -7, ST_Max_Cluster_Start) AND Discharge_Date <= DATEADD(dd, 3, ST_Max_Cluster_Start) 
AND Admission_Date <= ST_Max_Cluster_Start
GROUP BY ST_Max_Cluster_ID 

-- Create table joining the tables above
DROP TABLE IF EXISTS #Filter_Date
SELECT DISTINCT 
	a.*,
	b.Max_Discharge_Date AS Max_Discharge_Date
INTO #Filter_Date
FROM ASC_Sandbox.ASCOF2D_STMax_Linked_CLD_SUS a
LEFT JOIN #Max_Date b
  ON a.ST_Max_Cluster_ID = b.ST_Max_Cluster_ID 
 AND a.Discharge_Date = b.Max_Discharge_Date;

-- Remove those without the latest discharge date and save output
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_Discharges_Into_Reablement  --Will form part 2 numerator and also part 1 denominator
SELECT DISTINCT 
  Der_DHSC_Pseudo_NHS_Number,
  APCS_Ident,
  Discharge_Date,
  Der_Latest_Age_SUS,
  UTLA_Name,
  Client_Type,
  Reablement_LA_Code,
  Reablement_LA_Name,
  Der_NHS_LA_Combined_Person_ID,
  ST_Max_Cluster_ID,
  ST_Max_Cluster_Start,
  ST_Max_Cluster_End,
  Date_of_Death 
INTO ASC_Sandbox.ASCOF_2D_Discharges_Into_Reablement
FROM #Filter_Date
WHERE Discharge_Date = Max_Discharge_Date --if a reablement episode is linked with 2 discharges, the one with the later date is retained

/*Caveat:
There are more rows than reablement episodes because 475 are linked to more than one unique discharge despite having the same discharge date (APCS_Ident) 
These will be counted as one discharge-reablement linked episode
*/

--=================================================================================================================
--============================= STAGE 4: IDENTIFY OUTCOMES (PART 1 NUMERATOR) =====================================
-----------------------------------------------
-- Create a table of admissions to hospital ---
-----------------------------------------------
DROP TABLE IF EXISTS #Readmissions_Data
SELECT 
	Admission_Method AS Readmission_Admission_Method,
	Admission_Date AS Readmission_Admission_Date,
	Der_DHSC_Pseudo_NHS_Number AS Readmission_Der_DHSC_Pseudo_NHS_Number,
	APCS_Ident AS Readmission_APCS_Ident
INTO #Readmissions_Data
FROM ASC_Sandbox.SUS_250908
WHERE 
Admission_Date < Discharge_Date -- excludes DQ issues and zero lengths of stay
	AND Der_Management_Type IN ('EM', 'NE')
	AND Discharge_Method NOT IN ('5')
	AND APCS_Last_Ep_Ind = 1
	AND Admission_Date >= @ReportingPeriodStartDate 

------------------------------------------------------------
-- Create outcome flags (readmissions, deaths, LTS entry) --
------------------------------------------------------------
-- Link to the readmissions data and table of LTS to identify outcomes within the 12 week follow-up period
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2D_Discharge_Reablement_Outcomes;

SELECT
    X.*,
   MAX(LTS_ResNurs_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS LTS_Cluster_Flag,
   MAX(Readmissions_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS Readmissions_Cluster_Flag,
   MAX(Deaths_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS Deaths_Cluster_Flag
INTO ASC_Sandbox.ASCOF_2D_Discharge_Reablement_Outcomes
FROM (
    SELECT
        A.*,

        /* Readmission within 12 weeks */
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM #Readmissions_Data B
                WHERE B.Readmission_Der_DHSC_Pseudo_NHS_Number = A.Der_NHS_LA_Combined_Person_ID
                  AND B.Readmission_Admission_Date > A.ST_Max_Cluster_Start
                  AND B.Readmission_Admission_Date > A.Discharge_Date
                  AND B.Readmission_Admission_Date <= DATEADD(DAY, 84, A.Discharge_Date)
            ) THEN 1 ELSE 0
        END AS Readmissions_Flag,

        /* Death within 12 weeks */
        CASE
            WHEN A.Date_of_Death IS NOT NULL
             AND A.Date_of_Death >= A.Discharge_Date
             AND A.Date_of_Death <= DATEADD(DAY, 84, A.Discharge_Date)
            THEN 1 ELSE 0
        END AS Deaths_Flag,

        /* LTS Residential/Nursing within 12 weeks */
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM ASC_Sandbox.ASCOF_2D_LTS  C
                WHERE C.Der_NHS_LA_Combined_Person_ID = A.Der_NHS_LA_Combined_Person_ID
                  AND C.Service_Type_Cleaned IN ('Long Term Support: Residential Care', 'Long Term Support: Nursing Care')
                  AND C.Event_Start_Date >= A.Discharge_Date
                  AND C.Event_Start_Date <= DATEADD(DAY, 84, A.Discharge_Date)
            ) THEN 1 ELSE 0
        END AS LTS_ResNurs_Flag

    FROM ASC_Sandbox.ASCOF_2D_Discharges_Into_Reablement A  -- Base Table
) AS X;

--===================================================================================================
--============================= OUTPUTS: PART 1 METRIC BREAKDOWN ====================================

-- Generate Outcome Combination Source Table (all 8 possible combos from 000 to 111)
-- i.e stringing together DEATH + READMISSION + LTS NURS/RES. Such that 100 = death only, 110 = death and readmission etc
DROP TABLE IF EXISTS #Outcome_Combo_Event;
SELECT DISTINCT
    ST_Max_Cluster_ID,
    Der_NHS_LA_Combined_Person_ID,
    Reablement_LA_Name AS Area,
    Client_Type,
    
    -- Retain flags
    Deaths_Cluster_Flag,
    Readmissions_Cluster_Flag,
    LTS_Cluster_Flag,
    
    -- Create binary outcome combination string
    CAST(Deaths_Cluster_Flag AS VARCHAR(1)) 
      + CAST(Readmissions_Cluster_Flag AS VARCHAR(1)) 
      + CAST(LTS_Cluster_Flag AS VARCHAR(1)) AS Outcome_Combo

INTO #Outcome_Combo_Event
FROM ASC_Sandbox.ASCOF_2D_Discharge_Reablement_Outcomes;


-- Create numerator flag from outcome combo (i.e when Outcome_Combo = '000', absence of death + readmission + LTS)
DROP TABLE IF EXISTS #Discharge_With_Outcome;
SELECT DISTINCT
  ST_Max_Cluster_ID,
  Der_NHS_LA_Combined_Person_ID,
  Client_Type,
  Area,
  Outcome_Combo,
  Deaths_Cluster_Flag,
  Readmissions_Cluster_Flag,
  LTS_Cluster_Flag,
  CASE 
      WHEN Outcome_Combo = '000' THEN 1
      ELSE 0
  END AS IsNumerator
INTO #Discharge_With_Outcome
FROM #Outcome_Combo_Event

------------------------------------------------------------
-- England/Local Authority-Level Breakdown by Client Type --
------------------------------------------------------------
--Create ref table of all combinations of LA and Client Type to ensure nulls are outputted
DROP TABLE IF EXISTS #REF_Client_Type_LA
SELECT 
  Client_Type, 
  LA_Name as Area
INTO #REF_Client_Type_LA
FROM (
  SELECT DISTINCT Client_Type
  FROM #Discharge_With_Outcome
  UNION
  SELECT 'Total' AS Client_Type )A
CROSS JOIN (
SELECT DISTINCT LA_Name 
FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup 
UNION 
SELECT 'England' AS LA_Name) B

--Cross join the figures with the ref table
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Metrics;
SELECT
  r.Area, 
  r.Client_Type, 
  FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
  a.Denominator,
  a.Numerator,
  a.Outcome_Percent,
  a.Died,
  a.Readmitted,
  a.Entered_LT_ResNurs
INTO ASC_Sandbox.ASCOF2D_Part1_Metrics
FROM #REF_Client_Type_LA r
LEFT JOIN (
SELECT
	ISNULL(Area, 'England') AS Area,
    ISNULL(Client_Type, 'Total') AS Client_Type,
    COUNT(DISTINCT ST_Max_Cluster_ID) AS Denominator,
    COUNT(DISTINCT CASE WHEN IsNumerator = 1 THEN ST_Max_Cluster_ID END) AS Numerator,
    CAST(ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN IsNumerator = 1 THEN ST_Max_Cluster_ID END)
        / NULLIF(COUNT(DISTINCT ST_Max_Cluster_ID), 0), 2
    ) AS DECIMAL(5,2)) AS Outcome_Percent,
    COUNT(DISTINCT CASE WHEN Deaths_Cluster_Flag = 1 THEN ST_Max_Cluster_ID END) AS Died,
    COUNT(DISTINCT CASE WHEN Readmissions_Cluster_Flag = 1 THEN ST_Max_Cluster_ID END) AS Readmitted,
    COUNT(DISTINCT CASE WHEN LTS_Cluster_Flag = 1 THEN ST_Max_Cluster_ID END) AS Entered_LT_ResNurs
FROM #Discharge_With_Outcome 
GROUP BY ROLLUP(Area), ROLLUP(Client_Type)  --ROLLUP creates the total of client type for each LA and the total of all LAs for england
) a
ON r.Area = a.Area AND r.Client_Type = a.Client_Type

----------------------------------------
-- Outcome Breakdown for Venn Diagram --
----------------------------------------
DROP TABLE IF EXISTS #Base_Outcome_Combo;
SELECT 
    Area,
    Client_Type,
    ST_Max_Cluster_ID,
    Outcome_Combo
INTO #Base_Outcome_Combo
FROM #Outcome_Combo_Event;

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn;
SELECT v.*
INTO ASC_Sandbox.ASCOF2D_Venn
FROM (
    SELECT
        -- Reporting period
        FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' 
        + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,

        r.Area,
        r.Client_Type,

        -- DEATH BLOCK
        SUM(CASE WHEN o.Outcome_Combo <> '000' AND LEFT(o.Outcome_Combo,1) = '1'  THEN 1 ELSE 0 END) AS Total_Death,
        SUM(CASE WHEN o.Outcome_Combo = '100' THEN 1 ELSE 0 END) AS Death_Only,
        SUM(CASE WHEN o.Outcome_Combo = '110' THEN 1 ELSE 0 END) AS Death_Readmission,
        SUM(CASE WHEN o.Outcome_Combo = '101' THEN 1 ELSE 0 END) AS Death_LTS,

        -- READMISSION BLOCK
        SUM(CASE WHEN o.Outcome_Combo <> '000' AND SUBSTRING(o.Outcome_Combo,2,1)='1' THEN 1 ELSE 0 END) AS Total_Readmission,
        SUM(CASE WHEN o.Outcome_Combo = '010' THEN 1 ELSE 0 END)AS Readmission_Only,
        SUM(CASE WHEN o.Outcome_Combo = '011' THEN 1 ELSE 0 END) AS Readmission_LTS,

        -- LTS BLOCK
        SUM(CASE WHEN o.Outcome_Combo <> '000' AND RIGHT(o.Outcome_Combo,1)='1' THEN 1 ELSE 0 END) AS Total_LTS,
        SUM(CASE WHEN o.Outcome_Combo = '001' THEN 1 ELSE 0 END) AS LTS_Only,

        --ALL 3 OUTCOMES BLOCK (appear once)
        SUM(CASE WHEN o.Outcome_Combo = '111' THEN 1 ELSE 0 END) AS All_3_Outcomes

    FROM #REF_Client_Type_LA r
    --LEFT JOIN #Outcome_Combo_Event o
	LEFT JOIN #Base_Outcome_Combo o
		   --England rows aggregate across all areas
           ON (o.Area = r.Area OR r.Area = 'England')
		   -- 'Total' aggregate across all client type
          AND (o.Client_Type = r.Client_Type OR r.Client_Type = 'Total')
          AND o.Outcome_Combo <> '000'
    GROUP BY r.Area, r.Client_Type  -- didnt need the rollup here, I inherited the existing #REF_Client_Type_LA table you created
) v;

--===================================================================================================
--============================= OUTPUTS: PART 2 METRIC BREAKDOWN ====================================

--Select all discharges denominator from main table
DROP TABLE IF EXISTS #Denominator
SELECT 
  CASE  
        WHEN UTLA_Name = 'Cumberland' THEN 'Cumberland Council' --rename to match CLD
        WHEN UTLA_Name = 'Isle of Wight' THEN 'Isle of Wight Council'
        WHEN UTLA_Name = 'St. Helens' THEN 'St Helens'
        WHEN UTLA_Name = 'Telford and Wrekin' THEN 'Telford and the Wrekin'
        WHEN UTLA_Name = 'Westmorland and Furness' THEN 'Westmorland and Furness Council'
        WHEN UTLA_Name = 'County Durham' THEN 'Durham'
        WHEN UTLA_Name = 'Kensington and Chelsea' THEN 'Royal Borough of Kensington and Chelsea'
        WHEN UTLA_Name = 'Windsor and Maidenhead' THEN 'Royal Borough Windsor and Maidenhead'
        WHEN UTLA_Name = 'Southend-on-Sea' THEN 'Southend on Sea'
        WHEN UTLA_Name = 'Stoke-on-Trent' THEN 'Stoke on Trent'
        WHEN UTLA_Name IS NULL THEN 'England'
  ELSE UTLA_Name 
  END AS Area,
  ISNULL(Client_Type, 'Total') AS Client_Type,
  COUNT(DISTINCT APCS_Ident) AS Denominator
INTO #Denominator
FROM ASC_Sandbox.ASCOF_2D_All_Discharges
GROUP BY ROLLUP(Client_Type), ROLLUP(UTLA_Name)

--Select numerator as the denominator from part 1
DROP TABLE IF EXISTS #Numerator
SELECT 
  r.Area, 
  r.Client_Type, 
  n.Denominator AS Numerator
INTO #Numerator
FROM #REF_Client_Type_LA r  --Join to all combinations of LA and client type to ensure all outputted
LEFT JOIN ASC_Sandbox.ASCOF2D_Part1_Metrics n
ON r.Area = n.Area AND
r.Client_type = n.Client_Type

--Join together and create outcome
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Metrics;
SELECT 
    FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
	  n.Area,
    n.Client_Type,
    d.Denominator AS Discharges,
    n.Numerator AS Reablement_Count,
    CAST(ROUND(
        100.0 * n.Numerator / NULLIF(d.Denominator, 0), 2
    ) AS DECIMAL(5,2)) AS Outcome_Percent
INTO ASC_Sandbox.ASCOF2D_Part2_Metrics
FROM #Numerator n
LEFT JOIN #Denominator d
  ON d.Area = n.Area AND
  d.Client_type = n.Client_Type
 
/*--Caveats on part 2:
There are instances where one discharge is linked to multiple reablement epsiodes, due to:
> time thresholds (7d for reablement clustering, and reablement start date 3d before or 7d after discharge)
mean one discharge can legitimately be linked to two reablements if they are >7d apart but within the discharge window
> different LAs, one discharge could be linked to two reablement events from two different LAs CLD
Here two discharge-reablement episodes are counted in the part 2 numerator.
Affects approx. 40 reablement episodes ( <0.1% of reablement, <0.1% of discharges)

There are instances where multiple discharge records are linked to one reablement episode, due to:
> time thresholds, multiple discharges are within the window for linking to one reablement. 
> DQ of the discharge data, two different LSOA of patient or two different admission dates but same discharge date.
Here only one discharge-reablement episode is counted in the part 2 numerator.
Affects approx. 500 of cases ( <0.5% of reablement, <0.1% of discharges)
*/