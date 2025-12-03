--Main script for running each ASCOF measure and joining outputs together

--The reporting period start and end dates determine the statistical reporting year
--The input table selected dertermines the period the data covers for analysis, as some measures required an extended period beyond the reporting year
--Some measures have multiple output tables, where additional information is stored (excluded counts, disaggregated outcomes etc)

/*
The main input tables prior to October 2025 have had any release 2 data mapped back to release 1.
From OCtober 2025 onwards any release 1 data is mapped forwards to release 2
This is required as we transition from one specification to the other.

***Update for October 25 submissions
The figures for all reporting periods prior to Q1 July 24 - June 25 have been revised using the data from the July subsmissions. 
From October 2025 onwards, ASCOF figures for the latest reporting period only will be provided, except 2D which lags by 3 months.
The previous reporting periods figures are now fixed.

-------UPDATED SCRIPT---------- 
This script is an update for ASCOF R2 - from the next reporting period, new data will get added onto existing tables rather than redoing all the figures for previous periods (as described above). 
*/


--======= Create latest person details table ======
EXEC ASC_Sandbox.Create_person_details_table
  @ReportingPeriodStartDate = '2024-10-01',
  @ReportingPeriodEndDate = '2025-09-30',
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions', --use joined submissions table
  @OutputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data'


--================== ASCOF 2A =====================
--Latest reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards 
  @ReportingPeriodStartDate = '2024-10-01', 
  @ReportingPeriodEndDate = '2025-09-30',  
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_Latest',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_Latest'


--================== ASCOF 2B & 2C =====================
--Latest reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-10-01', 
  @ReportingPeriodEndDate = '2025-09-30',  
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_Latest'


--================== ASCOF 2E =====================

----Latest reporting year----

  --Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-10-01',
  @ReportingPeriodEndDate = '2025-09-30', 
  @LD_Filter = 1,
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_Latest'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-10-01',
  @ReportingPeriodEndDate = '2025-09-30', 
  @LD_Filter = 0,
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_Latest'

--================== ASCOF 3D =====================
----Latest reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-10-01',
  @ReportingPeriodEndDate = '2025-09-30', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_Latest'


--================== ASCOF 2D ====================
--To ensure the figures are reproducible a static cut of SUS data is required (as it updates daily). 
--Uncomment this code block if you wish to refresh the SUS base data, otherwise skip this step. Remember to edit 
--the date prefix on the table name to reflect the date of the refresh

/*
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_SUS_251125;  --update date
SELECT *
INTO ASC_Sandbox.ASCOF2D_SUS_251125  --update date
FROM DHSC_SUS.APCE
WHERE Discharge_Date BETWEEN '2023-04-01' AND CAST(GETDATE() AS DATE);
*/

----Latest reporting period  (2D is always 3 months behind the latest reporting period, as need 12 weeks extra data to measure outcomes)
EXEC ASC_Sandbox.Create_ASCOF2D
--Dates
  @ReportingPeriodStartDate = '2024-07-01', --these dates are always 3m behind the reporting period 
  @ReportingPeriodEndDate = '2025-06-30', 
--Inputs
  @InputTable = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250930_JoinedSubmissions_Latest_Person_Data',
  @InputTable_SUS = 'ASC_Sandbox.ASCOF2D_SUS_251125',  --should be updated with ASC sandbox table made in step above commented out
--Outputs
  @OutputTable_Part1 = 'ASC_Sandbox.ASCOF2D_Part1_Latest',
  @OutputTable_Part1_Demographics = 'ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest',
  @OutputTable_Part2 = 'ASC_Sandbox.ASCOF2D_Part2_Latest',
  @OutputTable_Part2_Demographics = 'ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest',
  @OutputTable_Venn = 'ASC_Sandbox.ASCOF2D_Venn_Latest'

--Final tables for ASCOF 2D-----

--ASCOF 2D Part 1 metrics
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Metrics
SELECT * 
INTO ASC_Sandbox.ASCOF2D_Part1_Metrics
FROM ASC_Sandbox.ASCOF2D_Part1_Latest --latest reporting period
UNION ALL
SELECT*
FROM DHSC_Reporting.ASCOF2D_Part1_Metrics --table with prev. reporting periods for part 1

--ASCOF 2D Part 1 metrics demographic breakdown
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Metrics_Demographic_Breakdown
SELECT * 
INTO ASC_Sandbox.ASCOF2D_Part1_Metrics_Demographic_Breakdown
FROM ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest --latest reporting period (demographics)
UNION ALL
SELECT *
FROM DHSC_Reporting.ASCOF2D_Part1_Metrics_Demographic_Breakdown --table containing prev. reporting periods (demographics) for part 1

--ASCOF 2D Part 2 metrics
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Metrics
SELECT * 
INTO ASC_Sandbox.ASCOF2D_Part2_Metrics
FROM ASC_Sandbox.ASCOF2D_Part2_Latest --latest reporting period
UNION ALL
SELECT *
FROM DHSC_Reporting.ASCOF2D_Part2_Metrics --table with prev. reporting periods for part 2

--ASCOF 2D Part 2 metrics demographic breakdown
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Metrics_Demographic_Breakdown
SELECT *
INTO ASC_Sandbox.ASCOF2D_Part2_Metrics_Demographic_Breakdown
FROM ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest ----latest reporting period
UNION ALL
SELECT *
FROM DHSC_Reporting.ASCOF2D_Part2_Metrics_Demographic_Breakdown --table containing prev. reporting periods (demographics) for part 2

--ASCOF 2D Venn diagram
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn
SELECT * 
INTO ASC_Sandbox.ASCOF2D_Venn
FROM ASC_Sandbox.ASCOF2D_Venn_Latest --latest reporting period for Venn diagram
UNION ALL
SELECT *
FROM DHSC_Reporting.ASCOF2D_Venn --table containing prev. reporting periods for venn diagram

--================== Join all outcomes tables together =====================

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF;

-- 2A
SELECT 
  Reporting_Period, --list fields for 2A to ensure columns are in the same order when unioned together
  LA_Code,
  LA_Name, 
  Measure, 
  [Description], 
  [Group],
  Numerator,
  Denominator,
  Outcome
INTO ASC_Sandbox.LA_PBI_ASCOF
FROM DHSC_Reporting.LA_PBI_ASCOF
UNION ALL
SELECT  
  Reporting_Period,
  LA_Code,
  LA_Name, 
  Measure, 
  [Description], 
  [Group],
  Numerator,
  Denominator,
  Outcome
FROM ASC_Sandbox.ASCOF_2A_Latest

UNION ALL

-- 2BC
SELECT *
FROM ASC_Sandbox.ASCOF_2BC_Latest

UNION ALL

-- 2E Part 1 (LD)

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Latest

UNION ALL

-- 2E Part 1 (All)

SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_Latest

UNION ALL

-- 3D 

SELECT *
FROM ASC_Sandbox.ASCOF_3D_Latest;


--================== Join together additional tables for some metrics =====================

--Output the following into one table:
--Number with unknown accommodation status for 2E (now incl. in denom)
--Number with unknown delivery mechanism for 3D

-- 2E Part 1
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_Excl;

SELECT*
INTO ASC_Sandbox.LA_PBI_ASCOF_Excl
FROM DHSC_Reporting.LA_PBI_ASCOF_Excl
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_Latest

UNION ALL

-- 2E Part 2
SELECT*
FROM ASC_Sandbox.ASCOF_2E_All_Unk_Latest

UNION ALL

-- 3D
SELECT*
FROM ASC_Sandbox.ASCOF_3D_Unk_Latest

--Output 2A figures disaggregated by final outcome
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated;

SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated
FROM DHSC_Reporting.LA_PBI_ASCOF_2A_Disaggregated
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_Latest;


-- Output 2C Figures for BCF dashboard page
--has remained the same for R2 as the R1 version of the ASCOF code

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_BCF

SELECT
  a.*,
  CASE 
    WHEN a.Numerator < 6 THEN 0 
    ELSE a.Numerator 
  END AS Numerator_Supressed,
  CASE 
    WHEN a.Denominator < 6 THEN 0 
    ELSE a.Denominator 
  END AS Denominator_Supressed,
  b.Region
INTO ASC_Sandbox.LA_PBI_ASCOF_BCF
FROM ASC_Sandbox.LA_PBI_ASCOF a
LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b
ON a.LA_Code = b.LA_Code
WHERE Measure = 'ASCOF 2C'
AND Reporting_Period <> '1 Apr 23 - 31 Mar 24'  --Exclude first year


--============= QA Checks =============================
--These assess that the union has worked correctly 
--Should be 0
SELECT COUNT(*) AS 'Rows where Num > Denom'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE Numerator > Denominator

--Should be 0
SELECT COUNT(*) AS 'Rows where Outcome > 100'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE Outcome > 100 AND Measure NOT IN ('ASCOF 2B', 'ASCOF 2C')

--Should be 0
SELECT COUNT(*) AS 'Group contains wrong info'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE [Group] NOT IN ('Male', 'Female', '65 and above', '18 to 64', 'Total')

--SHould be 0
SELECT COUNT(*) AS 'Measure contains wrong info'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE [Measure] NOT LIKE ('ASCOF%') 

--QA checks for ASCOF 2D (part 1)
--Should be 0
SELECT COUNT(*) AS 'Rows where Num > Denom'
FROM ASC_Sandbox.ASCOF2D_Part1_Metrics
WHERE Numerator > Denominator

--Should be 0
SELECT COUNT(*) AS 'Rows where Outcome_Percent > 100'
FROM ASC_Sandbox.ASCOF2D_Part1_Metrics
WHERE Outcome_Percent > 100 

--QA checks for ASCOF 2D (part 2)
--Should be 0
SELECT COUNT(*) AS 'Reablement_Count > Discharges'
FROM ASC_Sandbox.ASCOF2D_Part2_Metrics
WHERE Reablement_Count > Discharges

--Should be 0
SELECT COUNT(*) AS 'Rows where Outcome_Percent > 100'
FROM ASC_Sandbox.ASCOF2D_Part2_Metrics
WHERE Outcome_Percent > 100 

--============= Delete tables no longer required =====================


--2A
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Latest

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_Latest

--2BC
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_Latest

--2E
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_Latest

--3D
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_Latest

--2D 

--Part 1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest

--Part 2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest

--Venn 
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn_Latest
