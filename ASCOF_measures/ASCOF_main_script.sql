--Main script for running each ASCOF measure and joining outputs together

--The reporting period start and end dates determine the statistical reporting year figures are calculated for
--The input table selected dertermines the period the data covers for analysis, as some measures required an extended period beyond the reporting year
--Some measures have multiple output tables, where additional information is stored (excluded counts, disaggregated outcomes etc)

/*
The main input tables prior to October 2025 have had any release 2 data mapped back to release 1.
From October 2025 onwards any release 1 data is mapped forwards to release 2.
This is required as we transition from one specification to the other.

From January 2026 onwards, ASCOF figures for the latest reporting period are provided and those for the latest prior period are revised. 
E.g. In January 2026 figures are produced for Jan 25 - Dec 25 and figures for Oct 24 - Sept 25 are revised. 
ASCOF 2D follows the same approach but has a 3 month lag on the reporting periods.

*/

-- =====================================================
-- Create latest person details table
-- ===================================================== 
--NB this is currently run in this ASCOF script but will move to main table processing
--Comment out only when producing the new table for the latest period

/*
EXEC ASC_Sandbox.Create_person_details_table
  @ReportingPeriodStartDate = '2025-04-01',  --<<<<<<<<<< UPDATE
  @ReportingPeriodEndDate = '2026-03-31',    --<<<<<<<<<< UPDATE
  @InputTable = 'DHSC_Reporting.CLD_230401_260331_JoinedSubmissions', --use joined submissions table   --<<<<<<<<<< UPDATE
  @OutputTable = 'ASC_Sandbox.CLD_230401_260331_JoinedSubmissions_Latest_Person_Data'               --<<<<<<<<<< UPDATE
  */


-- =====================================================
-- Create cut of SUS data in order to reproduce figures
-- ===================================================== 
--To ensure the figures are reproducible a static cut of SUS data is required (as it updates daily). 
--Uncomment this code block if you wish to refresh the SUS base data, otherwise skip this step. 
--Update the date suffix with the date of the refresh

/*
SELECT DISTINCT
  Der_DHSC_Pseudo_NHS_Number, --only retain required fields
  APCS_Ident,
  APCS_Last_Ep_Ind,
  Admission_Date,
  Discharge_Date,
  Month_of_Birth_SUS,
  Year_of_Birth_SUS,
  Sex,
  Der_Postcode_LSOA_2011_Code,
  Admission_Method,
  Discharge_Method,
  Discharge_Destination,
  Der_Management_Type,
  Treatment_Function_Code
INTO ASC_Sandbox.ASCOF2D_SUS_260602   --<<<<<<<<<< UPDATE
FROM DHSC_SUS.APCE
WHERE Discharge_Date BETWEEN '2023-04-01' AND CAST(GETDATE() AS DATE);
*/

  
-- =====================================================
-- Set global parameters for producing ASCOF
-- ===================================================== 
--Latest statistical reporting period
DECLARE @LatestStartDate DATE = '2025-04-01';    --<<<<<<<<<< UPDATE
DECLARE @LatestEndDate   DATE = '2026-03-31';    --<<<<<<<<<< UPDATE
DECLARE @InputTable SYSNAME = CONCAT('DHSC_Reporting.CLD_230401_', CONVERT(char(6), @LatestEndDate, 12), '_JoinedSubmissions');
DECLARE @PersonDetailsTable SYSNAME = CONCAT('ASC_Sandbox.CLD_230401_', CONVERT(char(6), @LatestEndDate, 12), '_JoinedSubmissions_Latest_Person_Data');

-- Previous statistical reporting period
DECLARE @PreviousStartDate DATE = DATEADD(MONTH, -3, @LatestStartDate);
DECLARE @PreviousEndDate   DATE = EOMONTH(DATEADD(MONTH, -3, @LatestEndDate));

--ASCOF 2D specific reporting periods (additional 3 month lag to determine outcomes)
DECLARE @2D_LatestStartDate DATE = @PreviousStartDate
DECLARE @2D_LatestEndDate DATE = @PreviousEndDate
DECLARE @2D_PreviousStartDate DATE = DATEADD(MONTH, -3, @PreviousStartDate)
DECLARE @2D_PreviousEndDate DATE = DATEADD(MONTH, -3, @PreviousEndDate)
DECLARE @SUS_InputData SYSNAME = 'ASC_Sandbox.ASCOF2D_SUS_260602'   --<<<<<<<<<< UPDATE

-- ============================================================
-- Output latest figures and revise one previous set of figures
-- ============================================================

--================== ASCOF 2A =====================
--Latest statistical reporting period
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards 
  @ReportingPeriodStartDate = @LatestStartDate, 
  @ReportingPeriodEndDate = @LatestEndDate,  
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_Latest',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_Latest'

--Previous reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards 
  @ReportingPeriodStartDate = @PreviousStartDate, 
  @ReportingPeriodEndDate = @PreviousEndDate,  
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_Previous',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_Previous'

--================== ASCOF 2B & 2C =====================
--Latest statistical reporting period
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = @LatestStartDate, 
  @ReportingPeriodEndDate = @LatestEndDate,  
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_Latest'
  
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = @PreviousStartDate, 
  @ReportingPeriodEndDate = @PreviousEndDate,  
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_Previous'

--================== ASCOF 2E =====================

--Latest statistical reporting period
--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards
  @ReportingPeriodStartDate = @LatestStartDate,
  @ReportingPeriodEndDate = @LatestEndDate, 
  @LD_Filter = 1,
  @InputTable = @InputTable, 
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_Latest'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards
  @ReportingPeriodStartDate = @LatestStartDate,
  @ReportingPeriodEndDate = @LatestEndDate, 
  @LD_Filter = 0,
  @InputTable = @InputTable, 
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_Latest'

--Previous statistical reporting period
--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards
  @ReportingPeriodStartDate = @PreviousStartDate,
  @ReportingPeriodEndDate = @PreviousEndDate, 
  @LD_Filter = 1,
  @InputTable = @InputTable, 
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_Previous',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_Previous'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards
  @ReportingPeriodStartDate = @PreviousStartDate,
  @ReportingPeriodEndDate = @PreviousEndDate, 
  @LD_Filter = 0,
  @InputTable = @InputTable, 
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_Previous',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_Previous'

--================== ASCOF 3D =====================
----Latest reporting year----
EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate = @LatestStartDate,
  @ReportingPeriodEndDate = @LatestEndDate, 
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_Latest',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_Latest'

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate = @PreviousStartDate,
  @ReportingPeriodEndDate = @PreviousEndDate, 
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_Previous',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_Previous'


--================== ASCOF 2D ====================
----Latest reporting period  (2D is always 3 months behind the latest reporting period, as need 12 weeks extra data to measure outcomes)
EXEC ASC_Sandbox.Create_ASCOF2D
--Dates
  @ReportingPeriodStartDate = @2D_LatestStartDate, --dates specific to 2D due to 3m lag
  @ReportingPeriodEndDate = @2D_LatestEndDate, 
--Inputs
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @InputTable_SUS = @SUS_InputData, 
--Outputs
  @OutputTable_Part1 = 'ASC_Sandbox.ASCOF2D_Part1_Latest',
  @OutputTable_Part1_Demographics = 'ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest',
  @OutputTable_Part2 = 'ASC_Sandbox.ASCOF2D_Part2_Latest',
  @OutputTable_Part2_Demographics = 'ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest',
  @OutputTable_Venn = 'ASC_Sandbox.ASCOF2D_Venn_Latest'

----Previous reporting period  (will be an additional 3 months behind the latest 2D figures above)
EXEC ASC_Sandbox.Create_ASCOF2D
--Dates
  @ReportingPeriodStartDate = @2D_PreviousStartDate, --dates specific to 2D due to 3m lag
  @ReportingPeriodEndDate = @2D_PreviousEndDate, 
--Inputs
  @InputTable = @InputTable,
  @InputTable_PersonDetails = @PersonDetailsTable,
  @InputTable_SUS = @SUS_InputData, 
--Outputs
  @OutputTable_Part1 = 'ASC_Sandbox.ASCOF2D_Part1_Previous',
  @OutputTable_Part1_Demographics = 'ASC_Sandbox.ASCOF2D_Part1_Demographics_Previous',
  @OutputTable_Part2 = 'ASC_Sandbox.ASCOF2D_Part2_Previous',
  @OutputTable_Part2_Demographics = 'ASC_Sandbox.ASCOF2D_Part2_Demographics_Previous',
  @OutputTable_Venn = 'ASC_Sandbox.ASCOF2D_Venn_Previous'

  
-- ============================================================
-- Join together with figures for all previous periods
-- ============================================================

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF;

--Take data for previous periods
SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF
FROM DHSC_Reporting.LA_PBI_ASCOF
WHERE Reporting_Period <> --remove existing figures for previous reporting period, to replace with new ones
  FORMAT(CAST(@PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL

--Join with updated figures
--2A
SELECT *
FROM ASC_Sandbox.ASCOF_2A_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2A_Latest
UNION ALL 

--2BC
SELECT *
FROM ASC_Sandbox.ASCOF_2BC_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2BC_Latest
UNION ALL

--2E LD cohort
SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Latest
UNION ALL

--2E All clients
SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_Previous 
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_Latest
UNION ALL

--3D
SELECT *
FROM ASC_Sandbox.ASCOF_3D_Latest
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_3D_Previous

--2D

--ASCOF 2D Part 1 metrics
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Metrics
SELECT *
INTO ASC_Sandbox.ASCOF2D_Part1_Metrics
FROM DHSC_Reporting.ASCOF2D_Part1_Metrics --all previous data
WHERE Reporting_Period <> FORMAT(CAST(@2D_PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@2D_PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part1_Latest --latest reporting period
UNION ALL
SELECT*
FROM ASC_Sandbox.ASCOF2D_Part1_Previous --previous period revised



--ASCOF 2D Part 2 metrics
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Metrics
SELECT *
INTO ASC_Sandbox.ASCOF2D_Part2_Metrics
FROM DHSC_Reporting.ASCOF2D_Part2_Metrics --all previous data
WHERE Reporting_Period <> FORMAT(CAST(@2D_PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@2D_PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part2_Latest --latest reporting period
UNION ALL
SELECT*
FROM ASC_Sandbox.ASCOF2D_Part2_Previous --previous period revised


--================== Join together additional tables for some metrics =====================

--Join the latest figures produced with those for previous reporting periods for additional tables
--1. Number with unknown accommodation status for 2E (now incl. in denom)
--2. Number with unknown delivery mechanism for 3D
--3. BCF data for 2C
--4. Demographic data and venn diagram data for 2D

-- 2E unknown accommodation status
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_Excl;

SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_Excl
FROM DHSC_Reporting.LA_PBI_ASCOF_Excl
WHERE Reporting_Period <>  --remove existing figures for previous reporting period, to replace with new ones
  FORMAT(CAST(@PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@PreviousEndDate AS DATE), 'd MMM yy') 
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_Latest
UNION ALL
SELECT*
FROM ASC_Sandbox.ASCOF_2E_All_Unk_Previous
UNION ALL
SELECT*
FROM ASC_Sandbox.ASCOF_2E_All_Unk_Latest
UNION ALL

-- 3D unkown delivery mechansim
SELECT *
FROM ASC_Sandbox.ASCOF_3D_Unk_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_3D_Unk_Latest

-- 2A disaggregated by final outcome
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated;

SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated
FROM DHSC_Reporting.LA_PBI_ASCOF_2A_Disaggregated
WHERE Reporting_Period <>  --remove existing figures for previous reporting period, to replace with new ones
  FORMAT(CAST(@PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@PreviousEndDate AS DATE), 'd MMM yy') 
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_Previous
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_Latest


DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2C_BCF
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
INTO ASC_Sandbox.LA_PBI_ASCOF_2C_BCF
FROM ASC_Sandbox.LA_PBI_ASCOF a
LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b
ON a.LA_Code = b.LA_Code
WHERE Measure = 'ASCOF 2C'
AND Reporting_Period <> '1 Apr 23 - 31 Mar 24'  --Exclude first year

-- 2D part 1 for BCF Dashboard page
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2D_P1_BCF
SELECT b.LA_Code, b.Region, a.*
INTO ASC_Sandbox.LA_PBI_ASCOF_2D_P1_BCF
FROM ASC_Sandbox.ASCOF2D_Part1_Metrics a
LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b
ON a.Area = b.LA_Name
WHERE Reporting_Period <> '1 Apr 23 - 31 Mar 24'


-- 2D part 2 for BCF Dashboard page
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2D_P2_BCF
SELECT b.LA_Code, b.Region, a.*
INTO ASC_Sandbox.LA_PBI_ASCOF_2D_P2_BCF
FROM ASC_Sandbox.ASCOF2D_Part2_Metrics a
LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b
ON a.Area = b.LA_Name
WHERE Reporting_Period <> '1 Apr 23 - 31 Mar 24'


--ASCOF 2D Part 1 metrics demographic breakdown
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Metrics_Demographic_Breakdown

SELECT * 
INTO ASC_Sandbox.ASCOF2D_Part1_Metrics_Demographic_Breakdown
FROM DHSC_Reporting.ASCOF2D_Part1_Metrics_Demographic_Breakdown
WHERE Reporting_Period <> FORMAT(CAST(@2D_PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@2D_PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest --latest reporting period (demographics)
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part1_Demographics_Previous --previous period revised (demographics)

--ASCOF 2D Part 2 metrics demographic breakdown
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Metrics_Demographic_Breakdown

SELECT * 
INTO ASC_Sandbox.ASCOF2D_Part2_Metrics_Demographic_Breakdown
FROM DHSC_Reporting.ASCOF2D_Part2_Metrics_Demographic_Breakdown
WHERE Reporting_Period <> FORMAT(CAST(@2D_PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@2D_PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest --latest reporting period (demographics)
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Part2_Demographics_Previous --previous period revised (demographics)

--ASCOF 2D Venn diagram
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn
SELECT * 
INTO ASC_Sandbox.ASCOF2D_Venn
FROM DHSC_Reporting.ASCOF2D_Venn
WHERE Reporting_Period <> FORMAT(CAST(@2D_PreviousStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@2D_PreviousEndDate AS DATE), 'd MMM yy')
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Venn_Latest --latest reporting period (venn)
UNION ALL
SELECT *
FROM ASC_Sandbox.ASCOF2D_Venn_Previous --previous period revised (venn)

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

/*
--2A
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_Previous

--2BC
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_Previous

--2E
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_Previous

--3D
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_Latest

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_Previous

--2D 

--Part 1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Demographics_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part1_Demographics_Previous


--Part 2

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Demographics_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Previous
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Part2_Demographics_Previous

--Venn 
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn_Latest
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF2D_Venn_Previous
 */