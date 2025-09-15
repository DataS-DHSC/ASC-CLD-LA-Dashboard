--Main script for running each ASCOF measure and joining outputs together

--The reporting period start and end dates determine the statistical reporting year
--The input table selected dertermines the period the data covers for analysis, as some measures required an extended period beyond the reporting year
--Some measures have multiple output tables, where additional information is stored (excluded counts, disaggregated outcomes etc)

/*
The main input tables for 24/25 onwards have had a mapping applied to them to deal with differences between R1 and R2 specifications.
This is required as some LAs have begun to submit data against the R2 specification. 
To deal with this in the iterim before moving all scripts to R2, any data submitted under R2 is mapped back to R1. 
The tables for 24/25 have slightly different field names where mapping has been applied, therefore slight different codes are required to deal with these changes.

***Update for July 25 submissions
The figures for all reporting periods have been revised using the data from the latest subsmissions. 
This is to ensure the latest methods are applied to all figures and many LAs resubmitted data >12months.
Going forward, the previous reporting periods figures will be fixed and only new figures will be produced for the latest 12 month period. 


**ASCOF 2D update
The code for ASCOF 2D is currently a separate script and is not yet converted to a stored procedure or integrated into this pipeline.
*/


--================== ASCOF 2A =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP1',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP1'

--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP2',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP2'

--24/25 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards 
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP3',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP3'

--July 24/Jun 25 reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards 
  @ReportingPeriodStartDate = '2024-07-01', 
  @ReportingPeriodEndDate = '2025-06-30',  
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP4',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP4'


--================== ASCOF 2B & 2C =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425', 
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP1'

--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',  
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP2'

--24/25 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',  
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP3'

--July 24/Jun 25 reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-07-01', 
  @ReportingPeriodEndDate = '2025-06-30',  
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP4'


--================== ASCOF 2E =====================

----23/24 statistical reporting year----

--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @LD_Filter = 1,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP1'

--Part 2: All PSR Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @LD_Filter = 0, 
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP1'

--Jan 24/Dec 24 reporting year

--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @LD_Filter = 1,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP2'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @LD_Filter = 0,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP2'

----24/25 statistical reporting year----

  --Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 1,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP3'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 0,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP3'

----July 24/June 25 reporting year----

  --Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-07-01',
  @ReportingPeriodEndDate = '2025-06-30', 
  @LD_Filter = 1,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP4',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP4'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-07-01',
  @ReportingPeriodEndDate = '2025-06-30', 
  @LD_Filter = 0,
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP4',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP4'

--================== ASCOF 3D =====================

----23/24 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP1'

--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP2'

----24/25 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP3'

----July 24/June 25 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-07-01',
  @ReportingPeriodEndDate = '2025-06-30', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP4',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP4'

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
FROM ASC_Sandbox.ASCOF_2A_RP1

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
FROM ASC_Sandbox.ASCOF_2A_RP2

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
FROM ASC_Sandbox.ASCOF_2A_RP3

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
FROM ASC_Sandbox.ASCOF_2A_RP4

UNION ALL

-- 2BC
SELECT * 
FROM ASC_Sandbox.ASCOF_2BC_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2BC_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2BC_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2BC_RP4

UNION ALL

-- 2E Part 1 (LD)

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_LD_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_LD_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_RP4

UNION ALL

-- 2E Part 1 (All)

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_RP4

UNION ALL

-- 3D 
SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP4;


--================== Join together additional tables for some metrics =====================

--Output the following into one table:
--Number with unknown accommodation status for 2E (now incl. in denom)
--Number with unknown delivery mechanism for 3D

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_Excl;

-- 2E Part 1
SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_Excl
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2E_LD_Unk_RP4

UNION ALL

-- 2E Part 2
SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP2

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP3

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP4

UNION ALL

-- 3D
SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_Unk_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_Unk_RP4;

--Output 2A figures disaggregated by final outcome

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated;

SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP3

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP4;

-- Output 2C Figures for BCF dashboard page

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
SELECT COUNT(*) AS 'Rows where Num > Denom'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE Numerator > Denominator

SELECT COUNT(*) AS 'Rows where Outcome > 100'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE Outcome > 100 AND Measure NOT IN ('ASCOF 2B', 'ASCOF 2C')

SELECT COUNT(*) AS 'Group contains wrong info'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE [Group] NOT IN ('Male', 'Female', '65 and above', '18 to 64', 'Total')

SELECT COUNT(*) AS 'Measure contains wrong info'
FROM ASC_Sandbox.LA_PBI_ASCOF
WHERE [Measure] NOT LIKE ('ASCOF%')


--============= Delete tables no longer required =====================
/*
--2A
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP4

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP4

--2BC
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP4

--2E
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP4
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP4
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP4
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP4

--3D
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP4

*/