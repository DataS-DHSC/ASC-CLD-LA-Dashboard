--Main script for running each ASCOF measure and joining outputs together

--The reporting period start and end dates determine the statistical reporting year
--The input table selected dertermines the period the data covers for analysis, as some measures required an extended period beyond the reporting year
--Some measures have multiple output tables, where additional information is stored (excluded counts, disaggregated outcomes etc)

/*
The main input tables for 24/25 onwards have had a mapping applied to them to deal with differences between R1 and R2 specifications.
This is required as some LAs have begun to submit data against the R2 specification. 
To deal with this in the iterim before moving all scripts to R2, any data submitted under R2 is mapped back to R1. 
The tables for 24/25 have slightly different field names where mapping has been applied, therefore slight different codes are required to deal with these changes.
*/


--================== ASCOF 2A =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_240630_JoinedSubmissions',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP1',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP1'


--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_241231_JoinedSubmissions',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP2',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP2'

--24/25 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250331_JoinedSubmissions',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP3',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP3'



--================== ASCOF 2B & 2C =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', --First year of CLD only 23/24 can be used (ideally historic data would be analysed)
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP1'

--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_241231_JoinedSubmissions', --Extended analysis period to identify prior care home admissions
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP2'

--24/25 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250331_JoinedSubmissions', --Extended analysis period to identify prior care home admissions
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP3'

--================== ASCOF 2E =====================

----23/24 statistical reporting year----

--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @LD_Filter = 1,
  @InputTable1 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP1'

--Part 2: All PSR Cohort
EXEC ASC_Sandbox.Create_ASCOF2E 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @LD_Filter = 0, 
  @InputTable1 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP1'

--Jan 24/Dec 24 reporting year

--Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @LD_Filter = 1,
  @InputTable1 = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP2'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @LD_Filter = 0,
  @InputTable1 = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions_Latest_Person_Data', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP2'

----24/25 statistical reporting year----

  --Part 1: LD Cohort
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 1,
  @InputTable1 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk_RP3'

--Part 2: All PSR
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards  --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 0,
  @InputTable1 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_All_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_All_Unk_RP3'

--================== ASCOF 3D =====================

----23/24 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP1'

--Jan 24/Dec 24 reporting year
EXEC ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP2'

----24/25 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards --method is the same adapted for input table with R2 to R1 spec mapping applied
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP3',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP3'


--================== Join all outcomes tables together =====================

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF;

-- 2A
SELECT * 
INTO ASC_Sandbox.LA_PBI_ASCOF
FROM ASC_Sandbox.ASCOF_2A_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2A_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2A_RP3

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

-- 3D 
SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP3;


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

-- 3D
SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP2

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_Unk_RP3;

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
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP3;

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



--============= Delete tables no longer required =====================
/*
--2A
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP3

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP3

--2BC
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP3

--2E
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP3

--3D
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP3
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP3

*/