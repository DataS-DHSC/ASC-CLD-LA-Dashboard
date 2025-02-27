--Main script for running each ASCOF measure and joining outputs together

--The reporting period start and end dates determine the statistical reporting year
--The input table selected dertermines the period the data covers for analysis, as some measures required an extended period beyond the reporting year
--Some measures have multiple output tables, where additional information is stored (excluded counts, disaggregated outcomes etc)

--================== ASCOF 2A =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_240630_JoinedSubmissions',  --23/24 plus an extra 3 months
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP1',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP1'


--Latest reporting year
EXEC ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_241231_JoinedSubmissions',  --Jan-Dec 24 plus prior 12 months for new clients
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF_2A_Disaggregated_RP2',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP2'



--================== ASCOF 2B & 2C =====================

--23/24 statistical reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', --First year of CLD only 23/24 can be used (ideally historic data would be analysed)
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP1'

--Latest reporting year
EXEC ASC_Sandbox.Create_ASCOF2BC 
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_241231_JoinedSubmissions', --Extended analysis period to identify prior care home admissions
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_RP2'


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

----Latest reporting year----

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

--================== ASCOF 3D =====================

----23/24 statistical reporting year----

EXEC ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP1',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP1'

----Latest reporting year----
EXEC ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31',
  @InputTable = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D_RP2',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk_RP2'


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

-- 2BC
SELECT * 
FROM ASC_Sandbox.ASCOF_2BC_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2BC_RP2

UNION ALL

-- 2E Part 1 (LD)

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_LD_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_LD_RP2

UNION ALL

-- 2E Part 1 (All)

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_RP2

UNION ALL

-- 3D 
SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_3D_RP2;


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

-- 2E Part 2
SELECT *
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_2E_All_Unk_RP2

UNION ALL

-- 3D
SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP1

UNION ALL

SELECT * 
FROM ASC_Sandbox.ASCOF_3D_Unk_RP2;

--Output 2A figures disaggregated by final outcome

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated;

SELECT *
INTO ASC_Sandbox.LA_PBI_ASCOF_2A_Disaggregated
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP1

UNION ALL

SELECT *
FROM ASC_Sandbox.ASCOF_2A_Disaggregated_RP2


--============= Delete tables no longer required =====================
/*
--2A
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_RP2

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2A_Disaggregated_RP2

--2BC
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_RP2

--2E
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_LD_Unk_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2E_All_Unk_RP2

--3D
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_RP2
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP1
DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_3D_Unk_RP2

*/