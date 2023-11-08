---------------------------------------------------------------------------
-- Create LA_PBI_DQ_Values_Aggregated table
---------------------------------------------------------------------------

-------PRE-REQUISTIES--------
--1. Run create master table script

--------------------------------------------------------------------------
--Set the reporting period dates--
--------------------------------------------------------------------------

DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2023-09-30'
DECLARE @SubmissionsAsOfDate AS DATE = GETDATE()

--------------------------------------------------------------------------
--Call the procedure which generates the data quality values --
--------------------------------------------------------------------------
EXEC ASC_Sandbox.GetDQValues @InputR1Table = 'ASC_Sandbox.LA_PBI_Master_Table', 
@OutputDQTable = 'ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp'

--------------------------------------------------------------------------
-- Aggregate up to LA and National level --
--------------------------------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_DQ_Values_Aggregated;

SELECT
  LA_Name,
  LA_Code,
  Module,
  Variable,
  Applicable,
  Mandatory,
  DQ_Test_Result,
  Value,
  Sum(Count) AS Count
INTO ASC_Sandbox.LA_PBI_DQ_Values_Aggregated
FROM (
  SELECT
    'England' AS 'LA_Name',
    '99999' AS 'LA_Code',
    Module,
    Variable,
    Applicable,
    Mandatory,
    DQ_Test_Result,
    NULL AS 'Value',
    Count
  FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp dq
  UNION ALL
  SELECT
    LA_Name,
    LA_Code,
    Module,
    Variable,
    Applicable,
    Mandatory,
    DQ_Test_Result,
    Value,
    Count
  FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp
  ) a
GROUP BY
  LA_Name,
  LA_Code,
  Module,
  Variable,
  Applicable,
  Mandatory,
  DQ_Test_Result,
  Value

DROP TABLE ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp;