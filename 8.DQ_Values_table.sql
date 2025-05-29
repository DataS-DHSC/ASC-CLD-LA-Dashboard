---------------------------------------------------------------------------
-- Create LA_PBI_DQ_Values_Aggregated table
---------------------------------------------------------------------------

-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced
--2. Dashboard master table has been produced

--------------------------------------------------------------------------
--Set the reporting period dates--
--------------------------------------------------------------------------

DECLARE @ReportingPeriodStartDate AS DATE = '2024-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2025-03-31'
DECLARE @SubmissionsAsOfDate AS DATE = GETDATE()

--------------------------------------------------------------------------
--Call the procedure which generates the data quality values --
--------------------------------------------------------------------------

-- Create a temporary table to store the master table record IDs
DROP TABLE IF EXISTS #RecordIDs;
SELECT Der_Unique_Record_ID
INTO #RecordIDs
FROM ASC_Sandbox.LA_PBI_Master_Table

EXEC ASC_Sandbox.GetDQValues @InputTable = '#RecordIDs',
@OutputDQTable = 'ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp'

--------------------------------------------------------------------------
-- Aggregate up to LA and National level --
--------------------------------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_DQ_Values_Aggregated;

SELECT
  LA_Name,
  LA_Code,
  Module,
  DataField,
  FieldStatus,
  FieldValidity,
  [Value],
  Sum(Count) AS Count
INTO ASC_Sandbox.LA_PBI_DQ_Values_Aggregated
FROM (
  SELECT
    'England' AS 'LA_Name',
    '99999' AS 'LA_Code',
    Module,
    DataField,
    FieldStatus,
    FieldValidity,
    NULL AS 'Value',
    Count
  FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp dq
  UNION ALL
  SELECT
    LA_Name,
    LA_Code,
    Module,
    DataField,
    FieldStatus,
    FieldValidity,
    [Value],
    Count
  FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp
  ) a
GROUP BY
  LA_Name,
  LA_Code,
  Module,
  DataField,
  FieldStatus,
  FieldValidity,
  [Value]

DROP TABLE ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_tmp;