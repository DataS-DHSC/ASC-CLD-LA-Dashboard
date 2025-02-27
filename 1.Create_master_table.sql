---------------------------
--  Create master table  --
---------------------------
/*
This code now runs off a pre-processed data table which contains:
>Data from the single submissions which cover the latest reporting period (i.e. latest 12months)
>Some priority fields have been cleaned
>Some event end dates are amended when they are preceeded by a date of death
>Records are deduplicated, see methodology document for the fields used in the deduplication

The pre-processed data table is produced using the create_main_table_for_12mo_period.sql script

*/

-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced (this is the output of the create_main_table_for_12mo_period.sql script)


--Declare the input table variable
DECLARE @InputTable AS NVARCHAR(MAX);
SET @InputTable = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions';  --Update with the latest single submission data table

--Declare a variable to hold the dynamic SQL query
DECLARE @SQLQuery AS NVARCHAR(MAX);

--Construct the dynamic SQL query (required to pass the InputTable variable through the query)
SET @SQLQuery = '
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Master_Table

SELECT 
  a.*,
  Gender_Cleaned AS Gender,
  Ethnicity_Cleaned AS Ethnicity,
  Service_Type_Cleaned AS Service_Type,
  Der_Event_End_Date AS Event_End_Date,
  COALESCE(eo.Event_Outcome_Cleaned, ''Invalid and not mapped'') as Event_Outcome,
  eoh.Event_Outcome_Hierarchy
INTO ASC_Sandbox.LA_PBI_Master_Table
FROM ' + @InputTable + ' a
LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Mapping eo  --Clean up the event outcome field
ON a.Event_Outcome_Raw = eo.Event_Outcome_Raw   
LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Hierarchy eoh  --Join on the hierarchy for event outcome
ON eo.Event_Outcome_Cleaned = eoh.Event_Outcome_Spec';

--Execute the dynamic SQL query
EXEC sp_executesql @SQLQuery;