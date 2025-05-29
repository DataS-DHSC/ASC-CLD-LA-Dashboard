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
SET @InputTable = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions';  --Update with the latest single submission data table

--Declare a variable to hold the dynamic SQL query
DECLARE @SQLQuery AS NVARCHAR(MAX);

--Construct the dynamic SQL query (required to pass the InputTable variable through the query)
SET @SQLQuery = '
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Master_Table

SELECT 
  *,
  Gender_Cleaned AS Gender,
  Ethnicity_Cleaned AS Ethnicity,
  Service_Type_Cleaned AS Service_Type,
  Der_Event_End_Date AS Event_End_Date,
  Event_Outcome_Cleaned AS Event_Outcome,
  Client_Type_Cleaned AS Client_Type,
  Primary_Support_Reason_Cleaned AS Primary_Support_Reason,
  Service_Component_Cleaned AS Service_Component,
  Delivery_Mechanism_Cleaned AS Delivery_Mechanism,
  Cost_Frequency_Unit_Type_Cleaned AS Cost_Frequency_Unit_Type,
  Request_Route_of_Access_Cleaned AS Request_Route_of_Access,
  Assessment_Type_Cleaned AS Assessment_Type,
  Review_Reason_Cleaned AS Review_Reason,
  Review_Outcomes_Achieved_Cleaned as Review_Outcomes_Achieved
INTO ASC_Sandbox.LA_PBI_Master_Table
FROM ' + @InputTable;

--Execute the dynamic SQL query
EXEC sp_executesql @SQLQuery;