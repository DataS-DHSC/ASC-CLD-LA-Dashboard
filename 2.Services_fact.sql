------  SERVICES FACT TABLE ------
-- This code outputs the Services_Fact table
-- It filters the already processed table of latest submissions covering the latest reporting period to service events
-- It aggregates to LA level

-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced
--2. Dashboard master table has been produced


--Filter to services and save output
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Services_Fact;

SELECT 	
  LA_Code
  ,LA_Name
  ,Client_Type
  ,Gender
  ,Ethnicity
  ,Der_Age_Band
  ,Der_Working_Age_Band
  ,Primary_Support_Reason
  ,Event_Start_Date
  ,Event_End_Date
  ,Event_Outcome
  ,Event_Outcome_Grouped
  ,Date_of_Death
  ,Service_Type
  ,Service_Type_Grouped
  ,Service_Component
  ,Delivery_Mechanism
  ,Unit_Cost
  ,Cost_Frequency_Unit_Type
  ,Planned_Units_Per_Week
  ,Der_NHS_LA_Combined_Person_ID
INTO ASC_Sandbox.LA_PBI_Services_Fact
FROM ASC_Sandbox.LA_PBI_Master_Table
WHERE Event_Type = 'Service';