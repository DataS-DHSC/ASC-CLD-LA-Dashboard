------  SERVICES FACT TABLE ------
-- This code outputs the Services_Fact table
-- It filters the previously created master table and deduplicates to a table of unique events
-- It aggregates to LA level

-----------------------------------------------------
-- 1. Create table of unique services  ------------
-----------------------------------------------------

-- Deduplicate to output unique services based on the fields in the partition
--If any of LA_code, Person_ID, Event_Start_Date, Client_Type, Service_Type, Service_Component or Delivery_Mechanism
-- differ then the record will be considered unique

DROP TABLE IF EXISTS #Unique_Services;

SELECT T.*
INTO #Unique_Services
FROM (
  SELECT *, 
    DupRank = ROW_NUMBER() OVER (
      PARTITION BY 
        ISNULL(LA_Code, ''),
        ISNULL(Der_Person_id, ''),
        ISNULL(Event_Start_Date, ''),
        ISNULL(Client_Type, ''),
        ISNULL(Service_Type, ''),
        ISNULL(Service_Component, ''),
        ISNULL(Delivery_Mechanism, '')
      ORDER BY Reporting_Period_End_Date DESC,
        Reporting_Period_Start_Date DESC,
        Event_Outcome_hierarchy ASC, 
        Der_Unique_Record_ID DESC
        )
  FROM ASC_Sandbox.LA_PBI_Master_Table
  WHERE Event_Type LIKE '%service%'
) AS T
WHERE DupRank = 1;

--------------------------------------------------------------------------------------
-- Select fields for LA level aggregation, however data remains mostly at row level --
--------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #Services_Aggregated;

SELECT 	
  LA_Code
  ,LA_Name
  ,Client_Type
  ,Gender
  ,Ethnicity
  ,Der_Age_Band
  ,Der_Working_Age_Band
  ,Primary_Support_Reason
  ,Event_Reference
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
  ,Der_Person_ID
INTO #Services_Aggregated
FROM #Unique_Services;

-----------------------------------------------------
-- Output to fact table
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Services_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Services_Fact
FROM #Services_Aggregated;