------  SERVICES FACT TABLE ------
-- This code outputs the Services_Fact table
-- It filters the previously created master table and deduplicates to a table of unique events
-- It aggregates to LA level

-----------------------------------------------------
-- 1. Create table of unique services  ------------
-----------------------------------------------------

-- Deduplicate to output unique services based on the fields in the partition
-- If any of LA_code, Der_NHS_LA_Combined_Person_ID, Event_Start_Date, Client_Type, Service_Type, Service_Component, Delivery_Mechanism,
-- Unit_Cost, Planned_units_per_week or Cost_frequency_unit_type
-- differ then the record will be considered unique


--Partition by requires compatible datatypes and numeric and character fields cannot be mixed
--Fix data types for unit cost and planned units
DROP TABLE IF EXISTS #Services_Format;

SELECT *
INTO #Services_Format
FROM ASC_Sandbox.LA_PBI_Master_Table
WHERE Event_Type like '%service%';

--First convert to numeric and set decimal places as 2dp, 
--this is to ensure when converting to character they all only have 2 values after the point and then 0s
ALTER TABLE #Services_Format
ALTER COLUMN Unit_Cost NUMERIC(18, 2) NULL;
ALTER TABLE #Services_Format
ALTER COLUMN Planned_units_per_week NUMERIC(18, 2) NULL;

--Then convert to character 
--all fields in the partition need to be the same data type (character) for it to work
ALTER TABLE #Services_Format
ALTER COLUMN Unit_Cost VARCHAR(18) NULL;
ALTER TABLE #Services_Format								
ALTER COLUMN Planned_units_per_week VARCHAR(18) NULL;

-- Deduplicate to unique services by fields in the partition
DROP TABLE IF EXISTS #Unique_Services;

SELECT T.*
INTO #Unique_Services
FROM (
  SELECT *, 
    DupRank = ROW_NUMBER() OVER (
      PARTITION BY 
        ISNULL(LA_Code, ''),
        ISNULL(Der_NHS_LA_Combined_Person_ID, ''),
        ISNULL(Event_Start_Date, ''),
        ISNULL(Client_Type, ''),
        ISNULL(Service_Type, ''),
        ISNULL(Service_Component, ''),
        ISNULL(Delivery_Mechanism, ''),
        ISNULL(Unit_Cost, ''),
        ISNULL(Cost_Frequency_Unit_Type, ''),
        ISNULL(Planned_units_per_week, '')
      ORDER BY Reporting_Period_End_Date DESC,
        Reporting_Period_Start_Date DESC,
        Event_Outcome_Hierarchy ASC, 
        Der_Unique_Record_ID DESC
        )
  FROM #Services_format
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
INTO #Services_Aggregated
FROM #Unique_Services;

-----------------------------------------------------
-- Output to fact table
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Services_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Services_Fact
FROM #Services_Aggregated;