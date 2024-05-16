------  REVIEWS FACT TABLE ------
-- This code outputs the Reviews_Fact table
-- It filters the previously created master table and deduplicates to a table of unique events
-- It adds on a flag for those receiving a Long Term service when the review started
-- It aggregates to LA level

-------PRE-REQUISTIES--------
--1. Run create master table script
--2. Run services script

-----------------------------------------------------
-- Create table of unique reviews based on fields in the partition --
-----------------------------------------------------

--If any of LA_code, Der_NHS_LA_Combined_Person_ID, Event_Start_Date, Event_End_Date, Client_Type
-- differ then the record will be considered unique

DROP TABLE IF EXISTS #Unique_Reviews;

SELECT 
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Event_Start_Date AS Review_Start_Date,
  Event_End_Date AS Review_End_Date,
  Review_Reason,
  Review_Type,
  Review_Outcomes_Achieved,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID, 
  CAST(NEWID() AS VARCHAR(100)) AS Review_ID -- add a row id for joining
INTO #Unique_Reviews
FROM (
  SELECT *,
    DupRank = ROW_NUMBER() OVER (
      PARTITION BY 
        ISNULL(LA_Code, ''),
        ISNULL(Der_NHS_LA_Combined_Person_ID, ''),
        ISNULL(Event_Start_Date, ''),
        ISNULL(Event_End_Date, ''),
        ISNULL(Client_Type, '')
      ORDER BY 
        Reporting_Period_End_Date DESC,
        Reporting_Period_Start_Date DESC,
        Event_Outcome_Hierarchy ASC, 
        Der_Unique_Record_ID DESC  )
  FROM ASC_Sandbox.LA_PBI_Master_Table
  WHERE Event_Type LIKE '%review%'
) AS T
WHERE DupRank = 1;

-----------------------------------------------------------------------
-- Identify who had a long term service open when the review started --
-----------------------------------------------------------------------

--Get long term service events
DROP TABLE IF EXISTS #LTS_Events;

SELECT 
  LA_Code, 
  Der_NHS_LA_Combined_Person_ID, 
  Event_Start_Date as Service_Start_Date, 
  Event_End_Date as Service_End_Date
INTO #LTS_Events
FROM ASC_Sandbox.LA_PBI_Services_Fact
WHERE Service_Type_Grouped ='Long Term';


--Join together review and long term services
DROP TABLE IF EXISTS #Reviews_LTS_Joined;

SELECT 
  t1.*,
  t2.Der_NHS_LA_Combined_Person_ID as LTS_Person_ID,
  t2.Service_Start_Date,
  t2.Service_End_Date,
  CASE 
    WHEN t1.Review_Start_Date BETWEEN t2.Service_Start_Date AND t2.Service_End_Date THEN 'Yes'
    WHEN t1.Review_Start_Date >= t2.Service_Start_Date  AND t2.Service_End_Date IS NULL THEN 'Yes'
    ELSE 'No'
  END AS LTS_Flag
INTO #Reviews_LTS_Joined
FROM #Unique_Reviews  t1
FULL JOIN #LTS_Events  t2
  ON t1.Der_NHS_LA_Combined_Person_ID = t2.Der_NHS_LA_Combined_Person_ID AND t1.LA_Code = t2.LA_Code
WHERE t1.Der_NHS_LA_Combined_Person_ID IS NOT NULL
ORDER BY Review_ID, LTS_Flag DESC;

-- Deduplicate to keep only 1 row per review (using review id previously created) and retain row LTS_flag 1 over 0
DROP TABLE IF EXISTS #Reviews_LTS_Flagged;

SELECT *
INTO #Reviews_LTS_Flagged
FROM (
  SELECT *, 
    DupRank = ROW_NUMBER() OVER (
      PARTITION BY ISNULL(Review_ID, '')
      ORDER by LTS_flag DESC )
  FROM #Reviews_LTS_Joined )t
WHERE DupRank =1;


-----------------------------------------------------
-- Aggregate up to LA level --
-- Aggregation groups by multiple fields incl. person id and therefore retains mostly row-level data
-----------------------------------------------------
DROP TABLE IF EXISTS #Reviews_Aggregated;

SELECT 
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Review_Start_Date AS Event_Start_Date,
  Review_End_Date AS Event_End_Date,
  Review_Reason,
  Review_Type,
  Review_Outcomes_Achieved,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag,
  count(*) AS Event_Count
INTO #Reviews_Aggregated
FROM #Reviews_LTS_Flagged
GROUP BY LA_Code,
  LA_Name, 
  Gender,
  Ethnicity,
  Client_Type,
  Primary_Support_Reason,
  Review_Start_Date,
  Review_End_Date,
  Review_Reason,
  Review_Type,
  Review_Outcomes_Achieved,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag;

-----------------------------------------------------
-- Output to fact table
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Reviews_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Reviews_Fact
FROM #Reviews_Aggregated;


