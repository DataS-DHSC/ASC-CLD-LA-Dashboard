------  ASSESSMENTS FACT TABLE ------
-- This code outputs the Assessments_Fact table
-- It filters the previously created master table and deduplicates to a table of unique events
-- It adds on a flag for those receiving a Long Term service when the assessment started
-- It aggregates to LA level


-------PRE-REQUISTIES--------
--1. Run create master table script
--2. Run services script

-----------------------------------------------------
-- Create table of unique assessments based on fields in the partition --
-----------------------------------------------------
-- If any of LA_code, Der_NHS_LA_Combined_Person_ID, Event_Start_Date, Event_End_Date, Client_Type, Assessment_Type
-- differ then the record will be considered unique


DROP TABLE IF EXISTS #Unique_Assessments;

SELECT 
  Event_Type,
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Event_Start_Date AS Assessment_Start_Date,
  Event_End_Date AS Assessment_End_Date,
  Assessment_Type,
  Event_Outcome,
  Event_Outcome_Grouped,
  Eligible_Needs_Identified,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  Der_conversation,
  Der_conversation_1,
  CAST(NEWID() AS VARCHAR(100)) AS Assessment_ID -- add a row id for joining
INTO #Unique_Assessments
FROM (
  SELECT *,
    DupRank = ROW_NUMBER() OVER (
      PARTITION BY 
        ISNULL(LA_Code, ''),
        ISNULL(Der_NHS_LA_Combined_Person_ID, ''),
        ISNULL(Event_Start_Date, ''),
        ISNULL(Event_End_Date, ''),
        ISNULL(Client_Type, ''),
        ISNULL(Assessment_Type, '')
      ORDER BY Reporting_Period_End_Date DESC, 
        Reporting_Period_Start_Date DESC,
        Event_Outcome_Hierarchy ASC,
        Der_Unique_Record_ID DESC  
        )
  FROM ASC_Sandbox.LA_PBI_Master_Table
  WHERE Event_Type LIKE '%assessment%'
) AS T
WHERE DupRank = 1;


-----------------------------------------------------------------------
-- Identify who had a long term service open when the assessment started --
-----------------------------------------------------------------------

--Get long term service events
DROP TABLE IF EXISTS #LTS_Events;

SELECT 
  LA_Code, 
  Der_NHS_LA_Combined_Person_ID, 
  Event_Start_Date AS Service_Start_Date, 
  Event_End_Date AS Service_End_Date
INTO #LTS_Events
FROM ASC_Sandbox.LA_PBI_Services_Fact
WHERE Service_Type_Grouped ='Long Term';


--Join together assessment and long term services
DROP TABLE IF EXISTS #Assessments_LTS_Joined;

SELECT 
  t1.*,
  t2.Der_NHS_LA_Combined_Person_ID as LTS_Person_ID,
  t2.Service_Start_Date,
  t2.Service_End_Date,
  CASE 
    WHEN t1.Assessment_Start_Date BETWEEN t2.Service_Start_Date AND t2.Service_End_Date THEN 'Yes'
    WHEN t1.Assessment_Start_Date >= t2.Service_Start_Date  and t2.Service_End_Date IS NULL THEN 'Yes'
    ELSE 'No'
  END AS LTS_Flag
INTO #Assessments_LTS_Joined
FROM #Unique_Assessments  t1
FULL JOIN #LTS_Events  t2
  ON t1.Der_NHS_LA_Combined_Person_ID = t2.Der_NHS_LA_Combined_Person_ID AND t1.LA_Code = t2.LA_Code
WHERE t1.Der_NHS_LA_Combined_Person_ID IS NOT NULL
ORDER BY Assessment_ID, LTS_Flag DESC;

-- Deduplicate to keep only 1 row per assessment (using assessment id previously created) and retain row LTS_flag 1 over 0
DROP TABLE IF EXISTS #Assessments_LTS_flagged;

SELECT *
INTO #Assessments_LTS_Flagged
FROM (
  SELECT *
  , DupRank = ROW_NUMBER() OVER (
      PARTITION BY ISNULL(Assessment_ID, '')
      ORDER BY LTS_Flag DESC )
  FROM #Assessments_LTS_Joined )t
WHERE DupRank =1;


-----------------------------------------------------
-- Aggregate up to LA level --
-- Aggregation groups by multiple fields incl. person id and therefore retains mostly row-level data
-----------------------------------------------------
DROP TABLE IF EXISTS #Assessments_Aggregated

SELECT 
  Event_Type,
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Assessment_Start_Date as Event_Start_Date,
  Assessment_End_Date as Event_End_Date,
  assessment_Type,
  Event_Outcome,
  Event_Outcome_Grouped,
  Eligible_Needs_Identified,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag,
  Der_conversation,
  Der_conversation_1,
  count(*) AS Event_Count
INTO #Assessments_Aggregated
FROM #Assessments_LTS_Flagged
GROUP BY 
  Event_Type,
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Assessment_Start_Date,
  Assessment_End_Date,
  Assessment_Type,
  Event_Outcome,
  Event_Outcome_Grouped,
  Eligible_Needs_Identified,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag,
  Der_Conversation,
  Der_Conversation_1;


-----------------------------------------------------
-- Output to fact table
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Assessments_Fact
SELECT *
INTO ASC_Sandbox.LA_PBI_Assessments_Fact
FROM #Assessments_Aggregated;


