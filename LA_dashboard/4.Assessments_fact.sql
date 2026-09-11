------  ASSESSMENTS FACT TABLE ------
-- This code outputs the Assessments_Fact table
-- It filters the already processed table of latest submissions covering the latest reporting period to assessment events
-- It adds on a flag for those receiving a Long Term service when the assessment started
-- It aggregates to LA level


-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced
--2. Dashboard master table has been produced
--3. Services Fact table has been produced

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
  Der_Unique_Event_Ref
INTO #Unique_Assessments
FROM ASC_Sandbox.LA_PBI_Master_Table
WHERE Event_Type LIKE 'Assessment'

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
WHERE Service_Type_Grouped ='Long Term Support';


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
ORDER BY Der_Unique_Event_Ref, LTS_Flag DESC;

-- Deduplicate to keep only 1 row per assessment (using assessment id previously created) and retain row LTS_flag 1 over 0
DROP TABLE IF EXISTS #Assessments_LTS_flagged;

SELECT *
INTO #Assessments_LTS_Flagged
FROM (
  SELECT *
  , DupRank = ROW_NUMBER() OVER (
      PARTITION BY ISNULL(Der_Unique_Event_Ref, '')
      ORDER BY LTS_Flag DESC )
  FROM #Assessments_LTS_Joined )t
WHERE DupRank =1;


-----------------------------------------------------
-- Aggregate up to LA level --
-- Aggregation groups by multiple fields incl. person id and therefore retains mostly row-level data
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Assessments_Fact

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
INTO ASC_Sandbox.LA_PBI_Assessments_Fact
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


