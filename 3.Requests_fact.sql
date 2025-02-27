------  REQUESTS FACT TABLE ------
-- This code outputs the Requests_Fact table
-- It filters the already processed table of latest submissions covering the latest reporting period to request events
-- It adds on a flag for those receiving a Long Term service when the request was made
-- It aggregates to LA level


-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced
--2. Dashboard master table has been produced
--3. Services Fact table has been produced

-----------------------------------------------------
-- Create table of unique requests based on fields in the partition --
-----------------------------------------------------
--If any of LA_code, Der_NHS_LA_Combined_Person_ID, Event_Start_Date, Event_End_Date, Client_Type, Request_Route_Of_Access
-- differ then the record will be considered unique

DROP TABLE IF EXISTS #Unique_Requests

SELECT 
  Event_Type,
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Event_Start_Date AS Request_Start_Date,
  Event_End_Date AS Request_End_Date,
  Event_Outcome,
  Event_Outcome_Grouped,
  Request_Route_of_Access,
  Assessment_Type,
  Eligible_Needs_Identified,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  Der_Conversation,
  Der_Conversation_1,
  Der_Unique_Event_Ref 
INTO #Unique_Requests
FROM ASC_Sandbox.LA_PBI_Master_Table
WHERE Event_Type LIKE 'Request'

-----------------------------------------------------------------------
-- Identify who had a long term service open when the request started --
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


--Join together request and long term services
DROP TABLE IF EXISTS #Requests_LTS_Joined;

SELECT 
  t1.*,
  t2.Der_NHS_LA_Combined_Person_ID AS LTS_Person_ID,
  t2.Service_Start_Date,
  t2.Service_End_Date,
  CASE 
    WHEN t1.Request_Start_Date BETWEEN t2.Service_Start_Date AND t2.Service_End_Date THEN 'Yes'
    WHEN t1.Request_Start_Date >= t2.Service_Start_Date  and t2.Service_End_Date IS NULL THEN 'Yes'
    ELSE 'No'
  END AS LTS_flag
INTO #Requests_LTS_Joined
FROM #Unique_Requests  t1
FULL JOIN #LTS_Events  t2
  ON t1.Der_NHS_LA_Combined_Person_ID = t2.Der_NHS_LA_Combined_Person_ID AND t1.LA_Code = t2.LA_Code
WHERE t1.Der_NHS_LA_Combined_Person_ID IS NOT NULL
ORDER BY Der_Unique_Event_Ref, LTS_Flag DESC;

-- Deduplicate to keep only 1 row per request (using request id previously created) and retain row LTS_flag 1 over 0
DROP TABLE IF EXISTS #Requests_LTS_Flagged;

SELECT *
INTO #Requests_LTS_Flagged
FROM (
  SELECT *
  , DupRank = ROW_NUMBER() OVER (
      PARTITION BY ISNULL(Der_Unique_Event_Ref, '')
      ORDER BY LTS_flag DESC)
  FROM #Requests_LTS_Joined
  )t
WHERE DupRank =1;

-----------------------------------------------------
-- Aggregate up to LA level --
-- Aggregation groups by multiple fields incl. person id and therefore retains mostly row-level data
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Requests_Fact

SELECT
  LA_Code,
  LA_Name, 
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Request_Start_Date AS Event_Start_Date,
  Request_End_Date AS Event_End_Date,
  Event_Outcome,
  Event_Outcome_Grouped,
  Request_Route_of_Access,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag,
  count(*) AS Event_Count
INTO ASC_Sandbox.LA_PBI_Requests_Fact
FROM #Requests_LTS_Flagged
GROUP BY 
  LA_Code,
  LA_Name,
  Client_Type,
  Gender,
  Ethnicity,
  Primary_Support_Reason,
  Request_Start_Date,
  Request_End_Date,
  Event_Outcome,
  Event_Outcome_Grouped,
  Request_Route_of_Access,
  Der_Age_Band,
  Der_Working_Age_Band,
  Der_NHS_LA_Combined_Person_ID,
  LTS_Flag;