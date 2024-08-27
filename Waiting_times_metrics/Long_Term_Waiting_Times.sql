------  LONG TERM WAITING TIMES TABLE ------
-- This code outputs the Long_Term_Waiting_Times table
-- It filters the previously created tables for the dashboard to new clients and finds the time between their first long term service and the long term assessment and request before that

DROP TABLE IF EXISTS #New_Clients;
DROP TABLE IF EXISTS #New_Service_Clients;
DROP TABLE IF EXISTS #Long_Term_Clients;
DROP TABLE IF EXISTS #temp;
DROP TABLE IF EXISTS #Long_Term_Dates;

DECLARE @ReportingPeriodStartDate AS DATE = '2023-07-01';
DECLARE @FirstEventEndDate AS DATE = '2023-07-01';

-- SECTION 1 --
-- Create table of clients where the first event they receive is a not a service or review --

WITH FirstEventTypes AS (
    SELECT Der_NHS_LA_Combined_Person_ID
		, LA_Code
        ,FIRST_VALUE(Event_Type) OVER(PARTITION BY Der_NHS_LA_Combined_Person_ID, LA_Code
                            ORDER BY Event_Start_Date) AS first_event_type
    FROM DHSC_Reporting.LA_PBI_Master_Table
    WHERE Event_End_Date >= @FirstEventEndDate
    AND Client_Type NOT LIKE '%carer%'
)

SELECT 
	DISTINCT Der_NHS_LA_Combined_Person_ID
  , LA_Code
	, first_event_type
INTO #New_Clients
FROM FirstEventTypes 
WHERE first_event_type LIKE '%Request%' OR first_event_type LIKE '%Assessment%';

-- SECTION 2 --
-- Use that table to create a new temporary table of those people's first service where their first service is Long term --

SELECT
  LA_Code,
  Service_Type_Grouped,
  Der_NHS_LA_Combined_Person_ID,
  Event_Type,
  Service_Type,
  Event_Start_Date AS First_Service,
  Primary_Support_Reason,
  Gender,
  Der_Age_Band,
  Ethnicity
INTO #New_Service_Clients
FROM (
  SELECT
    MT.LA_Code,
    MT.Service_Type_Grouped,
    MT.Der_NHS_LA_Combined_Person_ID,
    MT.Event_Type,
    MT.Service_Type,
    MT.Event_Start_Date,
	  MT.Primary_Support_Reason,
	  MT.Gender,
	  MT.Der_Age_Band,
	  MT.Ethnicity,
    ROW_NUMBER() OVER (PARTITION BY MT.Der_NHS_LA_Combined_Person_ID, MT.LA_Code ORDER BY MT.Event_Start_Date) AS rn
  FROM DHSC_Reporting.LA_PBI_Master_Table AS MT
  JOIN #New_Clients AS NC
  ON MT.Der_NHS_LA_Combined_Person_ID = NC.Der_NHS_LA_Combined_Person_ID AND MT.LA_Code = NC.LA_Code
  WHERE MT.Event_Type = 'Service'
  AND Event_Start_Date >= @ReportingPeriodStartDate
) AS a
WHERE rn = 1;

SELECT *
INTO #Long_Term_Clients
FROM #New_Service_Clients
WHERE Service_Type_Grouped = 'Long Term';

-- SECTION 3 --
-- Use that table to make another table that contains date of that service, date of the assessment before that which has the relevant assessment type and finally the request before that assessment --

WITH Prev_Assessments AS(
SELECT
	ASS.LA_Code
	, ASS.LA_Name
	, ASS.Der_NHS_LA_Combined_Person_ID
	, LTC.Primary_Support_Reason
	, LTC.Gender
	, LTC.Der_Age_Band
	, LTC.Ethnicity
	, LTC.First_Service
	, LTC.Service_Type
	, ASS.assessment_Type
	, MAX(ASS.Event_Start_Date) AS Assessment_Start_Date

FROM [DHSC_Reporting].[LA_PBI_Assessments_Fact] AS ASS 
INNER JOIN #Long_Term_Clients AS LTC
	ON LTC.Der_NHS_LA_Combined_Person_ID = ASS.Der_NHS_LA_Combined_Person_ID 
  AND LTC.LA_Code = ASS.LA_Code
	AND LTC.First_Service >= ASS.Event_Start_Date
WHERE ASS.assessment_Type LIKE '%Long Term%'
	
GROUP BY 
	ASS.LA_Code
	, ASS.LA_Name
	, ASS.Der_NHS_LA_Combined_Person_ID
	, LTC.Primary_Support_Reason
	, LTC.Gender
	, LTC.Der_Age_Band
	, LTC.Ethnicity
	, LTC.First_Service
	, LTC.Service_Type
	, ASS.assessment_Type	
), RankedRequests AS (
    SELECT
        PA.LA_Code,
        PA.LA_Name,
        PA.Der_NHS_LA_Combined_Person_ID,
        PA.Primary_Support_Reason,
        PA.Gender,
        PA.Der_Age_Band,
        PA.Ethnicity,
        PA.First_Service,
        PA.Service_Type,
        PA.assessment_Type,
        PA.Assessment_Start_Date,
        REQ.Event_Start_Date AS Request_Start_Date,
        REQ.Event_Outcome,
        REQ.Request_Route_of_Access,
        ROW_NUMBER() OVER (PARTITION BY PA.Der_NHS_LA_Combined_Person_ID, PA.LA_Code ORDER BY REQ.Event_Start_Date DESC) AS RowNum
    FROM Prev_Assessments AS PA
    LEFT JOIN [DHSC_Reporting].[LA_PBI_Requests_Fact] AS REQ ON PA.Der_NHS_LA_Combined_Person_ID = REQ.Der_NHS_LA_Combined_Person_ID
    AND PA.LA_Code = REQ.LA_Code
    AND PA.Assessment_Start_Date >= REQ.Event_Start_Date
)

-- Selecting only the top-ranked request for each person ID
SELECT 
    LA_Code,
    LA_Name,
    Der_NHS_LA_Combined_Person_ID,
    Primary_Support_Reason,
    Gender,
    Der_Age_Band,
    Ethnicity,
    First_Service,
    Service_Type,
    assessment_Type,
    Assessment_Start_Date,
    Request_Start_Date,
    Event_Outcome AS Request_Event_Outcome,
    Request_Route_of_Access
INTO #Long_Term_Dates
FROM RankedRequests
WHERE RowNum = 1;

--SECTION 4--
--Create the final table with waiting times--

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Long_Term_Waiting_Times;

SELECT 
	*
	, DATEDIFF(DAY, Request_Start_Date, First_Service) AS Request_to_Service
	, DATEDIFF(DAY, Request_Start_Date, Assessment_Start_Date) AS Request_to_Assessment
	, DATEDIFF(DAY, Assessment_Start_Date, First_Service) AS Assessment_to_Service
INTO ASC_Sandbox.LA_PBI_Long_Term_Waiting_Times
FROM #Long_Term_Dates
ORDER BY
	LA_Code
	, Der_NHS_LA_Combined_Person_ID
