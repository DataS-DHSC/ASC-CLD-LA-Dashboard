------  UNRESTRICTED SHORT TERM WAITING TIMES TABLE ------
-- This code outputs the Short_Term_Waiting_Times_Unrestricted table
-- It filters the previously created tables for the dashboard to find new client's first short term service
-- Where the client did not have a short term assessment before the service it will calculate the time between the service and the previous request OR assessment

DROP TABLE IF EXISTS #New_Clients;
DROP TABLE IF EXISTS #New_Service_Clients;
DROP TABLE IF EXISTS #Short_Term_Clients;
DROP TABLE IF EXISTS #temp;
DROP TABLE IF EXISTS #Short_Term_Dates;

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
-- Use that table to create a new temporary table of those people's first service where their first service is Short term --

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
INTO #Short_Term_Clients
FROM #New_Service_Clients
WHERE Service_Type_Grouped = 'Short Term';

-- SECTION 3 --
-- Use that table to make another table that contains date of that service, date of the assessment before that which has the relevant assessment type and finally the request before that assessment --

WITH Prev_Events AS(
SELECT
	MAS.LA_Code
	, MAS.LA_Name
	, MAS.Der_NHS_LA_Combined_Person_ID
	, LTC.Primary_Support_Reason
	, LTC.Gender
	, LTC.Der_Age_Band
	, LTC.Ethnicity
	, LTC.First_Service
	, LTC.Service_Type
  , MAS.Request_Route_of_Access
	, MAX(MAS.Event_Start_Date) AS Previous_Start_Date

FROM [DHSC_Reporting].[LA_PBI_Master_Table] AS MAS 
INNER JOIN #Short_Term_Clients AS LTC
	ON LTC.Der_NHS_LA_Combined_Person_ID = MAS.Der_NHS_LA_Combined_Person_ID 
  AND LTC.LA_Code = MAS.LA_Code
	AND LTC.First_Service >= MAS.Event_Start_Date
WHERE MAS.Event_Type LIKE '%Request%' OR MAS.Event_Type LIKE '%Assessment%'

GROUP BY 
	MAS.LA_Code
	, MAS.LA_Name
	, MAS.Der_NHS_LA_Combined_Person_ID
	, LTC.Primary_Support_Reason
	, LTC.Gender
	, LTC.Der_Age_Band
	, LTC.Ethnicity
	, LTC.First_Service
	, LTC.Service_Type
	, MAS.Request_Route_of_Access	
), RankedRequests AS (
    SELECT
        PE.LA_Code,
        PE.LA_Name,
        PE.Der_NHS_LA_Combined_Person_ID,
        PE.Primary_Support_Reason,
        PE.Gender,
        PE.Der_Age_Band,
        PE.Ethnicity,
        PE.First_Service,
        PE.Service_Type,
        PE.Previous_Start_Date,
        PE.Request_Route_of_Access,
        ROW_NUMBER() OVER (PARTITION BY PE.Der_NHS_LA_Combined_Person_ID, PE.LA_Code ORDER BY PE.Previous_Start_Date DESC) AS RowNum
    FROM Prev_Events AS PE
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
    Previous_Start_Date,
    Request_Route_of_Access
INTO #Short_Term_Dates
FROM RankedRequests
WHERE RowNum = 1;

--SECTION 4--
--Create the final table with waiting times--

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times_Unrestricted;

SELECT 
	*
	, DATEDIFF(DAY, Previous_Start_Date, First_Service) AS Request_to_Service
INTO ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times_Unrestricted
FROM #Short_Term_Dates
ORDER BY
	LA_Code
	, Der_NHS_LA_Combined_Person_ID
