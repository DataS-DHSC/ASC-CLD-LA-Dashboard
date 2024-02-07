---------Creation of dimension tables for powerBI ---------

-------PRE-REQUISTIES--------
--1. Run create master table script

--------------------------------------
-------------CALENDAR DIM TABLE-------
DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2023-12-31';

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Calendar_Dim;

WITH seq(n) AS 
(
  SELECT 0 
  UNION ALL 
  SELECT n + 1 
  FROM seq
  WHERE n < DATEDIFF(DAY, @ReportingPeriodStartDate, @ReportingPeriodEndDate)
),
d(calendar_date) AS 
(
  SELECT DATEADD(DAY, n, @ReportingPeriodStartDate) 
  FROM seq
)
SELECT 
  Calendar_Date,
  DateName( month , DateAdd( month , month(calendar_date) , 0 ) - 1 ) AS [Month_Name],
  month(calendar_date) AS [Month_Num],
  year(calendar_date) AS [Year],
  eomonth(calendar_date) AS Last_Day_of_Month,
  CASE 
    --If the first week isn't a full week then null
    WHEN DATEPART(DAY, DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date)) BETWEEN 1 AND 6 AND MONTH(Calendar_Date) = MONTH(@ReportingPeriodStartDate) THEN NULL 
    --If the last week isn't a full week then null
    WHEN DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date) > @ReportingPeriodEndDate THEN NULL
    --Else output the last day in the week for weekly reporting
    ELSE DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date)
    END
  AS Last_Day_of_Week,  
  @ReportingPeriodStartDate AS Reporting_Period_Start_Date,
  @ReportingPeriodEndDate AS Reporting_Period_End_Date,
  CONCAT(FORMAT(@ReportingPeriodStartDate, 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy')) AS Current_Reporting_Period,
  CONCAT(FORMAT(DATEADD(month, -2, @ReportingPeriodEndDate), 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy')) AS Costs_Reporting_period,
  GETDATE() AS Refresh_Date
INTO ASC_Sandbox.LA_PBI_Calendar_Dim
FROM d
ORDER BY Calendar_Date
OPTION (MAXRECURSION 0);


------------------------
----GEOGRAPHY TABLE-----

/*Geography table - one row per combination of the listed fields*/
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Geography_Dim;

SELECT 
  LA_Code,
	LA_Name,
	Region
INTO ASC_Sandbox.LA_PBI_Geography_Dim
FROM DHSC_ASC.Reference_ODS_LA;

--Add in information for national

INSERT INTO ASC_Sandbox.LA_PBI_Geography_Dim 
VALUES	('99999', 'England', 'National');


---------------------------
--PRIMARY SUPPORT REASON---
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_PSR_Dim;

SELECT DISTINCT Primary_Support_Reason
INTO ASC_Sandbox.LA_PBI_PSR_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;


--------------------------
-----GENDER-----------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Gender_Dim;

SELECT DISTINCT Gender
INTO ASC_Sandbox.LA_PBI_Gender_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;

--------------------------
-----GENDER-----------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Ethnicity_Dim;

SELECT DISTINCT Ethnicity
INTO ASC_Sandbox.LA_PBI_Ethnicity_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;

--------------------------
-----CLIENT TYPE-----------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Client_type_Dim;

SELECT DISTINCT Client_Type
INTO ASC_Sandbox.LA_PBI_Client_Type_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;


-----------------------------
--------WORKING AGE BANDS----
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Working_Age_Dim;

SELECT DISTINCT Der_Working_Age_Band
INTO ASC_Sandbox.LA_PBI_Working_Age_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;

