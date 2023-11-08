---------Creation of dimension tables for powerBI ---------

-------PRE-REQUISTIES--------
--1. Run create master table script

--------------------------------------
-------------CALENDAR DIM TABLE-------
DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2023-09-30';

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
  DATEADD(DAY, (7 - DATEPART(WEEKDAY, Calendar_Date)) +1, Calendar_Date)  AS Last_Day_of_Week,
  @ReportingPeriodStartDate AS Reporting_Period_Start_Date,
  @ReportingPeriodEndDate AS Reporting_Period_End_Date,
  CONCAT(FORMAT(@ReportingPeriodStartDate, 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy')) AS Current_Reporting_Period,
  GETDATE() AS Refresh_Date
INTO ASC_Sandbox.LA_PBI_Calendar_dim
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

