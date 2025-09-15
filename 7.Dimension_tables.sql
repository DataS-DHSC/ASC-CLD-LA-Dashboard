---------Creation of dimension tables for powerBI ---------

-------PRE-REQUISTIES--------
--1. Single submission table for latest period has been produced
--2. Dashboard master table has been produced

--------------------------------------
-------------CALENDAR DIM TABLE-------
DECLARE @ReportingPeriodStartDate AS DATE = '2024-07-01' --Update to cover full 12 month reporting period
DECLARE @ReportingPeriodEndDate AS DATE = '2025-06-30';

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Calendar_Dim;

WITH seq(n) AS 
(
  SELECT 0 
  UNION ALL 
  SELECT n + 1 
  FROM seq
  WHERE n < DATEDIFF(DAY, @ReportingPeriodStartDate, @ReportingPeriodEndDate)
),
d(Calendar_Date) AS 
(
  SELECT DATEADD(DAY, n, @ReportingPeriodStartDate) 
  FROM seq
)
SELECT 
  Calendar_Date,
  DATENAME(MONTH , DATEADD(MONTH, MONTH(Calendar_Date), 0 ) - 1 ) AS [Month_Name],
  MONTH(Calendar_Date) AS [Month_Num],
  YEAR(Calendar_Date) AS [Year],
  CASE
    WHEN MONTH(Calendar_Date) IN (3, 6, 9, 12) THEN EOMONTH(Calendar_Date)
    WHEN MONTH(Calendar_Date) IN (2, 5, 8, 11) THEN EOMONTH(DATEADD(MONTH, 1, Calendar_Date))
    WHEN MONTH(Calendar_Date) IN (1, 4, 7, 10) THEN EOMONTH(DATEADD(MONTH, 2, Calendar_Date))
    ELSE NULL
    END AS Last_Day_of_Quarter,
  EOMONTH(Calendar_Date) AS Last_Day_of_Month,
  CASE 
    --If the first week isn't a full week then null
    WHEN DATEPART(DAY, DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date)) BETWEEN 1 AND 6 --check if date of the next Sunday is between 1st - 6th 
    AND MONTH(Calendar_Date) = MONTH(@ReportingPeriodStartDate) --check if the month is the same as the first month of the reporting period
    AND DAY(Calendar_Date) < 8  --and the date is in the first week of the month (to prevent dates at the end of the first month being nulled)
    THEN NULL 
    --If the last week isn't a full week then null
    WHEN DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date) > @ReportingPeriodEndDate THEN NULL
    --Else output the last day in the week for weekly reporting
    ELSE DATEADD(DAY, (8 - DATEPART(WEEKDAY, Calendar_Date)) % 7, Calendar_Date)
    END AS Last_Day_of_Week,  
  CASE 
    WHEN DATEPART(MONTH, Calendar_Date)>=4
    THEN CONCAT(
        'Q', DATEPART(QUARTER, Calendar_Date)-1)
    ELSE 'Q4'
    END AS [Quarter_Name],
  CASE
    WHEN DATEPART(MONTH, Calendar_Date)>=4
    THEN CONCAT(
       RIGHT(CAST(YEAR(Calendar_Date) AS VARCHAR(4)),2) + '-' + RIGHT(CAST(YEAR(Calendar_Date) +1 AS VARCHAR(4)), 2), '_', 'Q', DATEPART(QUARTER, Calendar_Date)-1)
    ELSE CONCAT(
        RIGHT(CAST(YEAR(Calendar_Date) -1 AS VARCHAR(4)),2) + '-' + RIGHT(CAST(YEAR(Calendar_Date) AS VARCHAR(4)), 2), '_', 'Q4')
    END AS [Quarter_Name_Year], 
  @ReportingPeriodStartDate AS Reporting_Period_Start_Date,
  @ReportingPeriodEndDate AS Reporting_Period_End_Date,
  CONCAT(FORMAT(@ReportingPeriodStartDate, 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy')) AS Current_Reporting_Period,
  CONCAT(FORMAT(DATEADD(MONTH, -2, @ReportingPeriodEndDate), 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy')) AS Costs_Reporting_period,
  CASE
    WHEN @ReportingPeriodStartDate BETWEEN '2023-04-01' AND '2023-06-30'
      THEN CONCAT(FORMAT(DATEADD(MONTH, +3, @ReportingPeriodStartDate), 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy'))
    ELSE CONCAT(FORMAT(DATEADD(MONTH, 0, @ReportingPeriodStartDate), 'MMM yy'), ' - ', FORMAT(@ReportingPeriodEndDate, 'MMM yy'))
    END AS Waiting_Times_Period_Covered,
    'Apr 23 - Apr 24' AS ASCOF_Period_Covered,
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
FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup;

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
-----ETHNICITY-----------
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
--------AGE BANDS----
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Age_Bands_Dim;

SELECT DISTINCT Der_Age_Band,
  CASE 
    WHEN Der_Age_Band = 'Unknown' THEN 9
    WHEN Der_Age_Band = 'Under 18' THEN 8
    WHEN Der_Age_Band = '18 to 24' THEN 7
    WHEN Der_Age_Band = '25 to 44' THEN 6
    WHEN Der_Age_Band = '45 to 64' THEN 5
    WHEN Der_Age_Band = '65 to 74' THEN 4
    WHEN Der_Age_Band = '75 to 84' THEN 3
    WHEN Der_Age_Band = '85 to 94' THEN 2
    WHEN Der_Age_Band = '95 and above' THEN 1
    WHEN Der_Age_Band = 'Under 18' THEN 8
  END AS Sort_order
INTO ASC_Sandbox.LA_PBI_Age_Bands_Dim
FROM ASC_Sandbox.LA_PBI_Master_Table;

