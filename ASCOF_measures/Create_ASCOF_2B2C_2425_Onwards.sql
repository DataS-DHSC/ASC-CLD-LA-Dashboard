--Revised and simplified code for ASCOF 2B/C

/* This code does the following:
1. Uses the joined submissions main table which takes the latest submission for the last 12m joined to previous submissions CLD start date
2. Finds all people with LTS res/nurs starting in the year
3. Finds all people with LTS res/nurs starting prior to the year
4. Remove the people in 3 from 2 = new admissions

Caveats:
> Can't distinguish between temporary and permanent admissions
> Some admissions at the start of the year might look 'new' but these are changes to service information 
e.g. cost, and we don't have the historic 22/23 information to check for prior start dates. 
This should resolve from 23/24 onwards

*24/25 onwards - this code has been adapted for use on the 24/25 main tables as these tables contain different field names where R2 to R1 mapping has been applied

*/



DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2BC_2425_Onwards
GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2BC_2425_Onwards
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(50),
  @OutputTable AS NVARCHAR(50)

AS

  --SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable
  EXEC(@Query)


--================== 1. Select data =====================

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_Build;

SELECT *,
     CASE
        WHEN Der_Birth_Year IS NOT NULL
          THEN FLOOR((DATEDIFF(DAY, CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-01') AS DATE), @ReportingPeriodEndDate)) / 365.25)
        ELSE NULL
        END AS Latest_Age  -- use age at the end of the reporting period period
INTO ASC_Sandbox.ASCOF_2BC_Build
FROM ASC_Sandbox.InputTable
WHERE  Event_Type = 'Service' 
AND Client_Type_Cleaned = 'Service User' 
AND (Service_Type_Cleaned = 'Long Term Support: Residential Care' or Service_Type_Cleaned = 'Long Term Support: Nursing Care') 
AND (Der_Birth_Month is not NULL and Der_Birth_Year is not NULL)
AND (Service_Component_Cleaned IS NULL OR Service_Component_Cleaned LIKE '%Residential%' OR Service_Component_Cleaned LIKE '%Nursing%')  --exclude any services which indicate they aren't LT res/nurs
AND Service_Component_Cleaned NOT LIKE '%Short%';

--================== 2. Admissions within the year =====================

--Output list of people with a LTS starting within the period
DROP TABLE IF EXISTS #Admissions;

SELECT DISTINCT 
  Der_NHS_LA_Combined_Person_ID, 
  LA_Code, 
  LA_Name, 
  Event_start_Date,
  Latest_Age
INTO #Admissions
FROM ASC_Sandbox.ASCOF_2BC_Build
WHERE Event_Start_Date BETWEEN @ReportingPeriodStartDate AND  @ReportingPeriodEndDate
AND (Event_Outcome_Cleaned IS NULL OR Event_Outcome_Cleaned <> 'NFA - Self-funded client (Inc. 12wk disregard)');  
-- ^ exclude admissions which ended as a person went onto self-fund


--================== 3. Determine which are new admissions =====================
--Removes anyone with an admission in the period also had a LT res/nurs within the last 12 months (i.e. not new)

DROP TABLE IF EXISTS #New_Admissions

SELECT *
INTO #New_Admissions
FROM (
SELECT
        a.*,
        CASE
            WHEN EXISTS (  --This logic flags when all of the below is TRUE for a row
                SELECT 1
                FROM ASC_Sandbox.ASCOF_2BC_Build b  --Table only contains res/nurs
                WHERE 
                --Person ID and LA code have to match
                b.Der_NHS_LA_Combined_Person_ID = a.Der_NHS_LA_Combined_Person_ID
                AND b.LA_Code = a.LA_Code
                --Check if LT res/nurs service started prior to the start date of the admission within the year of interest
                AND b.Event_Start_Date < a.Event_Start_Date 
                -- Check if LT res/nurs service ended within the last 12 months prior to the start date of the admission in the year of interest (or had null end date)
                AND (DATEDIFF(DAY, b.Der_Event_End_Date, a.Event_Start_Date) < 365
                  OR b.Der_Event_End_Date IS NULL)
            ) THEN 'Existing'
            ELSE 'New'
        END AS Person_Status      
    FROM
        #Admissions a
        ) b
WHERE Person_Status = 'New'

--================== 4. Aggregated counts by LA =====================

--Deduplicate for anyone with two new admissions, one within each age band and remove anyone under 18
DROP TABLE IF EXISTS #Deduplicated;

SELECT 
  LA_Code,
  LA_Name,
  Der_NHS_LA_Combined_Person_ID,
  MIN(CASE 
        WHEN Latest_Age BETWEEN 18 AND 64 THEN '18 to 64'
        WHEN Latest_Age > 64 then '65 and above'
        ELSE NULL
      END) AS Age_Band
INTO #Deduplicated
FROM #New_Admissions
WHERE Latest_Age >= 18
GROUP BY 
  LA_Code,
  LA_Name,
  Der_NHS_LA_Combined_Person_ID;

--Create numerators
DROP TABLE IF EXISTS #Numerators;

SELECT 
  LA_Code,
  Age_Band,
  COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Admissions
INTO #Numerators
FROM #Deduplicated
GROUP BY LA_Code, Age_Band
ORDER BY LA_Code, Age_Band;

--Create denominators
DROP TABLE IF EXISTS #Denominators;

SELECT 
  a.LA_Code,
  a.LA_Name,
  b.Age_Band,
  b.Population
INTO #Denominators
FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup a
LEFT JOIN ASC_Sandbox.REF_ONS_Pop_Gender_Pub_Age_Bands_23 b
ON a.LA_Area_Code = b.Code
WHERE 
  b.Gender = 'All' 
  AND b.Geography = 'Local Authority'
  AND b.Age_Band IN ('18 to 64', '65 and above');
  
--Create ASCOF 2B (18 to 64)
DROP TABLE IF EXISTS #OutputTable;

SELECT
  FORMAT(@ReportingPeriodStartDate, 'd MMM yy') + ' - ' + FORMAT(@ReportingPeriodEndDate, 'd MMM yy') AS Reporting_Period,
  d.LA_Code,
  d.LA_Name,
  'ASCOF 2B' AS Measure,
  'The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 18-64yrs (per 100,000 population)' as [Description],
  d.Age_Band AS [Group],
  COALESCE(n.Admissions, 0) AS Numerator,
  d.[Population] AS Denominator, 
  COALESCE(ROUND((CAST(n.Admissions AS FLOAT) / CAST(d.[Population] AS FLOAT)) * 100000, 1), 0) AS [Outcome]
INTO #OutputTable
FROM #Denominators d
LEFT JOIN #Numerators n
ON d.LA_Code = n.LA_Code AND d.Age_Band = n.Age_Band
WHERE d.Age_Band = '18 to 64'

UNION ALL 

--Create ASCOF 2C (65 and over)

SELECT
  FORMAT(@ReportingPeriodStartDate, 'd MMM yy') + ' - ' + FORMAT(@ReportingPeriodEndDate, 'd MMM yy') AS Reporting_Period,
  d.LA_Code,
  d.LA_Name,
  'ASCOF 2C' AS Measure,
  'The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 65 and over (per 100,000 population)' as [Description],
  d.Age_Band AS [Group],
  COALESCE(n.Admissions,0) AS Numerator,
  d.[Population] AS Denominator, 
  COALESCE(ROUND((CAST(n.Admissions AS FLOAT) / CAST(d.[Population] AS FLOAT)) * 100000, 1), 0) AS [Outcome]
FROM #Denominators d
LEFT JOIN #Numerators n
ON d.LA_Code = n.LA_Code AND d.Age_Band = n.Age_Band
WHERE d.Age_Band = '65 and above'
ORDER BY LA_Code;

SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable


GO

/*
---- Example execution:
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards 
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250331_JoinedSubmissions', 
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC_Revised'
*/