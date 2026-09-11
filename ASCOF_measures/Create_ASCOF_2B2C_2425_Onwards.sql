--Script for ASCOF 2B and ASCOF 2C

/* This code does the following:
1. Uses the joined submissions main table which takes the latest submission for the last 12m joined to previous submissions CLD start date
2. Finds all people with LTS res/nurs starting in the year
3. Finds all people with LTS res/nurs starting prior to the year
4. Remove the people in 3 from 2 = new admissions

Caveats:
> Can't distinguish between temporary and permanent admissions
> Some admissions at the start of the CLD might look 'new' but these are changes to service information 
e.g. cost, and we don't have the historic 22/23 information to check for prior start dates. 
This is not an issue for 24/25 onwards

*/



DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2BC_2425_Onwards
GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2BC_2425_Onwards
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(100),
  @InputTable_PersonDetails AS NVARCHAR(100),
  @PopulationData AS NVARCHAR(100),
  @PopulationYear AS NVARCHAR(4),
  @OutputTable AS NVARCHAR(100),
  @OutputTable_RLS AS NVARCHAR(100)

AS

  --SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails
  DROP SYNONYM IF EXISTS ASC_Sandbox.PopulationData


  SET @Query = N'DROP TABLE IF EXISTS ' + @OutputTable + ';
                DROP TABLE IF EXISTS ' + @OutputTable_RLS + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable +';
                CREATE SYNONYM ASC_Sandbox.InputTable_PersonDetails FOR ' + @InputTable_PersonDetails +';
                CREATE SYNONYM ASC_Sandbox.PopulationData FOR ' + @PopulationData +';'
  EXEC(@Query)


--================== 1. Select data =====================

DROP TABLE IF EXISTS ASC_Sandbox.ASCOF_2BC_Build;

SELECT 
	a.Der_NHS_LA_Combined_Person_ID,
  a.LA_Person_Unique_Identifier,
  a.LA_Code,
  a.LA_Name,
  a.Event_Start_Date,
  a.Der_Event_End_Date,
  a.Event_Outcome_Cleaned,
  b.Der_Birth_Date,
	b.Der_Date_of_Death,
    CASE
      WHEN b.Der_Birth_Date IS NOT NULL
      THEN FLOOR((DATEDIFF(DAY, b.Der_Birth_Date, a.Event_Start_Date)) / 365.25)
      ELSE NULL
    END AS Der_Age_Event_Start  -- age definition used for this metric
INTO ASC_Sandbox.ASCOF_2BC_Build
FROM ASC_Sandbox.InputTable a
LEFT JOIN ASC_Sandbox.InputTable_PersonDetails b
ON 
  a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID AND
  a.LA_Code = b.LA_Code
WHERE  
  Event_Type_Cleaned = 'Service' 
  AND Client_Type_Cleaned = 'Service user' 
  AND (Service_Type_Cleaned = 'Long term support: Residential care' or Service_Type_Cleaned = 'Long term support: Nursing care') 
  AND (b.Der_Birth_Date IS NOT NULL) 
  AND (b.Der_Date_of_Death >= @ReportingPeriodStartDate OR b.Der_Date_of_Death is NULL)
  AND (Service_Component_Cleaned IS NULL OR Service_Component_Cleaned LIKE '%Residential%' OR Service_Component_Cleaned LIKE '%Nursing%')  --exclude any services which indicate they aren't LT res/nurs
  AND Service_Component_Cleaned NOT LIKE '%Short%'
  AND (Event_Outcome_Cleaned IS NULL OR Event_Outcome_Cleaned <> 'NFA: Self-funded client or under 12wk disregard');
-- ^ exclude admissions which ended as a person went onto self-fund

--================== 2. Admissions within the year =====================

--Output list of people with a LTS starting within the period
DROP TABLE IF EXISTS #Admissions;

SELECT DISTINCT 
  Der_NHS_LA_Combined_Person_ID, 
  LA_Person_Unique_Identifier,
  LA_Code, 
  LA_Name, 
  Event_start_Date,
  Der_Age_Event_Start
INTO #Admissions
FROM ASC_Sandbox.ASCOF_2BC_Build
WHERE Event_Start_Date BETWEEN @ReportingPeriodStartDate AND  @ReportingPeriodEndDate


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
        ) c
WHERE Person_Status = 'New'

--================== 4. Create output tables =====================

--Deduplicate for anyone with two new admissions, one within each age band and remove anyone under 18
DROP TABLE IF EXISTS #Deduplicated;

SELECT 
  LA_Code,
  LA_Name,
  Der_NHS_LA_Combined_Person_ID,
  LA_Person_Unique_Identifier,
  MIN(CASE 
        WHEN Der_Age_Event_Start BETWEEN 18 AND 64 THEN '18 to 64'
        WHEN Der_Age_Event_Start > 64 then '65 and above'
        ELSE NULL
      END) AS Age_Band
INTO #Deduplicated
FROM #New_Admissions
WHERE Der_Age_Event_Start >= 18
GROUP BY 
  LA_Code,
  LA_Name,
  LA_Person_Unique_Identifier,
  Der_NHS_LA_Combined_Person_ID;

--Create numerators
DROP TABLE IF EXISTS #Numerators;

SELECT 
  LA_Code,
  Age_Band,
  COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Admissions,
  LA_Person_Unique_Identifier
INTO #Numerators
FROM #Deduplicated
GROUP BY 
  LA_Code, 
  Age_Band,
  LA_Person_Unique_Identifier
ORDER BY 
  LA_Code, 
  Age_Band;

--Create denominators
DROP TABLE IF EXISTS #Denominators;

SELECT 
  a.LA_Code,
  a.LA_Name,
  b.Age_Band,
  b.Population
INTO #Denominators
FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup a
LEFT JOIN ASC_Sandbox.PopulationData b
ON a.LA_Area_Code = b.Code
WHERE 
  b.Gender = 'Total'
  AND b.[Year] = @PopulationYear
  AND b.Geography = 'Local Authority'
  AND b.Age_Band IN ('18 to 64', '65 and above')
  AND b.Pub_Flag = 'ASCOF';
 
-----Create person level table for record level sharing report -----

--Create ASCOF 2B (18 to 64)
DROP TABLE IF EXISTS #OutputTable_RLS;

SELECT
  FORMAT(@ReportingPeriodStartDate, 'd MMM yy') + ' - ' + FORMAT(@ReportingPeriodEndDate, 'd MMM yy') AS Reporting_Period,
  d.LA_Code,
  d.LA_Name,
  n.LA_Person_Unique_Identifier,
  'ASCOF 2B' AS Measure,
  'The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 18-64yrs (per 100,000 population)' as [Description],
  d.Age_Band AS [Group],
  COALESCE(n.Admissions, 0) AS Numerator,
  d.[Population] AS Denominator
INTO #OutputTable_RLS
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
  n.LA_Person_Unique_Identifier,
  'ASCOF 2C' AS Measure,
  'The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 65 and over (per 100,000 population)' as [Description],
  d.Age_Band AS [Group],
  COALESCE(n.Admissions,0) AS Numerator,
  d.[Population] AS Denominator
FROM #Denominators d
LEFT JOIN #Numerators n
ON d.LA_Code = n.LA_Code AND d.Age_Band = n.Age_Band
WHERE d.Age_Band = '65 and above'
ORDER BY LA_Code;

-----Create LA level table for main dashboard -----
DROP TABLE IF EXISTS #OutputTable
SELECT
  Reporting_Period,
  LA_Code,
  LA_Name,
  Measure,
  [Description],
  [Group],
  SUM(Numerator) AS Numerator,
  Denominator,
  COALESCE(ROUND((CAST(SUM(Numerator) AS FLOAT) / CAST(Denominator AS FLOAT)) * 100000, 1), 0) AS [Outcome]
INTO #OutputTable
FROM #OutputTable_RLS
GROUP BY
  Reporting_Period,
  LA_Code,
  LA_Name,
  [Description],
  [Measure],
  [Group],
  Denominator


SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
  EXEC(@Query)
SET @Query = 'SELECT * INTO ' + @OutputTable_RLS + ' FROM #OutputTable_RLS'
  EXEC(@Query)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails
  DROP SYNONYM IF EXISTS ASC_Sandbox.PopulationData

GO

---- Example execution:
/*
EXEC ASC_Sandbox.Create_ASCOF2BC_2425_Onwards
  @ReportingPeriodStartDate = '2025-04-01',
  @ReportingPeriodEndDate = '2026-03-31', 
  @InputTable = 'DHSC_Reporting.CLD_230401_260331_JoinedSubmissions_V2', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_260331_JoinedSubmissions_V2_Latest_Person_Data',
  @PopulationData = 'ASC_Sandbox.REF_ONS_Mid_Year_Estimates_All_Pubs',
  @PopulationYear = '2025',
  @OutputTable = 'ASC_Sandbox.ASCOF_2BC',
  @OutputTable_RLS = 'ASC_Sandbox.ASCOF_2BC_RLS'
*/