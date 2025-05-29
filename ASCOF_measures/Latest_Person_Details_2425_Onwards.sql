/* SUMMARY:
 - This code creates a latest person data table, as at the end of the reporting period, for
    1. Accommodation Status
    2. Gender
    3. Age / Working age band

    The following logic is used:
    - Accommodation status and gender takes the latest known information.
        - It does this by favouring known over nulls / 'Unknown' values.
        - Then sorts by most recent event end/start date.
        - Instances where there are >1 conflicting accom/gender on the same date, both are overwritten with 'Unknown'.
    - Latest age is calculated from the birth year and birth month (taking the day as 01) as of the Ref_Period_End_Date.
        - Ref Period End Date is derived in the previous process to create single/joined submissions.
        - It is equivalent to the reporting period end date for single submissions.
        - If two different dates of birth exists for the same person, the maximum age is taken.
    - There is one record per person (distinct LA_Code, person ID), and these are joined to form the final table.

*/

DECLARE  @InputTable NVARCHAR(100) = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions'
DECLARE  @OutputTable NVARCHAR (100) = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions_Latest_Person_Data'

DECLARE @Query NVARCHAR(MAX)
DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + '; 
              CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + ';'
EXEC(@Query)


---------------------------------------------------
-- Getting latest accom status for each person
--------------------------------------------------

-- Create table with cleaned accommodation status where invalid entries have been be mapped to valid entries
-- Create a new variable Der_Accommodation_Known
DROP TABLE IF EXISTS #Accom_Cleaned
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  COALESCE(Accommodation_Status_Cleaned, 'Unknown')  AS Accommodation_Status, -- R2 to R1 mapping variable
  CASE
    WHEN Accommodation_Status_Cleaned = 'Unknown' OR Accommodation_Status_Cleaned IS NULL
    THEN 0
    ELSE 1
    END AS Der_Accommodation_Known
  INTO #Accom_Cleaned
  FROM ASC_Sandbox.InputTable

-- Identify latest known accommodation status for each person. This is done by:
-- 1. Sort by der_accommodation known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Accom_Row1;
WITH LatestAccomm
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Accommodation_Known DESC,
              (CASE WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' ELSE Der_Event_End_Date END) DESC,
              Event_Start_Date DESC
       ) AS Rn
    FROM #Accom_Cleaned)
SELECT 
  *
INTO #Accom_Row1
FROM LatestAccomm
WHERE Rn = 1

-- Identifies conflicting accommodation status' per person
DROP TABLE IF EXISTS #Accomm_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Accommodation_Status) AS [COUNT]
INTO #Accomm_Duplicates
FROM #Accom_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Accommodation_Status) >1



-- Overwrites conflicting accommodation status' with 'Unknown'
DROP TABLE IF EXISTS #Accom_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Accommodation_Status
    END AS [Accommodation_Status]
INTO #Accom_Deduped
FROM #Accom_Row1 a
LEFT JOIN #Accomm_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID


 ---------------------------------------------------
  -- Getting latest gender for each person
  ---------------------------------------------------

-- Create table with cleaned gender where invalid entries have been be mapped to valid entries
-- Create a new variable Der_Gender_Known
DROP TABLE IF EXISTS #Gender_Cleaned
SELECT
  *,
  CASE
    WHEN Gender_Cleaned = 'Unknown'
    THEN 0
    ELSE 1
    END AS Der_Gender_Known
  INTO #Gender_Cleaned
  FROM (
    SELECT 
      LA_Code,
      Der_NHS_LA_Combined_Person_ID,
      Event_Start_Date,
      Der_Event_End_Date,
      CASE
        WHEN Gender_Cleaned IS NULL
        THEN 'Unknown'
        ELSE Gender_Cleaned
        END AS Gender_Cleaned
      FROM ASC_Sandbox.InputTable ) A

-- Identify latest known gender for each person. This is done by:
-- 1. Sort by Der_Gender_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Gender_Row1;
WITH LatestGender
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Gender_Known DESC,
              (CASE WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' ELSE Der_Event_End_Date END) DESC,
              Event_Start_Date DESC
      ) AS Rn
    FROM #Gender_Cleaned)
SELECT 
  *
INTO #Gender_Row1
FROM LatestGender
WHERE Rn = 1



-- Identifies conflicting genders per person
DROP TABLE IF EXISTS #Gender_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Gender_Cleaned) AS [COUNT]
INTO #Gender_Duplicates
FROM #Gender_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Gender_Cleaned) >1
  


-- Overwrites conflicting genders with 'Unknown'
DROP TABLE IF EXISTS #Gender_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Gender_Cleaned
    END AS Gender
INTO #Gender_Deduped
FROM #Gender_Row1 a
LEFT JOIN #Gender_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID



  ---------------------------------------------------
  -- Getting latest age for each person
  ---------------------------------------------------
DROP TABLE IF EXISTS #Latest_Age
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  MAX(Latest_Age) as Latest_Age,
  CASE
      WHEN MAX(Latest_Age) BETWEEN 1 AND 17 THEN 'Under 18'
      WHEN MAX(Latest_Age) BETWEEN 18 AND 64 THEN '18 to 64'
      WHEN MAX(Latest_Age) >= 65 THEN '65 and above'
      ELSE 'Unknown'
    END AS Latest_Age_Band
INTO #Latest_Age
FROM (
  SELECT
    *,
     CASE
        WHEN Der_Birth_Year IS NOT NULL
          THEN FLOOR((DATEDIFF(DAY, CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-01') AS DATE), Ref_Period_End_Date)) / 365.25)
        ELSE NULL
        END AS Latest_Age
     FROM ASC_Sandbox.InputTable ) A
GROUP BY LA_Code, Der_NHS_LA_Combined_Person_ID



-- Join for one record per person
DROP TABLE IF EXISTS #OutputTable
SELECT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  a.Accommodation_Status,
  b.Gender,
  c.Latest_Age,
  c.Latest_Age_Band
INTO #OutputTable
FROM #Accom_Deduped a
LEFT JOIN #Gender_Deduped b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Latest_Age c
  ON a.LA_Code = c.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = c.Der_NHS_LA_Combined_Person_ID


  
SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
EXEC(@Query)

DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable