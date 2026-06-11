---------------------------------------------------------------------------
-- Create main deduplicated CLD tables covering a period using joined
-- submissions
--
-- NB no cost information is included in output
--
-- (See create_main_table_for_12mo_period.sql for main deduplicated CLD
-- tables covering a 12-month period using single submissions)
--
---------------------------------------------------------------------------

-- Amend reporting period below:

DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01';
DECLARE @ReportingPeriodEndDate AS DATE = '2026-03-31';

-- Set "as of" (cut-off) date to select submissions below:
DECLARE @SubmissionsAsOfDate AS DATE = '2026-05-05';

---------------------------------------------------------------------------

SET NOCOUNT ON; -- Hides "(X rows affected)" messages

---------------------------------------------------------------------------
-- 1. Get submissions covering the period
---------------------------------------------------------------------------

DROP TABLE IF EXISTS #Submissions;

CREATE TABLE #Submissions (
  LA_Name VARCHAR(256),
  ImportDate DATETIME,
  Ref_Period_Start_Date DATE,
  Ref_Period_End_Date DATE
);

-- For each quarter (ref period) of the reporting period, get the latest file
-- covering the period for each LA, as of specified date:

DECLARE @RefPeriodStartDate AS DATE = @ReportingPeriodStartDate;
DECLARE @RefPeriodEndDate AS DATE = DATEADD(day, -1, DATEADD(month, 3, @RefPeriodStartDate));

WHILE @RefPeriodEndDate <= @ReportingPeriodEndDate
BEGIN

  DROP TABLE IF EXISTS #TempSubmissions;
  CREATE TABLE #TempSubmissions (
    LA_Name VARCHAR(256),
    ImportDate DATETIME
  );

  BEGIN
    PRINT '';
    PRINT '=====================================================';
    PRINT CONCAT('Selecting latest submissions covering ', @RefPeriodStartDate, ' to ', @RefPeriodEndDate, '...');
  END

  -- Execute GetSubmissions procedure to select submissions covering period according
  -- to derived reporting period and insert results into #TempSubmissions
  INSERT INTO #TempSubmissions
  EXEC ASC_Sandbox.GetSubmissions
    @ReportingPeriodStartDate = @RefPeriodStartDate,
    @ReportingPeriodEndDate = @RefPeriodEndDate,
    @SubmissionReportingPeriod = 'Derived',
    @SubmissionsAsOfDate = @SubmissionsAsOfDate;

  -- Append results to #Submissions
  INSERT INTO #Submissions
  SELECT
    LA_Name,
    ImportDate,
    @RefPeriodStartDate AS Ref_Period_Start_Date,
    @RefPeriodEndDate AS Ref_Period_End_Date
  FROM #TempSubmissions;

  -- QA check: Identify any LAs missing data for period
  DROP TABLE IF EXISTS #MissingData;
  SELECT LA_Name INTO #MissingData FROM DHSC_ASC.Reference_ODS_LA
  WHERE LA_Name NOT IN (SELECT LA_Name FROM #TempSubmissions);

  IF EXISTS (SELECT * FROM #MissingData)

  -- Flag data missing for period (NB does not prevent script from continuing)
  BEGIN
    SELECT
      LA_Name AS 'WARNING! Data missing for:',
      @RefPeriodStartDate AS Ref_Period_Start_Date,
      @RefPeriodEndDate AS Ref_Period_End_Date
    FROM #MissingData
    ORDER BY LA_Name;

    PRINT 'WARNING: Data missing - see Results tab';
  END

  ELSE
    PRINT 'No missing data';

  -- Set ref period to next quarter
  SET @RefPeriodStartDate = DATEADD(month, 3, @RefPeriodStartDate);
  SET @RefPeriodEndDate = DATEADD(day, -1, DATEADD(month, 3, @RefPeriodStartDate));

END;

---------------------------------------------------------------------------
-- 2. Filter the full dataset to the list of submissions created above
---------------------------------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.Temp_JoinedSubs_RawSubmissions;

SELECT
  t2.*,
  Ref_Period_Start_Date,
  Ref_Period_End_Date
INTO ASC_Sandbox.Temp_JoinedSubs_RawSubmissions
FROM (
  SELECT
    LA_Name,
    ImportDate,
    MIN(Ref_Period_Start_Date) AS Ref_Period_Start_Date,
    MAX(Ref_Period_End_Date) AS Ref_Period_End_Date
  FROM #Submissions
  GROUP BY LA_Name, ImportDate
) t1
LEFT JOIN DHSC_ASC.CLD_Raw t2
ON t1.LA_Name = t2.LA_Name AND t1.ImportDate = t2.ImportDate;

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_JoinedSubs_RawSubmissions)
BEGIN
  PRINT '';
  PRINT '=====================================================';
  PRINT '';
  PRINT '> Filtered full dataset to above-selected submissions';
END;

---------------------------------------------------------------------------
-- 3. Filter the data to events in period
---------------------------------------------------------------------------

-- Execute FilterToEventsInPeriod procedure and save results to Temp_ sandbox table
EXEC ASC_Sandbox.FilterToEventsInPeriod @InputTable = 'ASC_Sandbox.Temp_JoinedSubs_RawSubmissions', @OutputTable = 'ASC_Sandbox.Temp_JoinedSubs_EventsInPeriod';

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_JoinedSubs_EventsInPeriod)
BEGIN
  PRINT '> Filtered to events in period';
END;

---------------------------------------------------------------------------
-- 4. Get cleaned and derived fields
---------------------------------------------------------------------------

-- Execute GetDerivedFields procedure and save results to Temp_ sandbox table
EXEC ASC_Sandbox.GetDerivedFields @InputTable = 'ASC_Sandbox.Temp_JoinedSubs_EventsInPeriod', @OutputTable = 'ASC_Sandbox.Temp_JoinedSubs_DerivedFields';

-- Add any new invalid values output to the data field mapping tables found at
-- https://healthsharedservice.sharepoint.com/sites/ORGASCCLDanalysis36567/Shared%20Documents/Forms/AllItems.aspx?FolderCTID=0x012000C8FD7D7D38E1174199C3C3A70E440AA8&id=%2Fsites%2FORGASCCLDanalysis36567%2FShared%20Documents%2FReference%20data%2FDatafield%20mapping'
-- and re-write ASC_Sandbox.REF_ table using create_REF_Data_Field_Mapping_tables.R (found in asc-cld-analysis/REF_tables/)

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_JoinedSubs_DerivedFields)
BEGIN
  PRINT '> Cleaned and derived fields';
END;

---------------------------------------------------------------------------
-- 5. Deduplicate
---------------------------------------------------------------------------

-- Execute GetUniqueEvents procedure and save results to Temp_ sandbox table
EXEC ASC_Sandbox.GetUniqueEvents @Submissions = 'Joined', @InputTable = 'ASC_Sandbox.Temp_JoinedSubs_DerivedFields', @OutputTable = 'ASC_Sandbox.Temp_JoinedSubs_UniqueEvents';

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_JoinedSubs_UniqueEvents)
BEGIN
  PRINT '> Deduplicated';
END;

---------------------------------------------------------------------------
-- 6. Write output table 
---------------------------------------------------------------------------

-- Create output table name and write final output to sandbox
-- NB if table already exists this will fail
DECLARE @TableName AS VARCHAR(256) = CONCAT('CLD_',
                                             FORMAT((SELECT MIN(Ref_Period_Start_Date) FROM ASC_Sandbox.Temp_JoinedSubs_UniqueEvents), 'yyMMdd'),
                                             '_',
                                             FORMAT((SELECT MAX(Ref_Period_End_Date) FROM ASC_Sandbox.Temp_JoinedSubs_UniqueEvents), 'yyMMdd'),
                                             '_JoinedSubmissions');

DECLARE @Query NVARCHAR(MAX);
SET @Query = 'SELECT *
              INTO ASC_Sandbox.' + @TableName + ' 
              FROM ASC_Sandbox.Temp_JoinedSubs_UniqueEvents;';
EXEC(@Query);

PRINT '' ;
PRINT 'Please now run quarto main table QA script to sense check table created';
PRINT 'then uncomment and run section 7 to drop "Temp_JoinedSubs" tables.';

---------------------------------------------------------------------------
-- 7. Drop temporary tables
---------------------------------------------------------------------------

--DROP TABLE IF EXISTS ASC_Sandbox.Temp_JoinedSubs_RawSubmissions;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_JoinedSubs_EventsInPeriod;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_JoinedSubs_DerivedFields;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_JoinedSubs_UniqueEvents;
