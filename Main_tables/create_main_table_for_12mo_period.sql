---------------------------------------------------------------------------
-- Create main deduplicated CLD tables covering a 12-month period using
-- single submissions
--
-- (See create_main_table_for_period.sql for main deduplicated CLD tables
-- covering a longer period using joined submissions)
--
---------------------------------------------------------------------------

-- Amend reporting period below:

DECLARE @ReportingPeriodStartDate AS DATE = '2025-04-01';
DECLARE @ReportingPeriodEndDate AS DATE = '2026-03-31';

-- Set "as of" (cut-off) date to select submissions below:
DECLARE @SubmissionsAsOfDate AS DATE = '2026-05-05';

---------------------------------------------------------------------------

SET NOCOUNT ON; -- Hides "(X rows affected)" messages

---------------------------------------------------------------------------
-- 1. Get submissions covering the period
---------------------------------------------------------------------------

DROP TABLE IF EXISTS #TempSubmissions;

CREATE TABLE #TempSubmissions (
  LA_Name VARCHAR(256),
  ImportDate DATETIME
);

PRINT '';
PRINT '=====================================================';
PRINT CONCAT('Selecting latest submissions covering ', @ReportingPeriodStartDate, ' to ', @ReportingPeriodEndDate, '...');

-- Execute GetSubmissions procedure to select submissions covering 12-mo period according
-- to derived reporting period and insert results into #TempSubmissions
INSERT INTO #TempSubmissions
EXEC ASC_Sandbox.GetSubmissions
  @ReportingPeriodStartDate = @ReportingPeriodStartDate,
  @ReportingPeriodEndDate = @ReportingPeriodEndDate,
  @SubmissionReportingPeriod = 'Derived',
  @SubmissionsAsOfDate = @SubmissionsAsOfDate;

--============== MANUALLY ADD/REMOVE SUBMISSIONS ===================

DROP TABLE IF EXISTS #Submissions;

SELECT
  *,
  @ReportingPeriodStartDate AS Ref_Period_Start_Date,
  @ReportingPeriodEndDate AS Ref_Period_End_Date
INTO #Submissions
FROM #TempSubmissions
--WHERE LA_Name <> '' -- Remove any submissions to be replaced
;

--INSERT INTO #Submissions
--VALUES
--  ('', -- LA_Name &
--  '', -- ImportDate of submissions to manually add
--  @ReportingPeriodStartDate,
--  @ReportingPeriodEndDate
--  ),
--  ('',
--  '',
--  @ReportingPeriodStartDate,
--  @ReportingPeriodEndDate
--  );

--====================== SECTION END ===============================

-- QA check: Identify any LAs missing data for period
DROP TABLE IF EXISTS #MissingData;
SELECT LA_Name INTO #MissingData FROM DHSC_ASC.Reference_ODS_LA
WHERE LA_Name NOT IN (SELECT LA_Name FROM #Submissions);

IF EXISTS (SELECT * FROM #MissingData)

-- Flag data missing for period (NB does not prevent script from continuing)
BEGIN
  SELECT LA_Name AS 'WARNING! Data missing for:'
  FROM #MissingData
  ORDER BY LA_Name;

  PRINT 'WARNING: Data missing - see Results tab';
END

ELSE
BEGIN
  PRINT 'No missing data';
END;

---------------------------------------------------------------------------
-- 2. Filter the full dataset to the list of submissions created above
---------------------------------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.Temp_SingleSubs_RawSubmissions;

SELECT
  t2.*,
  Ref_Period_Start_Date,
  Ref_Period_End_Date
INTO ASC_Sandbox.Temp_SingleSubs_RawSubmissions
FROM #Submissions t1
LEFT JOIN DHSC_ASC.CLD_Raw t2
ON t1.LA_Name = t2.LA_Name AND t1.ImportDate = t2.ImportDate;

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_SingleSubs_RawSubmissions)
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
EXEC ASC_Sandbox.FilterToEventsInPeriod @InputTable = 'ASC_Sandbox.Temp_SingleSubs_RawSubmissions', @OutputTable = 'ASC_Sandbox.Temp_SingleSubs_EventsInPeriod';

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_SingleSubs_EventsInPeriod)
BEGIN
  PRINT '> Filtered to events in period';
END;

---------------------------------------------------------------------------
-- 4. Get cleaned and derived fields
---------------------------------------------------------------------------

-- Execute GetDerivedFields procedure and save results to Temp_ sandbox table
EXEC ASC_Sandbox.GetDerivedFields @InputTable = 'ASC_Sandbox.Temp_SingleSubs_EventsInPeriod', @OutputTable = 'ASC_Sandbox.Temp_SingleSubs_DerivedFields';

-- Add any new invalid values output to the data field mapping tables found at
-- https://healthsharedservice.sharepoint.com/sites/ORGASCCLDanalysis36567/Shared%20Documents/Forms/AllItems.aspx?FolderCTID=0x012000C8FD7D7D38E1174199C3C3A70E440AA8&id=%2Fsites%2FORGASCCLDanalysis36567%2FShared%20Documents%2FReference%20data%2FDatafield%20mapping'
-- and re-write ASC_Sandbox.REF_ table using create_REF_Data_Field_Mapping_tables.R (found in asc-cld-analysis/REF_tables/)

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_SingleSubs_DerivedFields)
BEGIN
  PRINT '> Cleaned and derived fields';
END;

---------------------------------------------------------------------------
-- 5. Deduplicate
---------------------------------------------------------------------------

-- Execute GetUniqueEvents procedure and save results to Temp_ sandbox table
EXEC ASC_Sandbox.GetUniqueEvents @Submissions = 'Single', @InputTable = 'ASC_Sandbox.Temp_SingleSubs_DerivedFields', @OutputTable = 'ASC_Sandbox.Temp_SingleSubs_UniqueEvents';

IF EXISTS (SELECT * FROM ASC_Sandbox.Temp_SingleSubs_UniqueEvents)
BEGIN
  PRINT '> Deduplicated';
END;

---------------------------------------------------------------------------
-- 6. Write output table
---------------------------------------------------------------------------

-- Create output table name and write final output to sandbox
-- NB if table already exists this will fail
DECLARE @TableName AS VARCHAR(256) = CONCAT('CLD_',
                                            FORMAT((SELECT MIN(Ref_Period_Start_Date) FROM ASC_Sandbox.Temp_SingleSubs_UniqueEvents), 'yyMMdd'),
                                             '_',
                                             FORMAT((SELECT MAX(Ref_Period_End_Date) FROM ASC_Sandbox.Temp_SingleSubs_UniqueEvents), 'yyMMdd'),
                                             '_SingleSubmissions');

DECLARE @Query NVARCHAR(MAX);
SET @Query = 'SELECT *
              INTO ASC_Sandbox.' + @TableName + ' 
              FROM ASC_Sandbox.Temp_SingleSubs_UniqueEvents;';
EXEC(@Query);

PRINT '' ;
PRINT 'Please now run quarto main table QA script to sense check table created';
PRINT 'then uncomment and run section 7 to drop "Temp_SingleSubs" tables.';

---------------------------------------------------------------------------
-- 7. Drop temporary tables
---------------------------------------------------------------------------

--DROP TABLE IF EXISTS ASC_Sandbox.Temp_SingleSubs_RawSubmissions;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_SingleSubs_EventsInPeriod;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_SingleSubs_DerivedFields;
--DROP TABLE IF EXISTS ASC_Sandbox.Temp_SingleSubs_UniqueEvents;
