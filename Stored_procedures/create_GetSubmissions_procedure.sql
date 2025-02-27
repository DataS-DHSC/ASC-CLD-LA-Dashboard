---------------------------------------------------------------------------
-- create_GetSubmissions_procedure.sql
--
-- Create procedure to return the most recent submission from each LA that
--   1. fully covers the specified reporting period
--     (according to the event end dates within the file),
--   2. was submitted
--      i. after the end of the reporting period, and
--      ii. on or before the "as of" date 
--   (NB there may be no files submitted by an LA that meet the above criteria)
--
-- Returns table of LA_Name, ImportDate
-- See example executions of procedure below
--
-- NB uses reporting periods from ASC_Sandbox.REF_Submission_Reporting_Periods
-- > run Create_REF_Submission_Reporting_Periods_table.sql to update this table
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetSubmissions
GO

CREATE PROCEDURE ASC_Sandbox.GetSubmissions
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @SubmissionReportingPeriod AS NVARCHAR(256),
  @SubmissionsAsOfDate DATE
AS

  IF @SubmissionReportingPeriod NOT IN ('Stated', 'Derived')
  BEGIN

    PRINT 'EXITING. SubmissionReportingPeriod parameter not recognised. Please specify ''Stated'' or ''Derived''';
    RETURN

  END

  ELSE
  BEGIN

    SELECT DISTINCT
      LA_Name,
      ImportDate
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY
                             LA_Name
                           ORDER BY
                             ImportDate DESC
                          ) Row
      FROM ASC_Sandbox.REF_Submission_Reporting_Periods
      WHERE (
      (@SubmissionReportingPeriod = 'Derived'
      AND Der_Reporting_Period_Start_Date <= @ReportingPeriodStartDate
      AND Der_Reporting_Period_End_Date >= @ReportingPeriodEndDate)
      OR
      (@SubmissionReportingPeriod = 'Stated'
      AND Reporting_Period_Start_Date <= @ReportingPeriodStartDate
      AND Reporting_Period_End_Date >= @ReportingPeriodEndDate)
      )
      AND ImportDate > @ReportingPeriodEndDate
      AND CONVERT(date, ImportDate) <= @SubmissionsAsOfDate
    ) c
    -- take the latest file submitted by each LA (of those meeting the above criteria)
    WHERE Row = 1

  END;

GO

---- Example executions:

---- 1) Manually input reporting period and "as of" date

--EXEC ASC_Sandbox.GetSubmissions
--  @ReportingPeriodStartDate = '2023-04-01',
--  @ReportingPeriodEndDate = '2023-06-30',
--  @SubmissionReportingPeriod = 'Derived',
--  @SubmissionsAsOfDate = '2023-09-30'

---- 2) Declare reporting period and "as of" date

--DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01'
--DECLARE @ReportingPeriodEndDate AS DATE = '2024-03-31'
--DECLARE @SubmissionsAsOfDate AS DATE = '2024-07-31'

--EXEC ASC_Sandbox.Submissions
--  @ReportingPeriodStartDate = @ReportingPeriodStartDate,
--  @ReportingPeriodEndDate = @ReportingPeriodEndDate,
--  @SubmissionReportingPeriod = 'Stated',
--  @SubmissionsAsOfDate = @SubmissionsAsOfDate