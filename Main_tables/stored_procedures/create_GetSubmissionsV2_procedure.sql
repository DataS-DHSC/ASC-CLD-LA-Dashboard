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
-- (updated as part of AGEM pipeline)
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetSubmissionsV2
GO

CREATE PROCEDURE ASC_Sandbox.GetSubmissionsV2
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
      FROM ASC_Sandbox.REF_Submission_Reporting_Periods_V2
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

      -- Exclude files with known (major) issues
      AND ImportDate NOT IN (
        '2026-08-04 13:31:40.900',
        '2026-07-31 19:37:36.510',
        '2026-07-30 12:07:02.750',
        '2026-07-28 09:01:46.043',
        '2026-07-20 15:16:26.550',
        '2026-07-15 16:41:26.370',
        '2026-04-23 13:28:24.253',
        '2026-01-23 08:22:06.243',
        '2026-01-26 16:17:13.097'
        )

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