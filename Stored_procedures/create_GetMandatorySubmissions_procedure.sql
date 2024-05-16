---------------------------------------------------------------------------
-- create_GetMandatorySubmissions_procedure.sql
--
-- Create procedure to return the most recent submission by each LA that
--   1. fully covers the specified mandatory reporting period
--     (according to the reporting period start and end dates within the file),
--   2. was submitted
--      i. after the end of the mandatory reporting period, and
--      ii. on or before the "as of" date 
--   (NB there may be no files submitted by an LA that meet the above criteria)
--
-- Returns table of LA_Name, Der_Load_Filename
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetMandatorySubmissions
GO

CREATE PROCEDURE ASC_Sandbox.GetMandatorySubmissions
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @SubmissionsAsOfDate DATE
AS

  SELECT DISTINCT
    LA_Name,
    ImportDate
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY LA_Name ORDER BY ImportDate DESC) row
    FROM (
      SELECT
        LA_Name,
        ImportDate,
        MIN(Reporting_Period_Start_Date) Reporting_Period_Start_Date,
        MAX(Reporting_Period_End_Date) Reporting_Period_End_Date
      FROM DHSC_ASC.CLD_R1_Raw
	    WHERE LA_Name IS NOT NULL
      GROUP BY
        LA_Name,
        ImportDate
    ) a
    WHERE Reporting_Period_Start_Date <= @ReportingPeriodStartDate
    AND Reporting_Period_End_Date >= @ReportingPeriodEndDate
    AND ImportDate > @ReportingPeriodEndDate
    AND CONVERT(date, ImportDate) <= @SubmissionsAsOfDate
  ) b
  -- take the latest file submitted by each LA (of those meeting the above criteria)
  WHERE row = 1

GO

---- Example executions:

---- 1) Manually input reporting period and "as of" date

--EXEC ASC_Sandbox.GetMandatorySubmissions
--  @ReportingPeriodStartDate = '2023-04-01',
--  @ReportingPeriodEndDate = '2023-06-30',
--  @SubmissionsAsOfDate = '2023-09-30'

---- 2) Dynamically calculate reporting period and input "as of" date

---- SUBMISSION DATE
---- Set submission date in current submission window:
--DECLARE @SubmissionDate AS DATE = GETDATE()
---- Set submission date in previous quarter submission window:
---- DECLARE @SubmissionDate AS DATE = DATEADD(month, -3, GETDATE())

---- "AS OF" DATE
---- Set "as of" date to today
--DECLARE @SubmissionsAsOfDate AS DATE = GETDATE()
---- Set "as of" date to end of previous quarter submission window
---- DECLARE @SubmissionsAsOfDate AS DATE = DATEADD(quarter, DATEDIFF(quarter, 0, GETDATE()), -1)

--DECLARE @ReportingPeriodStartDate DATE
--DECLARE @ReportingPeriodEndDate DATE

--EXEC ASC_Sandbox.GetMandatoryReportingPeriod
--  @SubmissionDate = @SubmissionDate,
--  @ReportingPeriodStartDate = @ReportingPeriodStartDate OUTPUT,
--  @ReportingPeriodEndDate = @ReportingPeriodEndDate OUTPUT

--EXEC ASC_Sandbox.GetMandatorySubmissions
--  @ReportingPeriodStartDate = @ReportingPeriodStartDate,
--  @ReportingPeriodEndDate = @ReportingPeriodEndDate,
--  @SubmissionsAsOfDate = @SubmissionsAsOfDate