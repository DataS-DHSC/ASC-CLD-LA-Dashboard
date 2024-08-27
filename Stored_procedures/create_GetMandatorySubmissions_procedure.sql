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
-- Returns table of LA_Name, ImportDate
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

---- 2) Declare reporting period and "as of" date

--DECLARE @ReportingPeriodStartDate AS DATE = '2023-04-01'
--DECLARE @ReportingPeriodEndDate AS DATE = '2024-03-31'
--DECLARE @SubmissionsAsOfDate AS DATE = '2024-07-31'

--EXEC ASC_Sandbox.GetMandatorySubmissions
--  @ReportingPeriodStartDate = @ReportingPeriodStartDate,
--  @ReportingPeriodEndDate = @ReportingPeriodEndDate,
--  @SubmissionsAsOfDate = @SubmissionsAsOfDate