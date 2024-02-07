---------------------------------------------------------------------------
-- create_GetMandatorySubmissions_procedure.sql
--
-- This script creates the procedure to return the most recent submission by each LA that
--   1. fully covers the specified mandatory reporting period
--     (according to the reporting period start and end dates within the file),
--   2. was submitted
--      i. after the end of the mandatory reporting period, and
--      ii. on or before the "as of" date 
--   (NB there may be no files submitted by an LA that meet the above criteria)
--
-- Returns table of Der_Load_Filename which can then be used to filter the raw data
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetMandatorySubmissions
GO

CREATE PROCEDURE ASC_Sandbox.GetMandatorySubmissions
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @SubmissionsAsOfDate DATE
AS

  SELECT DISTINCT
    Der_Load_Filename
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY LA_Name, LA_Code ORDER BY ImportDate DESC) row
    FROM (
      SELECT
        LA_Name,
        LA_Code,
        ImportDate,
	      Der_Load_Filename,
        MIN(Reporting_Period_Start_Date) Reporting_Period_Start_Date,
        MAX(Reporting_Period_End_Date) Reporting_Period_End_Date
      FROM DHSC_ASC.CLD_R1_Raw
	  WHERE LA_Name IS NOT NULL
      GROUP BY
        LA_Name,
        LA_Code,
        ImportDate,
        Der_Load_Filename
    ) a
    WHERE Reporting_Period_Start_Date <= @ReportingPeriodStartDate
    AND Reporting_Period_End_Date >= @ReportingPeriodEndDate
    AND ImportDate > @ReportingPeriodEndDate
    AND CONVERT(date, ImportDate) <= @SubmissionsAsOfDate
  ) b
  -- take the latest file submitted by each LA (of those meeting the above criteria)
  WHERE row = 1

GO