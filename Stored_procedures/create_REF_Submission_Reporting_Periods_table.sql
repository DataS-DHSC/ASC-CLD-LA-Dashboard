---------------------------------------------------------------------------
-- Create ref table of submission stated and derived reporting periods
--
-- Derives reporting period using events ending per month - see below for
-- detail. NB intend to schedule this script to be run in AGEM pipeline.
---------------------------------------------------------------------------

DROP TABLE IF EXISTS ASC_Sandbox.REF_Submission_Reporting_Periods;

DECLARE @Min_n AS INT = 100;
DECLARE @Min_pct AS INT = 2;

WITH Events_Ending_by_Month AS (

  SELECT
    LA_Name,
    ImportDate,
    DATEFROMPARTS(YEAR(Event_End_Date), MONTH(Event_End_Date), 1) AS 'Month',
    COUNT(DISTINCT Event_Type) AS n_Event_Types_in_Month,
    COUNT(*) AS n_End_Dates_in_Month
  FROM DHSC_ASC.CLD_R1_Raw
  WHERE Event_End_Date < DATEFROMPARTS(YEAR(ImportDate), MONTH(ImportDate), 1)
  GROUP BY LA_Name, ImportDate, DATEFROMPARTS(YEAR(Event_End_Date), MONTH(Event_End_Date), 1)

)

SELECT
  a.*,
  DATEDIFF(MONTH, Reporting_Period_Start_Date, DATEADD(DAY, 1, Reporting_Period_End_Date)) AS Reporting_Period_Length,
  Der_Reporting_Period_Start_Date,
  Der_Reporting_Period_End_Date,
  DATEDIFF(MONTH, Der_Reporting_Period_Start_Date, DATEADD(DAY, 1, Der_Reporting_Period_End_Date)) AS Der_Reporting_Period_Length
INTO ASC_Sandbox.REF_Submission_Reporting_Periods
FROM (

  SELECT
    LA_Name,
    ImportDate,
    ------------------------------------------------------------------------------------
    -- Get min/max stated reporting period start/end in each submission
    ------------------------------------------------------------------------------------
    COUNT(DISTINCT CONCAT(Reporting_Period_Start_Date, Reporting_Period_End_Date)) AS n_Reporting_Periods,
    MIN(Reporting_Period_Start_Date) AS Reporting_Period_Start_Date,
    MAX(Reporting_Period_End_Date) AS Reporting_Period_End_Date
  FROM DHSC_ASC.CLD_R1_Raw
  GROUP BY LA_Name, ImportDate

  ) a
LEFT JOIN (

  SELECT
    f.LA_Name,
    f.ImportDate,
    ------------------------------------------------------------------------------------
    -- Derive reporting period from event end dates in each submission
    ------------------------------------------------------------------------------------
    -- Counts events ending per month and derives reporting period as start of first
    -- month to end of last month, excluding months containing less than 2% of the total
    -- events or less than 3 event types unless there are less than 100 events in total.
    -- N.B Excludes future event end dates (either ending in or after the import month)
    ------------------------------------------------------------------------------------
    MIN(CASE
          WHEN (100.0 * n_End_Dates_in_Month/n_End_Dates_in_File) >= @Min_pct
           AND n_Event_Types_in_Month >= n_Event_Types_in_File - 1
           AND n_End_Dates_in_File >= @Min_n THEN [Month]
          WHEN n_End_Dates_in_File < @Min_n THEN [Month]
        END) AS Der_Reporting_Period_Start_Date,
    MAX(CASE
          WHEN (100.0 * n_End_Dates_in_Month/n_End_Dates_in_File) >= @Min_pct
           AND n_Event_Types_in_Month >= n_Event_Types_in_File - 1 THEN EOMONTH([Month])
          WHEN n_End_Dates_in_File < @Min_n THEN EOMONTH([Month])
        END) AS Der_Reporting_Period_End_Date
  FROM Events_Ending_by_Month m
  LEFT JOIN (
    SELECT
      LA_Name,
      ImportDate,
      MAX(n_Event_Types_in_Month) AS n_Event_Types_in_File,
      SUM(n_End_Dates_in_Month) AS n_End_Dates_in_File
    FROM Events_Ending_by_Month
    GROUP BY LA_Name, ImportDate
  ) f
  ON m.LA_Name = f.LA_Name AND m.ImportDate = f.ImportDate
  GROUP BY f.LA_Name, f.ImportDate

) b
ON a.LA_Name = b.LA_Name AND a.ImportDate = b.ImportDate