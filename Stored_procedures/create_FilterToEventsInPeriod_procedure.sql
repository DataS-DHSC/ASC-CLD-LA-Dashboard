---------------------------------------------------------------------------
-- create_FilterToEventsInPeriod_procedure.sql
--
-- Create procedure to filter CLD R1 raw table to events in period - services
-- occurring at any point in period and req/ass/rev events ending in period
--
-- Note:
-- - Input table must contain Ref_Period_Start_Date and Ref_Period_End_Date
--   (filters to events occurring in this period)
-- - If date of death is populated and falls between event start and end date
--   or event end date is NULL, event end date is replaced with date of death
--
-- Returns table of same format as input table plus derived event end date
-- (Der_Event_End_Date) and original end date renamed (Event_End_Date_Raw)
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.FilterToEventsInPeriod
GO

CREATE PROCEDURE ASC_Sandbox.FilterToEventsInPeriod
  @InputTable SYSNAME = NULL,
  @OutputTable AS NVARCHAR(50)
AS
  SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable
  EXEC(@Query)

  -- Filter to events which fall within the period of interest, accounting for date of death
  
  SELECT *
  INTO #OutputTable
  FROM (
    SELECT
      *, 
      -- Check for date of death: when date of death is between event start and end dates
      -- or when event end date is null, replace event end date with date of death
      CASE 
        WHEN (Date_of_Death BETWEEN Event_Start_Date AND Event_End_Date)
        OR (Date_of_Death > Event_Start_Date AND Event_End_Date IS NULL) THEN Date_of_Death 
        ELSE Event_End_Date
      END AS Der_Event_End_Date,
      Event_End_Date AS Event_End_Date_Raw
    FROM ASC_Sandbox.InputTable
  ) a 
  WHERE
  -- select requests, assessments, reviews which start before the end of the period and end withing the reporting period
  ((Event_Type NOT LIKE '%service%' 
  AND Der_Event_End_Date BETWEEN Ref_Period_Start_Date AND Ref_Period_End_Date 
  AND Event_Start_Date <= Ref_Period_End_Date) -- any events with null start dates are by default excluded
  OR
  -- selects services which start before the reporting period end and end must be after the start or null
  (Event_Type LIKE '%service%' AND (Der_Event_End_Date >= Ref_Period_Start_Date OR Der_Event_End_Date IS NULL)
  AND Event_Start_Date <= Ref_Period_End_Date))
  AND
  -- select records where date of death is null or greater than the reporting period start and event start dates
  (Date_of_Death IS NULL
  OR (Date_of_Death >= Ref_Period_Start_Date AND Date_of_Death >= Event_Start_Date));

  -- Drop original Event_End_Date field to highlight distinction between "_Raw" / "Der_" fields
  ALTER TABLE #OutputTable
  DROP COLUMN Event_End_date;

  SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO

---- Example execution:
--EXEC ASC_Sandbox.FilterToEventsInPeriod @InputTable = 'ASC_Sandbox.Temp_RawSubmissions', @OutputTable = 'ASC_Sandbox.Temp_EventsInPeriod'
