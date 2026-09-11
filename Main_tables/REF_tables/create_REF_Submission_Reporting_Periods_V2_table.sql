---------------------------------------------------------------------------
-- create ref table of submission stated and derived reporting periods
-- (version 2)
-- NB - NOT YET SCHEDULED AS PART OF AGEM PIPELINE (i.e. needs to be run)
---------------------------------------------------------------------------
-- Derive the period covered by each submission using request, assessment
-- and service event end dates, and flag gaps in coverage.
---------------------------------------------------------------------------
/*
Method
======
- Define a monthly calendar from 2020-04-01 to the current month.
- Group request, assessment and service events by event type and event
  end month. Reviews are excluded as they are not consistently submitted
  by LAs.
- For each of the required event types, calculate:
    - the total number of events in the submission,
    - the number of events ending in each month, and
    - the median monthly count.
- Events ending before 2020-04-01 and in or after the import month are excluded.
- Events with no end date are excluded from monthly counts. Service events
  with no end date are included in the event-type total count only.
- Treat a month as complete for an event type if:
    - the event type has sufficient total volume relative to the median
      event type total for the file; and
    - the monthly count is at least the specified percentage of that
      event type's median monthly count and greater than the specified
      minimum count.
- Use the calendar to assess all months, including months with no events.
- Allow an isolated one-month gap within a run where an incomplete month
  is immediately preceded and followed by complete months for the same
  event type.
- For each event type, derive the covered period as the latest run of
  complete months, allowing isolated one-month gaps as described above.
- Derive the overall covered period as the latest run of months where
  request, assessment and service are all complete or allowed within the
  run.
- If any required event type is completely missing, or if an overall
  covered period cannot be derived, the derived reporting period is NULL.
- The LA supplied reporting period is only used as a fallback for Isles
  of Scilly, due to small event counts.

Coverage gap flags
========
Gap flag: flagged where there is at least one incomplete month between
the first and last complete month for any required event type.

This includes:
- months with no events recorded; and
- months with counts below the event type threshold.

Isolated one-month gaps may be tolerated when deriving the covered
period, but they are still flagged as gaps for QA.

Missing event type flag: flagged where a required event type is missing.

Thresholds
=========
Thresholds and permitted gaps set such that derived reporting
periods most closely match verified reporting periods, deduced by
scrutinising the underlying data. (Reporting periods verified for > 50
submissions with questionable reporting periods, across > 30 local
authorities.)

Rationale
=========
This approach derives reporting periods from sustained evidence of
coverage rather than from the first and last complete months alone. It
reduces the influence of isolated records, prevents periods spanning
larger gaps, requires evidence from all key event types, and highlights
submissions that may contain incomplete months or gaps in coverage.
*/

DROP TABLE IF EXISTS ASC_Sandbox.REF_Submission_Reporting_Periods_V2;

DECLARE @Min_Monthly_Count INT = 10;              -- month must have >= this number of events
DECLARE @Min_Pct_Of_Median INT = 20;              -- month must have >= this % of that event type's median monthly count
DECLARE @Min_Pct_Of_Median_Type_Total INT = 5;    -- event type total must be >= this % of median event type total count
-- thresholds lenient to allow for different recording practices and genuine event volume fluctuations

DECLARE @Calendar_Start_Month DATE = '2020-04-01';
DECLARE @Calendar_End_Month DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1);

WITH Calendar_Months AS (

  SELECT
    DATEADD(MONTH, n.n, @Calendar_Start_Month) AS [Month]
  FROM (
    SELECT
      d1.n
        + 10 * d2.n
        + 100 * d3.n AS n
    FROM (
      VALUES
        (0), (1), (2), (3), (4),
        (5), (6), (7), (8), (9)
    ) d1(n)
    CROSS JOIN (
      VALUES
        (0), (1), (2), (3), (4),
        (5), (6), (7), (8), (9)
    ) d2(n)
    CROSS JOIN (
      VALUES
        (0), (1), (2), (3), (4),
        (5), (6), (7), (8), (9)
    ) d3(n)
  ) n
  WHERE DATEADD(MONTH, n.n, @Calendar_Start_Month) <= @Calendar_End_Month

),

LA_Submissions AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    COUNT(
      DISTINCT CONCAT(
        CONVERT(char(10), Reporting_Period_Start_Date, 23),
        '|',
        CONVERT(char(10), Reporting_Period_End_Date, 23)
      )
    ) AS n_Reporting_Periods,
    MIN(Reporting_Period_Start_Date) AS Reporting_Period_Start_Date,
    MAX(Reporting_Period_End_Date) AS Reporting_Period_End_Date
  FROM DHSC_ASC.CLD_Raw
  GROUP BY
    LA_Code,
    LA_Name,
    ImportDate

),

Required_Event_Types AS (

  SELECT Event_Type
  FROM (
    VALUES
      ('Request'),
      ('Assessment'),
      ('Service')
  ) v(Event_Type)

),

Classified_Events AS (

  SELECT
    r.LA_Code,
    r.LA_Name,
    r.ImportDate,
    v.Event_Type,
    CASE
      WHEN r.Event_End_Date IS NOT NULL
      THEN DATEFROMPARTS(YEAR(r.Event_End_Date), MONTH(r.Event_End_Date), 1)
    END AS Event_End_Month
  FROM DHSC_ASC.CLD_Raw r
  CROSS APPLY (
    VALUES (
      CASE
        WHEN r.Event_Type LIKE 'Request%' THEN 'Request'
        WHEN r.Event_Type LIKE 'Assessment%' THEN 'Assessment'
        WHEN r.Event_Type LIKE 'Service%' THEN 'Service'
      END
    )
  ) v(Event_Type)
  WHERE v.Event_Type IS NOT NULL
    AND (
      (
        r.Event_End_Date >= @Calendar_Start_Month
        AND r.Event_End_Date < DATEFROMPARTS(YEAR(r.ImportDate), MONTH(r.ImportDate), 1)
      )
      OR (
        v.Event_Type = 'Service'
        AND r.Event_End_Date IS NULL
      )
    )

),

Monthly_Event_End_Date_Counts AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_End_Month AS [Month],
    Event_Type,
    COUNT(*) AS n_End_Dates_in_Month
  FROM Classified_Events
  WHERE Event_End_Month IS NOT NULL
  GROUP BY
    LA_Code,
    LA_Name,
    ImportDate,
    Event_End_Month,
    Event_Type

),

Event_Type_Totals AS (

  SELECT
    s.LA_Code,
    s.LA_Name,
    s.ImportDate,
    e.Event_Type,
    COUNT(c.Event_Type) AS n_Events_For_Event_Type
  FROM LA_Submissions s
  CROSS JOIN Required_Event_Types e
  LEFT JOIN Classified_Events c
    ON s.LA_Code = c.LA_Code
   AND s.LA_Name = c.LA_Name
   AND s.ImportDate = c.ImportDate
   AND e.Event_Type = c.Event_Type
  GROUP BY
    s.LA_Code,
    s.LA_Name,
    s.ImportDate,
    e.Event_Type

),

Event_Type_Totals_With_Median AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    n_Events_For_Event_Type,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n_Events_For_Event_Type)
      OVER (
        PARTITION BY
          LA_Code,
          LA_Name,
          ImportDate
      ) AS Median_Event_Type_Total_Events
  FROM Event_Type_Totals

),

Monthly_Event_End_Date_Counts_With_Medians AS (

  SELECT
    m.LA_Code,
    m.LA_Name,
    m.ImportDate,
    m.[Month],
    m.Event_Type,
    m.n_End_Dates_in_Month,
    t.n_Events_For_Event_Type,
    t.Median_Event_Type_Total_Events,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m.n_End_Dates_in_Month)
      OVER (
        PARTITION BY
          m.LA_Code,
          m.LA_Name,
          m.ImportDate,
          m.Event_Type
      ) AS Median_End_Dates_in_Month
  FROM Monthly_Event_End_Date_Counts m
  INNER JOIN Event_Type_Totals_With_Median t
    ON m.ImportDate = t.ImportDate
   AND m.LA_Code = t.LA_Code
   AND m.LA_Name = t.LA_Name
   AND m.Event_Type = t.Event_Type

),

Event_Type_Month_Status AS (

  SELECT
    s.LA_Code,
    s.LA_Name,
    s.ImportDate,
    e.Event_Type,
    c.[Month],
    ISNULL(m.n_End_Dates_in_Month, 0) AS n_End_Dates_in_Month,
    t.n_Events_For_Event_Type,
    t.Median_Event_Type_Total_Events,
    m.Median_End_Dates_in_Month,

    CASE
      WHEN t.n_Events_For_Event_Type >= t.Median_Event_Type_Total_Events * @Min_Pct_Of_Median_Type_Total / 100.0
       AND (ISNULL(m.n_End_Dates_in_Month, 0) >= @Min_Monthly_Count
            OR s.LA_Code = '714') -- City of London excluded from this criteria as low counts expected
        AND 100.0 * ISNULL(m.n_End_Dates_in_Month, 0) / NULLIF(m.Median_End_Dates_in_Month, 0) >= @Min_Pct_Of_Median
      THEN 1
      ELSE 0
    END AS Is_Complete_Month

  FROM LA_Submissions s
  CROSS JOIN Required_Event_Types e
  INNER JOIN Calendar_Months c
    ON c.[Month] < DATEFROMPARTS(YEAR(s.ImportDate), MONTH(s.ImportDate), 1)
  INNER JOIN Event_Type_Totals_With_Median t
    ON s.LA_Code = t.LA_Code
   AND s.LA_Name = t.LA_Name
   AND s.ImportDate = t.ImportDate
   AND e.Event_Type = t.Event_Type
  LEFT JOIN Monthly_Event_End_Date_Counts_With_Medians m
    ON s.LA_Code = m.LA_Code
   AND s.LA_Name = m.LA_Name
   AND s.ImportDate = m.ImportDate
   AND e.Event_Type = m.Event_Type
   AND c.[Month] = m.[Month]

),

Event_Type_Month_Status_With_Lag AS (

  SELECT
    *,
    LAG(Is_Complete_Month, 1, 0) OVER (
      PARTITION BY
        LA_Code,
        LA_Name,
        ImportDate,
        Event_Type
      ORDER BY
        [Month]
    ) AS Prev_Is_Complete_Month,

    LEAD(Is_Complete_Month, 1, 0) OVER (
      PARTITION BY
        LA_Code,
        LA_Name,
        ImportDate,
        Event_Type
      ORDER BY
        [Month]
    ) AS Next_Is_Complete_Month
  FROM Event_Type_Month_Status

),

Event_Type_Month_Status_With_Leniency AS (

  SELECT
    *,
    CASE
      WHEN Is_Complete_Month = 1 THEN 1
      WHEN Is_Complete_Month = 0
        AND Prev_Is_Complete_Month = 1
        AND Next_Is_Complete_Month = 1
      THEN 1
      ELSE 0
    END AS Is_Allowed_In_Run
  FROM Event_Type_Month_Status_With_Lag

),

Complete_Event_Type_Months AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    [Month],
    DATEDIFF(MONTH, @Calendar_Start_Month, [Month])
      - ROW_NUMBER() OVER (
          PARTITION BY
            LA_Code,
            LA_Name,
            ImportDate,
            Event_Type
          ORDER BY
            [Month]
        ) AS Consecutive_Month_Group

  -- Use lenient month status so isolated one-month gaps can be included
  -- in derived covered periods.
  FROM Event_Type_Month_Status_With_Leniency
  WHERE Is_Allowed_In_Run = 1

),

Event_Type_Complete_Periods AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    MIN([Month]) AS Type_Start_Date,
    MAX([Month]) AS Type_End_Month,
    COUNT(*) AS n_Consecutive_Complete_Months
  FROM Complete_Event_Type_Months
  GROUP BY
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    Consecutive_Month_Group

),

Latest_Event_Type_Complete_Periods AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    Type_Start_Date,
    EOMONTH(Type_End_Month) AS Type_End_Date,
    n_Consecutive_Complete_Months
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          LA_Code,
          LA_Name,
          ImportDate,
          Event_Type
        ORDER BY
          Type_End_Month DESC,
          n_Consecutive_Complete_Months DESC,
          Type_Start_Date DESC
      ) AS rn
    FROM Event_Type_Complete_Periods
  ) x
  WHERE rn = 1

),

Der_Periods_by_Event_Type AS (

  SELECT
    t.LA_Code,
    t.LA_Name,
    t.ImportDate,
    t.Event_Type,
    t.n_Events_For_Event_Type,
    t.Median_Event_Type_Total_Events,
    MAX(m.Median_End_Dates_in_Month) AS Median_End_Dates_in_Month,
    p.Type_Start_Date,
    p.Type_End_Date,
    p.n_Consecutive_Complete_Months
  FROM Event_Type_Totals_With_Median t
  LEFT JOIN Monthly_Event_End_Date_Counts_With_Medians m
    ON t.LA_Code = m.LA_Code
   AND t.LA_Name = m.LA_Name
   AND t.ImportDate = m.ImportDate
   AND t.Event_Type = m.Event_Type
  LEFT JOIN Latest_Event_Type_Complete_Periods p
    ON t.LA_Code = p.LA_Code
   AND t.LA_Name = p.LA_Name
   AND t.ImportDate = p.ImportDate
   AND t.Event_Type = p.Event_Type
  GROUP BY
    t.LA_Code,
    t.LA_Name,
    t.ImportDate,
    t.Event_Type,
    t.n_Events_For_Event_Type,
    t.Median_Event_Type_Total_Events,
    p.Type_Start_Date,
    p.Type_End_Date,
    p.n_Consecutive_Complete_Months

),

Overall_Complete_Months AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    [Month],
    DATEDIFF(MONTH, @Calendar_Start_Month, [Month])
      - ROW_NUMBER() OVER (
          PARTITION BY
            LA_Code,
            LA_Name,
            ImportDate
          ORDER BY
            [Month]
        ) AS Consecutive_Month_Group
  FROM (
    SELECT
      LA_Code,
      LA_Name,
      ImportDate,
      [Month]
    FROM Event_Type_Month_Status_With_Leniency
    GROUP BY
      LA_Code,
      LA_Name,
      ImportDate,
      [Month]
    HAVING SUM(Is_Allowed_In_Run) = 3
  ) x

),

Overall_Complete_Periods AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    MIN([Month]) AS Der_Reporting_Period_Start_Date,
    MAX([Month]) AS Der_Reporting_Period_End_Month,
    COUNT(*) AS n_Consecutive_Complete_Months
  FROM Overall_Complete_Months
  GROUP BY
    LA_Code,
    LA_Name,
    ImportDate,
    Consecutive_Month_Group

),

Latest_Overall_Complete_Period AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Der_Reporting_Period_Start_Date,
    EOMONTH(Der_Reporting_Period_End_Month) AS Der_Reporting_Period_End_Date,
    n_Consecutive_Complete_Months
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          LA_Code,
          LA_Name,
          ImportDate
        ORDER BY
          Der_Reporting_Period_End_Month DESC,
          n_Consecutive_Complete_Months DESC,
          Der_Reporting_Period_Start_Date DESC
      ) AS rn
    FROM Overall_Complete_Periods
  ) x
  WHERE rn = 1

),

Der_Periods AS (

  SELECT
    p.LA_Code,
    p.LA_Name,
    p.ImportDate,

    SUM(CASE WHEN p.n_Events_For_Event_Type > 0 THEN 1 ELSE 0 END) AS n_Event_Types_With_Events,

    CASE
      WHEN SUM(CASE WHEN p.n_Events_For_Event_Type > 0 THEN 1 ELSE 0 END) = 3
        AND COUNT(p.Type_Start_Date) = 3
        AND COUNT(p.Type_End_Date) = 3
      THEN MAX(o.Der_Reporting_Period_Start_Date)
    END AS Der_Reporting_Period_Start_Date,

    CASE
      WHEN SUM(CASE WHEN p.n_Events_For_Event_Type > 0 THEN 1 ELSE 0 END) = 3
        AND COUNT(p.Type_Start_Date) = 3
        AND COUNT(p.Type_End_Date) = 3
      THEN MAX(o.Der_Reporting_Period_End_Date)
    END AS Der_Reporting_Period_End_Date

  FROM Der_Periods_by_Event_Type p
  LEFT JOIN Latest_Overall_Complete_Period o
    ON p.LA_Code = o.LA_Code
   AND p.LA_Name = o.LA_Name
   AND p.ImportDate = o.ImportDate
  GROUP BY
    p.LA_Code,
    p.LA_Name,
    p.ImportDate

),

Event_Type_Complete_Month_Range AS (

  SELECT
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type,
    MIN([Month]) AS First_Complete_Month,
    MAX([Month]) AS Last_Complete_Month
  FROM Event_Type_Month_Status
  WHERE Is_Complete_Month = 1
  GROUP BY
    LA_Code,
    LA_Name,
    ImportDate,
    Event_Type

),

Gap_Flags AS (

  SELECT DISTINCT
    s.LA_Code,
    s.LA_Name,
    s.ImportDate,
    'Y' AS Reporting_Period_Has_Gaps
  FROM Event_Type_Month_Status s
  INNER JOIN Event_Type_Complete_Month_Range r
    ON s.LA_Code = r.LA_Code
   AND s.LA_Name = r.LA_Name
   AND s.ImportDate = r.ImportDate
   AND s.Event_Type = r.Event_Type
   AND s.[Month] BETWEEN r.First_Complete_Month AND r.Last_Complete_Month
  WHERE s.Is_Complete_Month = 0

)

SELECT
  a.*,

  CAST(
    ROUND(
      DATEDIFF(DAY, a.Reporting_Period_Start_Date, a.Reporting_Period_End_Date) / 30.436875,
      0
    ) AS INT
  ) AS Reporting_Period_Length,

  d.Der_Reporting_Period_Start_Date,
  d.Der_Reporting_Period_End_Date,

  CAST(
    ROUND(
      DATEDIFF(DAY, d.Der_Reporting_Period_Start_Date, d.Der_Reporting_Period_End_Date) / 30.436875,
      0
    ) AS INT
  ) AS Der_Reporting_Period_Length,

  CASE
    WHEN b.n_Event_Types_With_Events < 3 THEN 'Y'
    ELSE 'N'
  END AS Missing_Required_Event_Type,

  ISNULL(g.Reporting_Period_Has_Gaps, 'N') AS Reporting_Period_Has_Gaps

INTO ASC_Sandbox.REF_Submission_Reporting_Periods_V2

FROM LA_Submissions a

LEFT JOIN Der_Periods b
  ON a.ImportDate = b.ImportDate
 AND a.LA_Code = b.LA_Code
 AND a.LA_Name = b.LA_Name

LEFT JOIN Gap_Flags g
  ON a.ImportDate = g.ImportDate
 AND a.LA_Code = g.LA_Code
 AND a.LA_Name = g.LA_Name

CROSS APPLY (

  SELECT
    CASE
      WHEN a.LA_Code = '906' -- method does not work for Isles of Scilly due to small event counts
      THEN a.Reporting_Period_Start_Date
      ELSE b.Der_Reporting_Period_Start_Date
    END AS Der_Reporting_Period_Start_Date,

    CASE
      WHEN a.LA_Code = '906'
      THEN a.Reporting_Period_End_Date
      ELSE b.Der_Reporting_Period_End_Date
    END AS Der_Reporting_Period_End_Date

) d;