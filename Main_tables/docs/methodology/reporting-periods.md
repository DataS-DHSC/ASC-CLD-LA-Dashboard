# Deriving accurate reporting periods

> Processing applied in [`create_REF_Submission_Reporting_Periods_table`](/Main_tables/REF_tables/create_REF_Submission_Reporting_Periods_V2_table.sql).

Reporting periods are **derived from the data** - using the distribution of event end dates in the submitted records - not taken as stated in submissions.

The approach derives reporting periods from sustained evidence of coverage while allowing some leniency for different recording practices, small number volume fluctuations, and event date issues arising from case management system changes.

## Method

A submission's derived reporting period is based on request, assessment and service event end dates. Reviews are not included as they are not consistently submitted by all local authorities and therefore do not reliably indicate the period covered by the submission.

For each of the three required event types, the method calculates:

- the total number of events in the submission,
- the number of events ending in each month, and
- the median monthly count.

Events ending before 2020-04-01 and in or after the import month are excluded. Events with no end date are excluded from monthly counts. Service events with no end date are included in the event-type total count only. 

A month is treated as complete for an event type if 
- the monthly count is >= 10 and at least 20% of that event type's median monthly count; and
- the event type has sufficient total volume relative to the median event type total for the file
  <br> (5% of total - a low threshold to allow for large differences in total volumes of events, e.g. many more services than assessments, and small number fluctuations).

For each event type, the derived reporting period is then calculated as the latest run of consecutive complete months with allowed isolated one-month gap(s).

The final derived reporting period for the submission is calculated as the overlap across the reporting periods derived for each event type.

If any event types are completely missing from a submission or a period cannot be derived, or if the event type periods do not overlap, the derived reporting period is set to `NULL`. This ensures that the derived reporting period represents **the period that is complete across requests, assessments and services**.

A gap flag is produced to highlight submissions where the derived reporting period may be affected by gaps in coverage. A missing event type flag is produced to highlight submissions missing requests, assessments or services.

Percentage thresholds and permitted gaps set such that derived reporting periods most closely match verified reporting periods, deduced by scrutinising the underlying data. (Reporting periods were verified for > 50 submissions with questionable reporting periods, across > 30 local authorities.) Exceptions to this methodology apply to the smallest local authorities, due to small event counts.

<br>

[Back to Submission Selection](/Main_tables/docs/methodology/1-submission-selection.md)