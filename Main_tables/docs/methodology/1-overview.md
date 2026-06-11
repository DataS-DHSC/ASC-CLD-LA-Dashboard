# Overview of methodology

## Purpose

- To create cleaned and deduplicated ASC CLD data tables to form the starting point of analysis.
- To support analysis requiring > 12-month time‑series by joining multiple submissions.

## High‑level approach

- Two main tables are created:
   - Rolling 12-month "**single submissions**" table
   - Full reporting period to date "**joined submissions**" table
- Data is processed quarterly, extending the reporting period to date by three months each quarter.
- Submissions are selected "as of" a fixed date for reproducibility.
- Reporting periods are derived from the data, not taken as stated within submissions.
- The most recently submitted data is assumed to be the most accurate.

## Main processing steps

1. [**Select submissions covering the period**](/Main_tables/docs/methodology/3-submission-selection.md)  
   Latest submissions containing data covering the period, selected “as of” a fixed date.

2. [**Filter the data to events in the period**](/Main_tables/docs/methodology/4-event-filtering.md)  
   Requests, assessments and reviews must end within the period; services may be ongoing.

3. [**Create cleaned and derived fields**](/Main_tables/docs/methodology/5-data-cleaning.md)  
   Invalid values are mapped where possible; release 1 data is mapped to release 2; higher-level groupings and corrected end dates are derived.

4. [**Deduplicate records**](/Main_tables/docs/methodology/6-deduplication.md) (identify unique events)
   <br>One original record is retained per (DHSC‑defined) “unique event”.


## Guiding principles

- **Keep processing steps simple**  
  To maximise transparency and ease of reproduction, only introducing more complex processing where the impact warrants it.

- **Retain as much information as possible**  
  For example by cleaning (mapping) invalid values and preferring to retain duplicate records rather than risk losing events, with implications caveated in analysis.


## Use of "joined" vs "single" submissions tables

Joined submissions, i.e. main tables for full reporting period to date, are used when:

- Analysis requires a time-series longer than 12 months.
- Some loss of service‑level information is acceptable - packages of care submitted across multiple rows may lose detail and cost data fields are explicitly dropped.

Otherwise, latest rolling 12-month reporting period tables - based on single (12-month) submissions - are used.

<br>

[Back to Contents](/Main_tables/docs/README.md#Contents)
