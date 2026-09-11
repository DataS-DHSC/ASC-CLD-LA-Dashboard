# ASC CLD – Main table processing methodology

This documentation describes DHSC's method of processing [ASC CLD submissions](https://www.ardengemcsu.nhs.uk/asccld) to produce cleaned, deduplicated “main tables” for:

1. rolling 12-month reporting periods - [create_main_table_for_12mo_period.sql](/Main_tables/create_main_table_for_12mo_period.sql)
2. the full reporting period to date (i.e. > 12-month) - [create_main_table_for_period.sql](/Main_tables/create_main_table_for_period.sql)

The processing is carried out quarterly, extending the full reporting period by three months each quarter as new data is received.

## Contents

- [Overview](/Main_tables/docs/methodology/0-overview.md)
- [Selecting submissions](/Main_tables/docs/methodology/1-submission-selection.md)
- [Filtering events](/Main_tables/docs/methodology/2-event-filtering.md)
- [Data cleaning and derived fields](/Main_tables/docs/methodology/3-data-cleaning.md)
- [Deduplication](/Main_tables/docs/methodology/4-deduplication.md)

### Methodology change log

| Date | Details of change |
| --- | --- |
| 2026-Jul | Implement new, more rigorous method to derive reporting periods, to deal with submissions of varying length and set a higher bar for submission coverage/completeness and thus exclude submissions with significant gaps from processing & outputs. |
| 2026-Jul | Use cleaned rather than raw values in deduplication to ensure effective deduplication of records containing values updated from release 1 to release 2 |
| 2026-Jul | Bring in the ONS mortality dataset date of death for all records where an NHS number (traced or LA-provided) is available, and derive a new date of death field – ONS date of death where an NHS number present ELSE CLD date of death – to use instead of CLD date of death throughout the pipeline |
| 2026-Jul | Text string 'NULL' converted to NULL (previously mapped to 'Invalid and not mapped' or 'Unknown' for multiple fields) |
| 2026-May | Rows with no person ID excluded (NB code commentary stated this was being done to date but had erroneously not been implemented) |