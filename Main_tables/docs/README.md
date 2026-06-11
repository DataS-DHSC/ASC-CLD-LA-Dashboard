# ASC CLD – Main table processing methodology

This documentation describes DHSC's method of processing [ASC CLD submissions](https://www.ardengemcsu.nhs.uk/asccld) to produce cleaned, deduplicated “main tables” for:

1. rolling 12-month reporting periods - [create_main_table_for_12mo_period.sql](/Main_tables/create_main_table_for_12mo_period.sql)
2. the full reporting period to date (i.e. > 12-month) - [create_main_table_for_period.sql](/Main_tables/create_main_table_for_period.sql)

The processing is carried out quarterly, extending the full reporting period by three months each quarter as new data is received.

## Contents

- [Overview](/Main_tables/docs/methodology/1-overview.md)
- [Selecting submissions](/Main_tables/docs/methodology/3-submission-selection.md)
- [Filtering events](/Main_tables/docs/methodology/4-event-filtering.md)
- [Data cleaning and derived fields](/Main_tables/docs/methodology/5-data-cleaning.md)
- [Deduplication](/Main_tables/docs/methodology/6-deduplication.md)