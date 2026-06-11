## Main tables

This directory contains code to create deduplicated main tables that serve as the starting point for further processing/analysis. There are two versions, using:
* **single submissions** ([`create_main_table_for_12mo_period.sql`](/Main_tables/create_main_table_for_12mo_period.sql), and
* **joined submissions**, to enable analysis of data covering more than 12 month periods ([`create_main_table_for_period.sql`](/Main_tables/create_main_table_for_period.sql))

Processing steps are contained within dedicated stored procedures, available under [stored procedures](/Stored_procedures/).

To run these scripts locally and recreate DHSC processing, changes would be required to i) select data for only one local authority and skip steps where submissions are joined and ii) create variables that do not exist locally but exist in the DHSC database (or, alternatively, amend the code so that these variables are not used).

Documentation detailing the methodology and rationale for certain decisions can be found in [docs](/Main_tables/docs/).

### Methodology change log

| Date | Details of change |
| --- | --- |
| 2026-05-07 | Rows with no person ID excluded (NB code commentary stated this was being done to date but had erroneously not been implemented) |
