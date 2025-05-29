# ASC-CLD-LA-Dashboard
This repository contains the code used to produce the data and metrics underpinning the DHSC Client Level Data Dashboard which is available for Local Authorities.

Local Authorities submit Client Level Data to NHS England each quarter. NHS England pseudonymise the data to remove identifying personal information, and provide DHSC access to the pseudonymised national dataset, including submission meta data and a number of additional, derived fields, via a secure database. The scripts in this repository analyse this national dataset and therefore they are not directly transferable to local CLD.

Note, this git repository does not allow contributors and is solely for the purpose of sharing the code used to produce the CLD dashboard with Local Authority analysts.

## Main tables
This directory contains code to create deduplicated main tables that serve as the starting point for producing the dashboard tables and ASCOF measures. There are two versions, using:
* **single submissions** (create_main_table_for_12mo_period.sql), and
* **joined submissions**, to enable analysis of data covering more than 12 month periods (create_main_table_for_period.sql)

The code is designed to create new main tables on a quarterly basis and name these according to the start and end date of the reporting period (which for joined submissions may increase in length by 3 months each quarter as more data is received). Processing steps are contained within dedicated stored procedures, also available in this directory. 
The main processing steps are:
* selecting submissions covering the period,
* filtering the data to events in the period,
* creating cleaned and derived fields, and
* deduplicating records.

**Release 2 specification**

These scripts process the data as per the Release 1 CLD specification. For 2024/25 onwards, where local authorities have adopted the Release 2 specification early, the data is currently mapped back to Release 1 where possible. This interim approach is necessary until all methods and scripts are fully updated to support the Release 2 specification. No additional data rows are created, instead individual fields are amended as needed.

## Dashboard tables
All pages on the dashboard except for ASCOF use the single submissions table covering the latest 12 month reporting period. The codes used to produce the data underpinning the main dashboard pages are numbered and must be ran sequentially. More information on the main table methodology and processing of the data for the dashboard is available in the dashboard methodology document alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/). 


## ASCOF measures
Some ASCOF measures require 12 months of data and therefore use the single submissions table, whereas others which require a period longer than 12 months and use the joined submisisons table.

The codes for creating the ASCOF measures are set up as stored procedures. This allows figures to be generated for multiple statistical reporting years by altering the reporting period start and end dates and the starting main table. The Latest_Person_Details.sql script creates a table of latest age, gender and accommodation status for all clients. This script must be ran before the 2E stored procedure. The main script (ASCOF_main_script.sql) calls each of ASCOF stored procedures for multiple reporting periods and joins them together for the purpose of the dashboard. 

The full ASCOF methodolody document is available alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/). Whilst the methods are near final, certain aspects are under review with the potential for future development, these are highlighted in the methodology document. 



## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation. The documentation is © Crown copyright and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

