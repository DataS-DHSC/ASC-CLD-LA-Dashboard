
# ASC-CLD-LA-Dashboard
This repository contains the code used to produce the data and metrics underpinning the DHSC Client Level Data Dashboard which is available for Local Authorities.

## How to get started
Local Authorities submit Client Level Data to NHS England each quarter. NHS England pseudonymise the data to remove identifying personal information, and provide DHSC access to the pseudonymised national dataset, including submission meta data and a number of additional, derived fields, via a secure database. The scripts in this repository analyse this national dataset and therefore they are not directly transferable to local CLD.

To run these scripts locally and recreate DHSC processing, changes would be required to i) select data for only one local authority and skip steps where submissions are joined and ii) create variables that do not exist locally but exist in the DHSC database (or, alternatively, amend the code so that these variables are not used).

The code in the main tables directory must be run first, as they produce the input tables required for the rest of the dashboard codes. There are two types of tables which could be produced (single submission table or joined submission table). Which table is required for each dashboard page is outlined in the relevant sections below.

## Main tables
This directory contains code to create deduplicated main tables that serve as the starting point for producing the dashboard tables and ASCOF measures. These should be run first and input parameters are set within these. There are two versions, using:
* **single submissions** (create_main_table_for_12mo_period.sql), and
* **joined submissions**, to enable analysis of data covering more than 12 month periods (create_main_table_for_period.sql)

The code is designed to create new main tables on a quarterly basis and name these according to the start and end date of the reporting period (which for joined submissions may increase in length by 3 months each quarter as more data is received). Processing steps are contained within dedicated stored procedures, and called when running the scripts above. These stored procedures are also available in this directory. 
The main processing steps are:
* selecting submissions covering the period,
* filtering the data to events in the period,
* creating cleaned and derived fields, and
* deduplicating records.

## Dashboard tables
All event specific pages (requests, assessments, services, reviews, data quality) on the dashboard use the single submissions main table covering the latest 12 month reporting period. The codes used to produce the data underpinning the main dashboard pages are numbered and must be ran sequentially. More information on the processing of the data for the dashboard is available in the dashboard methodology document alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/).


## ASCOF measures
ASCOF measures typically require a period longer than 12 months and therefore they all use the joined submisisons table.

The codes for creating the ASCOF measures are set up as stored procedures. This allows figures to be generated for multiple statistical reporting years by altering the reporting period start and end dates and the starting main table. The Latest_Person_Details.sql script creates a table of latest age, gender and accommodation status for all clients and is also currently set up as a stored procedure. The main script (ASCOF_main_script.sql) is where all the input parameters are set and it calls each of ASCOF stored procedures for multiple reporting periods and joins them together for the purpose of the dashboard. 

The full ASCOF methodolody document is available alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/).


## Waiting times
This is a single script which outputs the two waiting times metrics which are on the dashboard. The input table is the latest joined submission table. The methodology document is available alongside the dashboard on [AGEM's website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/). Please note this method is provisional and subject to change following local authority feedback. 

## Quality assurance
All codes in this repository have been quality assured following DHSC's standard QA processes.  

## Code of Conduct
Please note that this project is released with a [Contributor Code of Conduct](https://crispy-adventure-qzrklvk.pages.github.io/code-of-conduct.html). However, this git repository does not allow contributors and is solely for the purpose of sharing the code used to produce the CLD dashboard with Local Authority analysts.


## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation. The documentation is © Crown copyright and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).



For any questions about this repository please contact socialcaredata@dhsc.gov.uk
