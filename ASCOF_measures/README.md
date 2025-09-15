
# ASCOF Measures
This repository contains the code used to produce 5 measures in the Adult Social Care Outcomes Framework from Client Level Data for 24/25 onwards. These measures are:
* ASCOF 2A - The proportion of people who received short-term services during the year – who previously were not receiving services – where no further request was made for ongoing support (%) 
* ASCOF 2B - The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 18-64yrs (per 100,000 population) 
* ASCOF 2C  - The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 65+yrs (per 100,000 population)
* ASCOF 2D - The proportion of people aged 65 and over discharged from hospital into reablement and who remained in the community within 12 weeks of discharge (%)
* ASCOF 2E - The proportion of people who receive long-term support who live in their home or with family (%) 
* ASCOF 3D - The proportion of clients who use services who receive self-directed support (%)

  
The codes for creating the ASCOF measures are set up as stored procedures, except for ASCOF 2D which is currently a standalone script. The stored procedures allow figures to be generated for multiple statistical reporting years by altering the reporting period start and end dates and the starting main table. The Create_person_details_table.sql script creates a table of latest demographic information such as date of birth, gender and accommodation status for all clients. This script must be ran and the table produced before any of the other stored procedures are run. The main script (ASCOF_main_script.sql) calls each of ASCOF stored procedures for multiple reporting periods and joins them together for the purpose of the dashboard. 


The input tables are a set of main tables produced centrally by DHSC. The scripts to produce these are available in the same git repositry git here: [main tables]( https://github.com/DataS-DHSC/ASC-CLD-LA-Dashboard/tree/main/Main_tables) and they call these [stored procedures]( https://github.com/DataS-DHSC/ASC-CLD-LA-Dashboard/tree/main/Stored_procedures). ASCOF measures typically require more than 12 months of data (for new/existing clients, identifying sequels etc) therefore they all use the joined submisisons table.


Whilst the scripts are designed for the 24/25 ASCOF publication onwards, they have been applied to the data for reporting periods prior to April 2024, to ensure the latest methods are reflected and figures are comparable over time.


The full ASCOF methodolody document is available alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/). 
