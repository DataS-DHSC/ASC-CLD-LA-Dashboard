
# ASCOF Measures
This repository contains the code used to produce 5 measures in the Adult Social Care Outcomes Framework from Client Level Data for 24/25 onwards. These measures are:
* ASCOF 2A - The proportion of people who received short-term services during the year – who previously were not receiving services – where no further request was made for ongoing support (%) 
* ASCOF 2B - The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 18-64yrs (per 100,000 population) 
* ASCOF 2C  - The number of adults whose long-term support needs are met by admission to residential and nursing care homes, for 65+yrs (per 100,000 population) 
* ASCOF 2E - The proportion of people who receive long-term support who live in their home or with family (%) 
* ASCOF 3D - The proportion of clients who use services who receive self-directed support (%)

  
The codes for creating the ASCOF measures are set up as stored procedures. This allows figures to be generated for multiple statistical reporting years by altering the reporting period start and end dates and the starting main table. The Latest_Person_Details.sql script creates a table of latest age, gender and accommodation status for all clients. This script must be ran before the 2E stored procedure. The main script (ASCOF_main_script.sql) calls each of ASCOF stored procedures for multiple reporting periods and joins them together for the purpose of the dashboard. 


The input tables are a set of main tables produced centrally by DHSC. The scripts to produce these are available in the same git repositry git here: [main tables]( https://github.com/DataS-DHSC/ASC-CLD-LA-Dashboard/tree/main/Main_tables) and they call these [stored procedures]( https://github.com/DataS-DHSC/ASC-CLD-LA-Dashboard/tree/main/Stored_procedures). Some ASCOF measures require 12 months of data and therefore use the single submissions table, whereas others which require a period longer than 12 months and use the joined submisisons table.


For each measure there are two scripts, Create_ASCOF and Create_ASCOF_2425_Onwards. This is because for 2024/25 onwards, where local authorities have adopted the Release 2 specification early, the data is currently mapped back to Release 1 where possible in the latest main tables. These tables have slightly different derived fields where this additional processing has taken place and therefore the ASCOF codes have been adapted to account for this. . Consequently the ASCOF scripts have been adapted to account for this change, and are identified as [ASCOF_Measure]_2425_onwards. The actual methodology is the same for both scripts.


The full ASCOF methodolody document is available alongside the dashboard on Athena and on [Agem’s website]( https://www.ardengemcsu.nhs.uk/adult-social-care-client-level-data/). Whilst the methods are near final, certain aspects are under review with the potential for future development, these are highlighted in the methodology document. 
