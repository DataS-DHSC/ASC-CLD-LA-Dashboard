------------------------------------------------------------------------------------------------------------------------------------------------
/*ASCOF 3D: Proportion of people using social care who receive self-directed support, and those receiving direct payments (formerly metric 1C)*/
------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------
/* 
This ASCOF measure is 4 parts, 2 x Client scores and 2 x Carers scores


**CLIENT BASED PARTS**
Denominator (3D Part 1a and 2a) = All Community-based Clients
Numerator (3D Part 1a) = Clients receiving self-directed support
Numerator (3D Part 2a) = Clients receiving Direct Payments only
Outcome = Numerator / Denominator * 100


**CARER BASED PARTS**
Denominator (3D Part 1b and 2b) = All Community-based Carers
Numerator (3D Part 1b) = Carers receiving self-directed support
Numerator (3D Part 2b) = Carers receiving Direct Payments only
Outcome = Numerator / Denominator * 100

-----------------------------------------------------------------------------

------------------------------------------------------------------------

The code is written to run through and create the Client-based measures 3D1a and 3D2a first and then the Carers measures
3D1b and 3D2b afterwards, before appending them all into one final ASCOF 3D Asset.

Dates are a snapshot for Clients, and in-year for Carers, as per ASCOF Handbook

**CLIENT BASED PARTS**
Code to produce the Client based measures is taken directly from LTS001b code process, with some adaptations

A full deduplication process must run through even though ASCOF 3D is only concerned with Community-based Clients.
This is to allow for de-duplication to occur based on setting, where all Clients present in multiple settings as at the 
snapshot date are placed in the highest setting i.e. Residential setting out-ranks Community, Nursing out-ranks Residential etc

Technically at a snapshot date, a Client should not be recorded against multiple conflicting settings, but this does
occur in the data submitted.

**CARER BASED PARTS**
Code to produce the Carer based measures is taken directly from the high-level LTS003 replication script. It is a slice
of this dataset, using only the Service Type of 'Carer Support: Direct to Carer'

A full deduplication process must run through even though ASCOF 3D is only concerned with Direct Payment / self-directed Carers.
This is to allow for de-duplication to occur based on support provided, where Carers receiving multiple conflicting support types 
as at the snapshot date are placed in the highest category as per the setting hierarchy i.e. Direct Payment out-ranks CASSR Managed Personal Budget


**DEALING WITH UNKNOWNS AND INVALIDS 

Clients: 
-- unknowns, blanks and invalids are anything where the delivery mechanism is null or invalid and the service component is not direct payment.
-- these are overwritten to '' at line ~340 and given a high number on the hierarchy (lowest down) so known valid values are selected over this
-- unknowns and invalids are kept in denominators

Carers:
-- There are two conditions where delivery mechanism becomes unknown:
--WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' and (Delivery_Mechanism not in  ('Direct Payment', 'CASSR Managed Personal Budget', 'CASSR Commissioned support') and Service_Component not like 'Direct Payment') THEN 'Support Direct to Carer: Unknown Delivery Mech'
--WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' and (Delivery_Mechanism is NULL and Service_Component not like 'Direct Payment') THEN 'Support Direct to Carer: Unknown Delivery Mech'
-- The denominator filters on ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only') 
-- therefore unknowns are excluded from the denominator and numerator

*/

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF3D

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(100),
  @OutputTable1 AS NVARCHAR(100),
  @OutputTable2 AS NVARCHAR(100)

AS         
          --SET NOCOUNT ON;
      DECLARE @Query NVARCHAR(MAX)
      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
      SET @Query = N'DROP TABLE IF EXISTS ' + @OutputTable1 + '; 
                    DROP TABLE IF EXISTS ' + @OutputTable2 + '; 
                    CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + ';'
      EXEC(@Query)

      ----------------------------------------------------------------------------------------------------------------
      /*CREATE REFERENCE TABLE #REF_SERVICE_TYPE_DELIVERY_MECH TO CATEGORISE AND RANK ITEMS FOR DISTINCT INDIVIDUALS*/
      ----------------------------------------------------------------------------------------------------------------

      DROP TABLE IF EXISTS #REF_Service_Type_Delivery_Mech	
      CREATE TABLE #REF_Service_Type_Delivery_Mech
        (Service_Type VARCHAR(200),
        Delivery_Mechanism VARCHAR(200),
        SALT_Category VARCHAR(200),
        SALT_Broad_Category VARCHAR(200),
        Sort_Order INT,
        Hierarchy INT);

      INSERT INTO #REF_Service_Type_Delivery_Mech
        (Service_Type,
        Delivery_Mechanism,
        SALT_Category,
        SALT_Broad_Category,
        Sort_Order,
        Hierarchy)
      VALUES
        ('Long Term Support: Nursing Care', '', 'Nursing', 'Nursing', 1, 1),
        ('Long Term Support: Residential Care', '', 'Residential', 'Residential', 2, 2),
        ('Long Term Support: Community', 'Direct Payment', 'Community: Direct Payment', 'Community', 3, 3),
        ('Long Term Support: Community', 'CASSR Managed Personal Budget', 'Community: CASSR Managed Personal Budget', 'Community', 4, 4),
        ('Long Term Support: Community', 'CASSR Commissioned Support', 'Community: CASSR Commissioned Support', 'Community', 5, 5),
        ('Long Term Support: Community', '', 'Community', 'Community', 6, 6),
        ('Long Term Support: Prison', 'CASSR Managed Personal Budget', 'Prison: CASSR Managed Personal Budget', 'Prison', 7, 7),
        ('Long Term Support: Prison', 'CASSR Commissioned Support', 'Prison: CASSR Commissioned Support', 'Prison', 8, 8),
        ('Long Term Support: Prison', '', 'Prison', 'Prison', 9, 9);

      ---------------------------------------------------------------------------------------------------
      /*CREATE REFERENCE TABLE #REF_DELIVERY_MECH TO CATEGORISE AND RANK ITEMS FOR CARERS*/
      ---------------------------------------------------------------------------------------------------
      --This Reference table will be used to de-duplicate Carers later in the process when they appear more than once in the period of interest
      --with conflicting support types. Each Carer can only be counted once as per SALT LTS003 so a hierarchy is applied.
      --NOTE: Check periodically to ensure the Defined List in CLD spec has not changed!

      DROP TABLE IF EXISTS #REF_CARER_SUPPORT_HIERARCHY
      CREATE TABLE #REF_CARER_SUPPORT_HIERARCHY
        (Support_Provided VARCHAR(200),
        Sort_Order INT,
        Hierarchy INT);

      INSERT INTO #REF_CARER_SUPPORT_HIERARCHY
        (Support_Provided,
        Sort_Order,
        Hierarchy)
      VALUES
        ('Direct Payment only', 1, 1),
        ('CASSR Managed Personal Budget', 2, 2),
        ('CASSR Commissioned Support only', 3, 3),
        ('Support Direct to Carer: Unknown Delivery Mech', 4, 4),
        ('Information, Advice and Other Universal Services / Signposting', 5, 5),
        ('No Direct Support Provided to Carer', 6, 6);




      --####################################################################
      -- CLIENTS SCRIPT:
      --####################################################################
        
        
      -------------------------------------------------------------------------------
      /*FILTER DATA APPROPRIATELY FOR ASCOF 3D INDIVIDUALS AND SELECT SNAPSHOT DATE*/
      -------------------------------------------------------------------------------

      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Build
      SELECT
        *
      INTO #ASCOF_3D_Clients_Build
      FROM ASC_Sandbox.InputTable
      WHERE 
        Service_Type_Cleaned IN ('Long Term Support: Nursing Care', 'Long Term Support: Residential Care', 'Long Term Support: Community', 'Long Term Support: Prison')
        AND Client_Type = 'Service User'
        AND Event_Start_Date <= @ReportingPeriodEndDate
        AND (Der_Event_End_Date >= @ReportingPeriodEndDate OR Der_Event_End_Date IS NULL)
        AND (Date_of_Death >= @ReportingPeriodEndDate OR Date_of_Death IS NULL) 
        AND (Der_Birth_Month IS NOT NULL AND Der_Birth_Year IS NOT NULL)
        AND Der_Working_Age_Band NOT IN ('Under 18', 'Unknown');


      -----------------------------------------------------------------------------------------
      /*OVER-WRITE ANY INVALID OR UNEXPECTED ENTRIES IN THE DELIVERY MECHANISM COLUMN TO ''  */
      -----------------------------------------------------------------------------------------
      --Delivery Mechanism is currently NOT a mandatory field and so we can not remove records based on inaccuracy in this field
      --but leaving invalid/unexpected entries in the field will cause problems with the Reference data join. 
      --Solution to this is to blank out ('') any entries that are invalid or where a Delivery Mechanism is not expected (Nursing and Res events)

      UPDATE a
      SET a.Delivery_Mechanism = 
      (CASE
        WHEN b.Delivery_Mechanism IS NOT NULL AND a.Service_Type_Cleaned in ('Long Term Support: Community', 'Long Term Support: Prison')
          THEN a.Delivery_Mechanism 
	        ELSE '' 
	        END)
      FROM #ASCOF_3D_Clients_Build a
      LEFT JOIN #REF_Service_Type_Delivery_Mech b
        ON TRIM (a.Service_Type_Cleaned) = TRIM (b.Service_Type)
        AND TRIM (a.Delivery_Mechanism) = TRIM (b.[Delivery_Mechanism]);


      ------------------------------------------------------
      /*CLIENTS PROCESSING, FILTERING AND DE-DUPLICATION*/
      ------------------------------------------------------

      /* 
      The Client-based ASCOF 3D measures are processed as per SALT
      In SALT each Client can only be counted once in each LTS table so de-duplication routines are needed to achieve this, where a Client
      is present in the period of interest with multiple settings. Code below joins to the Hierarchy/Rank reference data and chooses the record 
      with the lowest 'Rank' in Service_Type/Delivery_Mech Hierarchy as per the REF data 

      This needs doing to avoid a scenario where, upon just filtering for 'Direct Payment' clients from the raw data, a Client is included in the 
      Direct Payment cohort despite being also present in the data with a higher severity/acuity of setting e.g. Nursing or Residential
      */

      -----------------------------------------------------
      /*CREATE INITIAL TABLE JOIN TO REF DATA FOR CLIENTS*/
      -----------------------------------------------------
      --NOTE: CASE statement is needed here to capture all Community Service events where Service_Component is 'Direct Payment' as this is not covered
      --by the SALT #REF table

      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Join
      SELECT
        a.*,
        b.[Sort_Order],
        CASE WHEN a.Service_Type_Cleaned = 'Long Term Support: Community' AND Service_Component = 'Direct Payment'
          THEN '3'
          ELSE b.[Hierarchy]
          END AS [Hierarchy]
      INTO #ASCOF_3D_Clients_Join
      FROM #ASCOF_3D_Clients_Build a
      LEFT JOIN #REF_Service_Type_Delivery_Mech b
        ON TRIM (a.[Service_type_Cleaned]) = TRIM (b.Service_Type)
        AND TRIM (a.Delivery_Mechanism) = TRIM (b.[Delivery_Mechanism])

 
      ----------------------------
      /*AGED 18-64 INITIAL BUILD*/
      ----------------------------
      /*Create raw 18-64 table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_1864
      SELECT
        *
      INTO #ASCOF_3D_Clients_1864
      FROM #ASCOF_3D_Clients_Join a
      WHERE Der_Working_Age_Band = '18 to 64'; -- Using the Derived Age field created from Month and Year of Birth

      /*Choose the record with the lowest 'Rank' in Service_Type_Cleaned/Delivery_Mech Hierarchy as per the REF data*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_1864_MinRank
      SELECT
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID,
        MIN(Hierarchy) AS [RANK]
      INTO #ASCOF_3D_Clients_1864_MinRank
      FROM #ASCOF_3D_Clients_1864
      GROUP BY
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID;


      /*Select these records into the 'Build' table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_1864_Build
      SELECT
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism]
      INTO #ASCOF_3D_Clients_1864_Build
      FROM #ASCOF_3D_Clients_1864 a
      FULL JOIN #ASCOF_3D_Clients_1864_MinRank b
        ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.[Hierarchy] = b.[RANK]
      WHERE
        b.[RANK] is not NULL
      GROUP BY
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism];



      ----------------------------
      /*CREATE 18-64 DENOMINATOR*/
      ----------------------------
      --Filter for Community based only
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_1864_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [1864_Denom]
      INTO #ASCOF_3D_Clients_1864_DENOM
      FROM #ASCOF_3D_Clients_1864_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
      GROUP BY
        LA_Code,
        LA_Name;


      ---------------------------
      /*CREATE 18-64 NUMERATORS*/
      ---------------------------

      --3D Part 1a: Self-Directed Support
      --Choose all Community clients receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D1a_1864_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1a_1864_Num]
      INTO #ASCOF_3D1a_1864_NUM
      FROM #ASCOF_3D_Clients_1864_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND (   ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment') 
              OR ([Delivery_Mechanism] = 'CASSR Managed Personal Budget'))
      GROUP BY
        LA_Code,
        LA_Name;



      --3D Part 2a: Direct Payment
      --Choose all Community clients receiving a Direct Payment
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D2a_1864_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2a_1864_Num]
      INTO #ASCOF_3D2a_1864_NUM
      FROM #ASCOF_3D_Clients_1864_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment')
      GROUP BY
        LA_Code,
        LA_Name;





      --------------------------
      /*AGED 65+ INITIAL BUILD*/
      --------------------------
      /*Create raw 65+ table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_65Over
      SELECT
        *
      INTO #ASCOF_3D_Clients_65Over
      FROM #ASCOF_3D_Clients_Join
      WHERE
        Der_Working_Age_Band = '65 and above';


      /*Choose the record with the lowest 'Rank' in Service_Type_Cleaned/Delivery_Mech Hierarchy as per the REF data*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_65Over_MinRank
      SELECT
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID,
        MIN(Hierarchy) AS [RANK]
      INTO #ASCOF_3D_Clients_65Over_MinRank
      FROM #ASCOF_3D_Clients_65Over
      GROUP BY
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID;


      /*Select these records into the 'Build' table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_65Over_Build
      SELECT
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism]
      INTO #ASCOF_3D_Clients_65Over_Build
      FROM #ASCOF_3D_Clients_65Over a
      FULL JOIN #ASCOF_3D_Clients_65Over_MinRank b
        ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.[Hierarchy] = b.[RANK]
      WHERE
        b.[RANK] IS NOT NULL
      GROUP BY
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism];





      ----------------------------
      /*CREATE 65+ DENOMINATOR*/
      ----------------------------
      --Filter for Community based only 
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_65Over_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [65Over_Denom]
      INTO #ASCOF_3D_Clients_65Over_DENOM
      FROM #ASCOF_3D_Clients_65Over_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
      GROUP BY
        LA_Code,
        LA_Name;


      -------------------------
      /*CREATE 65+ NUMERATORS*/
      -------------------------

      --3D Part 1a: Self-Directed Support
      --Choose all Community clients receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D1a_65Over_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1a_65Over_Num]
      INTO #ASCOF_3D1a_65Over_NUM
      FROM #ASCOF_3D_Clients_65Over_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND (([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment') OR ([Delivery_Mechanism] = 'CASSR Managed Personal Budget'))
      GROUP BY
        LA_Code,
        LA_Name;



      --3D Part 2a: Direct Payment
      --Choose all Community clients receiving a Direct Payment
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D2a_65Over_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2a_65Over_Num]
      INTO #ASCOF_3D2a_65Over_NUM
      FROM #ASCOF_3D_Clients_65Over_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment')
      GROUP BY
        LA_Code,
        LA_Name;





      --------------------------------
      /*TOTAL ALL AGES INITIAL BUILD*/
      --------------------------------
      /*Create raw All Ages table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total
      SELECT
        *
      INTO #ASCOF_3D_Clients_Total
      FROM #ASCOF_3D_Clients_Join;


      /*Choose the record with the lowest 'Rank' in Service_Type_Cleaned/Delivery_Mech Hierarchy as per the REF data*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total_MinRank
      SELECT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      MIN(Hierarchy) AS [RANK]
      INTO #ASCOF_3D_Clients_Total_MinRank
      FROM #ASCOF_3D_Clients_Total
      GROUP BY
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID;


      /*Select these records into the 'Build' table*/
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total_Build
      SELECT
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism],
        a.Hierarchy,
        b.[rank]
      INTO #ASCOF_3D_Clients_Total_Build
      FROM #ASCOF_3D_Clients_Total a
      FULL JOIN #ASCOF_3D_Clients_Total_MinRank b
        ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.[Hierarchy] = b.[RANK]
      WHERE
        b.[RANK] IS NOT NULL
      GROUP BY
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Service_Type_Cleaned,
        a.Service_Component,
        a.[Delivery_Mechanism],
        a.Hierarchy,
        b.[rank];


      ----------------------------
      /*CREATE Total DENOMINATOR*/
      ----------------------------
      --Filter for Community based only, now each Client with multiple settings as at the snapshot date has been de-duplicated 
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_Denom]
      INTO #ASCOF_3D_Clients_Total_DENOM
      FROM #ASCOF_3D_Clients_Total_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
      GROUP BY
        LA_Code,
        LA_Name;



      -------------------------
      /*CREATE Total NUMERATORS*/
      -------------------------

      --3D Part 1a: Self-Directed Support
      --Choose all Community clients receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D1a_Total_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1a_Total_Num]
      INTO #ASCOF_3D1a_Total_NUM
      FROM #ASCOF_3D_Clients_Total_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND (([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment') OR ([Delivery_Mechanism] = 'CASSR Managed Personal Budget'))
      GROUP BY
        LA_Code,
        LA_Name;



      --3D Part 2a: Direct Payment
      --Choose all Community clients receiving a Direct Payment
      --Delivery Mechanism and Service Component are used here to ensure full coverage (Delivery Mechanism is not a mandatory field)
      DROP TABLE IF EXISTS #ASCOF_3D2a_Total_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2a_Total_Num]
      INTO #ASCOF_3D2a_Total_NUM
      FROM #ASCOF_3D_Clients_Total_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment')
      GROUP BY
        LA_Code,
        LA_Name;


      -------------------------------------------
      /*CREATE CLIENT Total Unknowns/invalids*/
      ------------------------------------------

      --This is based on null, unknown or invalid delivery mechanism, Service_Type_Cleaned is community and service component is not direct payment
      DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total_UN_IV
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_UN_IV]
      INTO #ASCOF_3D_Clients_Total_UN_IV
      FROM #ASCOF_3D_Clients_Total_Build
      WHERE
        Service_Type_Cleaned = 'Long Term Support: Community'
        AND (Delivery_Mechanism = '' AND (Service_Component <> 'Direct Payment' OR Service_Component IS NULL)) --unknowns/invalids occur when delivery mech is null or service comp is not direct payment
      GROUP BY
        LA_Code,
        LA_Name;


      --------------------------------------------------------------------------------------------------------------------------
      /*CARERS MEASURES*/
      --------------------------------------------------------------------------------------------------------------------------

      DROP TABLE IF EXISTS #ASCOF_3D_Carers
      SELECT
        * 
      INTO #ASCOF_3D_Carers                                                                                                                                                 
      FROM ASC_Sandbox.InputTable
      WHERE Client_Type in ('Carer', 'Carer known by association')                                                                                                                           
        AND Event_Start_Date <= @ReportingPeriodEndDate
        AND (Der_Event_End_Date >= @ReportingPeriodStartDate or Der_Event_End_Date is NULL)
        AND (Date_of_Death >= @ReportingPeriodStartDate OR Date_of_Death is NULL)
        AND Event_Type IN ('Request', 'Assessment', 'Service', 'Review')
        AND
      --three bespoke combinations of Event scenarios below are allowed to make up the Carers cohort as per LTS003. Only valid Service Types and Event Types will come 
      --through although Invalid Event Outcomes will still be permitted in cases where they aren't used to filter the data
        (
        (Service_Type_Cleaned is NULL AND Event_Outcome_Raw = 'NFA - Information & Advice / Signposting only')
        OR (Service_Type_Cleaned = 'Carer Support: Direct to Carer' OR Service_Type_Cleaned = 'Carer Support: Support involving the person cared-for')
        OR ((Event_Type = 'Assessment' OR Event_Type = 'Review') AND Service_Type_Cleaned IS NULL)
        )
        AND Der_Working_Age_Band NOT IN ('Under 18', 'Unknown');

      --------------------------------------------------------------------------------------------------------------

      --------------------------------------------
      /*CASE CARER OUTCOMES INTO SALT CATEGORIES*/
      --------------------------------------------
      /*The specific scenarios detailed in the initial #ASCOF_3D_CARERS table build are now CASED into the SALT LTS003 buckets
      so they can be de-duplicated and the Carers feeding into the ASCOF Numerator and Denominators reflect LTS003 methodology
      The CASE statement here differs slightly from the corresponding section of the LTS003 code, as Service Component = 'Direct Payment' 
      is also captured here in order to try and capture as many Direct Payment instances as possible and avoid under-counting the Numerators 
      and Denominator for 3D. Same rationale is applied to the Client-based parts of the measure (3D1a and 3D2a)

      NOTE: Ensure periodically that the wording of the CLD Specification Defined Lists has not changed, in order that the CASE continues
      to work as expected

      Process below is written as a CASE in lieu of a #REF table not being possible to implement - too many different fields are used to inform the
      building of the Support Provided column, with many permutations possible, which would require a v large REF table. Once given a 'Support Provided' 
      entry based on the CASE below, these are then mapped to a small REF table to attribute a 'Rank' to each type of Support Provided
      */
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_Case
      SELECT
        *,
      CASE 
        WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' AND (Delivery_Mechanism = 'Direct Payment' or Service_Component = 'Direct Payment') THEN 'Direct Payment only'
        WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' AND (Delivery_Mechanism = 'CASSR Managed Personal Budget' AND Service_Component NOT LIKE 'Direct Payment') THEN 'CASSR Managed Personal Budget'
        WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' AND (Delivery_Mechanism = 'CASSR Commissioned support' AND Service_Component NOT LIKE 'Direct Payment') THEN 'CASSR Commissioned Support only'
        WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' AND (Delivery_Mechanism NOT IN  ('Direct Payment', 'CASSR Managed Personal Budget', 'CASSR Commissioned support') AND Service_Component NOT LIKE 'Direct Payment') THEN 'Support Direct to Carer: Unknown Delivery Mech'
        WHEN Service_Type_Cleaned = 'Carer Support: Direct to Carer' AND (Delivery_Mechanism is NULL AND Service_Component NOT LIKE 'Direct Payment') THEN 'Support Direct to Carer: Unknown Delivery Mech'
        WHEN Service_Type_Cleaned = 'Carer Support: Support involving the person cared-for' THEN 'No Direct Support Provided to Carer'
        WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome_Raw NOT LIKE 'NFA - Information & Advice / Signposting only' THEN 'No Direct Support Provided to Carer'
        WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome_Raw IS NULL THEN 'No Direct Support Provided to Carer'
        WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome_Raw = 'NFA - Information & Advice / Signposting only' THEN 'Information, Advice and Other Universal Services / Signposting'
        WHEN Event_Type = 'Request' THEN 'Information, Advice and Other Universal Services / Signposting'
        END AS 'Support_Provided'
      INTO #ASCOF_3D_Carers_Case
      FROM #ASCOF_3D_Carers;


      --------------------
      /*JOIN TO REF DATA*/
      --------------------
      /*Join the Carers data to the Hierarchy reference data, in order to de-duplicate Carers appearing more than once with conflicting support types*/
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_JOIN
      SELECT
        a.*,
        b.Hierarchy
      INTO #ASCOF_3D_Carers_JOIN
      FROM #ASCOF_3D_Carers_Case a
      FULL JOIN #REF_CARER_SUPPORT_HIERARCHY b
        ON a.Support_Provided = b.Support_Provided;


      ----------------
      /*DE-DUPLICATE*/
      ----------------
      /*Pull out lowest Hierarchy / 'RANK' in instances where a Carer appears more than once within the period of interest and pull through the
      'Support_Provided' field containing the SALT support categories.
      Age information will need to be pulled through here but user needs to be aware that this may introduce duplicates (i.e. same Carer with different ages) 
      due to DQ issues. These will need to be pin-pointed and de-duplicated somehow to maintain unique headcounts*/

      DROP TABLE IF EXISTS #ASCOF_3D_Carers_MinRank
      SELECT DISTINCT
        a.LA_Code,
        a.LA_Name,
        a.Der_NHS_LA_Combined_Person_ID,
        a.Support_Provided,
        a.Der_Working_Age_Band
      INTO #ASCOF_3D_Carers_MinRank
      FROM #ASCOF_3D_Carers_JOIN a
      INNER JOIN
        (
        SELECT
          LA_Code,
          LA_Name,
          Der_NHS_LA_Combined_Person_ID,
          MIN(Hierarchy) AS [RANK]
        FROM #ASCOF_3D_Carers_JOIN
        GROUP BY
          LA_Code,
          LA_Name,
          Der_NHS_LA_Combined_Person_ID
        ) b
      ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
        AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.Hierarchy = b.[RANK];


      ----------------------------
      /*AGED 18-64 INITIAL BUILD*/
      ----------------------------
      /*Create raw 18-64 table and add relevant fields from Reference data*/
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_1864
      SELECT a.*
      INTO #ASCOF_3D_Carers_1864
      FROM #ASCOF_3D_Carers_MinRank a
      WHERE Der_Working_Age_Band = '18 to 64'


      ----------------------------
      /*CREATE 18-64 DENOMINATOR*/
      ----------------------------
      --Filter for the three support types with the 'Support Direct To Carer' Service Type that is in scope for the ASCOF Denominator
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_1864_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [1864_Denom]
      INTO #ASCOF_3D_Carers_1864_DENOM
      FROM #ASCOF_3D_Carers_1864
      WHERE
        Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only')
      GROUP BY
        LA_Code,
        LA_Name;


      ---------------------------
      /*CREATE 18-64 NUMERATORS*/
      ---------------------------

      --3D Part 1b: Self-Directed Support
      --Choose all Carers receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      DROP TABLE IF EXISTS #ASCOF_3D1b_1864_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1b_1864_Num]
      INTO #ASCOF_3D1b_1864_NUM
      FROM #ASCOF_3D_Carers_1864
      WHERE
        Support_Provided in  ('Direct Payment only', 'CASSR Managed Personal Budget')
      GROUP BY
        LA_Code,
        LA_Name;


      --3D Part 2b: Direct Payment
      --Choose all Carers receiving receiving a Direct Payment
      DROP TABLE IF EXISTS #ASCOF_3D2b_1864_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2b_1864_Num]
      INTO #ASCOF_3D2b_1864_NUM
      FROM #ASCOF_3D_Carers_1864
      WHERE
        Support_Provided IN  ('Direct Payment only')
      GROUP BY
        LA_Code,
        LA_Name;



      ----------------------------
      /*AGED 65+ INITIAL BUILD*/
      ----------------------------
      /*Create raw 65+ table and add relevant fields from Reference data
      NOTE: Filter here is designed to remove those Carers with an unknown age that were defaulted to 01/01/1800 */
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_65Over
      SELECT
        a.*
      INTO #ASCOF_3D_Carers_65Over
      FROM #ASCOF_3D_Carers_MinRank a
      WHERE
        Der_Working_Age_Band = '65 and above'



      ----------------------------
      /*CREATE 65+ DENOMINATOR*/
      ----------------------------
      --Filter for the three support types with the 'Support Direct To Carer' Service Type that is in scope for the ASCOF Denominator
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_65Over_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [65Over_Denom]
      INTO #ASCOF_3D_Carers_65Over_DENOM
      FROM #ASCOF_3D_Carers_65Over
      WHERE
        Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only')
      GROUP BY
        LA_Code,
        LA_Name;



      ---------------------------
      /*CREATE 65+ NUMERATORS*/
      ---------------------------

      --3D Part 1b: Self-Directed Support
      --Choose all Carers receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      DROP TABLE IF EXISTS #ASCOF_3D1b_65Over_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1b_65Over_Num]
      INTO #ASCOF_3D1b_65Over_NUM
      FROM #ASCOF_3D_Carers_65Over
      WHERE
        Support_Provided in  ('Direct Payment only', 'CASSR Managed Personal Budget')
      GROUP BY
        LA_Code,
        LA_Name;



      --3D Part 2b: Direct Payment
      --Choose all Carers receiving receiving a Direct Payment
      DROP TABLE IF EXISTS #ASCOF_3D2b_65Over_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2b_65Over_Num]
      INTO #ASCOF_3D2b_65Over_NUM
      FROM #ASCOF_3D_Carers_65Over
      WHERE
        Support_Provided in  ('Direct Payment only')
      GROUP BY
        LA_Code,
        LA_Name;



      ----------------------------
      /*TOTAL INITIAL BUILD*/
      ----------------------------
      /*
      Create raw All Ages table and add relevant fields from Reference data
      NOTE: Age Under 18 still excluded as per current NHSE/DHSC agreement
      This cohort will include the Unknown ages defaulted to 01/01/1800 at beginning of process
      */
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_Total
      SELECT
        a.*
      INTO #ASCOF_3D_Carers_Total
      FROM #ASCOF_3D_Carers_MinRank a


      ----------------------------
      /*CREATE TOTAL DENOMINATOR*/
      ----------------------------
      --Filter for the three support types with the 'Support Direct To Carer' Service Type that is in scope for the ASCOF Denominator
      DROP TABLE IF EXISTS #ASCOF_3D_Carers_Total_DENOM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_Denom]
      INTO #ASCOF_3D_Carers_Total_DENOM
      FROM #ASCOF_3D_Carers_Total
      WHERE
        Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only')
      GROUP BY
        LA_Code,
        LA_Name;



      ---------------------------
      /*CREATE TOTAL NUMERATORS*/
      ---------------------------

      --3D Part 1b: Self-Directed Support
      --Choose all Carers receiving self-directed support (Direct Payment or a CASSR Managed Personal Budget)
      DROP TABLE IF EXISTS #ASCOF_3D1b_Total_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D1b_Total_Num]
      INTO #ASCOF_3D1b_Total_NUM
      FROM #ASCOF_3D_Carers_Total 
      WHERE
        Support_Provided in  ('Direct Payment only', 'CASSR Managed Personal Budget')
      GROUP BY
        LA_Code,
        LA_Name;


      --3D Part 2b: Direct Payment
      --Choose all Carers receiving receiving a Direct Payment
      DROP TABLE IF EXISTS #ASCOF_3D2b_Total_NUM
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [3D2b_Total_Num]
      INTO #ASCOF_3D2b_Total_NUM
      FROM #ASCOF_3D_Carers_Total
      WHERE
        Support_Provided in  ('Direct Payment only')
      GROUP BY
        LA_Code,
        LA_Name;


      -------------------------------------------
      /*CREATE CARER Total Unknowns/invalids*/
      ------------------------------------------

      -- This is based on the earlier logic where unknown delivery mechanism is hard coded based on conditions

      DROP TABLE IF EXISTS #ASCOF_3D_Carer_Total_UN_IV
      SELECT
        LA_Code,
        LA_Name,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_UN_IV]
      INTO #ASCOF_3D_Carer_Total_UN_IV
      FROM #ASCOF_3D_Carers_Total
      WHERE
        Support_Provided = 'Support Direct to Carer: Unknown Delivery Mech'
      GROUP BY
        LA_Code,
        LA_Name;



      ----------------------------------
      -- Create summary table for PBI
      -- This code calculates the outcome for each subgroup of ASCOF3d, as well as age breakdowns
      -- Future development: simplify code through group by and rollup
      ----------------------------------


      DROP TABLE IF EXISTS #ASCOF_3D_Final

      SELECT -- client 1864 self directed (1a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '1a - The proportion of clients who use services who receive self-directed support (%)' AS [Description],
        '18 to 64' AS [Group],
        a.[3D1a_1864_Num] AS Numerator,
        b.[1864_Denom] AS Denominator,
        ROUND((CAST(a.[3D1a_1864_Num] AS FLOAT) / CAST(b.[1864_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      INTO #ASCOF_3D_Final
      FROM #ASCOF_3D1a_1864_NUM a
      FULL JOIN #ASCOF_3D_Clients_1864_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1a_1864_Num],
        b.[1864_Denom]

      UNION ALL

      SELECT -- client 65Over self directed (1a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '1a - The proportion of clients who use services who receive self-directed support (%)' AS [Description],
        '65 and above' AS [Group],
        a.[3D1a_65Over_Num] AS Numerator,
        b.[65Over_Denom] AS Denominator,
        ROUND((CAST(a.[3D1a_65Over_Num] AS FLOAT) / CAST(b.[65Over_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D1a_65Over_NUM a
      FULL JOIN #ASCOF_3D_Clients_65Over_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1a_65Over_Num],
        b.[65Over_Denom]

      UNION ALL

      SELECT -- client total self directed (1a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '1a - The proportion of clients who use services who receive self-directed support (%)' AS [Description],
        'Total' AS [Group],
        a.[3D1a_Total_Num] AS Numerator,
        b.[Total_Denom] AS Denominator,
        ROUND((CAST(a.[3D1a_Total_Num] AS FLOAT) / CAST(b.[Total_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D1a_Total_NUM a
      FULL JOIN #ASCOF_3D_Clients_Total_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1a_Total_Num],
        b.[Total_Denom]

      UNION ALL

      SELECT -- carers 1864 self directed (1b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '1b - The proportion of carers who use services who receive self-directed support (%)' AS [Description],
        '18 to 64' AS [Group],
        a.[3D1b_1864_Num] AS Numerator,
        b.[1864_Denom] AS Denominator,
        ROUND((CAST(a.[3D1b_1864_Num] AS FLOAT) / CAST(b.[1864_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D1b_1864_NUM a
      FULL JOIN #ASCOF_3D_Carers_1864_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1b_1864_Num],
        b.[1864_Denom]

      UNION ALL

      SELECT -- carers 65Over self directed (1b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '1b - The proportion of carers who use services who receive self-directed support (%)' AS [Description],
        '65 and above' AS [Group],
        a.[3D1b_65Over_Num] AS Numerator,
        b.[65Over_Denom] AS Denominator,
        ROUND((CAST(a.[3D1b_65Over_Num] AS FLOAT) / CAST(b.[65Over_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D1b_65Over_NUM a
      FULL JOIN #ASCOF_3D_Carers_65Over_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1b_65Over_Num],
        b.[65Over_Denom]

      UNION ALL

      SELECT -- carers Total self directed (1b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '1b - The proportion of carers who use services who receive self-directed support (%)' AS [Description],
        'Total' AS [Group],
        a.[3D1b_Total_Num] AS Numerator,
        b.[Total_Denom] AS Denominator,
        ROUND((CAST(a.[3D1b_Total_Num] AS FLOAT) / CAST(b.[Total_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D1b_Total_NUM a
      FULL JOIN #ASCOF_3D_Carers_Total_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c   --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D1b_Total_Num],
        b.[Total_Denom]

      UNION ALL

      SELECT -- clients 1864 direct payment (2a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '2a - The proportion of clients who use services who receive direct payments (%)' AS [Description],
        '18 to 64' AS [Group],
        a.[3D2a_1864_Num] AS Numerator,
        b.[1864_Denom] AS Denominator,
        ROUND((CAST(a.[3D2a_1864_Num] AS FLOAT) / CAST(b.[1864_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2a_1864_NUM a
      FULL JOIN #ASCOF_3D_Clients_1864_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2a_1864_Num],
        b.[1864_Denom]

      UNION ALL

      SELECT -- clients 65Over direct payment (2a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '2a - The proportion of clients who use services who receive direct payments (%)' AS [Description],
        '65 and above' AS [Group],
        a.[3D2a_65Over_Num] AS Numerator,
        b.[65Over_Denom] AS Denominator,
        ROUND((CAST(a.[3D2a_65Over_Num] AS FLOAT) / CAST(b.[65Over_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2a_65Over_NUM a
      FULL JOIN #ASCOF_3D_Clients_65Over_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2a_65Over_Num],
        b.[65Over_Denom]

      UNION ALL

      SELECT -- clients Total direct payment (2a)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        '2a - The proportion of clients who use services who receive direct payments (%)' AS [Description],
        'Total' AS [Group],
        a.[3D2a_Total_Num] AS Numerator,
        b.[Total_Denom] AS Denominator,
        ROUND((CAST(a.[3D2a_Total_Num] AS FLOAT) / CAST(b.[Total_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2a_Total_NUM a
      FULL JOIN #ASCOF_3D_Clients_Total_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2a_Total_Num],
        b.[Total_Denom]

      UNION ALL

      SELECT -- carer 1864 direct payment (2b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '2b - The proportion of carers who use services who receive direct payments (%)' AS [Description],
        '18 to 64' AS [Group],
        a.[3D2b_1864_Num] AS Numerator,
        b.[1864_Denom] AS Denominator,
        ROUND((CAST(a.[3D2b_1864_Num] AS FLOAT) / CAST(b.[1864_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2b_1864_NUM a
      FULL JOIN #ASCOF_3D_Carers_1864_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2b_1864_Num],
        b.[1864_Denom]

      UNION ALL

      SELECT -- carer 65Over direct payment (2b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '2b - The proportion of carers who use services who receive direct payments (%)' AS [Description],
        '65 and above' AS [Group],
        a.[3D2b_65Over_Num] AS Numerator,
        b.[65Over_Denom] AS Denominator,
        ROUND((CAST(a.[3D2b_65Over_Num] AS FLOAT) / CAST(b.[65Over_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2b_65Over_NUM a
      FULL JOIN #ASCOF_3D_Carers_65Over_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2b_65Over_Num],
        b.[65Over_Denom]

      UNION ALL

      SELECT -- carer Total direct payment (2b)
        c.LA_Code,
        c.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        '2b - The proportion of carers who use services who receive direct payments (%)' AS [Description],
        'Total' AS [Group],
        a.[3D2b_Total_Num] AS Numerator,
        b.[Total_Denom] AS Denominator,
        ROUND((CAST(a.[3D2b_Total_Num] AS FLOAT) / CAST(b.[Total_Denom] AS FLOAT)) * 100, 2) AS [Outcome]
      FROM #ASCOF_3D2b_Total_NUM a
      FULL JOIN #ASCOF_3D_Carers_Total_DENOM b
        ON a.LA_Code = b.LA_Code
        AND a.LA_Name = b.LA_Name
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup c --To output all LAs despite missing data
        ON b.LA_Code = c.LA_Code
      GROUP BY
        c.LA_Code,
        c.LA_Name,
        a.[3D2b_Total_Num],
        b.[Total_Denom]

      -- Add reporting period onto final table and set nulls to 0 for PBI
      SELECT 
        FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
        LA_Code, 
        LA_Name, 
        Measure, 
        Description, 
        [Group], 
        COALESCE(Numerator, 0) AS Numerator, 
        COALESCE(Denominator, 0) AS Denominator,
        COALESCE(Outcome, 0) AS Outcome
      INTO #OutputTable1
      FROM #ASCOF_3D_Final
      

        --------------------------------------------------
      -- Output Client and Carer unknowns and invalids--
      --------------------------------------------------


      -- To be updated with OutputTable2 logic

      DROP TABLE IF EXISTS #ASCOF_3D_Final_Unk

      SELECT 
        b.LA_Code,
        b.LA_Name,
        'ASCOF 3D (Clients)' AS [Measure],
        'Unknown or invalid delivery mechanism, or unable to deduce from service component' AS [Description],
        Total_UN_IV as [Count]
      INTO #ASCOF_3D_Final_Unk
      FROM #ASCOF_3D_Clients_Total_UN_IV a
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b --To output all LAs despite missing data
        ON a.LA_Code = b.LA_Code

      UNION ALL

      SELECT 
        b.LA_Code,
        b.LA_Name,
        'ASCOF 3D (Carers)' AS [Measure],
        'Unknown or invalid delivery mechanism, or unable to deduce from service component' AS [Description],
        Total_UN_IV as [Count]
      FROM #ASCOF_3D_Carer_Total_UN_IV a
      FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b   --To output all LAs despite missing data
        ON a.LA_Code = b.LA_Code

      -- Add reporting period to final table and set nulls as 0 for PBI
      SELECT 
        FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
        LA_Code, 
        LA_Name, 
        Measure, 
        Description, 
        COALESCE([Count], 0) AS [Count]
      INTO #OutputTable2
      FROM #ASCOF_3D_Final_Unk 


      --Store outputs
      SET @Query = 'SELECT * INTO ' + @OutputTable1 + ' FROM #OutputTable1'
      EXEC(@Query)

      SET @Query = 'SELECT * INTO ' + @OutputTable2 + ' FROM #OutputTable2'
      EXEC(@Query)


      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO


-----Example execution
/*
EXEC ASC_Sandbox.Create_ASCOF3D
  @ReportingPeriodStartDate = '2024-01-01',
  @ReportingPeriodEndDate = '2024-12-31', 
  @InputTable = 'ASC_Sandbox.CLD_240101_241231_SingleSubmissions', 
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk'
*/