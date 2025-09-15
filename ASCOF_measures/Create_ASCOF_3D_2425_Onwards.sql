------------------------------------------------------------------------------------------------------------------------------------------------
/*ASCOF 3D: Proportion of people using social care who receive self-directed support, and those receiving direct payments (formerly metric 1C)*/
------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------
/* 
This ASCOF measure is 4 parts:

**Client based measures**
Denominator (3D Part 1a and 2a) = All clients receiving long term community support at the year end
Numerator (3D Part 1a) = Clients receiving self-directed support
Numerator (3D Part 2a) = Clients receiving Direct Payments only
Outcome = Numerator / Denominator * 100


**Carer based measures**
Denominator (3D Part 1b and 2b) = All carers receiving carer-specific services via direct payment, personal budget or commissioned support during the year
Numerator (3D Part 1b) = Carers receiving self-directed support
Numerator (3D Part 2b) = Carers receiving Direct Payments only
Outcome = Numerator / Denominator * 100

-----------------------------------------------------------------------------

The code is written to run through and create the Client-based measures 3D1a and 3D2a first and then the Carers measures
3D1b and 3D2b afterwards, before appending them all into one final ASCOF 3D output.

Dates are a snapshot for Clients, and in-year for Carers, as per ASCOF Handbook.

**CLIENT BASED MEASURES**
A full deduplication process must run through even though ASCOF 3D is only concerned with Community-based Clients.
This is to allow for de-duplication to occur based on setting, where all Clients present in multiple settings as at the 
snapshot date are placed in the highest setting i.e. Residential setting out-ranks Community, Nursing out-ranks Residential etc

Technically at a snapshot date, a Client should not be recorded against multiple conflicting settings, but this does
occur in the data submitted.

**CARER BASED MEASURES**
A full deduplication process must run through even though ASCOF 3D is only concerned with Direct Payment / self-directed Carers.
This is to allow for de-duplication to occur based on support provided, where Carers receiving multiple conflicting support types 
as at the snapshot date are placed in the highest category as per the setting hierarchy i.e. Direct Payment out-ranks CASSR Managed Personal Budget

**DEALING WITH UNKNOWNS AND INVALIDS 

Clients: 
> unknowns, blanks and invalids are anything where the delivery mechanism is null or invalid and the service component is not direct payment.
> these are overwritten to blank and given a high number on the hierarchy (lowest down) so known valid values are selected over this
> unknowns and invalids are kept in denominators

Carers:
>There are two conditions where delivery mechanism becomes unknown:
  WHEN Service_Type = 'Carer Support: Direct to Carer' and 
       (Delivery_Mechanism not in  ('Direct Payment', 'CASSR Managed Personal Budget', 'CASSR Commissioned support') and 
       Service_Component not like 'Direct Payment') 
  WHEN Service_Type = 'Carer Support: Direct to Carer' and 
       (Delivery_Mechanism is NULL and Service_Component not like 'Direct Payment') 
>The denominator only includes 'Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only'
 therefore unknowns are excluded from the denominator and numerator

 *24/25 onwards - this code has been adapted for use on the 24/25 main tables as these tables contain different field names where R2 to R1 mapping has been applied

*/

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF3D_2425_Onwards

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(100),
  @InputTable_PersonDetails AS NVARCHAR(100),
  @OutputTable1 AS NVARCHAR(100),
  @OutputTable2 AS NVARCHAR(100)

AS         
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails

  SET @Query = N'DROP TABLE IF EXISTS ' + @OutputTable1 + '; 
                DROP TABLE IF EXISTS ' + @OutputTable2 + '; 
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + ';
				        CREATE SYNONYM ASC_Sandbox.InputTable_PersonDetails FOR ' + @InputTable_PersonDetails +';'
  EXEC(@Query)


    -------------------------------------------------------------------------------------------------------------
    ------------------------------ Create reference tables and inital build table -------------------------------
    -------------------------------------------------------------------------------------------------------------
    
    --Client reference table
    --Used to determine the highest ranking service for individuals with multiple services open at the reporting period end date for deduplication
    DROP TABLE IF EXISTS #REF_Service_Type_Delivery_Mech	
    CREATE TABLE #REF_Service_Type_Delivery_Mech
      (Service_Type VARCHAR(200),
      Delivery_Mechanism VARCHAR(200),
      Hierarchy INT);

    INSERT INTO #REF_Service_Type_Delivery_Mech
      (Service_Type,
      Delivery_Mechanism,
      Hierarchy)
    VALUES
      ('Long Term Support: Nursing Care', '', 1),
      ('Long Term Support: Residential Care', '', 2),
      ('Long Term Support: Community', 'Direct Payment', 3),
      ('Long Term Support: Community', 'CASSR Managed Personal Budget', 4),
      ('Long Term Support: Community', 'CASSR Commissioned Support', 5),
      ('Long Term Support: Community', '', 6),
      ('Long Term Support: Prison', 'CASSR Managed Personal Budget', 7),
      ('Long Term Support: Prison', 'CASSR Commissioned Support', 8),
      ('Long Term Support: Prison', '', 9);


    --Carer reference table
    --Used to determine the highest ranking service for individuals with multiple throughout the year for deduplication
    DROP TABLE IF EXISTS #Ref_Carer_Support_Hierarchy
    CREATE TABLE #Ref_Carer_Support_Hierarchy
      (Support_Provided VARCHAR(200),
      Hierarchy INT);

    INSERT INTO #Ref_Carer_Support_Hierarchy
      (Support_Provided,
      Hierarchy)
    VALUES
      ('Direct Payment only', 1),
      ('CASSR Managed Personal Budget', 2),
      ('CASSR Commissioned Support only', 3),
      ('Support Direct to Carer: Unknown Delivery Mech', 4),
      ('Information, Advice and Other Universal Services / Signposting', 5),
      ('No Direct Support Provided to Carer', 6); 


    --Initial build table created from single year of CLD with age at the end of the reporting period derived
    DROP TABLE IF EXISTS #Build;
      
    SELECT 
      a.*,
	    b.Der_Birth_Date AS Der_Birth_Date_Latest,
	    b.Date_of_Death AS Date_of_Death_Latest,
      CASE
        WHEN b.Der_Birth_Date IS NULL THEN NULL
        ELSE FLOOR(DATEDIFF(DAY, b.Der_Birth_Date, @ReportingPeriodEndDate) / 365.25)  
	    END AS Der_Age_Reporting_End
    INTO #Build
    FROM ASC_Sandbox.InputTable a
	  LEFT JOIN ASC_Sandbox.InputTable_PersonDetails b
      ON a.LA_Code = b.LA_Code
      AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID


    --------------------------------------------------------------------------------------------------------------
    ----------------------------------------- Client based measures ----------------------------------------------
    --------------------------------------------------------------------------------------------------------------

    -------------------------------------------
    ---- Create cleaned client build table ----
    -------------------------------------------

    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Build;

    SELECT
      *,
      Client_Type_Cleaned AS Client_Type,
      CASE 
        WHEN Service_Component_Cleaned = 'Direct Payment' THEN 'Direct Payment'  --Overwrite delivery mechanism when service comp is DP
        ELSE Delivery_Mechanism_Cleaned
        END
      AS Delivery_Mechanism,
      Service_Component_Cleaned AS Service_Component,
      Service_Type_Cleaned AS Service_Type,
      CASE  
        WHEN Der_Age_Reporting_End < 18 THEN 'Under 18'
        WHEN Der_Age_Reporting_End BETWEEN 18 AND 64 THEN '18 to 64'
        WHEN Der_Age_Reporting_End >= 65 THEN '65 and above'
        ELSE 'Unknown'
      END AS Der_Age_Band_Reporting_End
    INTO #ASCOF_3D_Clients_Build
    FROM #Build    
    WHERE 
      Service_Type_Cleaned IN 
        ('Long Term Support: Nursing Care', 
        'Long Term Support: Residential Care', 
        'Long Term Support: Community', 
        'Long Term Support: Prison')
      AND Client_Type_Cleaned = 'Service User'
      AND Event_Start_Date <= @ReportingPeriodEndDate  --this row and row below filters to services ongoing at the end of the period
      AND Der_NHS_LA_Combined_Person_ID IS NOT NULL
      AND Event_Start_Date IS NOT NULL
      AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) --removes DQ issues of event end date prior to start dateAND Event_Start_Date <= @ReportingPeriodEndDate  --this row and row below filters to services ongoing at the end of the period
      AND (Der_Event_End_Date >= @ReportingPeriodEndDate OR Der_Event_End_Date IS NULL)
      AND (Date_of_Death_Latest >= @ReportingPeriodEndDate OR Date_of_Death_Latest IS NULL) 

    
    --Delivery Mechanism is currently NOT a mandatory field and so we can not remove records based on inaccuracy in this field
    --but leaving invalid/unexpected entries in the field will cause problems with the Reference data join. 
    --Solution to this is to blank out ('') any entries that are invalid or where a Delivery Mechanism is not expected (Nursing and Res events)   
    UPDATE a
    SET a.Delivery_Mechanism = 
    (CASE
      WHEN b.Delivery_Mechanism IS NOT NULL AND a.Service_Type in ('Long Term Support: Community', 'Long Term Support: Prison')
        THEN a.Delivery_Mechanism 
	      ELSE '' 
	    END)
    FROM #ASCOF_3D_Clients_Build a
    LEFT JOIN #REF_Service_Type_Delivery_Mech b
      ON TRIM (a.Service_Type) = TRIM (b.Service_Type)
      AND TRIM (a.Delivery_Mechanism) = TRIM (b.[Delivery_Mechanism]);



    -----------------------------------------------------------
    ---- Process filter and deduplicate to highest service ----
    -----------------------------------------------------------
    --The Client-based ASCOF 3D measures are processed as previously done in SALT, where each Client can only be counted once.
    --Code below joins to the Hierarchy reference data and chooses the record with the lowest value (highest ranking) in Service_Type/Delivery_Mech 
    --This avoids counting people with a 'Direct Payment' who also receive a higher acuity service e.g. Nursing or Residential
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Join;

    SELECT
      a.*,
      CASE WHEN a.Service_Type = 'Long Term Support: Community' AND Service_Component = 'Direct Payment'
        THEN '3'
        ELSE b.[Hierarchy]
        END AS [Hierarchy]
    INTO #ASCOF_3D_Clients_Join
    FROM #ASCOF_3D_Clients_Build a
    LEFT JOIN #REF_Service_Type_Delivery_Mech b
      ON TRIM (a.Service_Type) = TRIM (b.Service_Type)
      AND TRIM (a.Delivery_Mechanism) = TRIM (b.[Delivery_Mechanism])


    --Choose the record with the lowest rank value (low = highest setting)
    --There are DQ issues where the same person has different ages at the end of the period (due to differ DOBs)
    --In this situation the MAX age is taken
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_MinRank;

    SELECT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      MIN(Hierarchy) AS [Rank],
      MAX(Der_Age_Reporting_End) AS Der_Age_Reporting_End --To prevent double dounting where DQ issues have multiple DOBs
    INTO #ASCOF_3D_Clients_MinRank
    FROM #ASCOF_3D_Clients_Join
    GROUP BY
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID;


    --Select these records into the final table from which ASCOF numerators and denominators are selected
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Final;

    SELECT
      DISTINCT 
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      a.Der_Age_Band_Reporting_End,
      a.Service_Type,
      a.Service_Component,
      a.[Delivery_Mechanism]
    INTO #ASCOF_3D_Clients_Final
    FROM #ASCOF_3D_Clients_Join a
    INNER JOIN #ASCOF_3D_Clients_MinRank b
      ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
      AND a.LA_Code = b.LA_Code
      AND a.[Hierarchy] = b.[Rank]
    WHERE
      b.[Rank] is not NULL
      AND a.Der_Age_Band_Reporting_End IN ('18 to 64', '65 and above', 'Unknown')

    ------------------------------------------------------
    ---- Create numerators, denominators and unknowns ----
    ------------------------------------------------------
    --Output numerators and denominators
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Aggregation;
      
    SELECT
      LA_Code,
      LA_Name,
      CASE WHEN Der_Age_Band_Reporting_End IS NULL THEN 'Total' ELSE Der_Age_Band_Reporting_End END AS [Group],
      '1a - The proportion of clients who use services who receive self-directed support (%)' AS [Description],
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator,
      COUNT(DISTINCT (
        CASE
          WHEN ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment') 
            OR [Delivery_Mechanism] = 'CASSR Managed Personal Budget'
          THEN Der_NHS_LA_Combined_Person_ID 
        END)) AS Numerator
    INTO #ASCOF_3D_Clients_Aggregation
    FROM #ASCOF_3D_Clients_Final
    WHERE Service_Type = 'Long Term Support: Community' 
    GROUP BY
      LA_Code,
      LA_Name,
      ROLLUP(Der_Age_Band_Reporting_End)

    UNION ALL 

    SELECT
      LA_Code,
      LA_Name,
      CASE WHEN Der_Age_Band_Reporting_End IS NULL THEN 'Total' ELSE Der_Age_Band_Reporting_End END AS [Group],
      '2a - The proportion of clients who use services who receive direct payments (%)' AS [Description],
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator,
      COUNT(DISTINCT (
        CASE
          WHEN ([Delivery_Mechanism] = 'Direct Payment' OR [Service_Component] = 'Direct Payment') 
          THEN Der_NHS_LA_Combined_Person_ID 
        END)) AS Numerator
    FROM #ASCOF_3D_Clients_Final
    WHERE Service_Type = 'Long Term Support: Community'
    GROUP BY
      LA_Code,
      LA_Name,
      ROLLUP(Der_Age_Band_Reporting_End);

    -- Remove unknowns from output table (but still included as part of the totals)
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Output

    SELECT *
    INTO #ASCOF_3D_Clients_Output
    FROM #ASCOF_3D_Clients_Aggregation
    WHERE [Group] NOT IN ('Unknown')


    --Output invalids and unknowns for the dashboard
    --This is based on null, unknown or invalid delivery mechanism when Service_Type is community and service component is not direct payment
    DROP TABLE IF EXISTS #ASCOF_3D_Clients_Total_UN_IV
    SELECT
      LA_Code,
      LA_Name,
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_UN_IV]
    INTO #ASCOF_3D_Clients_Total_UN_IV
    FROM #ASCOF_3D_Clients_Final
    WHERE
      Service_Type = 'Long Term Support: Community'
      AND (Delivery_Mechanism = '' AND (Service_Component <> 'Direct Payment' OR Service_Component IS NULL))
    GROUP BY
      LA_Code,
      LA_Name;

    --------------------------------------------------------------------------------------------------------------
    ----------------------------------------- Carer based measures -----------------------------------------------
    --------------------------------------------------------------------------------------------------------------

    ------------------------------------------
    ---- Create cleaned carer build table ----
    ------------------------------------------
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Build;

    SELECT
      *,
      Client_Type_Cleaned AS Client_Type,
      Delivery_Mechanism_Cleaned AS Delivery_Mechanism,
      Service_Component_Cleaned AS Service_Component,
      Service_Type_Cleaned AS Service_Type,
      COALESCE(Event_Outcome_Cleaned, 'Invalid and not mapped') AS Event_Outcome 
    INTO #ASCOF_3D_Carers_Build                                                                                                                                                 
    FROM #Build a
    WHERE
      Client_Type_Cleaned in ('Carer','Unpaid carer', 'Carer known by association', 'Unpaid carer known by association')  
      AND Event_Start_Date <= @ReportingPeriodEndDate  --this line and line below filters to events within the year
      AND Der_NHS_LA_Combined_Person_ID IS NOT NULL
      AND Event_Start_Date IS NOT NULL
      AND (Der_Event_End_Date >= @ReportingPeriodStartDate or Der_Event_End_Date is NULL)
      AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) --removes DQ issues of event end date prior to start dateAND Event_Start_Date <= @ReportingPeriodEndDate  --this row and row below filters to services ongoing at the end of the period
      AND (Date_of_Death_Latest >= @ReportingPeriodStartDate OR Date_of_Death_Latest is NULL)
    --three bespoke combinations of event scenarios below are allowed to make up the Carers cohort
      AND ((Service_Type_Cleaned IS NULL AND Event_Outcome_Cleaned = 'NFA - Information & Advice / Signposting only')
      OR (Service_Type_Cleaned = 'Carer Support: Direct to Carer' OR Service_Type_Cleaned = 'Carer Support: Support involving the person cared-for')
      OR (Event_Type IN ('Assessment','Review') AND Service_Type_Cleaned IS NULL))

    ----------------------------------------------------
    ---- Case into the different support categories ----
    ----------------------------------------------------
    --The specific scenarios detailed in the above table are now CASED into the type of support provided so they can be deduplicated 
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Case;

    SELECT
      *,
    CASE 
      WHEN Service_Type = 'Carer Support: Direct to Carer' AND 
            (Delivery_Mechanism = 'Direct Payment' or Service_Component = 'Direct Payment') 
      THEN 'Direct Payment only'

      WHEN Service_Type = 'Carer Support: Direct to Carer' AND 
            (Delivery_Mechanism = 'CASSR Managed Personal Budget' AND Service_Component NOT LIKE 'Direct Payment') 
      THEN 'CASSR Managed Personal Budget'

      WHEN Service_Type = 'Carer Support: Direct to Carer' AND 
            (Delivery_Mechanism = 'CASSR Commissioned support' AND Service_Component NOT LIKE 'Direct Payment') 
      THEN 'CASSR Commissioned Support only'

      WHEN Service_Type = 'Carer Support: Direct to Carer' AND 
            (Delivery_Mechanism NOT IN  ('Direct Payment', 'CASSR Managed Personal Budget', 'CASSR Commissioned support') 
            AND Service_Component NOT LIKE 'Direct Payment') 
      THEN 'Support Direct to Carer: Unknown Delivery Mech'

      WHEN Service_Type = 'Carer Support: Direct to Carer' AND 
            (Delivery_Mechanism is NULL AND Service_Component NOT LIKE 'Direct Payment') 
      THEN 'Support Direct to Carer: Unknown Delivery Mech'

      WHEN Service_Type = 'Carer Support: Support involving the person cared-for' 
      THEN 'No Direct Support Provided to Carer'

      WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome NOT LIKE 'NFA - Information & Advice / Signposting only' 
      THEN 'No Direct Support Provided to Carer'
        
      WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome IS NULL 
      THEN 'No Direct Support Provided to Carer'
        
      WHEN Event_Type IN ('Assessment', 'Review') AND Event_Outcome = 'NFA - Information & Advice / Signposting only' 
      THEN 'Information, Advice and Other Universal Services / Signposting'
        
      WHEN Event_Type = 'Request' 
      THEN 'Information, Advice and Other Universal Services / Signposting'
        
      END AS 'Support_Provided'
    INTO #ASCOF_3D_Carers_Case
    FROM #ASCOF_3D_Carers_Build;

    -----------------------------------------------------------
    ---- Process filter and deduplicate to highest support ----
    -----------------------------------------------------------
    --The Carer-based ASCOF 3D measures are processed as previously done in SALT, where each carer can only be counted once.
    --Code below joins to the Hierarchy reference data and chooses the record with the lowest value (highest ranking) support provided
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Join;

    SELECT
      a.*,
      b.Hierarchy
    INTO #ASCOF_3D_Carers_Join
    FROM #ASCOF_3D_Carers_Case a
    FULL JOIN #REF_CARER_SUPPORT_HIERARCHY b
      ON a.Support_Provided = b.Support_Provided;


    --Choose the record with the lowest rank value (low = highest support)
    --There are DQ issues where the same person has different ages at the end of the period (due to differ DOBs)
    --Suspect this is a DQ issue where both the carer's age and the cared-for person's age are provided therefore MIN age taken
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_MinRank

    SELECT
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID,
        CASE  
          WHEN MIN(Der_Age_Reporting_End) < 18 THEN 'Under 18'
          WHEN MIN(Der_Age_Reporting_End) BETWEEN 18 AND 64 THEN '18 to 64'
          WHEN MIN(Der_Age_Reporting_End) >= 65 THEN '65 and above'
          ELSE 'Unknown'
        END AS Der_Age_Band_Reporting_End,
        MIN(Hierarchy) AS [Rank]
      INTO #ASCOF_3D_Carers_MinRank
      FROM #ASCOF_3D_Carers_Join
      GROUP BY
        LA_Code,
        LA_Name,
        Der_NHS_LA_Combined_Person_ID
    

    --Select these records into the final table from which ASCOF numerators and denominators are selected     
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Final

    SELECT DISTINCT
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      b.Der_Age_Band_Reporting_End, 
      a.Support_Provided
    INTO #ASCOF_3D_Carers_Final
    FROM #ASCOF_3D_Carers_Join a
    INNER JOIN #ASCOF_3D_Carers_MinRank b
    ON a.LA_Code = b.LA_Code
      AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
      AND a.Hierarchy = b.[Rank]
    WHERE b.[Rank] IS NOT NULL
    AND b.Der_Age_Band_Reporting_End IN ('18 to 64', '65 and above', 'Unknown') --added to remove under 18s but include unknowns (just for totals)

    ------------------------------------------------------
    ---- Create numerators, denominators and unknowns ----
    ------------------------------------------------------
    --Output numerators and denominators
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Aggregation;
      
    SELECT
      LA_Code,
      LA_Name,
      CASE WHEN Der_Age_Band_Reporting_End IS NULL THEN 'Total' ELSE Der_Age_Band_Reporting_End END AS [Group],
      '1b - The proportion of carers who use services who receive self-directed support (%)' AS [Description],
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator,
      COUNT(DISTINCT (
        CASE
          WHEN Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget')
          THEN Der_NHS_LA_Combined_Person_ID 
        END)) AS Numerator
    INTO #ASCOF_3D_Carers_Aggregation
    FROM #ASCOF_3D_Carers_Final
    WHERE Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only')
    GROUP BY
      LA_Code,
      LA_Name,
      ROLLUP(Der_Age_Band_Reporting_End)

    UNION ALL 

    SELECT
      LA_Code,
      LA_Name,
      CASE WHEN Der_Age_Band_Reporting_End IS NULL THEN 'Total' ELSE Der_Age_Band_Reporting_End END AS [Group],
      '2b - The proportion of carers who use services who receive direct payments (%)' AS [Description],
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator,
      COUNT(DISTINCT (
        CASE
          WHEN Support_Provided = 'Direct Payment only'
          THEN Der_NHS_LA_Combined_Person_ID 
        END)) AS Numerator
    FROM #ASCOF_3D_Carers_Final
    WHERE Support_Provided IN  ('Direct Payment only', 'CASSR Managed Personal Budget', 'CASSR Commissioned Support only')
    GROUP BY
      LA_Code,
      LA_Name,
      ROLLUP(Der_Age_Band_Reporting_End)


    -- Remove unknowns from output table (but still included as part of the totals)
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Output

    SELECT *
    INTO #ASCOF_3D_Carers_Output
    FROM #ASCOF_3D_Carers_Aggregation
    WHERE [Group] NOT IN ('Unknown')


    --Output invalids and unknowns for the dashboard
    -- This is based on the earlier logic where unknown delivery mechanism is hard coded based on conditions
    DROP TABLE IF EXISTS #ASCOF_3D_Carers_Total_UN_IV;

    SELECT
      LA_Code,
      LA_Name,
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Total_UN_IV]
    INTO #ASCOF_3D_Carers_Total_UN_IV
    FROM #ASCOF_3D_Carers_Final
    WHERE
      Support_Provided = 'Support Direct to Carer: Unknown Delivery Mech'
    GROUP BY
      LA_Code,
      LA_Name;


    --------------------------------------------------------------------------------------------------------------
    ----------------------------------- Combine client and carer outputs -----------------------------------------
    --------------------------------------------------------------------------------------------------------------
    --Join together client and carer output tables
    DROP TABLE IF EXISTS #ASCOF_3D_Final;

    SELECT *
    INTO #ASCOF_3D_Final
    FROM #ASCOF_3D_Clients_Output
    UNION ALL 
    SELECT *
    FROM #ASCOF_3D_Carers_Output
    

    --Format output for dashboard
    --Join with list of all LAs to ensure LAs with missing data are included in the output
    DROP TABLE IF EXISTS #OutputTable1  

    SELECT 
      FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
      r.LA_Code, 
      r.LA_Name,
      CASE 
        WHEN d.[Description] LIKE '1a%' OR d.[Description] LIKE '2a%' THEN 'ASCOF 3D (Clients)' 
        WHEN d.[Description] LIKE '1b%' OR d.[Description] LIKE '2b%' THEN 'ASCOF 3D (Carers)' 
        END AS Measure,
      d.[Description],
      g.[Group],
      COALESCE(f.Numerator,0) AS Numerator,
      COALESCE(f.Denominator,0) AS Denominator,
      COALESCE(ROUND((CAST(f.Numerator AS FLOAT) / CAST(f.Denominator AS FLOAT)) * 100, 1),0) AS [Outcome]
    INTO #OutputTable1
    FROM ASC_SANDBOX.REF_ONS_Codes_LA_Region_Lookup r
    CROSS JOIN 
    (SELECT DISTINCT [Description] FROM #ASCOF_3D_Final) d
    CROSS JOIN 
    (SELECT DISTINCT [Group] FROM #ASCOF_3D_Final) g
    LEFT JOIN
      #ASCOF_3D_Final f
    ON r.LA_Code = f.LA_Code AND
    d.[Description] = f.[Description] AND
    g.[Group] = f.[Group]
    ORDER BY  LA_Name, [Description]


    --Format unknowns for dashboard
    --Join with list of all LAs to ensure LAs without any unknows are still included in the output with 0
    DROP TABLE IF EXISTS #OutputTable2
    SELECT 
      FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
      b.LA_Code,
      b.LA_Name,
      'ASCOF 3D (Clients)' AS Measure,
      'Unknown or invalid delivery mechanism, or unable to deduce from service component' AS [Description],
      COALESCE(Total_UN_IV, 0) AS [Count]
    INTO #OutputTable2
    FROM #ASCOF_3D_Clients_Total_UN_IV a
    FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b   --To output all LAs despite missing data
      ON a.LA_Code = b.LA_Code

    UNION ALL 

    SELECT 
      FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
      b.LA_Code,
      b.LA_Name,
      'ASCOF 3D (Carers)' AS Measure,
      'Unknown or invalid delivery mechanism, or unable to deduce from service component' AS [Description],
      COALESCE(Total_UN_IV, 0) AS [Count]
    FROM #ASCOF_3D_Carers_Total_UN_IV a
    FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b   --To output all LAs despite missing data
      ON a.LA_Code = b.LA_Code

    --Store outputs
    SET @Query = 'SELECT * INTO ' + @OutputTable1 + ' FROM #OutputTable1'
    EXEC(@Query)

    SET @Query = 'SELECT * INTO ' + @OutputTable2 + ' FROM #OutputTable2'
    EXEC(@Query)


    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO

-----Example execution
/*
EXEC ASC_Sandbox.Create_ASCOF3D_2425_Onwards
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_3D',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_3D_Unk'
*/
 