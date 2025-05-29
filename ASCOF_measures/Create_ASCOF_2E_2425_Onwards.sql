----------------------------------------------------------------------------------------------------------------------------
/*ASCOF 2E (formerly 1G)
The proportion of people who receive long-term support who live in their home or with family
 -- Note that this can be filtered to LD or All PSR (parts 1+2)

Denominator = Number of Clients who received Long Term Support during the year
Numerator = Number of Clients who received Long Term Support during the year who are living on their own or with family
Outcome = Proportion(%) of Clients who received Long Term Support during the year who are living on their own or with family

New methodology:
  -- Person details (age, gender, accommodation status) now pull from a table of the latest known details per person
  -- It is specific to each SingleSubmission, and needs to be run before the stored procedure is called in the main script.
  -- Therefore the accommodation status is taken at the end of the reporting period, rather than the latest long term service.

Pre-requisites:
  -- Latest person details for 2E script must be ran
  
** UNKNOWNS, NULLS AND INVALIDS **
  -- Where accommodation status is null or 'Unknown' (as per defined list) then [Der_Accommodation_Known] = 0 
  -- This field is then used in the latest person details script to choose the latest known status over unknown (even if the known ones are invalid)
  -- People with conflicting accommodation statuses within the same submission for the same LA and Event end date are overwritten to 'unknown' 
  -- If the latest accommodation status is unknown or invalid, service information is assessed to see if accommodation status can be deduced
  -- Unknowns and invalids are included in the denominator

*24/25 onwards - this code has been adapted for use on the 24/25 main tables as these tables contain different field names where R2 to R1 mapping has been applied

*/
----------------------------------------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2E_2425_Onwards

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2E_2425_Onwards
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @LD_Filter INT, -- This can be filtered to learning disability (1) or all clients (0)
  @InputTable1 AS NVARCHAR(100),
  @InputTable2 AS VARCHAR(100),
  @OutputTable1 AS NVARCHAR(100),
  @OutputTable2 AS NVARCHAR(100)

AS         
          --SET NOCOUNT ON;
      DECLARE @Query NVARCHAR(MAX)
      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable1
      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable2
      SET @Query = N'DROP TABLE IF EXISTS ' + @OutputTable1 + '; 
                    DROP TABLE IF EXISTS ' + @OutputTable2 + '; 
                    CREATE SYNONYM ASC_Sandbox.InputTable1 FOR ' + @InputTable1 + '; 
                    CREATE SYNONYM ASC_Sandbox.InputTable2 FOR ' + @InputTable2 + ';'
      EXEC(@Query)

    -- Execute the stored procedure which outputs the latest file for each LA covering the specified reporting period as of a given date
    -- Results are inserted into the temporary table


    -------------------------------------------------------------------------
    ------------------------ Create REF table -------------------------------
    -------------------------------------------------------------------------
    
    --To pull through Settled/Unsettled Accommodation

    DROP TABLE IF EXISTS #REF_Accommodation_Status;
    CREATE TABLE #REF_Accommodation_Status
      (Accommodation_Status VARCHAR(200),
      Accommodation_Status_Group INT,
      Accommodation_Sort_Order INT);


    INSERT INTO #REF_Accommodation_Status
      (Accommodation_Status,
      Accommodation_Status_Group,
      Accommodation_Sort_Order)
    VALUES
      ('Owner occupier or shared ownership scheme',1,1),
      ('Tenant',1,2),
      ('Tenant - private landlord',1,3),
      ('Settled mainstream housing with family / friends',1,4),
      ('Supported accommodation / supported lodgings / supported group home',1,5),
      ('Shared Lives scheme',1,6),
      ('Approved premises for offenders released from prison or under probation supervision',1,7),
      ('Sheltered housing / extra care housing / other sheltered housing',1,8),
      ('Mobile accommodation for Gypsy / Roma and Traveller communities',1,9),
      ('Rough sleeper / squatting',0,10),
      ('Night shelter / emergency hostel / direct access hostel',0,11),
      ('Refuge',0,12),
      ('Placed in temporary accommodation by the council (inc. homelessness resettlement)',0,13),
      ('Staying with family / friends as a short-term guest',0,14),
      ('Acute / long-term healthcare residential facility or hospital',0,15),
      ('Registered care home',0,16),
      ('Registered nursing home',0,17),
      ('Prison / Young offenders institution / detention centre',0,18),
      ('Other temporary accommodation',0,19),
      ('Unknown', 0,20)

    -----------------------------------------------------------------------------------
    ------------------------------ Create variables  ----------------------------------
    -----------------------------------------------------------------------------------

    -- Set variables to filter age bands/psr depending on LD cohort selection as part of stored procedure
  
    DECLARE @LD_PSR NVARCHAR(50)

    IF @LD_Filter = 1
      BEGIN
        SET @LD_PSR = 'Learning Disability Support'
      END
    ELSE
      BEGIN
        SET @LD_PSR = NULL
      END


    DECLARE @LD_Age NVARCHAR(50)
    IF @LD_Filter = 1
      BEGIN
        SET @LD_Age = '18 to 64'
      END
    ELSE 
      BEGIN
        SET @LD_Age = NULL
    END

    ---------------------------------------------------------------------
    ------------------- Select cohort for 2E  ---------------------------
    ---------------------------------------------------------------------

    -- Build the raw table of all Clients in scope of ASCOF 2E
    -- Uses variable LD_Filter which is declared at the top of the code to filter between LD and All cohorts. 
    -- Then declares variables @LD_PSR and @LD_Age which filter the dataset to PSR and age bands according to the measure

    DROP TABLE IF EXISTS #ASCOF_2E_Build
    SELECT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      Primary_Support_Reason_Cleaned AS Primary_Support_Reason
    INTO #ASCOF_2E_Build
    FROM ASC_Sandbox.InputTable1
    WHERE Service_Type_Cleaned in ('Long Term Support: Nursing Care', 'Long Term Support: Residential Care', 'Long Term Support: Community')
      AND (Event_Start_Date <= @ReportingPeriodEndDate AND Event_Start_Date IS NOT NULL) 
      AND (Der_Event_End_Date >= @ReportingPeriodStartDate or Der_Event_End_Date is NULL)
      AND (Date_of_Death >= @ReportingPeriodStartDate OR Date_of_Death is NULL)  
      AND (Der_Birth_Month IS NOT NULL and Der_Birth_Year IS NOT NULL)
      AND Client_Type_Cleaned = 'Service User'
      AND (@LD_PSR IS NULL OR Primary_Support_Reason_Cleaned = @LD_PSR) -- If @LD_Filter is 1, @LD_PSR is 'Learning Disability Support'. If @LD_Filter is 0, @LD_PSR is NULL, hence NULL=NULL so will just be ignored.

    ---------------------------------------------------------------------
    ------------------- Pull through latest person details --------------
    ---------------------------------------------------------------------

      -- Join to the latest person details table
    DROP TABLE IF EXISTS #ASCOF_2E_Person_Details
    SELECT DISTINCT
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      b.Accommodation_Status,
      b.Gender,
      b.Latest_Age,
      b.Latest_Age_Band
    INTO #ASCOF_2E_Person_Details
    FROM #ASCOF_2E_Build a
    LEFT JOIN ASC_Sandbox.InputTable2 b --
      ON a.LA_Code = b.LA_Code
        AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
    WHERE
      (@LD_Age IS NULL OR Latest_Age_Band = @LD_Age) -- If @LD_Filter is 1, @LD_Age is '18 to 64'. If @LD_Filter is 0, @LD_Age is NULL, hence NULL=NULL so will just be ignored.
      AND Latest_Age_Band  NOT IN ('Under 18', 'Unknown');  -- remove anyone under 18 at reporting period end or with unknown age

    
    -----------------------------------------------------------------------------------
    -- Replace unknown/invalid accommodation status with service-derived information --
    -----------------------------------------------------------------------------------
    -- Service type reference table with hierarchy, used when a person has 2 services with the same end date (incl both null)
    DROP TABLE IF EXISTS #REF_Service_Type

    CREATE TABLE #REF_Service_Type
    (Service_Type VARCHAR(200)
    ,Service_Type_Hierarchy INT)
    
    INSERT INTO #REF_Service_Type
    (Service_Type
    ,Service_Type_Hierarchy
    )
    VALUES
    ('Long Term Support: Nursing Care', 1)
    ,('Long Term Support: Residential Care', 2)
    ,('Long Term Support: Community',  3)
    ,('Long Term Support: Prison', 4)

    --Select people with an unknown/invalid accommodation status:
    DROP TABLE IF EXISTS #Unknown_IDs
    SELECT DISTINCT 
      LA_Code, 
      LA_Name, 
      Der_NHS_LA_Combined_Person_ID
    INTO #Unknown_IDs
    FROM #ASCOF_2E_Person_Details
    WHERE Accommodation_Status = 'Unknown' 


    --Merge unknowns with initial build table to find service information
    DROP TABLE IF EXISTS #Unknown_IDs_Service_Details
    SELECT
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      b.Event_Type,
      b.Service_Type_Cleaned AS Service_Type,
      b.Service_Component_Cleaned AS Service_Component,
      c.Service_Type_Hierarchy,
      b.Event_Start_Date,
      b.Der_Event_End_Date
    INTO #Unknown_IDs_Service_Details
    FROM #Unknown_IDs a
    LEFT JOIN ASC_Sandbox.InputTable1 b
    ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID AND 
    a.LA_Code = b.LA_Code
    LEFT JOIN #REF_Service_Type c
    ON b.Service_Type_Cleaned = c.Service_Type


    --Find latest services for each person (applying ordering and simple hierarchy)
    DROP TABLE IF EXISTS #Service_Row1;
    WITH LatestService
    AS (
      SELECT 
        *,
        DENSE_RANK() OVER ( --services with the same dates, service type hierarhcy, or one of the 3 service components which can be mapped are given the same row number
          PARTITION BY 
                  LA_Code,
                  LA_Name,
                  Der_NHS_LA_Combined_Person_ID
          ORDER BY
                  COALESCE(Der_Event_End_Date, '9999-01-01') DESC, --sort by latest end date first (nulls as priority)
                  COALESCE(Service_Type_Hierarchy, '99') ASC, --then by highest ranking service type (nulls lowest priority)
                  (CASE 
                    WHEN Service_Component IN ('Shared Lives', 'Community supported living', 'Extra care housing') 
                    THEN 1 ELSE 0 END) DESC, --selects service components we can map to accommodations over those we can't 
                  Event_Start_Date DESC  --latest event start as last resort
            ) AS Rn
        FROM #Unknown_IDs_Service_Details)
    SELECT 
      LA_Code, 
      LA_Name, 
      Der_NHS_LA_Combined_Person_ID, 
      Service_Type, 
      Service_Component
    INTO #Service_Row1
    FROM LatestService
    WHERE Rn = 1


    --Create table of distinct people (dups exist where people have multiple services with the same dates/types/components)
    --Anyone with latest services in 2 or more of shared lives, community supported living and extra care housing, the service component overwritten to unknown
    --as we are unable to determine which service is accurate and be used as the accommodation status
    DROP TABLE IF EXISTS #Unknowns_Services_Deduped
    SELECT 
      LA_Code,
      LA_Name, 
      Der_NHS_LA_Combined_Person_ID,
      Service_Type,
      CASE 
        WHEN COUNT(DISTINCT Service_Component) = 1 --only output service component when the latest is unqiue and in the list below, otherwise null (can't be mapped)
        THEN MAX(CASE WHEN Service_Component IN ('Shared Lives', 'Community supported living', 'Extra care housing') THEN Service_Component ELSE NULL END)
        ELSE NULL
      END AS Service_Component
    INTO #Unknowns_Services_Deduped
    FROM #Service_Row1
    GROUP BY 
      LA_Code,
      LA_Name, 
      Der_NHS_LA_Combined_Person_ID,
      Service_Type;


    -- Mapping service information to accommodation status for unknown/invalid data
    DROP TABLE IF EXISTS #Unknowns_Services_Mapped
    SELECT 
      *,
      CASE 
        WHEN  Accommodation_Status IN ('Unknown - at home', 
                                        'Sheltered housing, extra care housing or other sheltered housing', 
                                        'Shared Lives scheme', 'Supported accommodation / supported lodgings / supported group home') THEN 1
        WHEN  Accommodation_Status IN ('Unknown', 'Registered Care Home or Registered Nursing Home', 'Prison / Young offenders institution / detention centre')  THEN 0
        ELSE 0
        END AS Accommodation_Status_Group
      INTO #Unknowns_Services_Mapped
      FROM (
        SELECT
          LA_Code,
          LA_Name,
          Der_NHS_LA_Combined_Person_ID,
          Service_Type,
          Service_Component,
          CASE 
            WHEN Service_Type = 'Long Term Support: Nursing Care' THEN 'Registered nursing home'
            WHEN Service_Type = 'Long Term Support: Residential Care' THEN 'Registered care home'
            WHEN Service_Component = 'Shared Lives' THEN 'Shared Lives scheme'
            WHEN Service_Component = 'Extra care housing' THEN 'Sheltered housing, extra care housing or other sheltered housing'
            WHEN Service_Component = 'Community supported living' THEN 'Supported accommodation / supported lodgings / supported group home'
            WHEN Service_Type = 'Long Term Support: Community' THEN 'Unknown - at home'
            WHEN Service_Type = 'Long Term Support: Prison' THEN 'Prison / Young offenders institution / detention centre'
            ELSE 'Unknown'
          END AS Accommodation_Status
          FROM #Unknowns_Services_Deduped )A


    ----------------------------------------------
    ------------- Form final table ---------------
    ----------------------------------------------

    -- Find all known accomodation status from original table'
    DROP TABLE IF EXISTS #ASCOF_2E_Final
    SELECT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      a.Accommodation_Status,
      Gender,
      Latest_Age,
      Latest_Age_Band,
      b.Accommodation_Status_Group
    INTO #ASCOF_2E_Final
    FROM #ASCOF_2E_Person_Details a
    LEFT JOIN #REF_Accommodation_Status b
      ON a.Accommodation_Status = b.Accommodation_Status
    WHERE a.Accommodation_Status != 'Unknown' 

    UNION ALL
    
    -- Mapped values, pulling in their demographics
    SELECT
    a.LA_Code,
    a.LA_Name,
    a.Der_NHS_LA_Combined_Person_ID,
    a.Accommodation_Status,
    b.Gender,
    b.Latest_Age,
    b.Latest_Age_Band,
    a.Accommodation_Status_Group
    FROM #Unknowns_Services_Mapped a
    LEFT JOIN ASC_Sandbox.InputTable2 b --
        ON a.LA_Code = b.LA_Code
        AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID


    ----------------------------------------------------------------
    ------------- Create numerators and denominators ---------------
    ----------------------------------------------------------------
    
    -- Numerator
    DROP TABLE IF EXISTS #Numerator
    SELECT 
    a.LA_Code, 
    a.LA_Name,
    COALESCE (Gender, 'Total') as Gender, --the result of the ROLLUP is Gender = Null so needs replacing with total
    a.Latest_Age_Band,
    COUNT(DISTINCT a.Der_NHS_LA_Combined_Person_ID) AS Numerator 
    INTO #Numerator
    FROM #ASCOF_2E_Final a
    WHERE Accommodation_Status_Group = 1
    GROUP BY 
      LA_Code, 
      LA_Name,
      ROLLUP(Gender),  --ROLLUP used to output an overall total
      Latest_Age_Band;

    -- Denominator
    DROP TABLE IF EXISTS #Denominator;
    SELECT 
      LA_Code, 
      LA_Name,
      COALESCE (Gender, 'Total') as Gender,
      Latest_Age_Band,
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator 
    INTO #Denominator
    FROM #ASCOF_2E_Final
    GROUP BY 
      LA_Code, 
      LA_Name,
      ROLLUP(Gender), 
      Latest_Age_Band;

    --------------------------------------------------------------------------------------------
    ---- Create reference table which contains all combinations of LA, age group and gender ----
    --------------------------------------------------------------------------------------------

    -- Required to output null results
    DROP TABLE IF EXISTS #REF_Final_Format;
    CREATE TABLE #REF_Final_Format
      (Reporting_Period VARCHAR(200),
      LA_Code VARCHAR(200),
      LA_Name VARCHAR(200),
      Measure VARCHAR(200),
      [Description] VARCHAR(200),
      [Group] VARCHAR(200));
               
    IF @LD_Filter = 1
    BEGIN
        -- Add data
        INSERT INTO #REF_Final_Format (Reporting_Period, LA_Code, LA_Name, Measure, [Description], [Group])
        SELECT 
            FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
            LA_Code,
            LA_Name,
            'ASCOF 2E LD' AS Measure,
            'The proportion of people aged 18-64 who receive long-term support who live in their home or with family (%)' AS Description,
            Gender AS [Group]
        FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup a
        CROSS JOIN
        (
            SELECT DISTINCT Gender 
            FROM #Denominator
            WHERE Gender IN ('Male', 'Female', 'Total')
        ) b;
    END
    ELSE
    BEGIN
        -- Add values
        INSERT INTO #REF_Final_Format (Reporting_Period, LA_Code, LA_Name, Measure, [Description], [Group])
        SELECT 
            FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
            LA_Code,
            LA_Name,
            'ASCOF 2E' AS Measure,
            'The proportion of people aged 18-64 who receive long-term support who live in their home or with family (%)' AS [Description],
            Gender AS [Group]
        FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup v
        CROSS JOIN
        (
            SELECT DISTINCT Gender 
            FROM #Denominator
            WHERE Gender IN ('Male', 'Female', 'Total')
        ) a;

        INSERT INTO #REF_Final_Format (Reporting_Period, LA_Code, LA_Name, Measure, [Description], [Group])
        SELECT 
            FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
            LA_Code,
            LA_Name,
            'ASCOF 2E' AS Measure,
            'The proportion of people aged 65+ who receive long-term support who live in their home or with family (%)' AS [Description],
            Gender AS [Group]
        FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup v
        CROSS JOIN
        (
            SELECT DISTINCT Gender 
            FROM #Denominator
            WHERE Gender IN ('Male', 'Female', 'Total')
        ) a;
    END;

    ------------------------------------------------------------------------
    --- Create final output and join with reference table created above
    ------------------------------------------------------------------------

    --Final output
    DROP TABLE IF EXISTS #Final_Output

    SELECT 
      FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
      d.LA_Code,
      d.LA_Name,
      CASE 
        WHEN @LD_Filter = 1 THEN 'ASCOF 2E LD'
        ELSE 'ASCOF 2E' 
      END AS Measure,
      CASE 
        WHEN d.Latest_Age_Band = '18 to 64' --age isn't a column so set the correct descriptions
          THEN 'The proportion of people aged 18-64 who receive long-term support who live in their home or with family (%)'
        ELSE 'The proportion of people aged 65+ who receive long-term support who live in their home or with family (%)' 
      END AS [Description],
      d.Gender AS [Group],
      n.Numerator,
      d.Denominator,
      ROUND((CAST(n.Numerator AS FLOAT) / CAST(d.Denominator AS FLOAT)) * 100, 1) AS [Outcome]   --method as per previous
    INTO #Final_Output
    FROM #Denominator d
    LEFT JOIN #Numerator n  --starting table is denominator so can do a left join
      ON d.LA_Code = n.LA_Code
      AND d.LA_Name = n.LA_Name
      AND d.Gender = n.Gender
      AND d.Latest_Age_Band = n.Latest_Age_Band
    WHERE d.Gender IN ('Male', 'Female', 'Total'); --at the end remove the rows with genders not in this list (unknowns, others, invalids have been counted in the total)


    --Join with reference table to ensure all LAs are present in final output (with 0s if null)
    DROP TABLE IF EXISTS #OutputTable1
    
    SELECT 
      r.*,
      COALESCE(a.Numerator, 0) AS Numerator,
      COALESCE(a.Denominator, 0) AS Denominator,
      COALESCE(a.Outcome, 0) AS Outcome
    INTO #OutputTable1
    FROM #REF_Final_Format r
    LEFT JOIN #Final_Output a
    ON 
      r.LA_Code = a.LA_Code AND
      r.Reporting_Period = a.Reporting_Period AND
      r.Measure = a.Measure AND 
      r.[Description] = a.[Description] AND 
      r.[Group] = a.[Group]

    ------------------------------------------------------------------------
    ------------------  Output excluded counts  --------------------------
    ------------------------------------------------------------------------

    -- People in LTS with unknown, null or invalid accommodation status

    DROP TABLE IF EXISTS #Final_Unknowns
    
    SELECT 
      FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
      LA_Code,
      LA_Name,
      'ASCOF 2E' AS [Measure],
      'Accommodation status = unknown or invalid' AS [Description],
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS [Count]
    INTO #Final_Unknowns
    FROM #ASCOF_2E_Final
    WHERE (Accommodation_Status = 'Unknown'
      OR  Accommodation_Status NOT IN  
        (SELECT Accommodation_Status
          FROM #REF_Accommodation_Status)
      OR Accommodation_Status IS NULL)
    GROUP BY
      LA_Code,
      LA_Name;

        
     DROP TABLE IF EXISTS #OutputTable2
     SELECT 
       FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
       b.LA_Code,
       b.LA_Name,
       CASE 
         WHEN @LD_Filter = 1 THEN 'ASCOF 2E LD'
         ELSE 'ASCOF 2E' 
       END AS Measure,
       'Accommodation status = unknown or invalid' AS [Description],
       COALESCE (a.[Count],0) AS [Count]
     INTO #OutputTable2
     FROM #Final_Unknowns a
     RIGHT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup b
      ON a.LA_Code = b.LA_Code
      AND a.LA_Name = b.LA_Name
 

    ------------------------------------------------------------------------
    ---------------------- Save final output tables -----------------------
    ------------------------------------------------------------------------


    -- Save into output table, drop synonym
    
    SET @Query = 'SELECT * INTO ' + @OutputTable1 + ' FROM #OutputTable1'
    EXEC(@Query)

    SET @Query = 'SELECT * INTO ' + @OutputTable2 + ' FROM #OutputTable2'
    EXEC(@Query)


    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable1
    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable2

GO

/*
-----Example execution
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 1,  --Toggle on or off
  @InputTable1 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_240401_250331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk'
*/