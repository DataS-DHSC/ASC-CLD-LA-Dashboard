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
  -- This field is then used to choose the latest known status over unknown (even if the known ones are invalid)
  -- People with conflicting accommodation statuses within the same submission for the same LA and Event end date are overwritten to 'unknown' 
  -- Unknowns and invalids are included in the denominator


*/
----------------------------------------------------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2E

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2E
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


    -----------------------------------------------------------------------------------
    ------------------------ Create REF table --------------------------------
    -----------------------------------------------------------------------------------
    
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

    -----------------------------------------------------------------------------------
     ------------------ Select cohort for 2E  ---------------------------
     -----------------------------------------------------------------------------------

    -- Build the raw table of all Clients in scope of ASCOF 2E
    -- Uses variable LD_Filter which is declared at the top of the code to filter between LD and All cohorts. 
    -- Then declares variables @LD_PSR and @LD_Age which filter the dataset to PSR and age bands according to the measure

    DROP TABLE IF EXISTS #ASCOF_2E_Build
    SELECT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      Primary_Support_Reason
    INTO #ASCOF_2E_Build
    FROM ASC_Sandbox.InputTable1
    WHERE Service_Type_Cleaned in ('Long Term Support: Nursing Care', 'Long Term Support: Residential Care', 'Long Term Support: Community')
      AND (Event_Start_Date <= @ReportingPeriodEndDate AND Event_Start_Date IS NOT NULL) 
      AND (Der_Event_End_Date >= @ReportingPeriodStartDate or Der_Event_End_Date is NULL)
      AND (Date_of_Death >= @ReportingPeriodStartDate OR Date_of_Death is NULL)  
      AND (Der_Birth_Month IS NOT NULL and Der_Birth_Year IS NOT NULL)
      AND Client_Type = 'Service User'
      AND Der_Age_Event_Start_Date > 17 -- RJ kept in to avoid people <18 at the point of service from being counted (as their person table for age at reporting end could be 18)
      AND (@LD_PSR IS NULL OR Primary_Support_Reason = @LD_PSR) -- If @LD_Filter is 1, @LD_PSR is 'Learning Disability Support'. If @LD_Filter is 0, @LD_PSR is NULL, hence NULL=NULL so will just be ignored.

      -- Join to person details table
     DROP TABLE IF EXISTS #ASCOF_2E_Person_Details
     SELECT DISTINCT
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      a.Primary_Support_Reason,
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
        (@LD_Age IS NULL OR Latest_Age_Band = @LD_Age) -- If @LD_Filter is 1, @LD_Age is '18 to 64'. If @LD_Filter is 0, @LD_PSR is NULL, hence NULL=NULL so will just be ignored.
        AND Latest_Age_Band  NOT IN ('Under 18', 'Unknown'); -- AND Latest_Age_Band NOT IN ('Under 18', 'Unknown')  -- to remove younger group


      -- Numerator
      DROP TABLE IF EXISTS #Numerator
      SELECT 
      a.LA_Code, 
      a.LA_Name,
      COALESCE (Gender, 'Total') as Gender,
      a.Latest_Age_Band,
      COUNT(DISTINCT a.Der_NHS_LA_Combined_Person_ID) AS Numerator 
      INTO #Numerator
      FROM #ASCOF_2E_Person_Details a
      LEFT JOIN #REF_Accommodation_Status b
        ON a.Accommodation_Status = b.Accommodation_Status
      WHERE b.Accommodation_Status_Group = 1
      GROUP BY 
        LA_Code, 
        LA_Name,
        ROLLUP(Gender),
        Latest_Age_Band;

      -- Denominator
      -- Query counts person ID whilst grouping by LA, age, gender

      DROP TABLE IF EXISTS #Denominator;
      SELECT 
        LA_Code, 
        LA_Name,
        COALESCE (Gender, 'Total') as Gender,--the result of the ROLLUP is set to Gender = Null so needs replacing with total -- RJ replaced with coalesce
        Latest_Age_Band,
        COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator 
      INTO #Denominator
      FROM #ASCOF_2E_Person_Details
      GROUP BY 
        LA_Code, 
        LA_Name,
        ROLLUP(Gender), --ROLLUP means we get a sum of all genders (we don't need this for age or LA), unknowns, others still incl at this stage as we want them in the total
        Latest_Age_Band;

    -------------------------------------------------------------------------------------------
    ----Create reference table which contains all combinations of LA, age group and gender ----
    -------------------------------------------------------------------------------------------

    --Required to output null results
    -- Drop the temporary table if it exists
    -- Create the temporary table with the specified data
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
    FROM #ASCOF_2E_Person_Details
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
EXEC ASC_Sandbox.Create_ASCOF2E 
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31', 
  @LD_Filter = 1,  --Toggle on or off
  @InputTable1 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions', 
  @InputTable2 = 'ASC_Sandbox.CLD_230401_240331_SingleSubmissions_Latest_Person_Data',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk'
*/