----------------------------------------------------------------------------------------------------------------------------
/*ASCOF 2E (formerly 1G)
The proportion of people who receive long-term support who live in their home or with family
 -- Note that this can be filtered to LD or All PSR (parts 1+2)

Denominator = Number of Clients who received Long Term Support during the year
Numerator = Number of Clients who received Long Term Support during the year who are living on their own or with family
Outcome = Proportion(%) of Clients who received Long Term Support during the year who are living on their own or with family

New methodology:
  -- Person details (age, gender, accommodation status) now pull from a table of the latest known details per person
  -- It is specific to each JoinedSubmission, and needs to be run before the stored procedure is called in the main script.
  -- Therefore the accommodation status is taken at the end of the reporting period, rather than the latest long term service.

Pre-requisites:
  -- Latest person details script must be run - currently as part of CLAD project
  
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
  @InputTable AS NVARCHAR(100),
  @InputTable_PersonDetails AS VARCHAR(100),
  @OutputTable1 AS NVARCHAR(100),
  @OutputTable2 AS NVARCHAR(100)

AS         
          --SET NOCOUNT ON;
      DECLARE @Query NVARCHAR(MAX)
      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
      DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails
      SET @Query = N'DROP TABLE IF EXISTS ' + @OutputTable1 + '; 
                    DROP TABLE IF EXISTS ' + @OutputTable2 + '; 
                    CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + '; 
                    CREATE SYNONYM ASC_Sandbox.InputTable_PersonDetails FOR ' +  @InputTable_PersonDetails +';' 
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
      ('Unknown - presumed at home', 1,10),  --additional category derived from service information
      ('Rough sleeper / squatting',0,11),
      ('Night shelter / emergency hostel / direct access hostel',0,12),
      ('Refuge',0,13),
      ('Placed in temporary accommodation by the council (inc. homelessness resettlement)',0,14),
      ('Staying with family / friends as a short-term guest',0,15),
      ('Acute / long-term healthcare residential facility or hospital',0,16),
      ('Registered care home',0,17),
      ('Registered nursing home',0,18),
      ('Prison / Young offenders institution / detention centre',0,19),
      ('Other temporary accommodation',0,20),
      ('Unknown', 0,21),
      ('Unknown - presumed in community',0,22) --additional category derived from service information

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
      Der_Event_End_Date
    INTO #ASCOF_2E_Build
    FROM ASC_Sandbox.InputTable
    WHERE Client_Type_Cleaned IN ('Service User')--changed
      AND Event_Type = 'Service'
      AND Service_Type_Cleaned in ('Long Term Support: Nursing Care', 'Long Term Support: Residential Care', 'Long Term Support: Community', 'Long Term Support: Prison')
      AND Der_NHS_LA_Combined_Person_ID IS NOT NULL
      AND Event_Start_Date IS NOT NULL
      AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) --removes DQ issues of event end date prior to start date
      AND Event_Start_Date <= @ReportingPeriodEndDate
      AND (Der_Event_End_Date >= @ReportingPeriodStartDate OR Der_Event_End_Date IS NULL)

   --Set null end dates to future date for ease of processing
    UPDATE #ASCOF_2E_Build
    SET Der_Event_End_Date = 
    CASE 
        WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' 
        ELSE Der_Event_End_Date
    END;


    ---------------------------------------------------------------------
    ------------------- Pull through latest person details --------------
    ---------------------------------------------------------------------

      -- Join to the latest person details table
    DROP TABLE IF EXISTS #ASCOF_2E_Person_Details_temp

    SELECT DISTINCT
      a.LA_Code,
      a.LA_Name,
      a.Der_NHS_LA_Combined_Person_ID,
      a.Der_Event_End_Date,
      b.Accommodation_Status,
      b.Gender,
      b.Der_Birth_Date,
      b.Date_of_Death,
      CASE 
        WHEN a.Der_Event_End_Date >= @ReportingPeriodEndDate 
        THEN @ReportingPeriodEndDate
        ELSE a.Der_Event_End_Date
      END AS Capped_LTS_Service_End_Date, --for ongoing events end date is set to reporting end date
      b.Primary_Support_Reason
    INTO #ASCOF_2E_Person_Details_temp
    FROM #ASCOF_2E_Build a
    LEFT JOIN ASC_Sandbox.InputTable_PersonDetails b --
      ON a.LA_Code = b.LA_Code
        AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
    
    -- Need to set max capped end date for each person
    DROP TABLE IF EXISTS #Capped_end_dates
    SELECT *,
     MAX(Capped_LTS_Service_End_Date) OVER (PARTITION BY --For people with multiple services select the end date of their latest event to calculate age
                                       LA_Code, 
                                       Der_NHS_LA_Combined_Person_ID) AS Person_Capped_LTS_Service_End_Date
    INTO #Capped_end_dates
    FROM #ASCOF_2E_Person_Details_temp


    -- Create birth date and filter deaths
    DROP TABLE IF EXISTS #ASCOF_2E_Person_Details
    SELECT
        *,
      CASE
        WHEN Der_Birth_Date IS NULL THEN NULL
        ELSE FLOOR(DATEDIFF(DAY, Der_Birth_Date, Person_Capped_LTS_Service_End_Date) / 365.25) 
	    END AS Person_Age  --age at the end of their latest service (or end of reporting period for those with open services)
    INTO #ASCOF_2E_Person_Details
    FROM #Capped_end_dates
    WHERE
      (Date_of_Death >= @ReportingPeriodStartDate OR Date_of_Death is NULL) 
   --   AND Der_Birth_Date IS NOT NULL --removed this to align with LTS001a, including 'nulls/unknowns'
      AND 
   (@LD_PSR IS NULL OR Primary_Support_Reason = @LD_PSR) -- If @LD_Filter is 1, @LD_PSR is 'Learning Disability Support'. If @LD_Filter is 0, @LD_PSR is NULL, hence NULL=NULL so will just be ignored.
   

      DROP TABLE IF EXISTS #ASCOF_2E_Person_Details_Age

    SELECT *,
    CASE
        WHEN Person_Age BETWEEN 1 AND 17 THEN 'Under 18'
        WHEN Person_Age BETWEEN 18 AND 64 THEN '18 to 64'
        WHEN Person_Age >= 65 THEN '65 and above'
        ELSE 'Unknown'
      END AS Person_Working_Age_Band
    INTO #ASCOF_2E_Person_Details_Age
    FROM #ASCOF_2E_Person_Details
    WHERE (Person_Age >= 18 OR Person_Age IS NULL) --including those aged 18 and over or unknown age


    ----------------------------------------------
    ------------- Form final table ---------------
    ----------------------------------------------

    -- Find all known accomodation status from original table'
    DROP TABLE IF EXISTS #ASCOF_2E_Final
    SELECT DISTINCT
      LA_Code,
      LA_Name,
      Der_NHS_LA_Combined_Person_ID,
      a.Accommodation_Status,
      Gender,
      Person_Age,
      Person_Working_Age_Band,
      b.Accommodation_Status_Group
    INTO #ASCOF_2E_Final
    FROM #ASCOF_2E_Person_Details_Age a
    LEFT JOIN #REF_Accommodation_Status b
      ON a.Accommodation_Status = b.Accommodation_Status
    WHERE (@LD_Age IS NULL OR Person_Working_Age_Band = @LD_Age) -- If @LD_Filter is 1, @LD_Age is '18 to 64'. If @LD_Filter is 0, @LD_Age is NULL, hence NULL=NULL so will just be ignored.
    AND Person_Working_Age_Band NOT IN ('Under 18')

    ----------------------------------------------------------------
    ------------- Create numerators and denominators ---------------
    ----------------------------------------------------------------
    
    -- Numerator
    DROP TABLE IF EXISTS #Numerator
    SELECT 
    a.LA_Code, 
    a.LA_Name,
    COALESCE (Gender, 'Total') as Gender, --the result of the ROLLUP is Gender = Null so needs replacing with total
    a.Person_Working_Age_Band,
    COUNT(DISTINCT a.Der_NHS_LA_Combined_Person_ID) AS Numerator 
    INTO #Numerator
    FROM #ASCOF_2E_Final a
    WHERE Accommodation_Status_Group = 1
    AND Person_Working_Age_Band <> 'Unknown'
    GROUP BY 
      LA_Code, 
      LA_Name,
      ROLLUP(Gender),  --ROLLUP used to output an overall total
      Person_Working_Age_Band;

    -- Denominator
    DROP TABLE IF EXISTS #Denominator;
    SELECT 
      LA_Code, 
      LA_Name,
      COALESCE (Gender, 'Total') as Gender,
      Person_Working_Age_Band,
      COUNT(DISTINCT Der_NHS_LA_Combined_Person_ID) AS Denominator 
    INTO #Denominator
    FROM #ASCOF_2E_Final
    WHERE Person_Working_Age_Band <> 'unknown'
    GROUP BY 
      LA_Code, 
      LA_Name,
      ROLLUP(Gender), 
      Person_Working_Age_Band;

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
        WHEN d.Person_Working_Age_Band = '18 to 64' --age isn't a column so set the correct descriptions
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
      AND d.Person_Working_Age_Band = n.Person_Working_Age_Band
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
    WHERE (Accommodation_Status = 'Unknown' OR Accommodation_Status = 'Unknown - presumed at home' OR Accommodation_Status = 'Unknown - presumed in community'
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


    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails

GO

/*
-----Example execution
EXEC ASC_Sandbox.Create_ASCOF2E_2425_Onwards 
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31', 
  @LD_Filter = 1,  --Toggle on or off
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions', 
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable1 = 'ASC_Sandbox.ASCOF_2E_LD',
  @OutputTable2 = 'ASC_Sandbox.ASCOF_2E_LD_Unk'
*/