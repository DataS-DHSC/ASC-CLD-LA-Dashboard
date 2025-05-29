---------------------------------------------------------------------------
-- create_GetDerivedFields_procedure.sql
--
-- Create procedure to clean and derive key fields

-- - Cleans up gender, ethnicity, service type and event outcome fields
-- - Creates higher level groupings for ethnicity, service type, event
--   outcome and review reason
-- - Derives event outcome hierarchy
-- - Derives latest age, age band and working age band
-- - Creates a new person ID field: traced NHS number if present, else
--   LA-provided NHS number, else LA person ID (row excluded if none present)
-- - Derives a unique event reference (DHSC definition of a "unique" event)
--
-- Note:
-- - Input table must contain derived event end date - Der_Event_End_Date
--   (created by FilterToEventsInPeriod procedure) - and Ref_Period_Start_Date
--   /Ref_Period_End_Date. It must also contain the gender, ethnicity, service
--   type and event outcome fields.
--
-- Returns table of same format as input table plus derived fields and original
-- fields renamed ("_Raw")
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetDerivedFields
GO

CREATE PROCEDURE ASC_Sandbox.GetDerivedFields
  @InputTable SYSNAME = NULL,
  @OutputTable AS NVARCHAR(50)
AS
  SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable

  EXEC(@Query)
  
  ---------------------------------------------------------------------------
  -- - Clean up gender, ethnicity and service type fields
  -- - Create higher level groupings for ethnicity, service type, event
  --   outcome and review reason
  -- - Create a new person ID field
  -- - Derive a unique event reference (DHSC definition of a "unique" event)
  ---------------------------------------------------------------------------

  SELECT
    b.*,

    -- Derive a unique event reference (DHSC definition of a "unique" event)
    CONCAT(LA_Code, '_', DENSE_RANK() OVER (PARTITION BY
                                              LA_Code
                                            ORDER BY
                                              Event_Start_Date,
                                              (CASE WHEN Event_Type NOT LIKE '%service%' THEN Der_Event_End_Date END),
                                              Client_Type,
                                              Der_NHS_LA_Combined_Person_ID,
                                              Event_Type,
                                              (CASE WHEN Event_Type LIKE '%request%' THEN Request_Route_of_Access END),
                                              (CASE WHEN Event_Type LIKE '%assessment%' THEN Assessment_Type END),
                                              (CASE WHEN Event_Type LIKE '%service%' THEN Service_Type END),
                                              (CASE WHEN Event_Type LIKE '%service%' THEN Service_Component END))
          ) AS Der_Unique_Event_Ref
  INTO #Temp
  FROM (
    SELECT
      a.*,
      Gender AS Gender_Raw,
      Gender_Cleaned,
      Ethnicity AS Ethnicity_Raw,
      Ethnicity_Cleaned_R1 as Ethnicity_Cleaned,
      Event_Outcome AS Event_Outcome_Raw,
      Event_Outcome_Cleaned_R1 as Event_Outcome_Cleaned,
      Event_Outcome_Hierarchy,
      Service_Type AS Service_Type_Raw,
      Service_Type_Cleaned_R1 as Service_Type_Cleaned,

      -- Derive high level categories
      Ethnicity_Grouped_R1 as Ethnicity_Grouped,
      Event_Outcome_Grouped_R1 as Event_Outcome_Grouped,
      Service_Type_Grouped_R1 as Service_Type_Grouped,

      -- Derive review type field
      CASE
        WHEN Review_Reason LIKE '%unplanned%' AND Event_Type = 'Review' THEN 'Unplanned'
        WHEN Review_Reason LIKE 'planned%' AND Event_Type = 'Review' THEN 'Planned'
        WHEN (Review_Reason IS NULL OR Review_Reason = '') AND Event_Type = 'Review' THEN NULL
        WHEN Event_Type NOT LIKE '%Review%' THEN NULL
        ELSE 'Review Type Unknown'
      END AS Review_Type,

    -- Create a new person ID field: traced NHS number if present, else LA-provided NHS number, else LA person ID
    -- (row excluded if none present)
    CASE
      WHEN Der_NHS_Number_Traced_Pseudo IS NOT NULL THEN Der_NHS_Number_Traced_Pseudo
      WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NOT NULL THEN Der_NHS_Number_Pseudo
      WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NULL THEN CONCAT(LA_Code, '_', LA_Person_Unique_Identifier)
    END AS Der_NHS_LA_Combined_Person_ID

    FROM ASC_Sandbox.InputTable AS a
    LEFT JOIN ASC_Sandbox.REF_Ethnicity_Mapping e
    ON a.Ethnicity = e.Ethnicity_Raw
    LEFT JOIN ASC_Sandbox.REF_Gender_Mapping g
    ON a.Gender = g.Gender_Raw
    LEFT JOIN ASC_Sandbox.REF_Service_Type_Mapping st
    ON a.Service_Type = st.Service_Type_Raw
    LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Mapping eo
    ON a.Event_Outcome = eo.Event_Outcome_Raw
    LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Hierarchy eoh
    ON eo.Event_Outcome_Cleaned_R1 = eoh.Event_Outcome_Spec
  ) b

  -- Drop original Gender, Ethnicity, Service_Type and Event_Outcome fields to highlight "_Raw" / "_Cleaned" fields
  ALTER TABLE #Temp
  DROP COLUMN Gender, Ethnicity, Service_Type, Event_Outcome ;

  ---------------------------------------------------------------------------
  -- Close services with missing or incorrect end dates (before deriving ages)
  ---------------------------------------------------------------------------
  -- Services that appear to be ongoing at the end of a reference period should be found in the next period.
  -- If not found we assume the event end date is erroneously missing or incorrect and populate it with the
  -- reference period end date. Note this applies only to open services associated with reference periods
  -- where another reference period follows – i.e. open services associated with the last reference period
  -- remain open.

  -- Identify ref periods for each LA

  SELECT DISTINCT LA_Name, Ref_Period_Start_Date, Ref_Period_End_Date
  INTO #RefPeriods
  FROM #Temp

  -- Identify all "prior" ref periods, i.e. where another ref period follows

  SELECT LA_Name, Ref_Period_Start_Date, Ref_Period_End_Date
  INTO #PriorRefPeriods
  FROM #RefPeriods
  WHERE CONCAT(LA_Name, Ref_Period_Start_Date) NOT IN (
    SELECT CONCAT(LA_Name, MAX(Ref_Period_Start_Date))
    FROM #RefPeriods GROUP BY LA_Name)

  -- Identify services ongoing at end of period and not found in next period

  SELECT Der_Unique_Record_ID
  INTO #RecordIDs
  FROM (
    -- open services in each ref period (excl latest ref period)
    SELECT Ref_Period_End_Date, Der_Unique_Event_Ref, Der_Event_End_Date, Der_Unique_Record_ID
    FROM #Temp
    WHERE CONCAT(LA_Name, Ref_Period_Start_Date, Ref_Period_End_Date) IN (
      SELECT CONCAT(LA_Name, Ref_Period_Start_Date, Ref_Period_End_Date)
      FROM #PriorRefPeriods)
    AND (Der_Event_End_Date IS NULL OR Der_Event_End_Date > Ref_Period_End_Date)
  ) p
  -- join with events in the next ref period
  LEFT JOIN (
    SELECT DISTINCT Ref_Period_Start_Date, Der_Unique_Event_Ref
    FROM #Temp
  ) next_p
  ON DATEADD(day, 1, p.Ref_Period_End_Date) = next_p.Ref_Period_Start_Date
  AND p.Der_Unique_Event_Ref = next_p.Der_Unique_Event_Ref
  -- select record IDs for events not found in the next ref period
  WHERE next_p.Der_Unique_Event_Ref IS NULL

  -- Replace the event end date for the above records

  UPDATE #Temp
  SET Der_Event_End_Date = Ref_Period_End_Date
  WHERE Der_Unique_Record_ID IN (SELECT Der_Unique_Record_ID FROM #RecordIDs)

  ---------------------------------------------------------------------------
  -- Derive latest age, age band and working age band
  ---------------------------------------------------------------------------

  SELECT
    *,
  
    -- Derive age bands
    CASE
      WHEN Der_Latest_Age < 18 THEN 'Under 18'
      WHEN Der_Latest_Age BETWEEN 18 AND 24 THEN '18 to 24'
      WHEN Der_Latest_Age BETWEEN 25 AND 44 THEN '25 to 44'
      WHEN Der_Latest_Age BETWEEN 45 AND 64 THEN '45 to 64'
      WHEN Der_Latest_Age BETWEEN 65 AND 74 THEN '65 to 74'
      WHEN Der_Latest_Age BETWEEN 75 AND 84 THEN '75 to 84'
      WHEN Der_Latest_Age BETWEEN 85 AND 94 THEN '85 to 94'
      WHEN Der_Latest_Age >= 95 THEN '95 and above'
      ELSE 'Unknown'
    END AS Der_Age_Band,

    -- Derive working age bands
    CASE
      WHEN Der_Latest_Age < 18 THEN 'Under 18'
      WHEN Der_Latest_Age BETWEEN 18 AND 64 THEN '18 to 64'
      WHEN Der_Latest_Age >= 65 THEN '65 and above'
      ELSE 'Unknown'
    END AS Der_Working_Age_Band

  INTO #Temp_der_age
  FROM (
    SELECT
      *,

      -- Derive latest age at the end of an event (or reference period if a service without an end date)
      CASE
        -- requests, assessments and reviews use event end date
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type not like '%service%' 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25)
        -- services use event end date when not null
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type like '%service%' AND Der_Event_End_Date IS NOT NULL 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25)
        -- services use reference period end date when event end date is null
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type like '%service%' AND Der_Event_End_Date IS NULL 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Ref_Period_End_Date))/365.25)
        ELSE NULL 
      END AS Der_Latest_Age
      FROM #Temp
    ) c

  ---------------------------------------------------------------------------
  -- Map observation from release 2 to release 1
  -- These reference tables do not yet have complete cleaning, so they take cleaned variable if possible, if not raw variable
  -- Complete cleaning to be implemented at later date
  -- Variables named cleaned to prevent variable names in code having to be updated multiple time in a short period of time
  ---------------------------------------------------------------------------

    select 
        d.*,
        case when Client_Funding_Status_Cleaned_R1 is not null then Client_Funding_Status_Cleaned_R1
        else Client_Funding_Status end as Client_Funding_Status_Cleaned,
        Client_Funding_Status as Client_Funding_Status_Raw,

        case when Client_Type_Cleaned_R1 is not null then Client_Type_Cleaned_R1
        else Client_Type end as Client_Type_Cleaned,
        Client_Type as Client_Type_Raw,

        case when Delivery_Mechanism_Cleaned_R1 is not null then Delivery_Mechanism_Cleaned_R1
        else Delivery_Mechanism end as Delivery_Mechanism_Cleaned,
        Delivery_Mechanism as Delivery_Mechanism_Raw,

        case when Assessment_Type_Cleaned_R1 is not null then Assessment_Type_Cleaned_R1
        else Assessment_Type end as Assessment_Type_Cleaned,
        Assessment_Type as Assessment_Type_Raw,

        case when Hearing_Impairment_Cleaned_R1 is not null then Hearing_Impairment_Cleaned_R1
        else Hearing_Impairment end as Hearing_Impairment_Cleaned,
        Hearing_Impairment as Hearing_Impairment_Raw,

        case when Method_Of_Assessment_Cleaned_R1 is not null then Method_Of_Assessment_Cleaned_R1
        else Method_Of_Assessment end as Method_of_Assessment_Cleaned,
        Method_Of_Assessment as Method_of_Assessment_Raw,

        case when Method_Of_Review_Cleaned_R1 is not null then Method_Of_Review_Cleaned_R1
        else Method_Of_Review end as Method_of_Review_Cleaned,
        Method_Of_Review as Method_of_Review_Raw,

        case when Primary_Support_Reason_Cleaned_R1 is not null then Primary_Support_Reason_Cleaned_R1
        else Primary_Support_Reason end as Primary_Support_Reason_Cleaned,
        Primary_Support_Reason as Primary_Support_Reason_Raw,

        case when Request_Route_Of_Access_Cleaned_R1 is not null then Request_Route_Of_Access_Cleaned_R1
        else Request_Route_Of_Access end as Request_Route_Of_Access_Cleaned,
        Request_Route_Of_Access as Request_Route_Of_Access_Raw,

        case when Review_Outcomes_Achieved_Cleaned_R1 is not null then Review_Outcomes_Achieved_Cleaned_R1
        else Review_Outcomes_Achieved end as Review_Outcomes_Achieved_Cleaned,
        Review_Outcomes_Achieved as Review_Outcomes_Achieved_Raw,

        case when Review_Reason_Cleaned_R1 is not null then Review_Reason_Cleaned_R1
        else Review_Reason end as Review_Reason_Cleaned,
        Review_Reason as Review_Reason_Raw,

        case when Accommodation_Status_Cleaned_R1 is not null then Accommodation_Status_Cleaned_R1
        else Accommodation_Status end as Accommodation_Status_Cleaned,
        Accommodation_Status as Accommodation_Status_Raw,

        case when Cost_Frequency_Unit_Type_Cleaned_R1 is not null then Cost_Frequency_Unit_Type_Cleaned_R1
        else Cost_Frequency_Unit_Type end as Cost_Frequency_Unit_Type_Cleaned,
        Cost_Frequency_Unit_Type as Cost_Frequency_Unit_Type_Raw,

        case when Employment_Status_Cleaned_R1 is not null then Employment_Status_Cleaned_R1
        else Employment_Status end as Employment_Status_Cleaned,
        Employment_Status as Employment_Status_Raw,

        case when Service_Component_Cleaned_R1 is not null then Service_Component_Cleaned_R1
        else Service_Component end as Service_Component_Cleaned,
        Service_Component as Service_Component_Raw,

        case when Total_Hrs_Caring_Per_Week_Cleaned_R1 is not null then Total_Hrs_Caring_Per_Week_Cleaned_R1
        else Total_Hrs_Caring_Per_Week end as Total_Hrs_Caring_Per_Week_Cleaned,
        Total_Hrs_Caring_Per_Week as Total_Hrs_Caring_Per_Week_Raw,
        Total_Hrs_Caring_Per_Week_Cleaned_R2,

        case when Visual_Impairment_Cleaned_R1 is not null then Visual_Impairment_Cleaned_R1
        else Visual_Impairment end as Visual_Impairment_Cleaned,
        Visual_Impairment as Visual_Impairment_Raw

    into #OutputTable

    from #Temp_der_age as d left join
        ASC_Sandbox.REF_Client_Funding_Status_Mapping as cfs
        on d.Client_Funding_Status = cfs.Client_Funding_Status_Raw left join
        ASC_Sandbox.REF_Client_Type_Mapping as ct
        on d.Client_Type = ct.Client_Type_Raw left join
        ASC_Sandbox.REF_Delivery_Mechanism_Mapping as dm
        on d.Delivery_Mechanism = dm.Delivery_Mechanism_Raw left join
        ASC_Sandbox.REF_Assessment_Type_Mapping as aty
        on d.Assessment_Type = aty.Assessment_Type_Raw left join 
        ASC_Sandbox.REF_Hearing_Impairment_Mapping as hi
        on d.Hearing_Impairment = hi.Hearing_Impairment_Raw left join
        ASC_Sandbox.REF_Method_Of_Assessment_Mapping as moa
        on d.Method_Of_Assessment = moa.Method_Of_Assessment_Raw left join
        ASC_Sandbox.REF_Method_Of_Review_Mapping as mor
        on d.Method_Of_Review = mor.Method_Of_Review_Raw left join
        ASC_Sandbox.REF_Primary_Support_Reason_Mapping as psr
        on d.Primary_Support_Reason = psr.Primary_Support_Reason_Raw left join
        ASC_Sandbox.REF_Request_Route_of_Access_Mapping as rra
        on d.Request_Route_Of_Access = rra.Request_Route_Of_Access_Raw left join
        ASC_Sandbox.REF_Review_Outcomes_Achieved_Mapping as roa
        on d.Review_Outcomes_Achieved = roa.Review_Outcomes_Achieved_Raw left join
        ASC_Sandbox.REF_Review_Reason_Mapping as rr
        on d.Review_Reason = rr.Review_Reason_Raw left join
        ASC_Sandbox.REF_Accommodation_Status_Mapping as accom 
        on d.Accommodation_Status = accom.Accommodation_Status_Raw left join
        ASC_Sandbox.REF_Cost_Frequency_Mapping as cf
        on d.Cost_Frequency_Unit_Type = cf.Cost_Frequency_Unit_Type_Raw left join
        ASC_Sandbox.REF_Employment_Status_Mapping as es
        on d.Employment_Status = es.Employment_Status_Raw left join
        ASC_Sandbox.REF_Service_Component_Mapping as sc
        on d.Service_Component = sc.Service_Component_Raw left join
        ASC_Sandbox.REF_Total_Hrs_Caring_Per_Week_Mapping as tot 
        on d.Total_Hrs_Caring_Per_Week = tot.Total_Hrs_Caring_Per_Week_Raw left join
        ASC_Sandbox.REF_Visual_Impairment_Mapping as vi
        on d.Visual_Impairment = vi.Visual_Impairment_Raw
   

    -- Drop original [variable] to highlight copy named [variable]_raw and cleaned version [varaible]_cleaned 
    alter table #OutputTable
    drop column Client_Funding_Status, Client_Type, Delivery_Mechanism, Assessment_Type, Method_Of_Assessment, Method_Of_Review, Primary_Support_Reason, Request_Route_Of_Access, Review_Outcomes_Achieved, Review_Reason, Accommodation_Status, Cost_Frequency_Unit_Type, Employment_Status, Service_Component, Total_Hrs_Caring_Per_Week, Visual_Impairment

  SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'

  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

  ---------------------------------------------------------------------------
  -- Check for new invalid values (that need adding to mapping tables)
  ---------------------------------------------------------------------------

  SELECT DISTINCT Ethnicity_Raw
  INTO #InvalidEthnicities
  FROM #OutputTable
  WHERE NULLIF(Ethnicity_Raw, '') IS NOT NULL AND Ethnicity_Cleaned IS NULL

  SELECT DISTINCT Service_Type_Raw
  INTO #InvalidServiceTypes
  FROM #OutputTable
  WHERE NULLIF(Service_Type_Raw, '') IS NOT NULL AND Service_Type_Cleaned IS NULL

  SELECT DISTINCT Gender_Raw
  INTO #InvalidGenders
  FROM #OutputTable
  WHERE NULLIF(Gender_Raw, '') IS NOT NULL AND Gender_Cleaned IS NULL

  SELECT DISTINCT Event_Outcome_Raw
  INTO #InvalidEventOutcomes
  FROM #OutputTable
  WHERE NULLIF(Event_Outcome_Raw, '') IS NOT NULL AND Event_Outcome_Cleaned IS NULL

  IF EXISTS (SELECT * FROM #InvalidGenders)
    OR EXISTS (SELECT * FROM #InvalidEthnicities)
    OR EXISTS (SELECT * FROM #InvalidServiceTypes)
    OR EXISTS (SELECT * FROM #InvalidEventOutcomes)
  BEGIN

    IF EXISTS (SELECT * FROM #InvalidGenders)
      SELECT Gender_Raw 'NEW INVALID GENDERS FOUND - add to data field mapping xlsx, re-write REF_ table and re-run GetDerivedFields:'
      FROM #InvalidGenders ORDER BY Gender_Raw

    IF EXISTS (SELECT * FROM #InvalidEthnicities)
      SELECT Ethnicity_Raw 'NEW INVALID ETHNICITIES FOUND - add to data field mapping xlsx, re-write REF_ table and re-run GetDerivedFields:'
      FROM #InvalidEthnicities ORDER BY Ethnicity_Raw

    IF EXISTS (SELECT * FROM #InvalidServiceTypes)
      SELECT Service_Type_Raw 'NEW INVALID SERVICE TYPES FOUND - add to data field mapping xlsx, re-write REF_ table and re-run GetDerivedFields:'
      FROM #InvalidServiceTypes ORDER BY Service_Type_Raw

    IF EXISTS (SELECT * FROM #InvalidEventOutcomes)
      SELECT Event_Outcome_Raw 'NEW INVALID EVENT OUTCOMES FOUND - add to data field mapping xlsx, re-write REF_ table and re-run GetDerivedFields:'
      FROM #InvalidEventOutcomes ORDER BY Event_Outcome_Raw

    RETURN 
  END
GO

---- Example execution:
--EXEC ASC_Sandbox.GetDerivedFields @InputTable = 'ASC_Sandbox.Temp_EventsInPeriod', @OutputTable = 'ASC_Sandbox.Temp_DerivedFields'
