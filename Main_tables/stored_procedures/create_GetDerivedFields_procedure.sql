---------------------------------------------------------------------------
-- create_GetDerivedFields_procedure.sql
--
-- Create procedure to clean and derive key fields

-- - Converts text string 'NULL' to true NULL for fields affected
-- - Cleans almost all specification fields (except Autism spectrum disorder
--   dementia and eligible needs identified)
-- - Creates higher level groupings for ethnicity, service type, event
--   outcome and review reason (updated for 25/26 Q2 main tables)
-- - Derives latest age, age band and working age band
-- - Creates a new person ID field: traced NHS number if present, else
--   LA-provided NHS number, else LA person ID (row excluded if none present)
-- - Derives a unique event reference (DHSC definition of a "unique" event)
-- - Maps all data to release 2 specification
--
-- Note:
-- - Input table must be output by FilterToEventsInPeriod procedure - i.e. 
--   contain all release 2 specification fields, reference period start and
--   end dates, derived event end date and ONS date of death fields.
--
-- Returns table of same format as input table plus derived fields and original
-- fields renamed ("_Raw")
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetDerivedFields
GO

CREATE PROCEDURE ASC_Sandbox.GetDerivedFields
  @InputTable SYSNAME = NULL,
  @OutputTable AS NVARCHAR(100)
AS
  SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_GetDerivedFields
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable_GetDerivedFields FOR ' + @InputTable

  EXEC(@Query)
  
  ---------------------------------------------------------------------------
  -- Convert text string 'NULL' to true NULL across columns affected
  ---------------------------------------------------------------------------

  SELECT *
  INTO #Nulled
  FROM ASC_Sandbox.InputTable_GetDerivedFields;

  UPDATE #Nulled
  SET
    Accommodation_Status         = NULLIF(Accommodation_Status, 'NULL'),
    Adult_1_Linked_Person_ID     = NULLIF(Adult_1_Linked_Person_ID, 'NULL'),
    Adult_2_Linked_Person_ID     = NULLIF(Adult_2_Linked_Person_ID, 'NULL'),
    Adult_3_Linked_Person_ID     = NULLIF(Adult_3_Linked_Person_ID, 'NULL'),
    Assessment_Type              = NULLIF(Assessment_Type, 'NULL'),
    Autism_Spectrum_Disorder_ASD = NULLIF(Autism_Spectrum_Disorder_ASD, 'NULL'),
    Client_Funding_Status        = NULLIF(Client_Funding_Status, 'NULL'),
    Client_Type                  = NULLIF(Client_Type, 'NULL'),
    Cost_Frequency_Unit_Type     = NULLIF(Cost_Frequency_Unit_Type, 'NULL'),
    Delivery_Mechanism           = NULLIF(Delivery_Mechanism, 'NULL'),
    Dementia                     = NULLIF(Dementia, 'NULL'),
    Eligible_Needs_Identified    = NULLIF(Eligible_Needs_Identified, 'NULL'),
    Employment_Status            = NULLIF(Employment_Status, 'NULL'),
    Ethnicity                    = NULLIF(Ethnicity, 'NULL'),
    Event_Outcome                = NULLIF(Event_Outcome, 'NULL'),
    Event_Type                   = NULLIF(Event_Type, 'NULL'),
    Gender                       = NULLIF(Gender, 'NULL'),
    GP_Practice_Code             = NULLIF(GP_Practice_Code, 'NULL'),
    Has_Unpaid_Carer             = NULLIF(Has_Unpaid_Carer, 'NULL'),
    Hearing_Impairment           = NULLIF(Hearing_Impairment, 'NULL'),
    Method_Of_Assessment         = NULLIF(Method_Of_Assessment, 'NULL'),
    Method_Of_Review             = NULLIF(Method_Of_Review, 'NULL'),
    Primary_Support_Reason       = NULLIF(Primary_Support_Reason, 'NULL'),
    Request_Route_Of_Access      = NULLIF(Request_Route_Of_Access, 'NULL'),
    Review_Outcomes_Achieved     = NULLIF(Review_Outcomes_Achieved, 'NULL'),
    Review_Reason                = NULLIF(Review_Reason, 'NULL'),
    Service_Component            = NULLIF(Service_Component, 'NULL'),
    Service_Type                 = NULLIF(Service_Type, 'NULL'),
    Total_Hrs_Caring_Per_Week    = NULLIF(Total_Hrs_Caring_Per_Week, 'NULL'),
    Visual_Impairment            = NULLIF(Visual_Impairment, 'NULL')
  WHERE 'NULL' IN (
    Accommodation_Status,
    Adult_1_Linked_Person_ID,
    Adult_2_Linked_Person_ID,
    Adult_3_Linked_Person_ID,
    Assessment_Type,
    Autism_Spectrum_Disorder_ASD,
    Client_Funding_Status,
    Client_Type,
    Cost_Frequency_Unit_Type,
    Delivery_Mechanism,
    Dementia,
    Eligible_Needs_Identified,
    Employment_Status,
    Ethnicity,
    Event_Outcome,
    Event_Type,
    Gender,
    GP_Practice_Code,
    Has_Unpaid_Carer,
    Hearing_Impairment,
    Method_Of_Assessment,
    Method_Of_Review,
    Primary_Support_Reason,
    Request_Route_Of_Access,
    Review_Outcomes_Achieved,
    Review_Reason,
    Service_Component,
    Service_Type,
    Total_Hrs_Caring_Per_Week,
    Visual_Impairment
  );

  ---------------------------------------------------------------------------
  -- Join with mapping tables to get cleaned fields and high level groupings
  ---------------------------------------------------------------------------

  SELECT
    a.*,

    Accommodation_Status                     AS Accommodation_Status_Raw,
    accom.Accommodation_Status_Cleaned_R2    AS Accommodation_Status_Cleaned,

    Assessment_Type                          AS Assessment_Type_Raw,
    aty.Assessment_Type_Cleaned_R2           AS Assessment_Type_Cleaned,

    Client_Funding_Status                    AS Client_Funding_Status_Raw,
    cfs.Client_Funding_Status_Cleaned_R2     AS Client_Funding_Status_Cleaned,

    Client_Type                              AS Client_Type_Raw,
    ct.Client_Type_Cleaned_R2                AS Client_Type_Cleaned,

    Cost_Frequency_Unit_Type                 AS Cost_Frequency_Unit_Type_Raw,
    cf.Cost_Frequency_Unit_Type_Cleaned_R2   AS Cost_Frequency_Unit_Type_Cleaned,

    Delivery_Mechanism                       AS Delivery_Mechanism_Raw,
    dm.Delivery_Mechanism_Cleaned_R2         AS Delivery_Mechanism_Cleaned,

    Employment_Status                        AS Employment_Status_Raw,
    Employment_Status_Cleaned_R2             AS Employment_Status_Cleaned,

    Ethnicity                                AS Ethnicity_Raw,
    e.Ethnicity_Cleaned_R2                   AS Ethnicity_Cleaned,
    e.Ethnicity_Grouped_R2                   AS Ethnicity_Grouped,

    Event_Outcome                            AS Event_Outcome_Raw,
    eo.Event_Outcome_Cleaned_R2              AS Event_Outcome_Cleaned,
    eo.Event_Outcome_Grouped_R2              AS Event_Outcome_Grouped,
    eoh.Event_Outcome_Hierarchy,

    Event_Type                               AS Event_Type_Raw,
    et.Event_Type_Cleaned                    AS Event_Type_Cleaned,

    Gender                                   AS Gender_Raw,
    g.Gender_Cleaned                         AS Gender_Cleaned,

    Hearing_Impairment                       AS Hearing_Impairment_Raw,
    hi.Hearing_Impairment_Cleaned_R2         AS Hearing_Impairment_Cleaned,

    Method_Of_Assessment                     AS Method_Of_Assessment_Raw,
    moa.Method_Of_Assessment_Cleaned_R2      AS Method_Of_Assessment_Cleaned,

    Method_Of_Review                         AS Method_Of_Review_Raw,
    mor.Method_Of_Review_Cleaned_R2          AS Method_Of_Review_Cleaned,

    Primary_Support_Reason                   AS Primary_Support_Reason_Raw,
    psr.Primary_Support_Reason_Cleaned_R2    AS Primary_Support_Reason_Cleaned,

    Request_Route_of_Access                  AS Request_Route_Of_Access_Raw,
    rra.Request_Route_Of_Access_Cleaned_R2   AS Request_Route_Of_Access_Cleaned,

    Review_Outcomes_Achieved                 AS Review_Outcomes_Achieved_Raw,
    roa.Review_Outcomes_Achieved_Cleaned_R2  AS Review_Outcomes_Achieved_Cleaned,

    Review_Reason                            AS Review_Reason_Raw,
    rr.Review_Reason_Cleaned_R2              AS Review_Reason_Cleaned,

    Service_Component                        AS Service_Component_Raw,
    sc.Service_Component_Cleaned_R2          AS Service_Component_Cleaned,

    Service_Type                             AS Service_Type_Raw,
    st.Service_Type_Cleaned_R2               AS Service_Type_Cleaned,
    st.Service_Type_Grouped_R2               AS Service_Type_Grouped,

    Total_Hrs_Caring_per_week                AS Total_Hrs_Caring_Per_Week_Raw,
    tot.Total_Hrs_Caring_Per_Week_Cleaned_R2 AS Total_Hrs_Caring_Per_Week_Cleaned,

    Visual_Impairment                        AS Visual_Impairment_Raw,
    vi.Visual_Impairment_Cleaned_R2          AS Visual_Impairment_Cleaned

  INTO #Mapped
  FROM #Nulled a

  LEFT JOIN ASC_Sandbox.REF_Accommodation_Status_Mapping accom
  ON a.Accommodation_Status = accom.Accommodation_Status_Raw

  LEFT JOIN ASC_Sandbox.REF_Assessment_Type_Mapping aty
  ON a.Assessment_Type = aty.Assessment_Type_Raw

  LEFT JOIN ASC_Sandbox.REF_Client_Funding_Status_Mapping cfs
  ON a.Client_Funding_Status = cfs.Client_Funding_Status_Raw

  LEFT JOIN ASC_Sandbox.REF_Client_Type_Mapping ct
  ON a.Client_Type = ct.Client_Type_Raw

  LEFT JOIN ASC_Sandbox.REF_Cost_Frequency_Unit_Type_Mapping cf
  ON a.Cost_Frequency_Unit_Type = cf.Cost_Frequency_Unit_Type_Raw

  LEFT JOIN ASC_Sandbox.REF_Delivery_Mechanism_Mapping dm
  ON a.Delivery_Mechanism = dm.Delivery_Mechanism_Raw

  LEFT JOIN ASC_Sandbox.REF_Employment_Status_Mapping es
  ON a.Employment_Status = es.Employment_Status_Raw

  LEFT JOIN ASC_Sandbox.REF_Ethnicity_Mapping e
  ON a.Ethnicity = e.Ethnicity_Raw

  LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Mapping eo
  ON a.Event_Outcome = eo.Event_Outcome_Raw

  LEFT JOIN ASC_Sandbox.REF_Event_Type_Mapping et
  ON a.Event_Type = et.Event_Type_Raw

  LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Hierarchy_R2 eoh
  ON eo.Event_Outcome_Cleaned_R2 = eoh.Event_Outcome_Spec

  LEFT JOIN ASC_Sandbox.REF_Gender_Mapping g
  ON a.Gender = g.Gender_Raw

  LEFT JOIN ASC_Sandbox.REF_Hearing_Impairment_Mapping hi
  ON a.Hearing_Impairment = hi.Hearing_Impairment_Raw

  LEFT JOIN ASC_Sandbox.REF_Method_Of_Assessment_Mapping moa
  ON a.Method_Of_Assessment = moa.Method_Of_Assessment_Raw

  LEFT JOIN ASC_Sandbox.REF_Method_Of_Review_Mapping mor
  ON a.Method_Of_Review = mor.Method_Of_Review_Raw

  LEFT JOIN ASC_Sandbox.REF_Primary_Support_Reason_Mapping psr
  ON a.Primary_Support_Reason = psr.Primary_Support_Reason_Raw

  LEFT JOIN ASC_Sandbox.REF_Request_Route_Of_Access_Mapping rra
  ON a.Request_Route_Of_Access = rra.Request_Route_Of_Access_Raw

  LEFT JOIN ASC_Sandbox.REF_Review_Outcomes_Achieved_Mapping roa
  ON a.Review_Outcomes_Achieved = roa.Review_Outcomes_Achieved_Raw

  LEFT JOIN ASC_Sandbox.REF_Review_Reason_Mapping rr
  ON a.Review_Reason = rr.Review_Reason_Raw

  LEFT JOIN ASC_Sandbox.REF_Service_Component_Mapping sc
  ON a.Service_Component = sc.Service_Component_Raw

  LEFT JOIN ASC_Sandbox.REF_Service_Type_Mapping st
  ON a.Service_Type = st.Service_Type_Raw

  LEFT JOIN ASC_Sandbox.REF_Total_Hrs_Caring_Per_Week_Mapping tot
  ON a.Total_Hrs_Caring_Per_Week = tot.Total_Hrs_Caring_Per_Week_Raw

  LEFT JOIN ASC_Sandbox.REF_Visual_Impairment_Mapping vi
  ON a.Visual_Impairment = vi.Visual_Impairment_Raw

  -- Drop rows with no person ID
  WHERE COALESCE(
          Der_NHS_Number_Pseudo,
          Der_NHS_Number_Traced_Pseudo,
          LA_Person_Unique_Identifier
      ) IS NOT NULL;

  -- Drop original raw fields to highlight "_Raw" / "_Cleaned" fields
  ALTER TABLE #Mapped
  DROP COLUMN
    Accommodation_Status,
    Assessment_Type,
    Client_Funding_Status,
    Client_Type,
    Cost_Frequency_Unit_Type,
    Delivery_Mechanism,
    Employment_Status,
    Ethnicity,
    Event_Outcome,
    Event_Type,
    Gender,
    Hearing_Impairment,
    Method_of_Assessment,
    Method_of_Review,
    Primary_Support_Reason,
    Request_Route_of_Access,
    Review_Outcomes_Achieved,
    Review_Reason,
    Service_Component,
    Service_Type,
    Total_Hrs_Caring_per_week,
    Visual_Impairment

  ---------------------------------------------------------------------------
  -- Derive combined person ID field and unique event reference
  ---------------------------------------------------------------------------

  SELECT
    b.*,

    -- Derive a unique event reference (DHSC definition of a "unique" event)
    CONCAT(LA_Code, '_',
           DENSE_RANK() OVER (PARTITION BY
                                LA_Code
                              ORDER BY
                                Event_Start_Date,
                                (CASE WHEN Event_Type_Cleaned != 'Service'   THEN Der_Event_End_Date END),
                                Client_Type_Cleaned,
                                Der_NHS_LA_Combined_Person_ID,
                                Event_Type_Cleaned,
                                (CASE WHEN Event_Type_Cleaned = 'Request'    THEN Request_Route_of_Access_Cleaned END),
                                (CASE WHEN Event_Type_Cleaned = 'Assessment' THEN Assessment_Type_Cleaned END),
                                (CASE WHEN Event_Type_Cleaned = 'Service'    THEN Service_Type_Cleaned END),
                                (CASE WHEN Event_Type_Cleaned = 'Service'    THEN Service_Component_Cleaned END))
                            ) AS Der_Unique_Event_Ref

  INTO #Temp
  FROM (
    SELECT
      *,

      -- Derive review type field
      CASE
        WHEN Review_Reason_Cleaned LIKE 'Unplanned%' AND Event_Type_Cleaned = 'Review' THEN 'Unplanned'
        WHEN Review_Reason_Cleaned LIKE 'Planned%' AND Event_Type_Cleaned = 'Review' THEN 'Planned review of long term support' -- Category name updated for clarity
        WHEN Review_Reason_Cleaned = 'Review of short term support' AND Event_Type_Cleaned = 'Review' THEN 'Review of short term support' -- new category added
        WHEN Review_Reason_Cleaned = 'Invalid and not mapped' and Event_Type_Cleaned = 'Review' THEN 'Review Type Unknown' -- new to deal with addition from cleaning
        WHEN (Review_Reason_Cleaned IS NULL OR Review_Reason_Cleaned = '') AND Event_Type_Cleaned = 'Review' THEN NULL
        WHEN Event_Type_Cleaned != 'Review' THEN NULL
        ELSE 'Review Type Unknown'
      END AS Review_Type,

      -- Create a new person ID field: traced NHS number if present, else LA-provided NHS number, else LA person ID

      CASE
        WHEN Der_NHS_Number_Traced_Pseudo IS NOT NULL THEN Der_NHS_Number_Traced_Pseudo
        WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NOT NULL THEN Der_NHS_Number_Pseudo
        WHEN Der_NHS_Number_Traced_Pseudo IS NULL AND Der_NHS_Number_Pseudo IS NULL THEN CONCAT(LA_Code, '_', LA_Person_Unique_Identifier)
      END AS Der_NHS_LA_Combined_Person_ID

    FROM #Mapped
  ) b

  ---------------------------------------------------------------------------
  -- Close services with missing or incorrect end dates (before deriving ages)
  ---------------------------------------------------------------------------
  -- Services that appear to be ongoing at the end of a reference period should be found in the next period.
  -- If not found we assume the event end date is erroneously missing or incorrect and populate it with the
  -- reference period end date. Note this applies only to open services associated with reference periods
  -- where another reference period follows - i.e. open services associated with the last reference period
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

  INTO #OutputTable
  FROM (
    SELECT
      *,

      -- Derive latest age at the end of an event (or reference period if a service without an end date)
      CASE
        -- requests, assessments and reviews use event end date
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type_Cleaned != 'Service' 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25)
        -- services use event end date when not null
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type_Cleaned = 'Service' AND Der_Event_End_Date IS NOT NULL 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Der_Event_End_Date))/365.25)
        -- services use reference period end date when event end date is null
        WHEN Der_Birth_Year IS NOT NULL AND Event_Type_Cleaned = 'Service' AND Der_Event_End_Date IS NULL 
          THEN FLOOR((DATEDIFF (DAY, (CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-', '01') AS DATE)), Ref_Period_End_Date))/365.25)
        ELSE NULL 
      END AS Der_Latest_Age
      FROM #Temp
    ) c


  SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'

  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_GetDerivedFields

  ---------------------------------------------------------------------------
  -- Check for new invalid values (that need adding to mapping tables)
  ---------------------------------------------------------------------------

  SELECT DISTINCT
      v.Invalid_Type,
      v.Invalid_Value
  INTO #InvalidValues
  FROM #OutputTable o
  CROSS APPLY
  (
    VALUES
      ('Accommodation_Status',      o.Accommodation_Status_Raw,     o.Accommodation_Status_Cleaned),
      ('Assessment_Type',           o.Assessment_Type_Raw,          o.Assessment_Type_Cleaned),
      ('Client_Funding_Status',     o.Client_Funding_Status_Raw,    o.Client_Funding_Status_Cleaned),
      ('Client_Type',               o.Client_Type_Raw,              o.Client_Type_Cleaned),
      ('Cost_Frequency_Unit_Type',  o.Cost_Frequency_Unit_Type_Raw, o.Cost_Frequency_Unit_Type_Cleaned),
      ('Delivery_Mechanism',        o.Delivery_Mechanism_Raw,       o.Delivery_Mechanism_Cleaned),
      ('Employment_Status',         o.Employment_Status_Raw,        o.Employment_Status_Cleaned),
      ('Ethnicity',                 o.Ethnicity_Raw,                o.Ethnicity_Cleaned),
      ('Event_Outcome',             o.Event_Outcome_Raw,            o.Event_Outcome_Cleaned),
      ('Event_Type',                o.Event_Type_Raw,               o.Event_Type_Cleaned),
      ('Gender',                    o.Gender_Raw,                   o.Gender_Cleaned),
      ('Hearing_Impairment',        o.Hearing_Impairment_Raw,       o.Hearing_Impairment_Cleaned),
      ('Method_of_Assessment',      o.Method_of_Assessment_Raw,     o.Method_of_Assessment_Cleaned),
      ('Method_of_Review',          o.Method_of_Review_Raw,         o.Method_of_Review_Cleaned),
      ('Primary_Support_Reason',    o.Primary_Support_Reason_Raw,   o.Primary_Support_Reason_Cleaned),
      ('Request_Route_of_Access',   o.Request_Route_Of_Access_Raw,  o.Request_Route_Of_Access_Cleaned),
      ('Review_Outcomes_Achieved',  o.Review_Outcomes_Achieved_Raw, o.Review_Outcomes_Achieved_Cleaned),
      ('Review_Reason',             o.Review_Reason_Raw,            o.Review_Reason_Cleaned),
      ('Service_Component',         o.Service_Component_Raw,        o.Service_Component_Cleaned),
      ('Service_Type',              o.Service_Type_Raw,             o.Service_Type_Cleaned),
      ('Total_Hrs_Caring_Per_Week', o.Total_Hrs_Caring_Per_Week_Raw,o.Total_Hrs_Caring_Per_Week_Cleaned),
      ('Visual_Impairment',         o.Visual_Impairment_Raw,        o.Visual_Impairment_Cleaned)
  ) v (Invalid_Type, Invalid_Value, Cleaned_Value)
  WHERE NULLIF(v.Invalid_Value, '') IS NOT NULL
  AND v.Cleaned_Value IS NULL;

  IF EXISTS (SELECT 1 FROM #InvalidValues)
  BEGIN
    SELECT
      Invalid_Type,
      Invalid_Value
    FROM #InvalidValues
    ORDER BY Invalid_Type, Invalid_Value;
  END;

GO

---- Example execution:
--EXEC ASC_Sandbox.GetDerivedFields @InputTable = 'ASC_Sandbox.Temp_EventsInPeriod', @OutputTable = 'ASC_Sandbox.Temp_DerivedFields'