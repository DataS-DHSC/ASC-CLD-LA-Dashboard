---------------------------------------------------------------------------
-- create_GetUniqueEvents_procedure.sql
--
-- Create procedure to return a single row for each unique event
-- (deduplicate the data)
--
-- The record retained for each unique event is selected according to the
-- following ranking:
-- 1) latest reference period
-- 2) NULL end date (open/ongoing services), otherwise latest end date
-- 3) event outcome highest in hierarchy
-- 4) presence of conversation flag
-- 5) largest record ID
--
-- Note:
-- - Input table must contain Der_Unique_Event_Ref, Ref_Period_End_Date,
--   Der_Event_End_Date, Event_Outcome_Hierarchy and Der_Conversation
--   (created by GetDerivedFields procedure)
-- - Service cost information is excluded from the output for joined
--   submissions as it's often split over multiple records and therefore
--   a single record cannot be selected (would require summation).
--
-- Returns table with fewer columns than input table (and deduplicated rows!)
-- See example executions of procedure below
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetUniqueEvents
GO

CREATE PROCEDURE ASC_Sandbox.GetUniqueEvents
  @Submissions AS NVARCHAR(256),
  @InputTable SYSNAME = NULL,
  @OutputTable AS NVARCHAR(50)
AS
  SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable

  EXEC(@Query)

  -- Convert numeric data types to varchar for deduplication partitioning
  DROP TABLE IF EXISTS #InputTable;
  SELECT *
  INTO #InputTable
  FROM ASC_Sandbox.InputTable;

  ALTER TABLE #InputTable ALTER COLUMN Unit_Cost NUMERIC(18, 2) NULL;
  ALTER TABLE #InputTable ALTER COLUMN Unit_Cost VARCHAR(18) NULL;

  ALTER TABLE #InputTable ALTER COLUMN Planned_units_per_week NUMERIC(18, 2) NULL;
  ALTER TABLE #InputTable ALTER COLUMN Planned_units_per_week VARCHAR(18) NULL;


  -- Rank the rows associated with each unique event according to the
  -- partitioning and sort order below:

  IF @Submissions = 'Joined' OR @Submissions = 'Single'
  BEGIN

    SELECT *,
      -- For single submissions, partition using the derived unique event
      -- reference, and delivery mechanism + cost fields for services
      CASE WHEN @Submissions = 'Single' THEN (
        ROW_NUMBER() OVER (
          PARTITION BY
            Der_Unique_Event_Ref,
            (CASE WHEN Event_Type like '%service%' THEN ISNULL(Delivery_Mechanism_Cleaned, '') END),
            (CASE WHEN Event_Type like '%service%' THEN ISNULL(Unit_Cost, '') END),
            (CASE WHEN Event_Type like '%service%' THEN ISNULL(Cost_Frequency_Unit_Type_Cleaned, '') END),
            (CASE WHEN Event_Type like '%service%' THEN ISNULL(Planned_units_per_week, '') END)
          ORDER BY
            (CASE WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' ELSE Der_Event_End_Date END) DESC,
            (CASE WHEN Event_Outcome_Hierarchy IS NULL THEN 999 ELSE Event_Outcome_Hierarchy END) ASC,
            Der_Conversation DESC,
            Der_Unique_Record_ID DESC
        )
      )
      -- For joined submissions, partition using only the derived unique
      -- event reference
      ELSE (
        ROW_NUMBER() OVER (
          PARTITION BY
            Der_Unique_Event_Ref
          ORDER BY
            Ref_Period_End_Date DESC,
            (CASE WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' ELSE Der_Event_End_Date END) DESC,
            (CASE WHEN Event_Outcome_Hierarchy IS NULL THEN 999 ELSE Event_Outcome_Hierarchy END) ASC,
            Der_Conversation DESC,
            Der_Unique_Record_ID DESC
        )
      ) END AS Record_Rank
    INTO #RankedRecords
    FROM #InputTable;

  END

  ELSE
  BEGIN

    PRINT 'EXITING. Submissions parameter not recognised. Please specify ''Joined'' or ''Single''';
    RETURN

  END;

  -- Select top-ranked rows for each unique event and write deduplicated output table
  -- (NB not all available columns are selected)

  SELECT
    LA_Code,
    LA_Name,
    Ref_Period_Start_Date,
    Ref_Period_End_Date,
    GP_Practice_Name,
    GP_Practice_Code,
    Gender_Raw,
    Gender_Cleaned,
    Ethnicity_Raw,
    Ethnicity_Cleaned,
    Ethnicity_Grouped,
    Date_of_Death,
    Client_Type_Raw,
    Client_Type_Cleaned,
    Primary_Support_Reason_Raw,
    Primary_Support_Reason_Cleaned,
    Accommodation_Status_Raw,
    Accommodation_Status_Cleaned,
    Employment_Status_Raw,
    Employment_Status_Cleaned,
    Has_Unpaid_Carer,
    Autism_Spectrum_Disorder_ASD,
    Visual_Impairment_Raw,
    Visual_Impairment_Cleaned,
    Hearing_Impairment_Raw,
    Hearing_Impairment_Cleaned,
    Dementia,
    Client_Funding_Status_Raw,
    Client_Funding_Status_Cleaned,
    Event_Type,
    Event_Start_Date,
    Event_End_Date_Raw,
    Der_Event_End_Date,
    Der_Unique_Event_Ref,
    Event_Outcome_Raw,
    Event_Outcome_Cleaned,
    Event_Outcome_Hierarchy,
    Event_Outcome_Grouped,
    Request_Route_of_Access_Raw,
    Request_Route_of_Access_Cleaned,
    Der_Conversation,
    Der_Conversation_1,
    Assessment_Type_Raw,
    Assessment_Type_Cleaned,
    Eligible_Needs_Identified,
    Method_of_Assessment_Raw,
    Method_of_Assessment_Cleaned
    Total_Hrs_Caring_per_week_Raw,
    Total_Hrs_Caring_per_week_Cleaned,
    Total_Hrs_Caring_per_week_Cleaned_R2,
    No_of_adults_being_cared_for,
    Adult_1_Linked_Person_ID,
    Adult_2_Linked_Person_ID,
    Adult_3_Linked_Person_ID,
    Service_Type_Raw,
    Service_Type_Cleaned,
    Service_Type_Grouped,
    Service_Component_Raw,
    Service_Component_Cleaned,
    Delivery_Mechanism_Raw,
    Delivery_Mechanism_Cleaned,
    Provider_CQC_Location_Name,
    Provider_CQC_Location_ID,
    Unit_Cost,
    Cost_Frequency_Unit_Type_Raw,
    Cost_Frequency_Unit_Type_Cleaned,
    Planned_Units_Per_Week,
    Review_Reason_Raw,
    Review_Reason_Cleaned,
    Review_Type,
    Review_Outcomes_Achieved_Raw,
    Review_Outcomes_Achieved_Cleaned,
    Method_of_Review_Raw,
    Method_of_Review_Cleaned,
    Der_DBS_Check_Status,
    Der_NHS_Number_Pseudo,
    Der_NHS_Number_Traced_Pseudo,
    LA_Person_Unique_Identifier,
    Der_NHS_LA_Combined_Person_ID,
    Der_Birth_Year,
    Der_Birth_Month,
    Der_Age_Event_Start_Date,
    Der_Age_Reporting_Period_End_Date,
    Der_Latest_Age,
    Der_Age_Band,
    Der_Working_Age_Band,
    Der_Postcode_Sector,
    Der_Postcode_Constituency_Code,
    Der_Postcode_District_Unitary_Authority,
    Der_Postcode_Electoral_Ward,
    Der_Postcode_Local_Authority,
    Der_Postcode_CCG_Code,
    Der_Postcode_LSOA_Code,
    Der_Postcode_MSOA_Code,
    Der_Postcode_yr2011_LSOA,
    Der_Postcode_yr2011_MSOA,
    Der_Unique_Record_ID,
    ImportFileId,
    ImportDate
  INTO #OutputTable
  FROM #RankedRecords
  WHERE Record_Rank = 1;

  -- Drop cost fields from output for joined submissions

  IF @Submissions = 'Joined'
  BEGIN
    ALTER TABLE #OutputTable DROP COLUMN Unit_Cost;
    ALTER TABLE #OutputTable DROP COLUMN Cost_Frequency_Unit_Type_Raw;
    ALTER TABLE #OutputTable DROP COLUMN Cost_Frequency_Unit_Type_Cleaned;
    ALTER TABLE #OutputTable DROP COLUMN Planned_Units_Per_Week;
  END;

  -- Write output table

  SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable;';

  EXEC(@Query);
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable;

GO

---- Example executions:

--EXEC ASC_Sandbox.GetUniqueEvents @Submissions = 'Joined', @InputTable = 'ASC_Sandbox.Temp_DerivedFields', @OutputTable = 'ASC_Sandbox.Temp_UniqueEvents'

--EXEC ASC_Sandbox.GetUniqueEvents @Submissions = 'Single', @InputTable = 'ASC_Sandbox.Temp_DerivedFields', @OutputTable = 'ASC_Sandbox.Temp_UniqueEvents'
