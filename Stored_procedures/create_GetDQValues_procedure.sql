---------------------------------------------------------------------------
-- create_GetDQValues_procedure.sql
--
-- Takes an input table of Der_Unique_Record_ID, joins with CLD_DQ_Items_R1 and pivots
-- the data field values, field statuses (mandatory, applicable but not mandatory, not
-- applicable or undetermined in row) and data validation check results into long form
-- for easy summarising.
--
-- Writes output to user-defined table 
--
-- NB outputs feed DQ Assessment Tool (PBI) and LA dashboard so any changes must be
-- carried through
---------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS ASC_Sandbox.GetDQValues
GO

CREATE PROCEDURE ASC_Sandbox.GetDQValues
  @InputTable SYSNAME = NULL,
  @OutputDQTable AS NVARCHAR(50)
AS
  SET NOCOUNT ON;
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.DQ_InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputDQTable + ';
                CREATE SYNONYM ASC_Sandbox.DQ_InputTable FOR ' + @InputTable
  EXEC(@Query)
   
  SELECT
    LA_Name,
    LA_Code,
    ImportDate,
    Module,
    DataField,
    cond_,
    CASE
      WHEN cond_ = 2 THEN 'Mandatory'
      WHEN cond_ = 1 THEN 'Applicable but not mandatory'
      WHEN cond_ = 0 THEN 'Not applicable'
      WHEN cond_ = -1	THEN 'Undetermined'
    END FieldStatus,
    chk_,
    CASE
      WHEN chk_ >= 1 THEN 'Passed: Valid'
      WHEN chk_ = 0 THEN 'Passed: Blank (Not Mandatory)'
      WHEN chk_ = -1 THEN 'Failed: Blank (Mandatory)'
      WHEN chk_ < -1 THEN 'Failed: Invalid'
    END FieldValidity,
    [Value],
    COUNT(*) Count
  INTO #OutputTable
  FROM (
    SELECT
      LA_Name,
      LA_Code,
      ImportDate,
      Module,
      DataField,
      cond_,
      chk_,
      [Value]
    FROM (SELECT Der_Unique_Record_ID FROM ASC_Sandbox.DQ_InputTable) Record_IDs
    LEFT JOIN DHSC_ASC.CLD_DQ_Items_R1 DQ_Items
    ON DQ_Items.Der_Unique_Record_ID = Record_IDs.Der_Unique_Record_ID
    CROSS APPLY (
    VALUES

      -- module, data field,
      -- field status (mandatory/applicable but not mandatory/not applicable/undetermined)
      -- data validation test result (valid/invalid),
      -- field value

      ('Submission information', 'LA_Code',
      cond_LA_Code,
      chk_LA_Code,
      CAST(LA_Code AS VARCHAR(255))),

      ('Submission information', 'Reporting_Period_End_Date',
      cond_Reporting_Period_End_Date,
      chk_Reporting_Period_End_Date,
      CAST(Reporting_Period_End_Date AS VARCHAR(255))),

      ('Submission information', 'Reporting_Period_Start_Date',
      cond_Reporting_Period_Start_Date,
      chk_Reporting_Period_Start_Date,
      CAST(Reporting_Period_Start_Date AS VARCHAR(255))),

      ('Person details', 'NHS_Number',
      cond_NHS_Number,
      chk_NHS_Number,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Person details', 'LA_Person_Unique_Identifier',
      cond_LA_Person_Unique_identifier,
      chk_LA_Person_Unique_Identifier,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Person details', 'GP_Practice_Code',
      cond_GP_Practice_Code,
      chk_GP_Practice_Code,
      CAST(GP_Practice_Code AS VARCHAR(255))),

      ('Person details', 'GP_Practice_Name',
      cond_GP_Practice_Name,
      chk_GP_Practice_Name,
      CAST(GP_Practice_Name AS VARCHAR(255))),

      ('Person details', 'Gender',
      cond_Gender,
      chk_Gender,
      CAST(Gender AS VARCHAR(255))),

      ('Person details', 'Ethnicity',
      cond_Ethnicity,
      chk_Ethnicity,
      CAST(Ethnicity AS VARCHAR(255))),

      ('Person details', 'Date_of_Birth',
      cond_Date_of_Birth,
      chk_Date_of_Birth,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Person details', 'Date_of_Death',
      cond_Date_of_Death,
      chk_Date_of_Death,
      CAST(Date_of_Death AS VARCHAR(255))),

      ('Person details', 'Client_Type',
      cond_Client_Type,
      chk_Client_Type,
      CAST(Client_Type AS VARCHAR(255))),

      ('Person details', 'Primary_Support_Reason',
      cond_Primary_Support_Reason,
      chk_Primary_Support_Reason,
      CAST(Primary_Support_Reason AS VARCHAR(255))),

      ('Person details', 'Postcode',
      cond_Postcode,
      chk_Postcode,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Person details', 'Accommodation_Status',
      cond_Accommodation_Status,
      chk_Accommodation_Status,
      CAST(Accommodation_Status AS VARCHAR(255))),

      ('Person details', 'Employment_Status',
      cond_Employment_Status,
      chk_Employment_Status,
      CAST(Employment_Status AS VARCHAR(255))),

      ('Person details', 'Has_Unpaid_Carer',
      cond_Has_Unpaid_Carer,
      chk_Has_Unpaid_Carer,
      CAST(Has_Unpaid_Carer AS VARCHAR(255))),

      ('Person details', 'Autism_Spectrum_Disorder_ASD',
      cond_Autism_Spectrum_Disorder_ASD,
      chk_Autism_Spectrum_Disorder_ASD,
      CAST(Autism_Spectrum_Disorder_ASD AS VARCHAR(255))),

      ('Person details', 'Visual_Impairment',
      cond_Visual_Impairment,
      chk_Visual_Impairment,
      CAST(Visual_Impairment AS VARCHAR(255))),

      ('Person details', 'Hearing_Impairment',
      cond_Hearing_Impairment,
      chk_Hearing_Impairment,
      CAST(Hearing_Impairment AS VARCHAR(255))),

      ('Person details', 'Dementia',
      cond_Dementia,
      chk_Dementia,
      CAST(Dementia AS VARCHAR(255))),

      ('Person details', 'Client_Funding_Status',
      cond_Client_Funding_Status,
      chk_Client_Funding_Status,
      CAST(Client_Funding_Status AS VARCHAR(255))),

      ('Events (all)', 'Event_Type',
      cond_Event_Type,
      chk_Event_Type,
      CAST(Event_Type AS VARCHAR(255))),

      ('Events (all)', 'Event_Reference',
      cond_Event_Reference,
      chk_Event_Reference,
      CAST(Event_Reference AS VARCHAR(255))),

      ('Events (all)', 'Event_Start_Date',
      cond_Event_Start_Date,
      chk_Event_Start_Date,
      CAST(Event_Start_Date AS VARCHAR(255))),

      ('Events (all)', 'Event_End_Date',
      cond_Event_End_Date,
      chk_Event_End_Date,
      CAST(Event_End_Date AS VARCHAR(255))),

      ('Events (all)', 'Event_Description',
      cond_Event_Description,
      chk_Event_Description,
      NULL),

      ('Events (all)', 'Event_Outcome',
      cond_Event_Outcome,
      chk_Event_Outcome,
      CAST(Event_Outcome AS VARCHAR(255))),

      ('Events (requests only)', 'Request_Route_of_Access',
      cond_Request_Route_of_Access,
      chk_Request_Route_of_Access,
      CAST(Request_Route_of_Access AS VARCHAR(255))),

      ('Events (assessments only)', 'Assessment_Type',
      cond_Assessment_Type,
      chk_Assessment_Type,
      CAST(Assessment_Type AS VARCHAR(255))),

      ('Events (assessments only)', 'Eligible_Needs_Identified',
      cond_Eligible_Needs_Identified,
      chk_Eligible_Needs_Identified,
      CAST(Eligible_Needs_Identified AS VARCHAR(255))),

      ('Events (assessments only)', 'Method_of_Assessment',
      cond_Method_of_Assessment,
      chk_Method_of_Assessment,
      CAST(Method_of_Assessment AS VARCHAR(255))),

      ('Carers and linked service users', 'Total_Hrs_Caring_per_week',
      cond_Total_Hrs_Caring_per_week,
      chk_Total_Hrs_Caring_per_week,
      CAST(Total_Hrs_Caring_per_week AS VARCHAR(255))),

      ('Carers and linked service users', 'No_of_adults_being_cared_for',
      cond_No_of_adults_being_cared_for,
      chk_No_of_adults_being_cared_for,
      CAST(No_of_adults_being_cared_for AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_1_Linked_Person_ID',
      cond_Adult_1_Linked_Person_ID,
      chk_Adult_1_Linked_Person_ID,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_2_Linked_Person_ID',
      cond_Adult_2_Linked_Person_ID,
      chk_Adult_2_Linked_Person_ID,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_3_Linked_Person_ID',
      cond_Adult_3_Linked_Person_ID,
      chk_Adult_3_Linked_Person_ID,
      CAST('N/A - PID' AS VARCHAR(255))),

      ('Events (services only)', 'Service_Type',
      cond_Service_Type,
      chk_Service_Type,
      CAST(Service_Type AS VARCHAR(255))),

      ('Events (services only)', 'Service_Component',
      cond_Service_Component,
      chk_Service_Component,
      CAST(Service_Component AS VARCHAR(255))),

      ('Events (services only)', 'Delivery_Mechanism',
      cond_Delivery_Mechanism,
      chk_Delivery_Mechanism,
      CAST(Delivery_Mechanism AS VARCHAR(255))),

      ('Events (services only)', 'Provider_CQC_Location_ID',
      cond_Provider_CQC_Location_ID,
      chk_Provider_CQC_Location_ID,
      CAST(Provider_CQC_Location_ID AS VARCHAR(255))),

      ('Events (services only)', 'Provider_CQC_Location_Name',
      cond_Provider_CQC_Location_Name,
      chk_Provider_CQC_Location_Name,
      CAST(Provider_CQC_Location_Name AS VARCHAR(255))),

      ('Events (reviews only)', 'Review_Reason',
      cond_Review_Reason,
      chk_Review_Reason,
      CAST(Review_Reason AS VARCHAR(255))),

      ('Events (reviews only)', 'Review_Outcomes_Achieved',
      cond_Review_Outcomes_Achieved,
      chk_Review_Outcomes_Achieved,
      CAST(Review_Outcomes_Achieved AS VARCHAR(255))),

      ('Events (reviews only)', 'Method_of_Review',
      cond_Method_of_Review,
      chk_Method_of_Review,
      CAST(Method_of_Review AS VARCHAR(255))),

      ('Costs (services only)', 'Unit_Cost',
      cond_Unit_Cost,
      chk_Unit_Cost,
      CAST(Unit_Cost AS VARCHAR(255))),

      ('Costs (services only)', 'Cost_Frequency_Unit_Type',
      cond_Cost_Frequency_Unit_Type,
      chk_Cost_Frequency_Unit_Type,
      CAST(Cost_Frequency_Unit_Type AS VARCHAR(255))),

      ('Costs (services only)', 'Planned_units_per_week',
      cond_Planned_units_per_week,
      chk_Planned_units_per_week,
      CAST(Planned_units_per_week AS VARCHAR(255)))

    ) x(Module, DataField, cond_, chk_, [Value])
    WHERE LA_Code IS NOT NULL
    AND LA_Name IS NOT NULL
  ) DQ_Pivoted
  GROUP BY
    LA_Name,
    LA_Code,
    ImportDate,
    Module,
    DataField,
    cond_,
    chk_,
    [Value]

  SET @Query = 'SELECT * INTO ' + @OutputDQTable + ' FROM #OutputTable'
  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.DQ_InputTable

GO

---- Example executions

--EXEC ASC_Sandbox.GetDQValues @InputTable = '#RecordIDs', @OutputDQTable = 'ASC_Sandbox.DQ_Values_Aggregated_Test'
