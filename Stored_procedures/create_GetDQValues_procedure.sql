---------------------------------------------------------------------------
-- create_GetDQValues_procedure.sql
--
-- Takes an input table of Der_Unique_Record_ID, joins with CLD_DQ_Items_R1 and CLD_R1_Raw,
-- and pivots the variable values and data validation check results into long form for easy
-- summarising with different filter combinations, adding module of data specification
-- and conditions to check whether:
--
-- a) Applicable?
--    = whether the variable is applicable in the row in which it occurs
--    E.g. method of assessment not applicable for a review
--    Only DQ values where Applicable = Y saved to table
--
-- b) Mandatory?
--    = whether the variable is mandatory in the row in which it occurs
--    E.g. employment status only mandatory for 18-64s with learning disabilities
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

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  SET @Query = 'DROP TABLE IF EXISTS ' + @OutputDQTable + ';
                CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable
  EXEC(@Query)
   
  SELECT
    LA_Name,
    LA_Code,
    ImportDate,
    Module,
    Variable,
    Applicable,
    Mandatory,
    DQ_Value,
    CASE
      WHEN DQ_Value = 1 THEN 'Passed: Valid'
      WHEN DQ_Value = 0 THEN 'Passed: Blank (Not Mandatory)'
      WHEN DQ_Value = -1 THEN 'Failed: Blank (Mandatory)'
      WHEN DQ_Value < -1 THEN 'Failed: Invalid'
    END DQ_Test_Result,
    Val 'Value',
    COUNT(*) Count
  INTO #OutputTable
  FROM (
    SELECT
      LA_Name,
      LA_Code,
      ImportDate,
      Module,
      Variable,
      Applicable,
      Mandatory,
      DQ_Value,
      Val
    FROM ASC_Sandbox.InputTable Record_IDs
    LEFT JOIN DHSC_ASC.CLD_DQ_Items_R1 DQ_Items
    ON DQ_Items.Der_Unique_Record_ID = Record_IDs.Der_Unique_Record_ID
    LEFT JOIN (
      SELECT
        Der_Unique_Record_ID,
        Der_NHS_Number_Pseudo,
        Der_LA_Person_Unique_Identifier_Pseudo,
        Der_Birth_Year,
        Der_Age_Reporting_Period_End_Date,
        Der_Postcode_Sector
      FROM DHSC_ASC.CLD_R1_Raw
    ) Derived_Fields
    ON Derived_Fields.Der_Unique_Record_ID = Record_IDs.Der_Unique_Record_ID
    CROSS APPLY (
    VALUES

      -- (module, variable,
      -- is variable applicable?,
      -- is variable mandatory?,
      -- data validation test result,
      -- variable value)

      ('Submission information', 'LA_Code',
      'Y',
      'Y',
      chk_LA_Code,
      CAST(LA_Code AS VARCHAR(255))),

      ('Submission information', 'Reporting_Period_End_Date',
      'Y',
      'Y',
      chk_Reporting_Period_End_Date,
      CAST(Reporting_Period_End_Date AS VARCHAR(255))),

      ('Submission information', 'Reporting_Period_Start_Date',
      'Y',
      'Y',
      chk_Reporting_Period_Start_Date,
      CAST(Reporting_Period_Start_Date AS VARCHAR(255))),

      ('Person details', 'NHS_Number',
      'Y',
      'Y',
      chk_NHS_Number,
      CAST(Der_NHS_Number_Pseudo AS VARCHAR(255))),

      ('Person details', 'LA_Person_Unique_Identifier',
      'Y',
      'Y',
      chk_LA_Person_Unique_Identifier,
      CAST(Der_LA_Person_Unique_Identifier_Pseudo AS VARCHAR(255))),

      ('Person details', 'GP_Practice_Code',
      'Y',
      'N',
      chk_GP_Practice_Code,
      CAST(GP_Practice_Code AS VARCHAR(255))),

      ('Person details', 'GP_Practice_Name',
      'Y',
      'N',
      chk_GP_Practice_Name,
      CAST(GP_Practice_Name AS VARCHAR(255))),

      ('Person details', 'Gender',
      'Y',
      'Y',
      chk_Gender,
      CAST(Gender AS VARCHAR(255))),

      ('Person details', 'Ethnicity',
      'Y',
      'Y',
      chk_Ethnicity,
      CAST(Ethnicity AS VARCHAR(255))),

      ('Person details', 'Date_of_Birth',
      'Y',
      'Y',
      chk_Date_of_Birth,
      CAST(Der_Birth_Year AS VARCHAR(255))),

      ('Person details', 'Date_of_Death',
      'Y',
      'N',
      chk_Date_of_Death,
      CAST(Date_of_Death AS VARCHAR(255))),

      ('Person details', 'Client_Type',
      'Y',
      'Y',
      chk_Client_Type,
      CAST(Client_Type AS VARCHAR(255))),

      ('Person details', 'Primary_Support_Reason',
      'Y',
      CASE WHEN ISNULL(Event_Type,'') NOT LIKE 'Request%' AND ISNULL(Event_Type,'') NOT LIKE 'Referral%' THEN 'Y' ELSE 'N' END,
      chk_Primary_Support_Reason,
      CAST(Primary_Support_Reason AS VARCHAR(255))),

      ('Person details', 'Postcode',
      'Y',
      'Y',
      chk_Postcode,
      CAST(Der_Postcode_Sector AS VARCHAR(255))),

      ('Person details', 'Accommodation_Status',
      'Y',
      'Y',
      chk_Accommodation_Status,
      CAST(Accommodation_Status AS VARCHAR(255))),

      ('Person details', 'Employment_Status',
      'Y',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' AND Der_Age_Reporting_Period_End_Date >= 18 AND Der_Age_Reporting_Period_End_Date < 65 AND ISNULL(Primary_Support_Reason,'') = 'Learning Disability Support' THEN 'Y' ELSE 'N' END,
      chk_Employment_Status,
      CAST(Employment_Status AS VARCHAR(255))),

      ('Person details', 'Has_Unpaid_Carer',
      'Y',
      'Y',
      chk_Has_Unpaid_Carer,
      CAST(Has_Unpaid_Carer AS VARCHAR(255))),

      ('Person details', 'Autism_Spectrum_Disorder_ASD',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Autism_Spectrum_Disorder_ASD,
      CAST(Autism_Spectrum_Disorder_ASD AS VARCHAR(255))),

      ('Person details', 'Visual_Impairment',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Visual_Impairment,
      CAST(Visual_Impairment AS VARCHAR(255))),

      ('Person details', 'Hearing_Impairment',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Hearing_Impairment,
      CAST(Hearing_Impairment AS VARCHAR(255))),

      ('Person details', 'Dementia',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Dementia,
      CAST(Dementia AS VARCHAR(255))),

      ('Person details', 'Client_Funding_Status',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Service User%'
            AND ((ISNULL(Event_Type,'') LIKE 'Assessment%' AND ISNULL(Assessment_Type,'') LIKE 'Financial Assessment%')
              OR (ISNULL(Event_Type,'') LIKE 'Service%' OR ISNULL(Event_Type,'') LIKE 'Review%'))
            THEN 'Y' ELSE 'N' END,
      chk_Client_Funding_Status,
      CAST(Client_Funding_Status AS VARCHAR(255))),

      ('Events (all)', 'Event_Type',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      chk_Event_Type,
      CAST(Event_Type AS VARCHAR(255))),

      ('Events (all)', 'Event_Reference',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Event_Reference,
      CAST(Event_Reference AS VARCHAR(255))),

      ('Events (all)', 'Event_Start_Date',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      chk_Event_Start_Date,
      CAST(Event_Start_Date AS VARCHAR(255))),

      ('Events (all)', 'Event_End_Date',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      CASE WHEN (ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') NOT LIKE 'Service%') OR Date_of_Death IS NOT NULL THEN 'Y' ELSE 'N' END,
      chk_Event_End_Date,
      CAST(Event_End_Date AS VARCHAR(255))),

      ('Events (all)', 'Event_Description',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Event_Description,
      NULL),

      ('Events (all)', 'Event_Outcome',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Assessment_Type,'') != 'Financial Assessment' THEN 'Y' ELSE 'N' END,
      chk_Event_Outcome,
      CAST(Event_Outcome AS VARCHAR(255))),

      ('Events (requests only)', 'Request_Route_of_Access',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND (ISNULL(Event_Type,'') LIKE 'Request%' OR ISNULL(Event_Type,'') LIKE 'Referral%') THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND (ISNULL(Event_Type,'') LIKE 'Request%' OR ISNULL(Event_Type,'') LIKE 'Referral%') THEN 'Y' ELSE 'N' END,
      chk_Request_Route_of_Access,
      CAST(Request_Route_of_Access AS VARCHAR(255))),

      ('Events (assessments only)', 'Assessment_Type',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Assessment%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Assessment%' THEN 'Y' ELSE 'N' END,
      chk_Assessment_Type,
      CAST(Assessment_Type AS VARCHAR(255))),

      ('Events (assessments only)', 'Eligible_Needs_Identified',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Assessment%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Eligible_Needs_Identified,
      CAST(Eligible_Needs_Identified AS VARCHAR(255))),

      ('Events (assessments only)', 'Method_of_Assessment',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Assessment%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Assessment%' AND ISNULL(Assessment_Type,'') NOT LIKE 'Financial Assessment%' THEN 'Y' ELSE 'N' END,
      chk_Method_of_Assessment,
      CAST(Method_of_Assessment AS VARCHAR(255))),

      ('Carers and linked service users', 'Total_Hrs_Caring_per_week',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Carer%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Total_Hrs_Caring_per_week,
      CAST(Total_Hrs_Caring_per_week AS VARCHAR(255))),

      ('Carers and linked service users', 'No_of_adults_being_cared_for',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Carer%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_No_of_adults_being_cared_for,
      CAST(No_of_adults_being_cared_for AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_1_Linked_Person_ID',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Carer%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Adult_1_Linked_Person_ID,
      CAST(Der_Adult_1_Linked_Person_ID_Pseudo AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_2_Linked_Person_ID',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Carer%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Adult_2_Linked_Person_ID,
      CAST(Der_Adult_2_Linked_Person_ID_Pseudo AS VARCHAR(255))),

      ('Carers and linked service users', 'Adult_3_Linked_Person_ID',
      CASE WHEN ISNULL(Client_Type,'') LIKE 'Carer%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Adult_3_Linked_Person_ID,
      CAST(Der_Adult_3_Linked_Person_ID_Pseudo AS VARCHAR(255))),

      ('Events (services only)', 'Service_Type',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      chk_Service_Type,
      CAST(Service_Type AS VARCHAR(255))),

      ('Events (services only)', 'Service_Component',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      chk_Service_Component,
      CAST(Service_Component AS VARCHAR(255))),

      ('Events (services only)', 'Delivery_Mechanism',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Delivery_Mechanism,
      CAST(Delivery_Mechanism AS VARCHAR(255))),

      ('Events (services only)', 'Provider_CQC_Location_ID',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Provider_CQC_Location_ID,
      CAST(Provider_CQC_Location_ID AS VARCHAR(255))),

      ('Events (services only)', 'Provider_CQC_Location_Name',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      'N',
      chk_Provider_CQC_Location_Name,
      CAST(Provider_CQC_Location_Name AS VARCHAR(255))),

      ('Events (reviews only)', 'Review_Reason',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' THEN 'Y' ELSE 'N' END,
      chk_Review_Reason,
      CAST(Review_Reason AS VARCHAR(255))),

      ('Events (reviews only)', 'Review_Outcomes_Achieved',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' AND ISNULL(Review_Reason,'') = 'Planned' THEN 'Y' ELSE 'N' END,
      chk_Review_Outcomes_Achieved,
      CAST(Review_Outcomes_Achieved AS VARCHAR(255))),

      ('Events (reviews only)', 'Method_of_Review',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Review%' THEN 'Y' ELSE 'N' END,
      chk_Method_of_Review,
      CAST(Method_of_Review AS VARCHAR(255))),

      ('Costs (services only)', 'Unit_Cost',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Event_Type,'') LIKE 'Service%'
            AND ISNULL(Delivery_Mechanism,'') NOT LIKE '%CASSR Commissioned Support%'
            AND (ISNULL(Service_Type,'') LIKE 'Long Term%'
                OR ISNULL(Service_Component,'') IN ('Short Term Residential Care', 'Short Term Nursing Care', 'Carer Respite')
                OR ISNULL(Service_Type,'') LIKE 'Carer Support%')
            THEN 'Y' ELSE 'N' END,
      chk_Unit_Cost,
      CAST(Unit_Cost AS VARCHAR(255))),

      ('Costs (services only)', 'Cost_Frequency_Unit_Type',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Event_Type,'') LIKE 'Service%'
            AND ISNULL(Delivery_Mechanism,'') NOT LIKE '%CASSR Commissioned Support%'
            AND (ISNULL(Service_Type,'') LIKE 'Long Term%'
                  OR ISNULL(Service_Component,'') IN ('Short Term Residential Care', 'Short Term Nursing Care', 'Carer Respite')
                  OR ISNULL(Service_Type,'') LIKE 'Carer Support%')
            THEN 'Y' ELSE 'N' END,
      chk_Cost_Frequency_Unit_Type,
      CAST(Cost_Frequency_Unit_Type AS VARCHAR(255))),

      ('Costs (services only)', 'Planned_units_per_week',
      CASE WHEN ISNULL(Client_Type,'') != 'Carer known by association' AND ISNULL(Event_Type,'') LIKE 'Service%' THEN 'Y' ELSE 'N' END,
      CASE WHEN ISNULL(Event_Type,'') LIKE 'Service%'
            AND ISNULL(Delivery_Mechanism,'') NOT LIKE '%CASSR Commissioned Support%'
            AND (ISNULL(Service_Type,'') LIKE 'Long Term%'
                  OR ISNULL(Service_Component,'') IN ('Short Term Residential Care', 'Short Term Nursing Care', 'Carer Respite')
                  OR ISNULL(Service_Type,'') LIKE 'Carer Support%')
            THEN 'Y' ELSE 'N' END,
      chk_Planned_units_per_week,
      CAST(Planned_units_per_week AS VARCHAR(255)))

    ) x(Module, Variable, Applicable, Mandatory, DQ_Value, Val)
    WHERE LA_Code IS NOT NULL
    AND LA_Name IS NOT NULL
  ) DQ_Pivoted
  GROUP BY
    LA_Name,
    LA_Code,
    ImportDate,
    Module,
    Variable,
    Applicable,
    Mandatory,
    DQ_Value,
    Val

  SET @Query = 'SELECT * INTO ' + @OutputDQTable + ' FROM #OutputTable'
  EXEC(@Query)
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO

---- Example executions - note run-time between 1 and 30 minutes, depending on table size

--EXEC ASC_Sandbox.GetDQValues @InputTable = '#RecordIDs', @OutputDQTable = 'ASC_Sandbox.DQ_Values_Aggregated_Test'
