------  SHORT TERM WAITING TIMES TABLE ------
-- This code outputs the Short_Term_Waiting_Times table
-- It appends the restricted and unrestricted tables using data from the restricted where the person ID exists in both 

-- Section 1: Create final table by appending restricted and unrestricted tables

DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times;

SELECT 
  t2.[LA_Code]
  ,t2.[LA_Name]
  ,t2.[Der_NHS_LA_Combined_Person_ID]
  ,t2.[Primary_Support_Reason]
  ,t2.[Gender]
  ,t2.[Der_Age_Band]
  ,t2.[Ethnicity]
  ,t2.[First_Service]
  ,t2.[Service_Type]
  ,t2.[assessment_Type]
  ,t2.[Assessment_Start_Date]
  ,NULL AS Previous_Start_Date 
  ,t2.[Request_Start_Date]
  ,t2.[Request_Event_Outcome]
  ,t2.[Request_Route_of_Access]
  ,t2.[Request_to_Service]
  ,t2.[Request_to_Assessment]
  ,t2.[Assessment_to_Service]
INTO ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times
FROM ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times_Restricted t2

UNION ALL

SELECT 
  t1.[LA_Code]
  ,t1.[LA_Name]
  ,t1.[Der_NHS_LA_Combined_Person_ID]
  ,t1.[Primary_Support_Reason]
  ,t1.[Gender]
  ,t1.[Der_Age_Band]
  ,t1.[Ethnicity]
  ,t1.[First_Service]
  ,t1.[Service_Type]
  ,NULL AS assessment_Type
  ,NULL AS Assessment_Start_Date
  ,t1.[Previous_Start_Date] 
  ,NULL AS Request_Start_Date
  ,NULL AS Request_Event_Outcome
  ,t1.[Request_Route_of_Access]
  ,t1.[Request_to_Service]
  ,NULL AS Request_to_Assessment
  ,NULL AS Assessment_to_Service
FROM ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times_Unrestricted t1
LEFT JOIN ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times_Restricted t2 ON t1.Der_NHS_LA_Combined_Person_ID = t2.Der_NHS_LA_Combined_Person_ID
WHERE t2.Der_NHS_LA_Combined_Person_ID IS NULL;

-- Section 2: Fill Previous_Start_Date with the earlier of Request_Start_Date or Assessment_Start_Date

UPDATE ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times
SET Previous_Start_Date = 
    CASE
        WHEN Previous_Start_Date IS NULL THEN 
            CASE
                WHEN Request_Start_Date IS NOT NULL AND (Assessment_Start_Date IS NULL OR Request_Start_Date <= Assessment_Start_Date) THEN Request_Start_Date
                WHEN Assessment_Start_Date IS NOT NULL THEN Assessment_Start_Date
                ELSE NULL  -- Handle cases where both dates are NULL
            END
        ELSE 
            Previous_Start_Date  -- Keep existing value if not NULL
    END
WHERE Previous_Start_Date IS NULL;

-- Section 3: Rename Previous_Start_Date to First_Contact_Date

EXEC sp_rename 'ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times.Previous_Start_Date', 'First_Contact_Date', 'COLUMN';

-- Section 4: Update Request_to_Service with Assessment_to_Service when Request_to_Service is NULL

UPDATE ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times
SET Request_to_Service = Assessment_to_Service
WHERE Request_to_Service IS NULL;

-- Section 5: Rename Request_to_Service as First_Contact_to_Service

EXEC sp_rename 'ASC_Sandbox.LA_PBI_Short_Term_Waiting_Times.Request_to_Service', 'First_Contact_to_Service', 'COLUMN';




