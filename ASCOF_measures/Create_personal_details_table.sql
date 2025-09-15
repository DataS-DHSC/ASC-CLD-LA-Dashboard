/*
This scripts produces cleaned latest person details for each of the required breakdowns 
from the Single- and Joined- submission tables, and saves them into
ASC_Sandbox.<submission name>_Latest_Person_Data

The breakdowns that are processed are:

  Accommodation_Status,
  Accommodation_Status_Group,
  Employment_Status,
  Gender,
  Ethnicity,
  Der_Birth_Year,
  Der_Birth_Month,
  Der_Age_End_Of_Period,
  Der_Working_Age_Band_End_Of_Period,
  Date_of_Death,
  Has_Unpaid_Carer,
  Primary_Support_Reason,
  Client_Funding_Status
*/

-- If any Nulls occur this maps them to unknown

--Set year period within which reablement must have ended:
DECLARE @ReportingPeriodStartDate AS DATE = '2024-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2025-03-31'

DROP TABLE IF EXISTS #CLD
SELECT 
  a.LA_Code,
  a.LA_Name,
  a.Der_NHS_LA_Combined_Person_ID,
  a.LA_Person_Unique_Identifier, -- needed for linking with carers data
  a.Event_Start_Date,
  COALESCE(a.Der_Event_End_Date, '9999-01-01') AS Der_Event_End_Date,
  a.Der_Birth_Year,
  a.Der_Birth_Month,
  a.Date_of_Death,
  a.Ref_Period_End_Date,
  -- Change suffix R1/R2 as appropriate
  COALESCE(ct.Client_Type_Cleaned_R1, 'Invalid and not mapped') AS Client_Type_Cleaned,
  COALESCE(psr.Primary_Support_Reason_Cleaned_R1, 'Invalid and not mapped') AS Primary_Support_Reason_Cleaned,
  COALESCE(e.Ethnicity_Cleaned_R1, 'Invalid and not mapped') AS Ethnicity_Cleaned,
  COALESCE(e.Ethnicity_Grouped_R1, 'Invalid and not mapped') AS Ethnicity_Grouped,
  COALESCE(cfs.Client_Funding_Status_Cleaned_R2, 'Invalid and not mapped') AS Client_Funding_Status_Cleaned,
  COALESCE(es.Employment_Status_Cleaned_R1, 'Invalid and not mapped') AS Employment_Status_Cleaned,
  COALESCE(ast.Accommodation_Status_Cleaned_R1, 'Invalid and not mapped') AS Accommodation_Status_Cleaned,
  --The next two are the same in R1 as in R2
  COALESCE(uc.Has_Unpaid_Carer_Cleaned, 'Invalid and not mapped') AS Has_Unpaid_Carer_Cleaned, -- same in R2 as R1
  COALESCE(g.Gender_Cleaned, 'Invalid and not mapped') AS Gender_Cleaned, -- same in R2 as R1
  COALESCE(a.Event_Type, 'Invalid and not mapped') AS Event_Type,
  COALESCE(st.Service_Type_Cleaned_R1, 'Invalid and not mapped') AS Service_Type_Cleaned,
  COALESCE(st.Service_Type_Grouped_R1, 'Invalid and not mapped') AS Service_Type_Grouped,
  COALESCE(sc.Service_Component_Cleaned_R1, 'Invalid and not mapped') AS Service_Component_Cleaned,
  a.Adult_1_Linked_Person_ID,
  a.Adult_2_Linked_Person_ID,
  a.Adult_3_Linked_Person_ID
INTO #CLD
FROM ASC_Sandbox.CLD_230401_250630_JoinedSubmissions a
LEFT JOIN ASC_Sandbox.REF_Client_Type_Mapping ct
ON a.Client_Type_Raw = ct.Client_Type_Raw
LEFT JOIN ASC_Sandbox.REF_Primary_Support_Reason_Mapping psr
ON a.Primary_Support_Reason_Raw = psr.Primary_Support_Reason_Raw
LEFT JOIN ASC_Sandbox.REF_Client_Funding_Status_Mapping cfs
ON a.Client_Funding_Status_Raw = cfs.Client_Funding_Status_Raw
LEFT JOIN ASC_Sandbox.REF_Has_Unpaid_Carer_Mapping uc
ON a.Has_Unpaid_Carer = uc.Has_Unpaid_Carer_Raw
LEFT JOIN ASC_Sandbox.REF_Ethnicity_Mapping e
ON a.Ethnicity_Raw = e.Ethnicity_Raw
LEFT JOIN ASC_Sandbox.REF_Gender_Mapping g
ON a.Gender_Raw = g.Gender_Raw
LEFT JOIN ASC_Sandbox.REF_Employment_Status_Mapping es
ON a.Employment_Status_Raw = es.Employment_Status_Raw
LEFT JOIN ASC_Sandbox.REF_Accommodation_Status_Mapping ast
ON a.Accommodation_Status_Raw= ast.Accommodation_Status_Raw
LEFT JOIN ASC_Sandbox.REF_Service_Type_Mapping st
ON a.Service_Type_Raw= st.Service_Type_Raw
LEFT JOIN ASC_Sandbox.REF_Service_Component_Mapping sc
ON a.Service_Component_Raw= sc.Service_Component_Raw


--#####################################################################
-- Get latest accommodation status
--#####################################################################

DROP TABLE IF EXISTS #Accom_Cleaned
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  Ref_Period_End_Date,
  CASE
    WHEN Accommodation_Status_Cleaned = 'Invalid and not mapped' OR Accommodation_Status_Cleaned IS NULL
    THEN 'Unknown'
    ELSE Accommodation_Status_Cleaned
  END AS Accommodation_Status,
  CASE
    WHEN Accommodation_Status_Cleaned IN ('Unknown','Invalid and not mapped') OR Accommodation_Status_Cleaned IS NULL
    THEN 0
    ELSE 1
  END AS Der_Accommodation_Known
  INTO #Accom_Cleaned
  FROM #CLD



-- Identify latest known accommodation status for each person. This is done by:
-- 1. Sort by der_accommodation known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Accom_Row1;
WITH LatestAccomm
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Accommodation_Known DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
       ) AS Rn
    FROM #Accom_Cleaned)
SELECT 
  *
INTO #Accom_Row1
FROM LatestAccomm
WHERE Rn = 1

-- Identifies conflicting accommodation status' per person
DROP TABLE IF EXISTS #Accomm_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Accommodation_Status) AS [COUNT]
INTO #Accomm_Duplicates
FROM #Accom_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Accommodation_Status) >1

-- Overwrites conflicting accommodation status' with 'Unknown'
DROP TABLE IF EXISTS #Accom_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Accommodation_Status
    END AS [Accommodation_Status]
INTO #Accom_Deduped
FROM #Accom_Row1 a
LEFT JOIN #Accomm_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID

-----------------------------------------------------------------------
--Select people with an unknown/invalid accommodation status:

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
      ('Unknown', 0,20),
	    ('Unknown - Presumed at home', 1,21),
      ('Unknown - Presumed in community', 0,22)


-- now for Employment Status tables  
DROP TABLE IF EXISTS #REF_Employment_Status;
CREATE TABLE #REF_Employment_Status
  (
    Employment_Status VARCHAR(200),
    Employment_Status_Group INT,
    Employment_Status_Order INT
  );

INSERT INTO #REF_Employment_Status
  (Employment_Status, Employment_Status_Group, Employment_Status_Order)
VALUES
  ('Invalid and not mapped', 0, 1),
  ('Not in Paid Employment (not actively seeking work / retired)', 0, 2),
  ('Not in Paid Employment (seeking work)', 0, 3),
  ('Not in Paid Employment (voluntary work only)', 0, 4),
  ('Paid: 16 or more hours a week', 1, 5),
  ('Paid: Hours per week unknown', 1, 6),
  ('Paid: Less than 16 hours a week', 1, 7),
  ('Unknown', 0, 8);

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
	,('Short Term Support: ST-Max', 5)
    ,('Short Term Support: Ongoing Low Level', 6)
    ,('Short Term Support: Other Short Term', 7)

----------------------------------------------------------
   
    DROP TABLE IF EXISTS #Unknown_Acc_Status_IDs
    SELECT DISTINCT 
      LA_Code, 
      Der_NHS_LA_Combined_Person_ID
    INTO #Unknown_Acc_Status_IDs
    FROM #Accom_Deduped
    WHERE Accommodation_Status = 'Unknown' 


    --Merge unknowns with initial build table to find service information
    DROP TABLE IF EXISTS #Unknown_IDs_Service_Details
    SELECT
      a.LA_Code,
      a.Der_NHS_LA_Combined_Person_ID,
      b.Event_Type,
      b.Service_Type_Cleaned,
      b.Service_Component_Cleaned,
      c.Service_Type_Hierarchy,
      b.Event_Start_Date,
      b.Der_Event_End_Date
    INTO #Unknown_IDs_Service_Details
    FROM #Unknown_Acc_Status_IDs a
    LEFT JOIN  (select * from #CLD where Event_Type = 'Service') b
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
                  Der_NHS_LA_Combined_Person_ID
          ORDER BY
                  COALESCE(Der_Event_End_Date, '9999-01-01') DESC, --sort by latest end date first (nulls as priority)
				  CASE
				    WHEN Service_Type_Hierarchy IS NULL THEN 99
					ELSE Service_Type_Hierarchy
				  END ASC, --then by highest ranking service type (nulls lowest priority)
                  CASE 
                    WHEN Service_Component_Cleaned IN ('Shared Lives', 'Community Supported Living', 'Extra care housing', 'Home Support') 
                    THEN 1 ELSE 0 
                  END DESC, --selects service components we can map to accommodations over those we can't 
                  Event_Start_Date DESC  --latest event start as last resort
            ) AS Rn
        FROM #Unknown_IDs_Service_Details)
    SELECT 
      LA_Code, 
      Der_NHS_LA_Combined_Person_ID,
	  CASE
		WHEN (CASE WHEN Service_Type_Hierarchy IS NULL THEN 99 ELSE Service_Type_Hierarchy END) = 99
        THEN 'Not useful'
        ELSE Service_Type_Cleaned 
      END AS Service_Type_Cleaned, 
      CASE
        WHEN Service_Component_Cleaned IN ('Shared Lives', 'Community Supported Living', 'Extra care housing', 'Home Support') 
        THEN Service_Component_Cleaned
        ELSE 'Not useful'
	  END AS Service_Component_Cleaned
    INTO #Service_Row1
    FROM LatestService
    WHERE Rn = 1


    --Create table of distinct people (dups exist where people have multiple services with the same dates/types/components)
    --Anyone with latest services in 2 or more of shared lives, community supported living and extra care housing, the service component overwritten to unknown
    --as we are unable to determine which service is accurate and be used as the accommodation status

DROP TABLE IF EXISTS #Unknowns_Services_Deduped;

SELECT 
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  CASE 
    WHEN 
      COUNT(DISTINCT CASE 
        WHEN Service_Type_Cleaned != 'Not useful' 
        THEN Service_Type_Cleaned 
      END) = 1
    THEN 
      MAX(CASE 
        WHEN Service_Type_Cleaned != 'Not useful' 
        THEN Service_Type_Cleaned 
      END)
    ELSE NULL
  END AS Service_Type_Cleaned,
  CASE 
    WHEN 
      COUNT(DISTINCT CASE 
        WHEN Service_Component_Cleaned != 'Not useful'
        THEN Service_Component_Cleaned 
      END) = 1
    THEN 
      MAX(CASE 
        WHEN Service_Component_Cleaned != 'Not useful' 
        THEN Service_Component_Cleaned 
      END)
    ELSE NULL
  END AS Service_Component
INTO #Unknowns_Services_Deduped
FROM #Service_Row1
GROUP BY 
  LA_Code,
  Der_NHS_LA_Combined_Person_ID;


DROP TABLE IF EXISTS #Unknowns_Services_Mapped;

SELECT 
  A.LA_Code,
  A.Der_NHS_LA_Combined_Person_ID,
  A.Service_Type_Cleaned,
  A.Service_Component,
  A.Accommodation_Status,
  CASE 
    WHEN A.Accommodation_Status = 'Unknown - Presumed at home' THEN 1
    ELSE R.Accommodation_Status_Group
  END AS Accommodation_Status_Group
INTO #Unknowns_Services_Mapped
FROM (
  SELECT
    LA_Code,
    Der_NHS_LA_Combined_Person_ID,
    Service_Type_Cleaned,
    Service_Component,
    CASE 
      WHEN Service_Type_Cleaned = 'Long Term Support: Nursing Care' THEN 'Registered nursing home'
      WHEN Service_Type_Cleaned = 'Long Term Support: Residential Care' THEN 'Registered care home'
      WHEN Service_Component = 'Shared Lives' THEN 'Shared Lives scheme'
      WHEN Service_Component = 'Extra care housing' THEN 'Sheltered housing / extra care housing / other sheltered housing'
      WHEN Service_Component = 'Community supported living' THEN 'Supported accommodation / supported lodgings / supported group home'
      WHEN Service_Type_Cleaned = 'Long Term Support: Community' AND  Service_Component = 'Home Support' THEN 'Unknown - Presumed at home'-- new constructed category: not in spec
      WHEN Service_Type_Cleaned = 'Long Term Support: Community' AND Service_Component IS NULL  THEN 'Unknown - Presumed in community' -- new constructed category: not in spec
      WHEN Service_Type_Cleaned = 'Long Term Support: Prison' THEN 'Prison / Young offenders institution / detention centre'
      ELSE 'Unknown'
    END AS Accommodation_Status
  FROM #Unknowns_Services_Deduped
) A
LEFT JOIN #REF_Accommodation_Status R
  ON A.Accommodation_Status = R.Accommodation_Status;


DROP TABLE IF EXISTS #Updated_Accom_Status
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  a.Accommodation_Status,
  b.Accommodation_Status_Group
INTO #Updated_Accom_Status
FROM #Accom_Deduped a
LEFT JOIN #REF_Accommodation_Status b
  ON a.Accommodation_Status = b.Accommodation_Status
WHERE a.Accommodation_Status != 'Unknown' 

UNION ALL
    
-- Mapped values
SELECT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  a.Accommodation_Status,
  a.Accommodation_Status_Group
FROM #Unknowns_Services_Mapped a


--#####################################################################
-- getting latest employment status for each person 
--#####################################################################

DROP TABLE IF EXISTS #Employ_Cleaned
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  Ref_Period_End_Date,
  COALESCE(Employment_Status_Cleaned, 'Unknown')  AS Employment_Status, -- R2 to R1 mapping variable
  CASE
    WHEN Employment_Status_Cleaned IN ('Unknown','Invalid and not mapped') OR Employment_Status_Cleaned IS NULL
    THEN 0
    ELSE 1
    END AS Der_Employment_Known
  INTO #Employ_Cleaned
  FROM #CLD


-- Sort by der_employment known (to favour known over unknown values)
DROP TABLE IF EXISTS #Employ_Row1;
WITH LatestEmploy
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Employment_Known DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
       ) AS Rn
    FROM #Employ_Cleaned)
SELECT 
  *
INTO #Employ_Row1
FROM LatestEmploy
WHERE Rn = 1

-- Identifies conflicting Employment status' per person
DROP TABLE IF EXISTS #Employ_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Employment_Status) AS [COUNT]
INTO #Employ_Duplicates
FROM #Employ_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Employment_Status) >1



-- Overwrites conflicting Employment status' with 'Unknown'
DROP TABLE IF EXISTS #Employ_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Employment_Status
    END AS [Employment_Status]
INTO #Employ_Deduped
FROM #Employ_Row1 a
LEFT JOIN #Employ_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID



--#####################################################################
  -- Getting latest gender for each person
--#####################################################################

-- Create table with cleaned gender where invalid entries have been be mapped to valid entries
-- Create a new variable Der_Gender_Known
DROP TABLE IF EXISTS #Gender_Cleaned
SELECT
  *,
  CASE
    WHEN Gender_Cleaned = 'Unknown'
    THEN 0
    ELSE 1
    END AS Der_Gender_Known
  INTO #Gender_Cleaned
  FROM (
    SELECT 
      LA_Code,
      Der_NHS_LA_Combined_Person_ID,
      Event_Start_Date,
      Der_Event_End_Date,
	  Ref_Period_End_Date,
      CASE
        WHEN Gender_Cleaned IN ('Unknown','Invalid and not mapped') OR Gender_Cleaned IS NULL
        THEN 'Unknown'
        ELSE Gender_Cleaned
        END AS Gender_Cleaned
      FROM #CLD ) A

-- Identify latest known gender for each person. This is done by:
-- 1. Sort by Der_Gender_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Gender_Row1;
WITH LatestGender
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Gender_Known DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
      ) AS Rn
    FROM #Gender_Cleaned)
SELECT 
  *
INTO #Gender_Row1
FROM LatestGender
WHERE Rn = 1



-- Identifies conflicting genders per person
DROP TABLE IF EXISTS #Gender_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Gender_Cleaned) AS [COUNT]
INTO #Gender_Duplicates
FROM #Gender_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Gender_Cleaned) >1

-- Overwrites conflicting genders with 'Unknown'
DROP TABLE IF EXISTS #Gender_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Gender_Cleaned
    END AS Gender
INTO #Gender_Deduped
FROM #Gender_Row1 a
LEFT JOIN #Gender_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID

  
--#####################################################################
  -- Getting latest Ethnicity for each person
--#####################################################################

-- Create table with cleaned gender where invalid entries have been be mapped to valid entries
-- Create a new variable Der_Gender_Known
DROP TABLE IF EXISTS #Ethnicity_Cleaned
SELECT
  *,
  CASE
    WHEN Ethnicity_Cleaned = 'No data - Undeclared or Not known' THEN 0
	WHEN Ethnicity_Cleaned = 'No data - Refused' THEN 1
    ELSE 2
    END AS Der_Ethnicity_Rank
  INTO #Ethnicity_Cleaned
  FROM (
    SELECT 
      LA_Code,
      Der_NHS_LA_Combined_Person_ID,
      Event_Start_Date,
      Der_Event_End_Date,
	  Ref_Period_End_Date,
      CASE
        WHEN Ethnicity_Cleaned = 'Invalid and not mapped' OR Ethnicity_Cleaned IS NULL
        THEN 'No data - Undeclared or Not known'
        ELSE Ethnicity_Cleaned
        END AS Ethnicity_Cleaned
      FROM #CLD ) A

-- Identify latest known Ethnicity for each person. This is done by:
-- 1. Sort by Der_Ethnicity_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Ethnicity_Row1;
WITH LatestEthnicity
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Ethnicity_Rank DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
      ) AS Rn
    FROM #Ethnicity_Cleaned)
SELECT 
  *
INTO #Ethnicity_Row1
FROM LatestEthnicity
WHERE Rn = 1



-- Identifies conflicting Ethnicity per person
DROP TABLE IF EXISTS #Ethnicity_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Ethnicity_Cleaned) AS [COUNT]
INTO #Ethnicity_Duplicates
FROM #Ethnicity_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Ethnicity_Cleaned) >1
  


-- Overwrites conflicting Ethnicity with 'Unknown'
DROP TABLE IF EXISTS #Ethnicity_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'No data - Undeclared or Not known'
    ELSE a.Ethnicity_Cleaned
    END AS Ethnicity
INTO #Ethnicity_Deduped
FROM #Ethnicity_Row1 a
LEFT JOIN #Ethnicity_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID


--#####################################################################
-- Get latest Client Funding Status
--#####################################################################

DROP TABLE IF EXISTS #Client_Funding_Status_Cleaned
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  Ref_Period_End_Date,
  CASE
    WHEN Client_Funding_Status_Cleaned NOT IN ('Fully social care funded','Joint client and social care funded','Fully client funded') OR 
         Client_Funding_Status_Cleaned IS NULL
    THEN 'Unknown'
    ELSE Client_Funding_Status_Cleaned
  END AS Client_Funding_Status,
  CASE
    WHEN Client_Funding_Status_Cleaned NOT IN ('Fully social care funded','Joint client and social care funded','Fully client funded') OR 
         Client_Funding_Status_Cleaned IS NULL
    THEN 0
    ELSE 1
  END AS Der_Client_Funding_Status_Known
  INTO #Client_Funding_Status_Cleaned
  FROM #CLD
  
-- Identify latest known Client_Funding_Status status for each person. This is done by:
-- 1. Sort by Der_Client_Funding_Status_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Client_Funding_Status_Row1;
WITH Latest_Client_Funding_Status
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Client_Funding_Status_Known DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
       ) AS Rn
    FROM #Client_Funding_Status_Cleaned)
SELECT 
  *
INTO #Client_Funding_Status_Row1
FROM Latest_Client_Funding_Status
WHERE Rn = 1

-- Identifies conflicting client funding status' per person
DROP TABLE IF EXISTS #Client_Funding_Status_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Client_Funding_Status) AS [COUNT]
INTO #Client_Funding_Status_Duplicates
FROM #Client_Funding_Status_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Client_Funding_Status) >1

-- Overwrites conflicting client funding status' with 'Unknown'
DROP TABLE IF EXISTS #Client_Funding_Status_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Client_Funding_Status
    END AS [Client_Funding_Status]
INTO #Client_Funding_Status_Deduped
FROM #Client_Funding_Status_Row1 a
LEFT JOIN #Client_Funding_Status_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID



--######################################################################################################
-- Getting latest birth month/year info, and calculate the age at the end of the reporting period
--######################################################################################################

-- Identify a birth date for each person and calculate their age at the end of the reporting period.
-- This is done by pioritising (1) valid entries (2) the most recent submissions (3) the earliest birth dates.
DROP TABLE IF EXISTS #Birth_Row1;
WITH LatestBirth
AS (
  SELECT 
    LA_Code,
    Der_NHS_LA_Combined_Person_ID,
	Der_Birth_Year,
	Der_Birth_Month,
    CASE
      WHEN Der_Birth_Year IS NOT NULL AND Der_Birth_Month IS NOT NULL
        THEN FLOOR((DATEDIFF(DAY, CAST(CONCAT(Der_Birth_Year, '-', Der_Birth_Month, '-01') AS DATE), '2025-03-31')) / 365.25)
      ELSE NULL
    END AS Der_Age_End_Of_Period,
    DENSE_RANK() OVER (
      PARTITION BY 
        LA_Code,
        Der_NHS_LA_Combined_Person_ID
      ORDER BY
        CASE --prioritise non-NULL entries
          WHEN Der_Birth_Year IS NULL OR Der_Birth_Month IS NULL THEN 1
          ELSE 2
        END DESC,
		Ref_Period_End_Date DESC, -- prioritise the later submissions
		Der_Birth_Year ASC, -- prioritise earlier birth dates
		Der_Birth_Month ASC
    ) AS Rn
  FROM #CLD)
SELECT DISTINCT 
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Der_Birth_Year,
  Der_Birth_Month,
  Der_Age_End_Of_Period,
  CASE
    WHEN Der_Age_End_Of_Period BETWEEN 1 AND 17 THEN 'Under 18'
    WHEN Der_Age_End_Of_Period BETWEEN 18 AND 64 THEN '18 to 64'
    WHEN Der_Age_End_Of_Period >= 65 THEN '65 and above'
    ELSE 'Unknown'
  END AS Der_Working_Age_Band_End_Of_Period
INTO #Birth_Row1
FROM LatestBirth
WHERE Rn = 1

--######################################################################################################
-- Getting latest date of death
--######################################################################################################

-- Identify a birth date for each person and calculate their age at the end of the reporting period.
-- This is done by pioritising (1) valid entries (2) the most recent submissions (3) the earliest birth dates.
DROP TABLE IF EXISTS #Death_Row1;
WITH LatestDeath
AS (
  SELECT 
    LA_Code,
    Der_NHS_LA_Combined_Person_ID,
	Date_of_Death,
    DENSE_RANK() OVER (
      PARTITION BY 
        LA_Code,
        Der_NHS_LA_Combined_Person_ID
      ORDER BY
        CASE --prioritise non-NULL entries
          WHEN Date_of_Death IS NULL THEN 1
          ELSE 2
        END DESC,
		Ref_Period_End_Date DESC, -- prioritise the later submissions
		Date_of_Death DESC -- prioritise later dates of death
    ) AS Rn
  FROM #CLD)
SELECT DISTINCT 
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Date_of_Death
INTO #Death_Row1
FROM LatestDeath
WHERE Rn = 1


--#####################################################################
-- Getting latest Has Unpaid Carer for each person
--#####################################################################

-- Create table with cleaned Has_Unpaid_Carer where invalid entries have been be mapped to valid entries
-- Create a new variable Der_Has_Unpaid_Carer_Rank

DROP TABLE IF EXISTS #Has_Unpaid_Carer_Cleaned
SELECT
  *,
  CASE
    WHEN Has_Unpaid_Carer = 'Unknown' THEN 0
    ELSE 1
    END AS Der_Has_Unpaid_Carer_Known
  INTO #Has_Unpaid_Carer_Cleaned
  FROM (
    SELECT 
      LA_Code,
      Der_NHS_LA_Combined_Person_ID,
      Event_Start_Date,
      Der_Event_End_Date,
	  Ref_Period_End_Date,
      CASE
        WHEN Has_Unpaid_Carer_Cleaned IN ('Unknown','Invalid and not mapped') OR Has_Unpaid_Carer_Cleaned IS NULL
        THEN 'Unknown'
        ELSE Has_Unpaid_Carer_Cleaned
        END AS Has_Unpaid_Carer
      FROM #CLD ) A


-- Identify latest known Has_Unpaid_Carer for each person. This is done by:
-- 1. Sort by Der_Has_Unpaid_Carer_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #Has_Unpaid_Carer_Row1;
WITH Latest_Has_Unpaid_Carer
AS (
  SELECT 
    *,
    DENSE_RANK() OVER (
      PARTITION BY 
              LA_Code,
              Der_NHS_LA_Combined_Person_ID
      ORDER BY
              Der_Has_Unpaid_Carer_Known DESC,
			  Ref_Period_End_Date DESC, -- prioritise later submissions
              Der_Event_End_Date DESC,
              Event_Start_Date DESC
      ) AS Rn
    FROM #Has_Unpaid_Carer_Cleaned)
SELECT 
  *
INTO #Has_Unpaid_Carer_Row1
FROM Latest_Has_Unpaid_Carer
WHERE Rn = 1



-- Identifies conflicting Has_Unpaid_Carer per person
DROP TABLE IF EXISTS #Has_Unpaid_Carer_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Has_Unpaid_Carer) AS [COUNT]
INTO #Has_Unpaid_Carer_Duplicates
FROM #Has_Unpaid_Carer_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Has_Unpaid_Carer) >1
  

-- Overwrites conflicting Has_Unpaid_Carer with 'Unknown'
DROP TABLE IF EXISTS #Has_Unpaid_Carer_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Has_Unpaid_Carer
    END AS Has_Unpaid_Carer
INTO #Has_Unpaid_Carer_Deduped
FROM #Has_Unpaid_Carer_Row1 a
LEFT JOIN #Has_Unpaid_Carer_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID

-- Now look for people with unpaid carer in the carer's fields


-------
-- Build all possible persons for the reporting period
DROP TABLE IF EXISTS #All_Persons;
SELECT DISTINCT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
INTO #All_Persons
FROM #CLD;

-- Build table of all linked Adult Person IDs (potential cared-for people)
DROP TABLE IF EXISTS #Carers_shell;

SELECT DISTINCT
  LA_Code,
  Adult_1_Linked_Person_ID AS Adult_Linked_Person_ID
INTO #Carers_shell
FROM #CLD
WHERE Client_Type_Cleaned IN ('Carer', 'Carer known by association')
  AND Adult_1_Linked_Person_ID IS NOT NULL
  AND Der_Event_End_Date >= @ReportingPeriodStartDate --look for evidence within the reporting year only
  AND Event_Start_Date <= @ReportingPeriodEndDate

UNION ALL

SELECT DISTINCT
  LA_Code,
  Adult_2_Linked_Person_ID AS Adult_Linked_Person_ID
FROM #CLD
WHERE Client_Type_Cleaned IN ('Carer', 'Carer known by association')
  AND Adult_2_Linked_Person_ID IS NOT NULL
  AND Der_Event_End_Date >= @ReportingPeriodStartDate --look for evidence within the reporting year only
  AND Event_Start_Date <= @ReportingPeriodEndDate

UNION ALL

SELECT DISTINCT
  LA_Code,
  Adult_3_Linked_Person_ID AS Adult_Linked_Person_ID
FROM #CLD
WHERE Client_Type_Cleaned IN ('Carer', 'Carer known by association')
  AND Adult_3_Linked_Person_ID IS NOT NULL
  AND Der_Event_End_Date >= @ReportingPeriodStartDate --look for evidence within the reporting year only
  AND Event_Start_Date <= @ReportingPeriodEndDate
  ;

-- Table of people who DO have an unpaid carer, as determined from the carers fields
DROP TABLE IF EXISTS #People_With_Unpaid_Carer;
SELECT DISTINCT
  a.LA_Code,
  b.Der_NHS_LA_Combined_Person_ID
INTO #People_With_Unpaid_Carer
FROM #Carers_shell a
JOIN #CLD b
  ON a.LA_Code = b.LA_Code
  AND a.Adult_Linked_Person_ID = b.LA_Person_Unique_Identifier;

-- Final full person table with Has_Unpaid_Carer as 'Yes' or 'No'
DROP TABLE IF EXISTS #Has_Unpaid_Carer_Full;
SELECT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.Der_NHS_LA_Combined_Person_ID IS NOT NULL THEN 'Yes'
    ELSE a.Has_Unpaid_Carer
  END AS Has_Unpaid_Carer
INTO #Has_Unpaid_Carer_Full
FROM #Has_Unpaid_Carer_Deduped a
LEFT JOIN #People_With_Unpaid_Carer b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID;


--##############################################################################
-- Get Primary Support Reason
--##############################################################################

DROP TABLE IF EXISTS #PSR_Cleaned
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  Event_Start_Date,
  Der_Event_End_Date,
  Ref_Period_End_Date,
  CASE
    WHEN Primary_Support_Reason_Cleaned = 'Invalid and not mapped' OR Primary_Support_Reason_Cleaned IS NULL
    THEN 'Unknown'
    ELSE Primary_Support_Reason_Cleaned
  END AS Primary_Support_Reason,
  CASE
    WHEN Primary_Support_Reason_Cleaned IN ('Unknown','Invalid and not mapped','Social Support: Support to Carer') OR 
         Primary_Support_Reason_Cleaned IS NULL
    THEN 0
    ELSE 1
  END AS Der_Primary_Support_Reason_Known
  INTO #PSR_Cleaned
  FROM #CLD

-- Identify latest known PSR for each person. This is done by:
-- 1. Sort by Der_Primary_Support_Reason_Known (to favour known over unknown values)
-- 2. Sort by latest event end date (nulls overwritten to 9999 to ensure they appear first)
-- 3. Sort by latest event start date
DROP TABLE IF EXISTS #PSR_Row1;
WITH LatestPSR
AS (
  SELECT 
  *,
  DENSE_RANK() OVER (
    PARTITION BY 
      LA_Code,
      Der_NHS_LA_Combined_Person_ID
    ORDER BY
      Der_Primary_Support_Reason_Known DESC,
      Ref_Period_End_Date DESC, -- prioritise later submissions
      Der_Event_End_Date DESC,
      Event_Start_Date DESC
    ) AS Rn
  FROM #PSR_Cleaned)
SELECT 
  *
INTO #PSR_Row1
FROM LatestPSR
WHERE Rn = 1

-- Identifies conflicting accommodation status' per person
DROP TABLE IF EXISTS #PSR_Duplicates
SELECT
  LA_Code,
  Der_NHS_LA_Combined_Person_ID,
  COUNT(DISTINCT Primary_Support_Reason) AS [COUNT]
INTO #PSR_Duplicates
FROM #PSR_Row1
GROUP BY
  LA_Code,
  Der_NHS_LA_Combined_Person_ID
HAVING
  COUNT(DISTINCT Primary_Support_Reason) >1


-- Overwrites conflicting accommodation status' with 'Unknown'
DROP TABLE IF EXISTS #PSR_Deduped
SELECT DISTINCT
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  CASE
    WHEN b.[COUNT] IS NOT NULL
    THEN 'Unknown'
    ELSE a.Primary_Support_Reason
    END AS [Primary_Support_Reason]
INTO #PSR_Deduped
FROM #PSR_Row1 a
LEFT JOIN #PSR_Duplicates b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID

-- Join for one record per person
DROP TABLE IF EXISTS #OutputTable

SELECT
  FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period_End_Date,
  a.LA_Code,
  a.Der_NHS_LA_Combined_Person_ID,
  a.Accommodation_Status,
  a.Accommodation_Status_Group,
  d.Employment_Status,
  b.Gender,
  f.Ethnicity,
  c.Der_Birth_Year,
  c.Der_Birth_Month,
  CASE
      WHEN Der_Birth_Year IS NULL OR Der_Birth_Month IS NULL THEN NULL
      ELSE CAST(CONCAT(c.Der_Birth_Year, '-', c.Der_Birth_Month, '-', '01') AS DATE) 
  END AS Der_Birth_Date,
  c.Der_Age_End_Of_Period,
  c.Der_Working_Age_Band_End_Of_Period,
  h.Date_of_Death,
  e.Has_Unpaid_Carer,
  g.Primary_Support_Reason,
  i.Client_Funding_Status
INTO #OutputTable
FROM #Updated_Accom_Status a
LEFT JOIN #Gender_Deduped b
  ON a.LA_Code = b.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Birth_Row1 c
  ON a.LA_Code = c.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = c.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Employ_Deduped d
  ON a.LA_Code = d.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = d.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Has_Unpaid_Carer_Full e
  ON a.LA_Code = e.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = e.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Ethnicity_Deduped f
  ON a.LA_Code = f.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = f.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #PSR_Deduped g
  ON a.LA_Code = g.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = g.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Death_Row1 h
  ON a.LA_Code = h.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = h.Der_NHS_LA_Combined_Person_ID
LEFT JOIN #Client_Funding_Status_Deduped i
  ON a.LA_Code = i.LA_Code
  AND a.Der_NHS_LA_Combined_Person_ID = i.Der_NHS_LA_Combined_Person_ID


DROP TABLE IF EXISTS ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425;

SELECT *
INTO  ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425
FROM #OutputTable
ORDER BY
  LA_Code

-- END


-- ESSENTIAL QA: 
-- Uncomment the following to check that the number of rows in the final table equals the number of (LA, Der_NHS_LA_Combined_Person_ID) pairs


/*
--QA: Number of unique (LA_Code, Der_NHS_LA_Combined_Person_ID) pairs in #CLD
SELECT COUNT(*) AS Unique_Person_Count
FROM (
    SELECT LA_Code, Der_NHS_LA_Combined_Person_ID
    FROM #CLD
    GROUP BY LA_Code, Der_NHS_LA_Combined_Person_ID
) AS distinct_pairs;

--QA: Number of unique (LA_Code, Der_NHS_LA_Combined_Person_ID) pairs in #OutputTable
SELECT COUNT(*) AS Unique_Person_Count
FROM (
    SELECT LA_Code, Der_NHS_LA_Combined_Person_ID
    FROM #OutputTable
    GROUP BY LA_Code, Der_NHS_LA_Combined_Person_ID
) AS distinct_pairs;

--QA: Number of Rows in #OutputTable
SELECT COUNT(*) FROM #OutputTable
*/
