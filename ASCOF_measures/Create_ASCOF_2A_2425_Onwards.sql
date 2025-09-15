------------------------------------------
/*  ASCOF 2A
This code is split into 2 main sections:
1. Finds reablement services within the year, clusters them, filters to new clients
2. Identifies the sequels to the reablement epsiodes

24/25 onwards - this code has been adapted for use on the 24/25 main tables as these tables contain different field names where R2 to R1 mapping has been applied

*/
--------------------------------------------------------------------------------------------------------
-----------------------------SECTION 1 -  Select reablement events -------------------------------------
--------------------------------------------------------------------------------------------------------

--================== Select analysis perid and submissions =====================
/* Select the relevant joined submissions table, ideally 16 months of data:
> ASCOF 2A counts all reablement which ended within 12 month period
> 7 day lag at the end is required to determine the immediate outcome for reablement which ended right at the end of the year
> Data for the previous 3 months is required to determine whether someone is a new client or not (No LTS in 3 months prior to reablement)
> Total = 15 months + 7 days
*/

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2A_2425_Onwards

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2A_2425_Onwards
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(100),
  @InputTable_PersonDetails AS NVARCHAR(100),
  @OutputTable AS NVARCHAR(100),
  @OutputTable_Disaggregated AS NVARCHAR(100)

AS
  
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable_PersonDetails

  SET @Query =  N'DROP TABLE IF EXISTS ' + @OutputTable + '; 
                 DROP TABLE IF EXISTS ' + @OutputTable_Disaggregated + '; 
                 CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + ';
                 CREATE SYNONYM ASC_Sandbox.InputTable_PersonDetails FOR ' + @InputTable_PersonDetails +';'
  EXEC(@Query)

        --Latest person details table
        DROP TABLE IF EXISTS #CLD_Latest_Person_Details

        SELECT * 
        INTO #CLD_Latest_Person_Details
        FROM ASC_Sandbox.InputTable_PersonDetails
        
        --Initial build table
        DROP TABLE IF EXISTS #ASCOF2A_Build_Temp

        SELECT *
        INTO #ASCOF2A_Build_Temp
        FROM ASC_Sandbox.InputTable  
        WHERE 
          Client_Type_Cleaned = 'Service User'
          AND Der_NHS_LA_Combined_Person_ID IS NOT NULL
          AND Event_Start_Date IS NOT NULL
          AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) --removes DQ issues of event end date prior to start date
        --Note age and date of death filters are applied later

        --Set null end dates to future date for ease of processing
        UPDATE #ASCOF2A_Build_Temp
        SET Der_Event_End_Date = 
          CASE 
            WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' 
            ELSE Der_Event_End_Date
        END;


        --================= Pull through cleaned event outcome ===================

        --add a penalty to the Event Outcome hierarchy to ensure outcomes that 
        --can be used as sequels are prioritised over 'Unable to Classify' sequels
        DROP TABLE IF EXISTS #REF_Event_Outcome_Hierarchy_UTC

        SELECT 
          CASE
            WHEN (R.Event_Outcome_Spec IN ('No change in package',
                                           'Progress to Support Planning / Services',
                                           'Progress to financial assessment',
                                           'Progress to Re-assessment / Unplanned Review',
                                           'Progress to Assessment',
                                           'Progress to Reablement/ST-Max',
                                           'Provision of service',
                                           'Progress to End of Life Care',
                                           'Release 2 multiple mappings: Progress to assessment, review or reassessment',
                                           'Release 2 multiple mappings: Continuation of support or services',
                                           'Invalid and not mapped'))
              THEN R.Event_Outcome_Hierarchy + 100
	        ELSE R.Event_Outcome_Hierarchy
          END AS Event_Outcome_Hierarchy,
          Event_Outcome_Spec
        INTO #REF_Event_Outcome_Hierarchy_UTC
        FROM ASC_Sandbox.REF_Event_Outcome_Hierarchy_R1 R

        DROP TABLE IF EXISTS #ASCOF2A_Build

        SELECT 
          a.LA_Code,
          a.LA_Name,
          a.Client_Type_Cleaned AS Client_Type,
          a.Event_Type,
          a.Event_Start_Date,
          a.Der_Event_End_Date,
          a.Service_Type_Cleaned AS Service_Type,
          a.Service_Component_Cleaned AS Service_Component,
          a.Der_NHS_LA_Combined_Person_ID,
          COALESCE(a.Event_Outcome_Cleaned, 'Invalid and not mapped') as Event_Outcome,
          COALESCE(eoh.Event_Outcome_Hierarchy, 999) as Event_Outcome_Hierarchy,
          a.Der_unique_record_id
        INTO #ASCOF2A_Build  
        FROM #ASCOF2A_Build_Temp a
        LEFT JOIN #REF_Event_Outcome_Hierarchy_UTC eoh
        ON a.Event_Outcome_Cleaned = eoh.Event_Outcome_Spec

        --================== Select ST-Max events ============

        --Identify ST-Max (reablement) events
        DROP TABLE IF EXISTS #ST_Max_All

        SELECT *
        INTO #ST_Max_All
        FROM #ASCOF2A_Build
        WHERE Event_Type = 'Service'
        AND Service_Type = 'Short Term Support: ST-Max' 


         --================== Cluster ST-Max events together =====================

        --1. Orders the data based on the fields listed, assigns row numbers and the previous event end date
        DROP TABLE IF EXISTS #ST_Max_Grouped

        SELECT  
          LA_Code,
          LA_Name,
          ROW_NUMBER () OVER (ORDER BY 
                                LA_Code,
                                LA_Name,
                                Der_NHS_LA_Combined_Person_ID, 
                                Event_Start_Date, 
                                Der_Event_End_Date, 
                                COALESCE(Event_Outcome_Hierarchy, 999) ASC, --Ensures null event outcomes are lowest ranked
                                Der_unique_record_id DESC) AS RN, 
          Der_NHS_LA_Combined_Person_ID,
          Event_Start_Date,
          Der_Event_End_Date,
          Service_Type,
          Service_Component,
          Event_Outcome,
          Der_unique_record_id,
          Event_Outcome_Hierarchy,
          MAX(Der_Event_End_Date) OVER (PARTITION BY 
                                      LA_Code, 
                                      LA_Name,
                                      Der_NHS_LA_Combined_Person_ID 
                                    ORDER BY 
                                      Event_Start_Date, 
                                      Der_Event_End_Date,
                                      COALESCE(Event_Outcome_Hierarchy, 999) ASC,
                                      Der_unique_record_id DESC 
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS Previous_End_Date                                 
        INTO #ST_Max_Grouped
        FROM #ST_Max_All


        --2. Determine whether events are consecutive (max 7 day apart) or concurrent (overlapping), then assign cluster id 
        --   (note: the choice of 7 days ensures that there cannot be another ST-Max event within 7 days of an ST-Max cluster. 
        --    This means that an ST-Max can never be the sequel of an ST-Max cluster, 
        --    because sequels are determined by looking only in the 7 days following an ST-Max cluster.)

        DROP TABLE IF EXISTS #ST_Max_Clusters_Assigned

        SELECT 
          *,
          DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) AS Day_Diff,
          CASE 
            WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 7 THEN 0 
            ELSE 1 
          END AS ClusterStartInd,
          SUM (CASE 
                  WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 7 THEN 0 
                  ELSE 1 END) 
          OVER (ORDER BY LA_Code, RN) AS ST_Max_Cluster_ID
        INTO #ST_Max_Clusters_Assigned
        FROM #ST_Max_Grouped


        --3: Assign event outcome to the cluster (based on latest end date, if a tie then use the event outcome hierarchy)
        DROP TABLE IF EXISTS #ST_Max_Grouped_Event_Outcome

        SELECT 
          *,
          --LAST VALUE takes the event outcome of the last row in the partition, i.e. row with the latest event end date
          --If there are two rows with conflicting event outcomes for the same latest date the hierarchy is applied
          LAST_VALUE(Event_Outcome_Hierarchy) OVER (PARTITION BY 
                                                       LA_Code, 
                                                       LA_Name,
                                                       Der_NHS_LA_Combined_Person_ID,
                                                        ST_Max_Cluster_ID
                                                     ORDER BY 
                                                       Der_Event_End_Date ASC,  
                                                       COALESCE(Event_Outcome_Hierarchy, 999) DESC, 
                                                       Der_unique_record_id ASC 
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Cluster_EO_Hierarchy,
          LAST_VALUE(Event_Outcome) OVER (PARTITION BY  
                                                        LA_Code,  
                                                        LA_Name, 
                                                        Der_NHS_LA_Combined_Person_ID, 
                                                        ST_Max_Cluster_ID 
                                                    ORDER BY  
                                                        Der_Event_End_Date ASC,  
                                                        COALESCE(Event_Outcome_Hierarchy, 999) DESC, 
                                                        Der_unique_record_id ASC  
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Cluster_EO

        INTO #ST_Max_Grouped_Event_Outcome
        FROM #ST_Max_Clusters_Assigned

        --4: Create a table of one line per cluster
        DROP TABLE IF EXISTS #ST_Max_Clusters

        SELECT DISTINCT
          LA_Code,
          LA_Name,
          Der_NHS_LA_Combined_Person_ID, 
          ST_Max_Cluster_ID,
          MIN (Event_Start_Date) AS ST_Max_Cluster_Start,
          MAX(Der_Event_End_Date) AS ST_Max_Cluster_End,
          Cluster_EO_Hierarchy AS ST_Max_Cluster_Event_Outcome_Hierarchy,
          Cluster_EO AS ST_Max_Cluster_Event_Outcome
        INTO #ST_Max_Clusters
        FROM #ST_Max_Grouped_Event_Outcome
        GROUP BY 
          LA_Code, 
          LA_Name, 
          Der_NHS_LA_Combined_Person_ID, 
          ST_Max_Cluster_ID, 
          Cluster_EO_Hierarchy, 
          Cluster_EO 
          
          --================== Select ST-Max events ending in the year =====================

        DROP TABLE IF EXISTS #ST_Max_Clusters_In_period

        SELECT *
        INTO #ST_Max_Clusters_In_period
        FROM #ST_Max_Clusters
        WHERE ST_Max_Cluster_End BETWEEN @ReportingPeriodStartDate AND @ReportingPeriodEndDate

        --================== Join ST-Max clusters to all other events for the same person =====================

        --1. Merge ST-Max events in period with ASCOF 2A Build table (all events) flagging where St-Max joins with itself
        --At this stage also join to the latest person details table to pull through DOB and DOD
        DROP TABLE IF EXISTS #ST_Max_Joined

        SELECT 
          a.ST_Max_Cluster_ID,
          a.ST_Max_Cluster_Start,
          a.ST_Max_Cluster_End,
          a.ST_Max_Cluster_Event_Outcome,
          a.ST_Max_Cluster_Event_Outcome_Hierarchy,
          c.Date_of_Death,
          CASE 
            WHEN FLOOR(DATEDIFF(DAY, c.Der_Birth_Date, a.ST_Max_Cluster_End) / 365.25) < 18 THEN 'Under 18' -- Under 18s filtered out below
            WHEN FLOOR(DATEDIFF(DAY, c.Der_Birth_Date, a.ST_Max_Cluster_End) / 365.25) BETWEEN 18 AND 64 THEN '18 to 64'
            WHEN FLOOR(DATEDIFF(DAY, c.Der_Birth_Date, a.ST_Max_Cluster_End) / 365.25) >= 65 THEN '65 and above'
            ELSE 'Unknown'
          END AS ST_Max_Cluster_Working_Age_Band,
          b.*,
	        CASE
		        WHEN 
              Event_Start_Date BETWEEN ST_Max_Cluster_Start AND ST_Max_Cluster_End  --all start dates will be within the cluster start/end if they were used to form the cluster
		          AND Event_Type = 'Service'
		          AND Service_Type = 'Short Term Support: ST-Max'
		        THEN 1
		        ELSE 0
	        END AS Same_ST_Max
        INTO #ST_Max_Joined
        FROM #ST_Max_Clusters_In_period a
        LEFT JOIN #CLD_Latest_Person_Details c    
        ON a.Der_NHS_LA_Combined_Person_ID = c.Der_NHS_LA_Combined_Person_ID
        AND a.LA_Code = c.LA_Code
        LEFT JOIN #ASCOF2A_Build b    
        ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.LA_Code = b.LA_Code
        -- Exclude under 18s but include unknown ages. 
        -- Exclude people who died before cluster start date.
        WHERE (FLOOR(DATEDIFF(DAY, c.Der_Birth_Date, a.ST_Max_Cluster_End) / 365.25) >= 18 OR c.Der_Birth_Date IS NULL) AND
              (c.Date_of_Death >= a.ST_Max_Cluster_Start OR c.Date_of_Death IS NULL)


        --2. Set fields to null where ST-Max joins onto itself
        --Can't delete the row as we would lose ST-Max records where their only joined entry is them joining on themselves
        Update #ST_Max_joined SET 
          Client_Type = NULL,
          Event_Type = NULL,
          Event_Start_Date = NULL,
          Der_Event_End_Date = NULL,
          Event_Outcome = NULL,
          Event_Outcome_Hierarchy = NULL,
          Service_Type = NULL,
          Service_Component = NULL,
          Der_Unique_Record_ID = NULL
        WHERE Same_ST_Max = 1

        --================== Identify new clients =====================

        --1. Create flag for new clients (no LTS in the 3m prior to reablement start)
        --Inital flag is at event level (cluster may have joined onto multiple events), New_Client_Cluster_Flag therefore assigns the flag at cluster level
        --If there is a record joined to the cluster where new_client = 0 then this takes prescedent

        --N.B. This code also creates a flag for whether there's a request present within 28 days prior to reablement start
        --This is not a requirement for this metric but potentially helpful for sharing back with LAs
        DROP TABLE IF EXISTS #ST_Max_Joined_Flags

        SELECT *,
          --Set new_client = 0 for the cluster where LTS present in 3 months prior to ST-Max:
           MIN( New_Client_Record_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS New_Client_Cluster_Flag,  
           --Set prior_request = 1 for the cluster where prior request is present
           MAX( Prior_Request_Record_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS Prior_Request_Cluster_Flag 
        INTO #ST_Max_Joined_Flags
        FROM (
            SELECT *,
              CASE  --New client flag logic: any LTS end date not within 365 days of ST-Max start date
                WHEN Service_Type IN ('Long Term Support: Nursing Care',
                                      'Long Term Support: Residential Care',
                                      'Long Term Support: Community',
                                      'Long Term Support: Prison')
	                   AND Event_Start_Date < ST_Max_Cluster_Start  
                     AND (DATEDIFF(DAY, Der_Event_End_Date, ST_Max_Cluster_Start) <  91 --Alter this to change threshold for new clients  
                     OR Der_Event_End_Date IS NULL)
                THEN 0
  	            ELSE 1
	            END AS New_Client_Record_Flag,

	          CASE --Prior request logic: any request within 91 days of ST-Max start, excluding requests that are prior to any prior ST-Max events found 
                 --(not actually required with updated methodology, 91 days aligns with recreation of SALT metrics)
                WHEN Event_Type = 'Request'
                     AND Event_Start_Date < ST_Max_Cluster_Start  
                     AND (DATEDIFF(DAY, Der_Event_End_Date, ST_Max_Cluster_Start) < 28)
                     AND NOT EXISTS (
                        SELECT 1
                        FROM #ST_Max_Joined AS b
                        WHERE b.Service_Type = 'Short Term Support: ST-Max'
                          AND b.ST_Max_Cluster_ID = main.ST_Max_Cluster_ID
                          AND DATEDIFF(DAY, b.Der_Event_End_Date, main.ST_Max_Cluster_Start) BETWEEN 0 AND 27
                          AND main.Der_Event_End_Date <= b.Der_Event_End_Date
                     )
                THEN 1
                ELSE 0
              END AS Prior_Request_Record_Flag
            FROM #ST_Max_Joined main
        )A


        --2. Filter to new clients only 
        DROP TABLE IF EXISTS #ST_MAX_New_Clients_Final   --The final table of all reablement events to be counted in final output

        SELECT *
        INTO #ST_MAX_New_Clients_Final
        FROM #ST_Max_Joined_Flags a
        WHERE a.New_Client_Cluster_Flag = 1

        --QA: Count STMAX
        --select count(distinct ST_Max_Cluster_ID) from #ST_MAX_New_Clients_Final

        --================== Create reference table =====================
        -- Service type reference table with hierarchy, used to choose between services when two services are present in the sequel chain
        DROP TABLE IF EXISTS #REF_Service_Type

        CREATE TABLE #REF_Service_Type
        (Service_Type VARCHAR(200)
        ,Sort_Order INT
        ,Hierarchy INT)

        INSERT INTO #REF_Service_Type
        (Service_Type
        ,Sort_Order
        ,Hierarchy
        )
        VALUES
        ('Long Term Support: Nursing Care', 1, 1)
        ,('Long Term Support: Residential Care',  2, 2)
        ,('Long Term Support: Community',  3, 3)
        ,('Long Term Support: Prison', 4, 4)
        ,('Short Term Support: ST-Max', 5, 5)
        ,('Short Term Support: Ongoing Low Level', 6, 6)
        ,('Short Term Support: Other Short Term', 7, 7)


        --------------------------------------------------------------------------------------------------------
        ----------------------------- SECTION 2 -  Determine sequels -------------------------------------------
        --------------------------------------------------------------------------------------------------------
        /* Sequels are identified by processing the data following the steps below. 
        Only if a sequel is not identified in a step does the process continue onto the next step to identify a sequel.

        Key for sequel types:

        1.  Date of death which precedes, or is within 7 days of the ST-Max end date, sequel = Deceased
        2.  ST-Max cluster event outcome of 'Admitted to hospital', sequel = Admitted to hospital
        3.  Service open on the ST-Max end date, or within 7 days of ST-Max end, sequel = service type
        4.  Concluding event outcome information is present either on the ST-Max epsiode, non-service events in the sequel period,
            or assessment/review events near the end of the ST-Max:
               a) ST-Max event outcome
               b) Non-service event sequel event outcome
               c) Assessment or review event outcome, near end of ST-Max 
        5.  Equipment was provided during the reablement, sequel = short term support ongoing low-level
        6.  Unable to classify,  sequel = ST-Max event outcome

        */

        --================= Step 1: Check the Date of Death ============================================================
        -- Check whether date of death precedes, or is within 7 days of the end date of the STMAX. 
        -- These episodes should be staged with Final Outcome = 'NFA - Deceased'.

        DROP TABLE IF EXISTS #ASCOF2A
        CREATE TABLE #ASCOF2A (
            LA_Code VARCHAR(64),
            Der_NHS_LA_Combined_Person_ID VARCHAR(512),
            ST_Max_Cluster_ID INT,
            Final_Outcome VARCHAR(128),
            Sequel_Type VARCHAR(2),
            ST_Max_Cluster_Working_Age_Band VARCHAR(64)
        );

        INSERT INTO #ASCOF2A
        SELECT DISTINCT
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          'NFA - Deceased' AS 'Final_Outcome',
          '1' as 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #ST_MAX_New_Clients_Final
        WHERE DATEDIFF(DAY, ST_Max_Cluster_End, Date_of_Death) <= 7

        -- remove staged clusters 
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );

        --================= Step 2: Check whether ST-Max EO is Admitted to Hospital =======================================================

        --Insert into Final table, using the Sequel Service Type as the final outcome 
        INSERT INTO #ASCOF2A
        SELECT DISTINCT
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome AS 'Final_Outcome',
          '2' AS 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #ST_MAX_New_Clients_Final
        WHERE ST_Max_Cluster_Event_Outcome = 'Admitted to hospital'

        -- remove staged clusters 
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );

        --================== Prep steps 3 & 4: Find ST-Max episodes with future events within 7 days of the reablment end date ========

        DROP TABLE IF EXISTS #RankedEvents
          SELECT 
            *,
            Event_Start_Date AS Sequel_Event_Start_Date,
            Der_Event_End_Date AS Sequel_Event_End_Date,
            Event_Type AS Sequel_Event_Type,
            Service_Type AS Sequel_Service_Type,
            Service_Component AS Sequel_Service_Component,
            Event_Outcome AS Sequel_Event_Outcome,
            DENSE_RANK() OVER (
              PARTITION BY 
                LA_Code, 
                Der_NHS_LA_Combined_Person_ID, 
                ST_Max_Cluster_ID
              ORDER BY 
                Event_Start_Date
            ) AS RN  --assigns row number for each sequel event with a different event start date, in order of event start date
          INTO #RankedEvents
          FROM #ST_MAX_New_Clients_Final
          WHERE
            Event_Start_Date >= ST_Max_Cluster_Start --incl. events that are ongoing at the end of reablement, but which started during the reablement
            AND Der_Event_End_Date > ST_Max_Cluster_End
            AND DATEDIFF(DAY, ST_Max_Cluster_End, Event_Start_Date) <= 7;   -- sequel period following reablement end, QA: try other sequel window time periods: 14, 30, 60 
    
        -- The next two select statements deal with cases where STMAX events are found within the sequel time window (7 days currently)

        -- Note: No STMAX should be found in the 7 days following ST-Max, because clustering parameter has been set to 7 days (i.e. they would be clustered together)
        -- If the clustering parameter was set to less than 7, or the sequel window changed to greater than 7, then we would expect to find STMAX sequels.
        -- The code here deals with this scenario so that events aren't considered if they start after the start date first STMAX encountered in the sequel window.

        -- Identify the first sequel event of type 'Short Term Support: ST-Max' per cluster
        DROP TABLE IF EXISTS #FirstSTMaxEvent
          SELECT 
            ST_Max_Cluster_ID,
            MIN(RN) AS First_STMax_RN
          INTO #FirstSTMaxEvent
          FROM #RankedEvents
          WHERE Sequel_Service_Type = 'Short Term Support: ST-Max'
          GROUP BY ST_Max_Cluster_ID

        -- only rows where RN < First_STMax_RN (or no ST-Max found)
        DROP TABLE IF EXISTS #Has_Future_Events_In_Window
        SELECT RE.*
        INTO #Has_Future_Events_In_Window
        FROM #RankedEvents RE
        LEFT JOIN #FirstSTMaxEvent FME
          ON RE.ST_Max_Cluster_ID = FME.ST_Max_Cluster_ID
        WHERE 
          FME.First_STMax_RN IS NULL       -- keep all if no ST-Max sequel exists
          OR RE.RN < FME.First_STMax_RN    -- keep only rows before that ST-Max sequel
          --OR RE.RN <= FME.First_STMax_RN    -- uncomment this instead to keep that ST-Max sequel and all events before it. This would mean that another ST-Max could end up being the sequel to an ST-Max.


        --================== Step 3: Determine the sequel for ST-Max with services in the chronology =====================

        --pick out reablements episodes that have service sequels

        --1. Join with service hierarchy reference table
        DROP TABLE IF EXISTS #Has_Future_Events_Service_Hierarchy

        SELECT 
          b.*,
          CASE 
              WHEN c.Hierarchy IS NULL THEN 99 
              ELSE c.Hierarchy 
          END AS [Service_Hierarchy]
        INTO #Has_Future_Events_Service_Hierarchy
        FROM #Has_Future_Events_In_Window b
        LEFT JOIN #REF_Service_Type c 
            ON b.Sequel_Service_Type = c.Service_Type;


        --2. pick out reablement clusters with onward service events
        DROP TABLE IF EXISTS #Has_Future_Services_In_Window

        SELECT *
        INTO #Has_Future_Services_In_Window
        FROM #Has_Future_Events_Service_Hierarchy
        WHERE ST_Max_Cluster_ID IN (
          SELECT ST_Max_Cluster_ID
          FROM #Has_Future_Events_Service_Hierarchy
          WHERE Service_Hierarchy != 99 --i.e. a recognised service exists (doesn't include support to carers)
          GROUP BY ST_Max_Cluster_ID
        )

        /*Choose the lowest Hierarchy (i.e. highest ranking Service) against each ST-Max cluster in cases where multiple Service Events were found in the 7 days 
        > i.e. if they received short term support and long term support within the 7 days, the outcome should be long term support. 
        > if they received other short term support, then prioritise service component of short term res/nurs over anything else */

        -- 3. Rank services first by hierarhcy on service type, then by service component Residential/Nursing preference
        DROP TABLE IF EXISTS #Ranked_Services;

        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY 
              LA_Code, 
              Der_NHS_LA_Combined_Person_ID, 
              ST_Max_Cluster_ID
            ORDER BY 
              Service_Hierarchy ASC,  -- lowest hierarchy wins
              CASE 
                WHEN Sequel_Service_Component IN ('Short Term Residential Care', 'Short Term Nursing Care') THEN 1
                ELSE 2
              END  -- residential/nursing preferred within same hierarchy
            ) AS Service_Rank
        INTO #Ranked_Services
        FROM #Has_Future_Services_In_Window

        -- 4. Pick the top-ranked service and apply the custom outcome logic
        DROP TABLE IF EXISTS #Find_Top_Service;

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End, 
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Working_Age_Band,
          Sequel_Service_Component AS Top_Service_Component,
          CASE 
            WHEN Sequel_Service_Type = 'Short Term Support: Other Short Term'
              AND Sequel_Service_Component IN ('Short Term Residential Care', 'Short Term Nursing Care')
            THEN 'Short Term Support: Residential or Nursing Care'
            ELSE Sequel_Service_Type
          END AS Final_Outcome
        INTO #Find_Top_Service
        FROM #Ranked_Services 
        WHERE Service_Rank = 1;  -- take only the top-ranked

        --Insert into Final table, using the Sequel Service Type as the final outcome 
        INSERT INTO #ASCOF2A
        SELECT 
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          Final_Outcome,
          '3' AS 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #Find_Top_Service

        -- remove staged clusters 
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );

        --================== Step 4: Determine the sequel for ST-Max using the event outcomes from:
        --==================                (1) non-service events in the forward chronology
        --==================                (2) the stmax episode
        --==================                (3) the reviews/assessments that occur towards the end of the ST-Max episode. 


        --Prepare a table containing usable STMAX Event Outcomes for those without future events
        DROP TABLE IF EXISTS #Has_Usable_Stmax_EO_Only

        SELECT DISTINCT
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Start,
          ST_Max_Cluster_End,
          ST_Max_Cluster_Event_Outcome,
          ST_Max_Cluster_Event_Outcome_Hierarchy,
          ST_Max_Cluster_Working_Age_Band
        INTO #Has_Usable_Stmax_EO_Only
        FROM #ST_MAX_New_Clients_Final h
        WHERE NOT EXISTS (
            SELECT 1
            FROM #Has_Future_Events_In_Window a
            WHERE a.ST_Max_Cluster_ID = h.ST_Max_Cluster_ID)
        AND ST_Max_Cluster_Event_Outcome_Hierarchy < 100; -- with a usable event outcome
        ;


        -- pick out the stmax episodes with only non-service events in the onward chain
        -- map the Event Outcome of the STMAX episode to the event outcome hierarchy

        DROP TABLE IF EXISTS #Has_Usable_Future_NonServices_Only

        SELECT 
          H.*
        INTO #Has_Usable_Future_NonServices_Only
        FROM #Has_Future_Events_Service_Hierarchy H
        WHERE NOT EXISTS ( -- remove clusters that have services in the chronology.
            SELECT 1
            FROM #Find_Top_Service a
            WHERE a.ST_Max_Cluster_ID = H.ST_Max_Cluster_ID) 
        -- remove rows with no usable outcomes, either in the cluster outcome or in the joined event outcome.
        AND (H.ST_Max_Cluster_Event_Outcome_Hierarchy < 100 OR H.Event_Outcome_Hierarchy < 100) 
        --remove these because, a 'Service ended as planned' outcome on a non-service future event is almost certainly not related an outcome of the STMAX. 
        AND H.Event_Outcome != 'Service ended as planned' 
        --get rid of remaining service sequels (services that didn't get picked up in step 1, mostly (entirely) 'support to carer')
        AND Event_Type != 'Service' ;


        -- Prepare a table of nested events (events that begin and end within the ST-Max episode period.
        -- Search for events that occur betwen the start and end date of the ST-Max cluster to see if they provide more information
        -- as to the likely outcome of the ST-Max

        --identify which clusters have nested events
        DROP TABLE IF EXISTS #Has_Nested_Events

         SELECT *,
           Event_Start_Date AS Sequel_Event_Start_Date,
           Der_Event_End_Date AS Sequel_Event_End_Date,
           Event_Type AS Sequel_Event_Type,
           Service_Type AS Sequel_Service_Type,
           Service_Component AS Sequel_Service_Component,
           Event_Outcome AS Sequel_Event_Outcome
        INTO #Has_Nested_Events
        FROM #ST_MAX_New_Clients_Final
        WHERE
           Event_Start_Date >= ST_Max_Cluster_Start AND 
           Der_Event_End_Date > ST_Max_Cluster_Start AND -- strictly greater than, to avoid picking up the request/assessement leading to the current STMAX episode
           Der_Event_End_Date <= ST_Max_Cluster_End  --if it was after the ST-Max end it would have already been picked up in the prior steps

        --Assign event outcome to the cluster of nested services based on the event outcome hierarchy
        DROP TABLE IF EXISTS #Nested_Assessments_Reviews
 
        SELECT 
          *,
          --LAST VALUE takes the event outcome of the last row in the partition, i.e. row with the latest event end date
          --If there are two rows with conflicting event outcomes for the same latest date the hierarchy is applied
          LAST_VALUE(Sequel_Event_Outcome) OVER (PARTITION BY
                                                   LA_Code,  
                                                   LA_Name, 
                                                   Der_NHS_LA_Combined_Person_ID, 
                                                   ST_Max_Cluster_ID 
                                                 ORDER BY
										                               Der_Event_End_Date ASC, 
                                                   COALESCE(Event_Outcome_Hierarchy, 999) DESC, 
                                                   Der_unique_record_id ASC 
                                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Sequel_Cluster_EO,
          LAST_VALUE(Event_Outcome_Hierarchy) OVER (PARTITION BY
                                                   LA_Code,  
                                                   LA_Name, 
                                                   Der_NHS_LA_Combined_Person_ID, 
                                                   ST_Max_Cluster_ID 
                                                 ORDER BY
										                               Der_Event_End_Date ASC, 
                                                   COALESCE(Event_Outcome_Hierarchy, 999) DESC, 
                                                   Der_unique_record_id ASC 
                                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Sequel_Cluster_EO_hierarchy
        INTO #Nested_Assessments_Reviews
        FROM #Has_Nested_Events b
        WHERE Sequel_Event_Type IN ('Assessment','Review')
        AND (DATEDIFF(DAY, Der_Event_End_Date, ST_Max_Cluster_End) <  14)
        AND Event_Outcome_Hierarchy < 100; -- i.e., only include non-service events with usable outcomes.

        -- Choose the Event Outcome with the highest ranking in the EO hierarchy 
        -- Combine the event outcomes from (1) the ST-Max epsiode (4a)
        --                                 (2) non-service events following the ST-Max  (4b)
        --                                 (3) the reviews/assessments that occur towards the end of the ST-Max episode.(4c)

        DROP TABLE IF EXISTS #All_Outcome_Hierarchies

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End,
          ST_Max_Cluster_Working_Age_Band,
          ST_Max_Cluster_Event_Outcome_Hierarchy AS Event_Outcome_Hierarchy,
          1 AS Event_Outcome_Source -- will take the value 1 if the Event Outcome comes from the ST_Max_Cluster and 2 if it comes from the Sequel Event
        INTO #All_Outcome_Hierarchies
        FROM #Has_Usable_Future_NonServices_Only

        UNION ALL

        --creates a row for each sequel event that has an NFA outcome
        SELECT
	        LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
	        ST_Max_Cluster_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End,
          ST_Max_Cluster_Working_Age_Band,
          Event_Outcome_Hierarchy,
          2 AS Event_Outcome_Source
        FROM #Has_Usable_Future_NonServices_Only
        WHERE Event_Outcome_Hierarchy < 100 --only keep sequels with usable NFA outcomes. 
        --'Service ended as planned' as already been excluded from #Has_Usable_Future_NonServices_Only 

        UNION ALL

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID, 
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End,
          ST_Max_Cluster_Working_Age_Band,
          Sequel_Cluster_EO_hierarchy AS Event_Outcome_Hierarchy,
          3 AS Event_Outcome_Source  --from the review/assessment near the end of the ST-Max
        FROM #Nested_Assessments_Reviews

        UNION ALL

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID, 
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End,
          ST_Max_Cluster_Working_Age_Band,
          ST_Max_Cluster_Event_Outcome_Hierarchy AS Event_Outcome_Hierarchy,
          1 AS Event_Outcome_Source  --from the ST-Max as there aren't any future events
        FROM #Has_Usable_Stmax_EO_Only


        -- Get minimum hierarchy event outcome per cluster (considering both the ST-Max outcome and the sequel event outcomes)
        DROP TABLE IF EXISTS #Final_Min_Outcome_Hierarchy

        SELECT 
          a.*,
          r.Event_Outcome_Spec as 'Final_Outcome',
          CASE 
            WHEN EXISTS (
              SELECT 1 
              FROM #All_Outcome_Hierarchies h
              WHERE 
                h.LA_Code = a.LA_Code AND
                h.Der_NHS_LA_Combined_Person_ID = a.Der_NHS_LA_Combined_Person_ID AND
                h.ST_Max_Cluster_ID = a.ST_Max_Cluster_ID AND
                h.Event_Outcome_Hierarchy = a.Cluster_Min_Event_Outcome_Hierarchy AND
                h.Event_Outcome_Source = 1
            ) THEN 1
	        WHEN EXISTS (
              SELECT 1 
              FROM #All_Outcome_Hierarchies h
              WHERE 
                h.LA_Code = a.LA_Code AND
                h.Der_NHS_LA_Combined_Person_ID = a.Der_NHS_LA_Combined_Person_ID AND
                h.ST_Max_Cluster_ID = a.ST_Max_Cluster_ID AND
                h.Event_Outcome_Hierarchy = a.Cluster_Min_Event_Outcome_Hierarchy AND
                h.Event_Outcome_Source = 2
            ) THEN 2
	        WHEN EXISTS (
              SELECT 1 
              FROM #All_Outcome_Hierarchies h
              WHERE 
                h.LA_Code = a.LA_Code AND
                h.Der_NHS_LA_Combined_Person_ID = a.Der_NHS_LA_Combined_Person_ID AND
                h.ST_Max_Cluster_ID = a.ST_Max_Cluster_ID AND
                h.Event_Outcome_Hierarchy = a.Cluster_Min_Event_Outcome_Hierarchy AND
                h.Event_Outcome_Source = 3
            ) THEN 3
	        ELSE 99 -- i.e., ERROR: 99 should not exist in final data.
          END AS ST_Max_Cluster_Event_Outcome_Source
        INTO #Final_Min_Outcome_Hierarchy
        FROM (
          SELECT
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID,
            ST_Max_Cluster_ID,
            ST_Max_Cluster_Start, 
            ST_Max_Cluster_End,
            ST_Max_Cluster_Working_Age_Band,
            MIN(Event_Outcome_Hierarchy) AS Cluster_Min_Event_Outcome_Hierarchy
          FROM #All_Outcome_Hierarchies
          GROUP BY 
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID,
            ST_Max_Cluster_ID,
            ST_Max_Cluster_Start, 
            ST_Max_Cluster_End,
            ST_Max_Cluster_Working_Age_Band
	        ) a
        LEFT JOIN #REF_Event_Outcome_Hierarchy_UTC r
        ON a.Cluster_Min_Event_Outcome_Hierarchy = r.Event_Outcome_Hierarchy;

        --Insert into Final table
        INSERT INTO #ASCOF2A

        SELECT 
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          CASE 
            WHEN Cluster_Min_Event_Outcome_Hierarchy = 999 THEN 'Invalid and not mapped'
            ELSE Final_Outcome
          END AS 'Final_Outcome',
          CASE
            WHEN ST_Max_Cluster_Event_Outcome_Source = 1 THEN '4a' -- from an ST-Max cluster event outcome (either with or without future events)
	        WHEN ST_Max_Cluster_Event_Outcome_Source = 2 THEN '4b' -- from an event following the ST-Max cluster
	        WHEN ST_Max_Cluster_Event_Outcome_Source = 3 THEN '4c' -- from a nested assessement or review
            ELSE 'NA' -- should not occur
          END AS 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #Final_Min_Outcome_Hierarchy

        -- remove staged clusters 
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );


        --================== Step 5: Where equipment was delivered during the ST-Max =====================

        --Check if any of the remaining events are services with Service_Component = Equipment
        DROP TABLE IF EXISTS #Nested_Short_Term_Ongoing

        --assign a final outcome of 'Short Term Support: Ongoing Low Level' to the STMAX cluster if there is a nested 'Equipment' service.
        SELECT DISTINCT
           LA_Code, 
           LA_Name, 
           Der_NHS_LA_Combined_Person_ID, 
           ST_Max_Cluster_ID,
           'Short Term Support: Ongoing Low Level' as 'Final_Outcome',
           ST_Max_Cluster_Working_Age_Band
        INTO #Nested_Short_Term_Ongoing
        FROM #Has_Nested_Events b
        WHERE Sequel_Event_Type = 'Service' 
         AND Sequel_Service_Component = 'Equipment' 
         AND NOT EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = b.ST_Max_Cluster_ID
        )

        --Insert into Final table
        INSERT INTO #ASCOF2A

        SELECT 
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          Final_Outcome,
          '5' AS 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #Nested_Short_Term_Ongoing

        -- remove staged clusters 
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );

        --================== Step 6: The remaining unable to classify sequels =====================

        --Insert the remaining ST-Max with Unable to Classify Sequels into the ASCOF table 
        INSERT INTO #ASCOF2A

        SELECT DISTINCT
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome AS Final_Outcome,
          '6' AS 'Sequel_Type',
          ST_Max_Cluster_Working_Age_Band
        FROM #ST_MAX_New_Clients_Final

        -- remove staged clusters (for completeness, should be 0 clusters remaining in #ST_MAX_New_Clients_Final)
        DELETE FROM #ST_MAX_New_Clients_Final
        WHERE EXISTS (
            SELECT 1
            FROM #ASCOF2A a
            WHERE a.ST_Max_Cluster_ID = #ST_MAX_New_Clients_Final.ST_Max_Cluster_ID
        );

        --QA check
        --select * from #ST_MAX_New_Clients_Final   --should be 0

        --====================Create final output tables ===================================

        --1. Final breakdown of ST-Max services by final outcome:
        DROP TABLE IF EXISTS #OutputTable_Disaggregated

        SELECT 
          FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + 
          FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
          A.LA_Code,
          R.LA_Name,
          ST_Max_Cluster_Working_Age_Band AS Age_Band,
          Sequel_Type,
          Final_Outcome,
          Included_In_Denom,
          Included_In_Num,
          Incl_In_Unable_To_Classify,
          COUNT(*) AS ST_Max_Count
        INTO #OutputTable_Disaggregated
        FROM (
          SELECT 
            *,
            CASE 
              WHEN Final_Outcome IN (
                          'Long Term Support: Community'
                          ,'Long Term Support: Nursing Care'
                          ,'Long Term Support: Residential Care'
                          ,'Long Term Support: Prison'
                          ,'Short Term Support: Ongoing Low Level'
                          ,'Short Term Support: Other Short Term'
                          ,'Short Term Support: Residential or Nursing Care'
                          ,'NFA - Information & Advice / Signposting only'
                          ,'Service ended as planned'
                          ,'NFA - Moved to another LA'
                          ,'NFA- Other'
                          ,'NFA - No services offered: Other reason'
                          ,'NFA - Support ended: Other reason'
                          )
              THEN 'Y' ELSE 'N'
            END AS Included_In_Denom,
            CASE 
              WHEN Final_Outcome IN (
                           'Short Term Support: Ongoing Low Level'
                          ,'Short Term Support: Other Short Term'
                          ,'NFA - Information & Advice / Signposting only'
                          ,'Service ended as planned'
                          ,'NFA - Moved to another LA'
                          ,'NFA- Other'
                          ,'NFA - No services offered: Other reason'
                          ,'NFA - Support ended: Other reason'
                          )
              THEN 'Y' ELSE 'N'
            END AS Included_In_Num,
	          CASE 
              WHEN Final_Outcome IN (
                            'No change in package',
						                'Progress to Support Planning / Services',
						                'Progress to financial assessment',
						                'Progress to Re-assessment / Unplanned Review',
						                'Progress to Assessment',
						                'Progress to Reablement/ST-Max',
					                  'Provision of service',
                            'Progress to End of Life Care',
                            'Invalid and not mapped',
                            'Release 2 multiple mappings: Progress to assessment, review or reassessment',
                            'Release 2 multiple mappings: Continuation of support or services'
                             )
              THEN 'Y' ELSE 'N'
            END AS Incl_In_Unable_To_Classify
          FROM #ASCOF2A) A
        LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup_April_2024 R
        ON A.LA_Code = R.LA_Code
        GROUP BY  
          A.LA_Code, 
          R.LA_Name,
          ST_Max_Cluster_Working_Age_Band,
          Final_Outcome,
          Sequel_Type, 
          Included_In_Denom,
          Included_In_Num,
          Incl_In_Unable_To_Classify
        ORDER BY 
          A.LA_Code, 
          R.LA_Name,
          ST_Max_Cluster_Working_Age_Band,
          Final_Outcome,
          Sequel_Type


        -- 2. Summary table for PBI

        -- Step 1: Define all age band categories including 'Total'
        DROP TABLE IF EXISTS #AgeBands

        SELECT '18 to 64' AS [Age_Band]
        INTO #AgeBands
        UNION ALL
        SELECT '65 and above'
        UNION ALL
        SELECT 'Total'


        -- Step 2: Create all LA - Age Band combinations
        DROP TABLE IF EXISTS #LA_AgeBand_Cross

        SELECT 
          r.LA_Code,
          r.LA_Name,
          ab.Age_Band
        INTO #LA_AgeBand_Cross
        FROM ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup_April_2024 r
        CROSS JOIN #AgeBands ab

        -- Step 3: Aggregate actual data (including 'Total' per LA using GROUP BY)
        DROP TABLE IF EXISTS #Aggregated

        SELECT 
            a.LA_Code, 
            ISNULL(a.Age_Band, 'Total') AS Age_Band,
            SUM(CASE WHEN Included_In_Num = 'Y' THEN ST_Max_Count ELSE 0 END) AS Numerator,
            SUM(CASE WHEN Included_In_Denom = 'Y' THEN ST_Max_Count ELSE 0 END) AS Denominator
        INTO #Aggregated
        FROM #OutputTable_Disaggregated a
        GROUP BY a.LA_Code, ROLLUP(a.Age_Band)


        -- Step 4: Combine full grid with actual data
        DROP TABLE IF EXISTS #OutputTable

        SELECT 
          FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
          lac.LA_Code,
          lac.LA_Name,
          lac.Age_Band AS [Group],
          'ASCOF 2A' AS Measure,
          'The proportion of people who received short-term services during the year - who previously were not receiving services - where no further request was made for ongoing support (%)' AS [Description],
          ISNULL(agg.Numerator, 0) AS Numerator,
          ISNULL(agg.Denominator, 0) AS Denominator,
          CASE 
            WHEN ISNULL(agg.Denominator, 0) = 0 THEN 0
            ELSE ROUND(
              (CAST(ISNULL(agg.Numerator, 0) AS FLOAT) / 
               CAST(agg.Denominator AS FLOAT)) * 100, 1)
          END AS [Outcome]
        INTO #OutputTable
        FROM #LA_AgeBand_Cross lac
        LEFT JOIN #Aggregated agg 
          ON lac.LA_Code = agg.LA_Code 
             AND lac.Age_Band = agg.Age_Band
        ORDER BY lac.LA_Code, 
                 CASE 
                   WHEN lac.Age_Band = '18 to 64' THEN 1
                   WHEN lac.Age_Band = '65 and above' THEN 2
		               --WHEN lac.Age_Band = 'Unknown' THEN 3 -- don't display unknown ages in final table
                   ELSE 4
                 END;
   
    SET @Query = 'SELECT * INTO ' + @OutputTable_Disaggregated + ' FROM #OutputTable_Disaggregated'
    EXEC(@Query)

    SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
    EXEC(@Query)


    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO

-----Example execution
/*
EXEC ASC_Sandbox.Create_ASCOF2A_2425_Onwards
  @ReportingPeriodStartDate = '2024-04-01',
  @ReportingPeriodEndDate = '2025-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions',
  @InputTable_PersonDetails = 'ASC_Sandbox.CLD_230401_250630_JoinedSubmissions_Latest_Person_Data_2425',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF2A_Disaggregated',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A'
*/
