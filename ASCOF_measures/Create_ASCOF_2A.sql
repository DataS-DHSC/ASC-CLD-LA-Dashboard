--------------------------------------------------------------------------------------------------------
-----------------------------SECTION 1 -  Determine reablement events ----------------------------------
--------------------------------------------------------------------------------------------------------

--================== Select analysis perid and submissions =====================
/* Select the relevant joined submissions table, ideally 27 months of data:
> ASCOF 2A counts all reablement which ended within 12 month period
> 3 months lag at the end is required to determine the immediate outcome for reablement which ended right at the end of the year
> Data for the previous year is required to determine whether someone is a new client or not (No LTS in 12 months prior to reablement)
> Total = 27 months
*/

DROP PROCEDURE IF EXISTS ASC_Sandbox.Create_ASCOF2A

GO

CREATE PROCEDURE ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate DATE,
  @ReportingPeriodEndDate DATE,
  @InputTable AS NVARCHAR(50),
  @OutputTable AS NVARCHAR(50),
  @OutputTable_Disaggregated AS NVARCHAR(50)

AS
  
  DECLARE @Query NVARCHAR(MAX)

  DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

  SET @Query =  'DROP TABLE IF EXISTS ' + @OutputTable + '; 
                 DROP TABLE IF EXISTS ' + @OutputTable_Disaggregated + '; 
                 DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable
                 CREATE SYNONYM ASC_Sandbox.InputTable FOR ' + @InputTable + ';'
  EXEC(@Query)

        DROP TABLE IF EXISTS #ASCOF2A_Build_Temp

        SELECT *
        INTO #ASCOF2A_Build_Temp
        FROM ASC_Sandbox.InputTable
        WHERE 
          Client_Type = 'Service User'
          AND Der_Age_Event_Start_Date >= 18
          AND Der_NHS_LA_Combined_Person_ID IS NOT NULL
          AND Event_Start_Date IS NOT NULL
          AND (Date_of_Death >= @ReportingPeriodStartDate OR Date_of_Death is NULL)  -- set date to reporting period start
          AND (Der_Event_End_Date >= Event_Start_Date OR Der_Event_End_Date IS NULL) --removes DQ issues of event end date prior to start date

        --Set null end dates to future date for ease of processing
        UPDATE #ASCOF2A_Build_Temp
        SET Der_Event_End_Date = 
          CASE 
            WHEN Der_Event_End_Date IS NULL THEN '9999-01-01' 
            ELSE Der_Event_End_Date
        END;


        --================= Pull through cleaned event outcome ===================
        DROP TABLE IF EXISTS #ASCOF2A_Build

        SELECT 
          a.*,
          COALESCE(eo.Event_Outcome_Cleaned, 'Invalid and not mapped') as Event_Outcome_Cleaned,
          eoh.Event_Outcome_Hierarchy
        INTO #ASCOF2A_Build  
        FROM #ASCOF2A_Build_Temp a
        LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Mapping eo
        ON a.Event_Outcome_Raw = eo.Event_Outcome_Raw
        LEFT JOIN ASC_Sandbox.REF_Event_Outcome_Hierarchy eoh
        ON eo.Event_Outcome_Cleaned = eoh.Event_Outcome_Spec

        --Remove raw field to prevent it being used:
        ALTER TABLE #ASCOF2A_Build
        DROP COLUMN Event_Outcome_Raw;


        --================== Select ST-Max events ============

        --Identify ST-Max (reablement) events
        DROP TABLE IF EXISTS #ST_Max_All

        SELECT *
        INTO #ST_Max_All
        FROM #ASCOF2A_Build
        WHERE Event_Type = 'Service'
        AND Service_Type_Cleaned = 'Short Term Support: ST-Max' 


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
          Service_Type_Cleaned,
          Service_Component,
          Event_Outcome_Cleaned,
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


        --2. Determine whether events are consecutive (max 1 day apart) or concurrent (overlapping), then assign cluster id 
        DROP TABLE IF EXISTS #ST_Max_Clusters_Assigned

        SELECT 
          *,
          DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) AS Day_Diff,
          CASE 
            WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 1 THEN 0 
            ELSE 1 
          END AS ClusterStartInd,
          SUM (CASE 
                  WHEN DATEDIFF(DAY, Previous_End_Date, Event_Start_Date) <= 1 THEN 0 
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
          LAST_VALUE(Event_Outcome_Cleaned) OVER (PARTITION BY  
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
        DROP TABLE IF EXISTS #ST_Max_Joined

        SELECT 
          a.ST_Max_Cluster_ID,
	        a.ST_Max_Cluster_Start,
	        a.ST_Max_Cluster_End,
          a.ST_Max_Cluster_Event_Outcome,
	        b.*,
	        CASE
		        WHEN 
              Event_Start_Date BETWEEN ST_Max_Cluster_Start AND ST_Max_Cluster_End  --all start dates will be within the cluster start/end if they were used to form the cluster
		          AND Event_Type = 'Service'
		          AND Service_Type_Cleaned = 'Short Term Support: ST-Max'
		        THEN 1
		        ELSE 0
	        END AS Same_ST_Max
        INTO #ST_Max_Joined
        FROM #ST_Max_Clusters_In_period a
        LEFT JOIN #ASCOF2A_Build b    
        ON a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID
        AND a.LA_Code = b.LA_Code


        --2. Set fields to null where ST-Max joins onto itself
        --Can't delete the row as we would lose ST-Max records where their only joined entry is them joining on themselves
        Update #ST_Max_joined SET 
          Ref_Period_Start_Date = NULL,
          Ref_Period_End_Date = NULL,
          Client_Type = NULL,
          Gender_Cleaned = NULL,
          Ethnicity_Cleaned = NULL,
          Date_of_Death = NULL,
          Accommodation_Status = NULL,
          Employment_Status = NULL,
          Has_Unpaid_Carer = NULL,
          Client_Funding_Status = NULL,
          Primary_Support_Reason = NULL,
          Event_Type = NULL,
          Event_Start_Date = NULL,
          Der_Event_End_Date = NULL,
          Event_Outcome_Cleaned = NULL,
          Event_Outcome_Grouped = NULL,
          Request_Route_of_Access = NULL,
          Assessment_Type = NULL,
          Eligible_Needs_Identified = NULL,
          Method_of_assessment = NULL,
          Review_Reason = NULL,
          Review_Type = NULL,
          Review_Outcomes_Achieved = NULL,
          Method_of_Review = NULL,
          Service_Type_Cleaned = NULL,
          Service_Type_Grouped = NULL,
          Service_Component = NULL,
          Delivery_Mechanism = NULL,
          Der_Conversation = NULL,
          Der_Conversation_1 = NULL,
          Der_Unique_Record_ID = NULL
        WHERE Same_ST_Max = 1

        --================== Identify new clients =====================

        --1. Create flag for new clients (no LTS in the 12m prior to reablement start)
        --Inital flag is at event level (cluster may have joined onto multiple events), New_Client_Cluster_Flag therefore assigns the flag at cluster level
        --If there is a record joined to the cluster where new_client = 0 then this takes prescedent

        --N.B. This code also creates a flag for whether there's a request present within 28 days prior to reablement start
        --This is not a requirement for this metric but potentially helpful for sharing back with LAs
        DROP TABLE IF EXISTS #ST_Max_Joined_Flags

        SELECT *,
          --Set new_client = 0 for the cluster where LTS present in 12m prior to ST-Max:
           MIN( New_Client_Record_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS New_Client_Cluster_Flag,  
           --Set prior_request = 1 for the cluster where prior request is present
           MAX( Prior_Request_Record_Flag) OVER (PARTITION BY ST_Max_Cluster_ID) AS Prior_Request_Cluster_Flag 
        INTO #ST_Max_Joined_Flags
        FROM (
            SELECT *,
              CASE  --New client flag logic: no LTS with an end date within 12 months (365 days) prior to ST-Max start date
                WHEN Service_Type_Cleaned IN ('Long Term Support: Nursing Care',
                                      'Long Term Support: Residential Care',
                                      'Long Term Support: Community',
                                      'Long Term Support: Prison')
	                   AND Event_Start_Date < ST_Max_Cluster_Start  
                     AND (DATEDIFF(DAY, Der_Event_End_Date, ST_Max_Cluster_Start) <  365 --Alter this to change threshold for new clients  
                     OR Der_Event_End_Date IS NULL)
                THEN 0
  	            ELSE 1
	            END AS New_Client_Record_Flag,
              CASE --Prior request logic: any request within 28 days of ST-Max start (not actually required with updated methodology)
                WHEN Event_Type = 'Request' 
	                   AND Event_Start_Date < ST_Max_Cluster_Start  
                     AND (DATEDIFF(DAY, Der_Event_End_Date, ST_Max_Cluster_Start) < 28)
                THEN 1 
                ELSE 0
              END AS Prior_Request_Record_Flag
            FROM #ST_Max_Joined
        )A


        --2. Filter to new clients only 
        DROP TABLE IF EXISTS #ST_MAX_New_Clients_Final   --The final table of all reablement events to be counted in final output

        SELECT *
        INTO #ST_MAX_New_Clients_Final
        FROM #ST_Max_Joined_Flags
        WHERE New_Client_Cluster_Flag = 1


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
        ,('Short Term Support: Ongoing Low Level', 5, 5)
        ,('Short Term Support: Other Short Term', 6, 6)



        --------------------------------------------------------------------------------------------------------
        ----------------------------- SECTION 2 -  Determine sequels -------------------------------------------
        --------------------------------------------------------------------------------------------------------

        /* ST-MAX clusters will now be staged into final table along with their correct Sequel (where known)
        As they are staged they will be recorded with a 'Sequel Type' to track/record how each ST-Max cluster has been categorised

        Category Key:
        1 = No Further Chronology (at all). Sequel derived from Event Outcome of ST-Max cluster
        2 = ST-Max with a ‘Service’ sequel (ranked by Hierarchy)
        3 = No sequel ‘Service’ found, but an ‘NFA’ outcome encountered in the chronology (e.g. outcome of an Assessment)
        4 = No NFA outcomes observed (as per ‘3’ above) in the sequel chronology, defer back to the Event Outcome of the ST-Max cluster
        5 = Out Of Scope. Onward chronology was found, but was either a) too long after the ST-Max event ended or b) another ST-Max was encountered in the chronology, superseding this cluster.

        */

        --================== ST-Max with future events =====================
        -- Used to determine sequel categories 2-5

        DROP TABLE IF EXISTS #Has_Future_Events

        SELECT *,
          Event_Start_Date AS Sequel_Event_Start_Date,
          Der_Event_End_Date AS Sequel_Event_End_Date,
          Event_Type AS Sequel_Event_Type,
          Service_Type_Cleaned AS Sequel_Service_Type,
          Service_Component AS Sequel_Service_Component,
          Event_Outcome_Cleaned AS Sequel_Event_Outcome,
          DENSE_RANK() OVER (PARTITION BY 
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID, 
            ST_Max_Cluster_ID 
            ORDER BY 
            Event_Start_Date,--assigns row number for each sequel event with a different event start date, in order of event start date
            Der_Event_End_Date) as RN --have added event end date to order those with the same start date (need to check impact and if this is right)
        INTO #Has_Future_Events
        FROM #ST_MAX_New_Clients_Final
        WHERE 
          Event_Start_Date >= ST_Max_Cluster_End --incl. events which start on or after reablement end  OR 
          OR (Event_Start_Date >= ST_Max_Cluster_Start AND Der_Event_End_Date >= ST_Max_Cluster_End) --incl. events which are ongoing at the end of reablement  

        --================== STEP 1: ST-Max with no further chronology =====================

        --1. Identify those with no future events by removing those with future events
        DROP TABLE IF EXISTS #No_Future_Events

        SELECT DISTINCT 
          ST_Max_Cluster_ID,
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_Event_Outcome
        INTO #No_Future_Events
        FROM #ST_MAX_New_Clients_Final
        WHERE ST_Max_Cluster_ID NOT IN 
          (SELECT ST_Max_Cluster_ID
          FROM #Has_future_events)

  
        --Insert these clusters into the final #ASCOF2A with their event outcome as the final outcome 
        DROP TABLE IF EXISTS #ASCOF2A

        SELECT  
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome as 'Final_Outcome',
          '1' as 'Sequel_Type'
        INTO #ASCOF2A
        FROM #No_Future_Events;


        --================== STEP 2: Determine the sequel chains for ST-Max with further chronology =====================

        -----------------------------
        /*Building chain of Sequels*/
        -----------------------------
        --NB previsouly descibed as 'clustering' in NHSE code but renamed 'Building chain' here as to not confuse with clustering of ST-Max 
        --Split the code out in order to understand the process (removed any nesting) and simplified

        /*This code creates a process called 'Build_chain' where Sequel Events that begin up to and including 3 days after the previous Sequel Event ending are flagged '1' (to include)

        The 'In_Chain' field created flags whether or not a sequel event is still part of an unbroken chain/sequence of events following the ST-Max (>3 day gap between
        one Event ending and another starting breaks the chain/cluster and everything after this point is considered unrelated to the original ST-Max cluster)

        The 'Chain_ID' assigns a unique ID for each chain associated with an ST-Max cluster. This is used to determine when a chain breaks.

        The 'In_Scope' field created flags whether or not the first event following the ST-Max is considered within scope.
        If the first is either open on the ST-Max end date or starts up to and including 3 days after, this chronology is in scope. 
        If the first event is 4 or more days after the ST-Max end then as too much time has elapsed and it is not in scope.

        */

        --1. Create the 'In_chain' flag to identify whether an event is in a chain with the row above (previous event)
        --First rows are flagged as 1 to start the chain
        DROP TABLE IF EXISTS #Build_chain

        SELECT 
            *,
            CASE 
                WHEN DATEDIFF(DAY, LAG(Sequel_Event_End_Date) OVER (
                    PARTITION BY 
                        LA_Code, 
                        Der_NHS_LA_Combined_Person_ID, 
                        ST_Max_Cluster_ID 
                    ORDER BY 
                        RN), --Original ordered by sequel event end date, changed to RN which is assigned using start and end dates
                    Sequel_Event_Start_Date) > 3 THEN 0   --Alter this and line 464 to change threshold for linking events together
                ELSE 1 
            END AS In_Chain -- Build chain/cluster of sequel events with a gap of no more than 4 days
        INTO #Build_chain
        FROM #Has_Future_Events;


        --2. Revised & simplified - Assign a chain ID within each ST-Max, to identify when a sequl chain breaks
        DROP TABLE IF EXISTS #Build_chain_IDs

        SELECT 
            *,
            SUM(CASE 
                    WHEN In_Chain = 0 THEN 1 -- Increment the chain ID when the chain breaks
                    ELSE 0 
                END) OVER (
                    PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID, ST_Max_Cluster_ID
                    ORDER BY RN
                ) AS Chain_ID
        INTO #Build_chain_IDs
        FROM #Build_chain;


        --3. Revised & simplified - determine chains which are in scope (the first event is within 3 days of ST-max end or open on ST-max end date)
        --Initial event in scope flag
        DROP TABLE IF EXISTS #Initial_In_Scope

        SELECT
          *,
          CASE 
          --Earlier restricted only to events starting after ST-Max end, or they started after ST-Max start and were open and ongoing at ST-Max end
            WHEN RN = 1 AND DATEDIFF(DAY, ST_Max_Cluster_End, Sequel_Event_Start_Date) <= 3 THEN 1 ELSE 0 --Alter this and line 431 to change threshold for linking events together
          END AS Initial_In_Scope
        INTO #Initial_In_Scope
        FROM #Build_chain_IDs
        ORDER BY 
          ST_Max_Cluster_ID, 
          RN


        --Chain in scope flag
        DROP TABLE IF EXISTS #Chain_In_Scope

        SELECT 
            *,
            MAX(CASE 
                    WHEN Initial_In_Scope = 1 THEN 1 -- Start of the chain is in scope
                    ELSE 0 
                END) OVER (
                    PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID, ST_Max_Cluster_ID, Chain_ID
                    ORDER BY RN
                ) AS Chain_In_Scope
        INTO #Chain_In_Scope
        FROM #Initial_In_Scope;

        ---------------------------------
        /*Another ST-Max encountered in Sequels*/
        ---------------------------------

        /*Addition of a flag that signifies a 'break' in the series when another ST-MAX record is encountered in the chronology (Cluster_No_STMax flag)

        A more recent ST-Max cluster should supplant and supersede the existing one, so the chronology search is stopped at this stage if Cluster_No_ST_Max flag does not = 1
        The superseded ST-Max cluster gets staged into #ASCOF2A at the end of the process

        This table also pulls through the 'Hierarchy' column from REF data to allow for subsequent selection of the highest ranking Service (lowest Hierarchy number)
        using a join to the #REF_Service_Type table where more than one Service appears in an ST-Max cluster. Records that have not joined to the Service Reference Table 
        (e.g. Assessments, Requests) have been CASED as '99' to ensure they are always out-ranked by a Service Event*/


        --1. Calculate flag at event level for where ST-Max appear in sequel
        DROP TABLE IF EXISTS #ST_Max_Sequel_Flag

        SELECT *,
            CASE 
                WHEN Sequel_Service_Type LIKE 'Short Term Support: ST-Max' 
                AND Chain_In_Scope = 1 
                THEN 1 
                ELSE 0 
            END AS ST_Max_sequel  --Changed to flag where ST-max is present rather than not present (confusing the other way round)
        INTO #ST_Max_Sequel_Flag
        FROM #Chain_In_Scope;


        --2. Assign the flag to the whole ST-Max cluster
        DROP TABLE IF EXISTS #Cluster_ST_Max_Sequel_Flag

        SELECT *,
            MAX(ST_Max_sequel) OVER (  --changed to MAX as flagging presence not absence now of ST-Max
                PARTITION BY 
                    LA_Code, 
                    Der_NHS_LA_Combined_Person_ID, 
                    ST_Max_Cluster_ID
            ) AS Cluster_ST_Max_Sequel
        INTO #Cluster_ST_Max_Sequel_Flag
        FROM #ST_Max_Sequel_Flag;


        --3. Join with service hierarchy reference table
        DROP TABLE IF EXISTS #ST_Max_Sequels_Service_Hierarchy

        SELECT 
            b.*,
            CASE 
                WHEN c.Hierarchy IS NULL THEN 99 
                ELSE c.Hierarchy 
            END AS [Service_Hierarchy]
        INTO #ST_Max_Sequels_Service_Hierarchy
        FROM #Cluster_ST_Max_Sequel_Flag b
        LEFT JOIN #REF_Service_Type c 
            ON b.Sequel_Service_Type = c.Service_Type;


        --4. Select ST-Max clusters without ST-Max in their sequel chain, and the events subsequent which are in scope for determining the sequel
        DROP TABLE IF EXISTS #ST_Max_With_Sequels 

        SELECT *
        INTO #ST_Max_With_Sequels
        FROM #ST_Max_Sequels_Service_Hierarchy
        WHERE Chain_In_Scope = 1 
          AND Cluster_ST_Max_Sequel = 0;


        --================== STEP 3: Find those with services in the sequel chain =====================

        --Category 2: ST-Max with a ‘Service’ sequel (ranked by Hierarchy)

        /*Choose the lowest Hierarchy (i.e. highest ranking Service) against each ST-Max cluster in cases where multiple Service Events were found in the chronology 
        --i.e. if they received some short term support and then long term support within the chain, the outcome should be long term support. 
        These can then be added into #ASCOF2A*/
        DROP TABLE IF EXISTS #Find_Next_Services

        SELECT DISTINCT 
          a.LA_Code, 
          a.Der_NHS_LA_Combined_Person_ID, 
          a.ST_Max_Cluster_Start, 
          a.ST_Max_Cluster_End, 
          a.ST_Max_Cluster_Event_Outcome,  
          a.ST_Max_Cluster_ID,
          a.Sequel_Service_Type as 'Final_Outcome',
          Service_Hierarchy
        INTO #Find_Next_Services
        FROM #ST_Max_With_Sequels a
        INNER JOIN
        (
          --Subquery finds the highest ranked service for each combination of LA Code, Person ID and ST Max ID
          SELECT 
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID, 
            ST_Max_Cluster_ID, 
            MIN([Service_Hierarchy]) as [Cluster_Service_Hierarchy]
          FROM #ST_Max_With_Sequels
          GROUP BY 
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID, 
            ST_Max_Cluster_ID
        ) b
        ON 
          a.LA_Code = b.LA_Code AND
          a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID AND 
          a.ST_Max_Cluster_ID = b.ST_Max_Cluster_ID AND 
          a.Service_Hierarchy = b.Cluster_Service_Hierarchy
        WHERE Cluster_Service_Hierarchy != 99
        ;


        --Insert into Final table, using the Sequel Service Type as the final outcome 
        INSERT INTO #ASCOF2A

        SELECT 
          LA_Code,
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          Final_Outcome,
          '2' AS 'Sequel_Type'
        FROM #Find_Next_Services

        --================== STEP 4: Find those with non-service events in the sequel chain and assign outcomes =====================

        /*Extract all the ST-Max Sequels where no Service Event is present in the Sequel chronology
        This is either because the Client has no onward Services in their onward Event activity, or the Service(s) they do have are not part of
        the sequel chain of Events following the ST-Max period ending (see chain in scope flag above for further info on the date thresholds)

        The Assessment/Review events associated with these ST-Max clusters can still be analysed to see if any further information
        can be gleaned from their Event Outcomes before staging into the final table (i.e do they have any 'NFA' type Event Outcomes recorded?)
        which may give us more useful information than simply defaulting back to the Event Outcome of the initial ST-Max event*/

        --1. Select those where a service was not found 
        DROP TABLE IF EXISTS #Find_Next_Other

        SELECT DISTINCT 
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID, 
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End, 
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome,
          Sequel_Event_Type,
          Sequel_Event_Start_Date,
          Sequel_Event_End_Date,
          Sequel_Event_Outcome, --this needs adding now as we need to evaluate the outcome of these Assessment/Request rows but it may introduce duplicates
          DENSE_RANK() OVER (PARTITION BY 
                                LA_Code,
                                Der_NHS_LA_Combined_Person_ID,
                                ST_Max_Cluster_ID 
                             ORDER BY Sequel_Event_End_Date DESC) AS Rn  --assigns rank to each sequel event based on sequel event end date (same end date = same rank)
        INTO #Find_Next_Other
        FROM #ST_Max_With_Sequels
        WHERE ST_Max_Cluster_ID NOT IN (SELECT ST_Max_Cluster_ID FROM #Find_Next_Services)



        --2. Separate out any clusters with No Further Action outcomes in the sequel activity
        --Event outcome is chosen based on the latest end date if multiple NFA outcomes exist
        DROP TABLE IF EXISTS #Find_Next_Other_NFA

        SELECT 
          a.LA_Code, 
          a.Der_NHS_LA_Combined_Person_ID, 
          a.ST_Max_Cluster_Start, 
          a.ST_Max_Cluster_End, 
          a.ST_Max_Cluster_ID, 
          a.Sequel_Event_Outcome,
          COUNT(a.Der_NHS_LA_Combined_Person_ID) OVER (PARTITION BY 
                                                            a.LA_Code, 
                                                            a.Der_NHS_LA_Combined_Person_ID, 
                                                            a.ST_Max_Cluster_ID) as [NFA_Count]
        INTO #Find_Next_Other_NFA
        FROM #Find_Next_Other a
        INNER JOIN 
        (
          --Subquery finds the latest event where NFA is present, then the outer query filters to retain this event outcome as the chain event outcome for the cluster
          SELECT 
            LA_Code, 
            Der_NHS_LA_Combined_Person_ID, 
            ST_Max_Cluster_ID, 
            MIN([Rn]) as [Chain_Event_Outcome]  --Rn was assigned to flag the latest ending sequel event as 1
          FROM #Find_Next_Other
          WHERE Sequel_Event_Outcome LIKE 'NFA%'
            OR Sequel_Event_Outcome IN ('Admitted to hospital', 'Progress to end of life care')
          GROUP BY LA_Code, Der_NHS_LA_Combined_Person_ID, ST_Max_Cluster_ID
        ) b 
        ON a.LA_Code = b.[LA_Code] AND 
           a.Der_NHS_LA_Combined_Person_ID = b.Der_NHS_LA_Combined_Person_ID AND 
           a.ST_Max_Cluster_ID= b.ST_Max_Cluster_ID AND 
           a.Rn = b.[Chain_Event_Outcome]
        WHERE Sequel_Event_Outcome LIKE 'NFA%'  
          OR Sequel_Event_Outcome IN ('Admitted to hospital', 'Progress to end of life care')
        ;

        --3. Stage these NFA outcomes into the final #ASCOF 2A table
        --NOTE: when there are conflicting types of NFA for the same ST-Max Event on the same end date, this is over-written as 'NFA- Other'
        --Potential improvement here to using the event outcome hierarchy
        INSERT INTO #ASCOF2A

        SELECT DISTINCT 
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          CASE 
            WHEN NFA_Count = 1 THEN Sequel_Event_Outcome
            ELSE 'NFA- Other' 
          END AS 'Final_Outcome',
          '3' AS 'Sequel_Type'
        FROM #Find_Next_Other_NFA 


        --4. For the remaining ST-Max clusters in #Find_Next_Other, revert back to event outcome of the ST Max
        --This is where no Service Event is present, and no NFA outcomes are present in non-service events 

        --Identify those with outcomes still tp be determined
        DROP TABLE IF EXISTS #Find_Next_Other_No_Outcome

        SELECT DISTINCT 
          a.LA_Code, 
          a.Der_NHS_LA_Combined_Person_ID, 
          a.ST_Max_Cluster_ID, 
          a.ST_Max_Cluster_Event_Outcome,
          a.Sequel_Event_Outcome
        INTO #Find_Next_Other_No_Outcome
        FROM #Find_Next_Other a
        WHERE ST_Max_Cluster_ID NOT IN 
          (SELECT ST_Max_Cluster_ID 
          FROM #Find_Next_Other_NFA)


        --Insert those into ASCOF 2A with the event outcome of the ST-Max cluster as the final outcome
        INSERT INTO #ASCOF2A

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome AS 'Final_Outcome',
          '4' AS 'Sequel_Type'
        FROM #Find_Next_Other_No_Outcome


        --================== STEP 5: Deal with the remaining events where any chronology is not scope =====================

        /*Extract the remaining St-Max events that were in the #ST_Max_New_Clients_Final table but have not been given an outcome and outputted to the ASCOF2A table
        These are effectively any St-Max clusters that contain some onward event activity, but this activity is out of scope for consideration, either:
        a) Flagged '0' in the [Initial_In_Scope] flag i.e. the first Event that appears in the Clients event chronology following the end of the ST-Max
           was too long after the ST-Max ended to be considered related/connected
        b) Flagged '1' in the [Cluster_ST_Max_sequel] flag i.e. another separate ST-Max Event was encountered in the chronology chain before any sequel was found, 
        superceding the initial event

        These ST-Max events still need staging into the final ASCOF 2A table, even though no sequel events could be found, 
        and we defer back to the event outcome of the ST-Max cluster. */

        --1. Filter the original table of ST-Max clusters for new clients to those not in the ASCOF 2A table
        DROP TABLE IF EXISTS #Find_Next_Out_Of_Scope

        SELECT DISTINCT 
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_Start, 
          ST_Max_Cluster_End, 
          ST_Max_Cluster_ID, 
          ST_Max_Cluster_Event_Outcome
        INTO #Find_Next_Out_Of_Scope
        FROM #ST_MAX_New_Clients_Final
        WHERE ST_Max_Cluster_ID NOT IN 
          (SELECT ST_Max_Cluster_ID 
          FROM #ASCOF2A)

        --2. Insert these into final #ASCOF2A table with event outcome as their final outcome
        INSERT INTO #ASCOF2A

        SELECT DISTINCT
          LA_Code, 
          Der_NHS_LA_Combined_Person_ID,
          ST_Max_Cluster_ID,
          ST_Max_Cluster_Event_Outcome AS 'Final_Outcome',
          '5' as 'Sequel_Type'
        FROM #Find_Next_Out_Of_Scope;



        --====================Create final output tables ===================================


        /*

        Key for Sequel_Types in final ASCOF table

        1 = No Further Chronology (at all). Sequel derived from Event Outcome of ST-Max cluster
        2 = All ST-Max with a ‘Service’ sequel (ranked by Hierarchy)
        3 = No sequel ‘Service’ found, but an ‘NFA’ outcome encountered in the chronology (e.g. outcome of an Assessment).
        4 = No NFA outcomes observed (as per ‘3’ above) in the sequel chronology, defer back to the Event Outcome of the ST-Max cluster
        5 = Out Of Scope. Onward chronology was found, but was either a) too long after the ST-Max event ended or b) another ST-Max was encountered in the chronology, superseding this cluster

        */

        --1. Final breakdown of ST-Max services by final outcome:
        DROP TABLE IF EXISTS #OutputTable_Disaggregated

        SELECT 
          FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
          A.LA_Code,
          R.LA_Name,
          Sequel_Type,
          Final_Outcome,
          Included_In_Denom,
          Included_In_Num,
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
            END AS Included_In_Num
          FROM #ASCOF2A) A
        LEFT JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup_April_2024 R
        ON A.LA_Code = R.LA_Code
        GROUP BY  
          A.LA_Code, 
          R.LA_Name, 
          Final_Outcome,
          Sequel_Type, 
          Included_In_Denom,
          Included_In_Num
        ORDER BY 
          A.LA_Code, 
          R.LA_Name, 
          Final_Outcome,
          Sequel_Type


        --2. Summary table for PBI
        DROP TABLE IF EXISTS #OutputTable2

        SELECT 
          FORMAT(CAST(@ReportingPeriodStartDate AS DATE), 'd MMM yy') + ' - ' + FORMAT(CAST(@ReportingPeriodEndDate AS DATE), 'd MMM yy') AS Reporting_Period,
          r.LA_Code, 
          r.LA_Name,
          'ASCOF 2A' AS Measure,
          'The proportion of people who received short-term services during the year – who previously were not receiving services – where no further request was made for ongoing support (%)' AS [Description],
          'Total' AS [Group],
          SUM(CASE WHEN Included_In_Num = 'Y' THEN ST_Max_Count ELSE 0 END) AS 'Numerator',
          SUM(CASE WHEN Included_In_Denom = 'Y' THEN ST_Max_Count ELSE 0 END) AS 'Denominator',
          CASE 
            WHEN SUM(CASE WHEN Included_In_Denom = 'Y' THEN ST_Max_Count ELSE 0 END) = 0 
            THEN 0
            ELSE ROUND((CAST(SUM(CASE WHEN Included_In_Num = 'Y' THEN ST_Max_Count ELSE 0 END) AS FLOAT) / 
                        CAST(SUM(CASE WHEN Included_In_Denom = 'Y' THEN ST_Max_Count ELSE 0 END) AS FLOAT)) * 100, 1)
          END AS [Outcome]
        INTO #OutputTable
        FROM #OutputTable_Disaggregated a
        FULL JOIN ASC_Sandbox.REF_ONS_Codes_LA_Region_Lookup_April_2024 r  --Join to the full list of LA codes so LAs without values for 2A are still pulled through
        ON a.LA_Code = r.LA_Code
        GROUP BY r.LA_Code, r.LA_Name

    SET @Query = 'SELECT * INTO ' + @OutputTable_Disaggregated + ' FROM #OutputTable_Disaggregated'
    EXEC(@Query)

    SET @Query = 'SELECT * INTO ' + @OutputTable + ' FROM #OutputTable'
    EXEC(@Query)


    DROP SYNONYM IF EXISTS ASC_Sandbox.InputTable

GO

-----Example execution
/*
EXEC ASC_Sandbox.Create_ASCOF2A
  @ReportingPeriodStartDate = '2023-04-01',
  @ReportingPeriodEndDate = '2024-03-31',
  @InputTable = 'ASC_Sandbox.CLD_230401_240630_JoinedSubmissions',
  @OutputTable_Disaggregated = 'ASC_Sandbox.ASCOF2A_Disaggregated_RP1',
  @OutputTable = 'ASC_Sandbox.ASCOF_2A_RP1'
*/