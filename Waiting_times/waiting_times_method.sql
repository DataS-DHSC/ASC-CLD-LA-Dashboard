--###################################################################################
-- Preamble
--###################################################################################

-- DHSC waiting times method for calculating LA level median waiting times. Method is provisional and subject to change following local authority feedback.
-- Version 1.1, 19 May 2026
-- See AGEM CLD website for assocaited methodology document.

-- Note: Quality of life improvements, such as improved table and variable names, are planned for future update.

--###################################################################################
-- Stage 1 - Filter for events of interest using the CLD joined submission table
--###################################################################################

---- Stage 1.1 Create an initial table of CLD data
	--- Also create cleaned Event Type
	DROP TABLE IF EXISTS #all_events

		SELECT 
		  *,
		  CASE 
			WHEN Event_Type LIKE '%service%' THEN 'Service'
			WHEN Event_Type LIKE '%assessment%' THEN 'Assessment'
			WHEN Event_Type LIKE '%request%' THEN 'Request'
			WHEN Event_Type LIKE '%review%' THEN 'Review'
			ELSE 'Invalid and not mapped'
		  END AS Event_Type_Cleaned
		INTO #all_events
		FROM DHSC_Reporting.CLD_230401_260331_JoinedSubmissions

---- Stage 1.2 Filter for relevant assessments and services
	DROP TABLE IF EXISTS #filtered_assessments_and_services

	SELECT *
	INTO #filtered_assessments_and_services
	FROM #all_events
	WHERE 
		-- Assessment critera
		(Event_Type_Cleaned = 'Assessment'
		AND Assessment_Type_Cleaned IN ('Short term assessment', 'Long term assessment')
		) OR
		-- Service critera
		(Event_Type_Cleaned = 'Service'
		AND Service_Type_Cleaned IN (
		   'Long term support: Nursing care',
           'Long term support: Residential care',
           'Long term support: Community',
		   'Long term support: Prison',
           'Short term support: ST-Max',
           'Short term support: Ongoing low level',
           'Short term support: Other short term') 
			);

---- Stage 1.3 Filter for valid requests
	--- Stage 1.3a Filter requests by client type and start date
		DROP TABLE IF EXISTS #all_requests

		SELECT *
		INTO #all_requests
		FROM #all_events
		WHERE Event_Type_Cleaned = 'Request'
		  AND Client_Type_Cleaned = 'Service user'
		  AND Event_Start_Date >= '2023-04-01'

	--- Stage 1.3b Filter requests by age at start of request
		-- Note: DHSC do not have access to date of birth for data privacy reasons. But DHSC are supplied derived fields including Der_Age_Event_Start_Date field. ...
		-- ... The Der_Working_Age_Band is created in the 'derived fields' stored procedure, which is part of the joined submission table creation process.
		DROP TABLE IF EXISTS #requests_over_25

		SELECT 
			*,
			CASE
			WHEN Der_Working_Age_Band = '18 to 64' THEN '25 to 64'
			ELSE Der_Working_Age_Band
			END AS Modified_Working_Age_Band
		INTO #requests_over_25
		FROM #all_requests
		WHERE Der_Age_Event_Start_Date >= 25;
	
	--- Stage 1.3c Filter out requests from existing clients
		DROP TABLE IF EXISTS #filtered_requests
		SELECT r.*
		INTO #filtered_requests
		FROM  #requests_over_25 r
		LEFT JOIN #filtered_assessments_and_services s
			  ON 
			  r.LA_Code = s.LA_Code
			  AND r.Der_NHS_LA_Combined_Person_ID = s.Der_NHS_LA_Combined_Person_ID
			  AND s.Event_Type_Cleaned = 'Service'
			  AND(
				-- A long term service ends within 12 months before the request, or after the request, or is still open
				  (s.Service_Type_Cleaned IN ('Long term support: Nursing care',
											   'Long term support: Residential care',
											   'Long term support: Community',
											   'Long term support: Prison')
				  AND (s.Der_Event_End_Date >= DATEADD(MONTH, -12, r.Event_Start_Date) OR s.Der_Event_End_Date IS NULL ) )
				OR
				-- A 'Short term support: ST-Max' or 'Short term support: Other short term' service ends within 3 months before the request, or after the request, or is still open
					(s.Service_Type_Cleaned IN ('Short term support: ST-Max',
												'Short term support: Other short term')
				  AND (s.Der_Event_End_Date >= DATEADD(MONTH, -3, r.Event_Start_Date) OR s.Der_Event_End_Date IS NULL ) )
				  )
			  -- The service starts before the request
			  AND s.Event_Start_Date < r.Event_Start_Date
		WHERE s.Der_NHS_LA_Combined_Person_ID IS NULL;

---- Stage 1.4 Filter out requests which do not indicate progress and create chronological request order for each client
	--- Note: The methodology document references Event Outcome as the critera. However, for efficient purposed, Event Outcome Grouped has been used in the code, to acheive the same critera.
	--- DHSC derives the variable Event_Outcome_Grouped as part of its data cleaning processes. This process is not available on Github. ...
	--- ... Event_Outcome_Grouped has value 'NFA' for all NFA NFA values for the cleaned version of the event outcome variable. It has value 'Admitted to hospital' where the cleaned event outcome is also 'Admitted to hospital'

		DROP TABLE IF EXISTS #valid_requests

		SELECT *,
			ROW_NUMBER() OVER (PARTITION BY 
                              LA_Code,
                              Der_NHS_LA_Combined_Person_ID 
                            ORDER BY
                              Event_Start_Date,
							  ImportDate desc,
							  Der_Unique_Record_ID desc )
							as person_valid_request_order
		INTO #valid_requests
		FROM #filtered_requests
		WHERE Event_Outcome_Grouped NOT IN ('NFA','Admitted to hospital');


--###################################################################################
-- Stage 2  - Link assessments and services to requests
--###################################################################################

---- Stage 2.1 Create single table of requests, assessments and services 
	--- Where requests have passed all previous criteria and assessments and services events where age at start of event is 25 or above. For each person, create chronological order of all of their events.
	--- Chronology logic is expanded form of request chronology where events have order of priorty as request, assessments, services.

	DROP TABLE IF EXISTS #filtered_requests_assessments_and_services

	SELECT u.*,
			ROW_NUMBER() OVER (PARTITION BY 
                               u.LA_Code,
                               u.Der_NHS_LA_Combined_Person_ID 
                            ORDER BY
                               u.Event_Start_Date,
							  CASE 
                                -- Priority of events if they start on the same day
								WHEN  u.Event_Type_Cleaned = 'Request' THEN 1
                                WHEN  u.Event_Type_Cleaned = 'Assessment' THEN 2
                                WHEN  u.Event_Type_Cleaned = 'Service' THEN 3
                                ELSE 4
                              END, 
							   u.ImportDate desc,
							   u.Der_Unique_Record_ID desc)
							as person_valid_event_order	
	INTO #filtered_requests_assessments_and_services
	FROM(
		SELECT *
		FROM #valid_requests
		UNION ALL
		SELECT *, 
			CASE
				WHEN Der_Working_Age_Band = '18 to 64' THEN '25 to 64'
				ELSE Der_Working_Age_Band
			END AS Modified_Working_Age_Band, 
			NULL as person_valid_request_order -- Create variable with NULL content as union requires inputs to have same variables
		FROM #filtered_assessments_and_services
		WHERE Der_Age_Event_Start_Date >= 25
		) u

	---- Temporarily save down table to cut re-processing time
	--DROP TABLE IF EXISTS ASC_Sandbox.Temp_Waiting_Times_S2_1
	--SELECT *
	--INTO ASC_Sandbox.Temp_Waiting_Times_S2_1
	--FROM #filtered_requests_assessments_and_services;

	--- Temporarily reinstate to cut re-processing time
	--DROP TABLE IF EXISTS #filtered_requests_assessments_and_services
	--SELECT * 
	--INTO #filtered_requests_assessments_and_services
	--FROM ASC_Sandbox.Temp_Waiting_Times_S2_1;

---- Stage 2.2 Remove clients who have assessments or services but no associated requests
	--- No valid assessment/service flag not filtered for as some requests may be proportionate assessments and therefore also act as the assessment. Flag retained for contextual data.
	DROP TABLE IF EXISTS #filtered_requests_assessments_and_services_2;

	SELECT a.*
	INTO #filtered_requests_assessments_and_services_2
	FROM (
		SELECT	*,
			-- Flag clients with valid assessments and services but no valid requests
			CASE 
				WHEN MAX(CASE WHEN Event_Type_Cleaned = 'Request' THEN 1 ELSE 0 END)
					OVER (PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID) = 0
				THEN 1
				ELSE 0
			END AS person_no_valid_requests,
			-- Flag clients with valid requests but no valid assessments and services
			CASE
				WHEN MIN(CASE WHEN Event_Type_Cleaned = 'Request' THEN 1 ELSE 0 END)
					 OVER (PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID) = 1
				THEN 1
				ELSE 0
			END AS person_all_events_requests
		FROM #filtered_requests_assessments_and_services) a
	where a.person_no_valid_requests = 0 ;
		--AND a.person_all_events_requests = 0; -- see note

---- Stage 2.3 Identify requests and assessments which indicate progress
	--- Stage 2.3a Identify requests and single assessments which indicate progress
		--- The source table for this analysis has had all data transformed to release 2 values. ...
		--- ... When Event_Outcome_Cleaned is 'Release 1 specification only: Not mapped' it is capturing the release 1 only values of 'Progress to financial assessment ' and 'Progress to End of Life Care'.

	DROP TABLE IF EXISTS #filtered_requests_assessments_and_services_3;

	SELECT *,
		--CASE 
		--	WHEN Event_Type_Cleaned = 'Assessment' AND Event_Outcome_Grouped IS NULL THEN 0
		--	WHEN Event_Type_Cleaned = 'Assessment' AND Event_Outcome_Grouped IN ('NFA','Admitted to hospital', 'Invalid and not mapped') THEN 0
		--	WHEN Event_Type_Cleaned <> 'Assessment' THEN NULL
		--	ELSE 1
		--END AS assessment_indicates_progress,

		CASE 
			WHEN Event_Type_Cleaned = 'Assessment' 
							AND Event_Outcome_Cleaned IN ('Progress to reablement/ST-Max', 
															'Progress to support planning or services',
															'Continuation of support or services')
														THEN 1
			WHEN Event_Type_Cleaned <> 'Assessment' THEN NULL
			ELSE 0
		END AS assessment_indicates_progress_to_service,

		CASE 
			WHEN Event_Type_Cleaned = 'Request' 
				and Event_Outcome_Cleaned IN ('Progress to reablement/ST-Max',
												'Progress to assessment, review or reassessment',
												'Progress to support planning or services',
												'Continuation of support or services',
												'Release 1 specification only: Not mapped') THEN 1
			WHEN Event_Type_Cleaned <> 'Request' THEN NULL
			ELSE 0
		END AS request_indicates_progress,


		CASE 
			WHEN Event_Type_Cleaned = 'Request' 
				and Event_Outcome_Cleaned IN ('Progress to reablement/ST-Max',
												'Progress to support planning or services',
												'Continuation of support or services',
												'Release 1 specification only: Not mapped') THEN 1
			WHEN Event_Type_Cleaned <> 'Request' THEN NULL
			ELSE 0
		END AS request_indicates_progress_to_service

	INTO #filtered_requests_assessments_and_services_3
	FROM #filtered_requests_assessments_and_services_2;

	--- Stage 2.3b Identify consecutive assessments where at least one of them indicates progress to service
		-- Use WITH function to build a temporary table to use in the next part of function. This instance chains two temporary tables (create_assessment_block and determine_block_progress) before the outputing the final table
		WITH 
			-- Assign IDs to events to identify when there are consecutive assessments
			create_assessment_blocks AS 
				(
				SELECT base.*,
						-- Create assessment flag
							CASE WHEN base.Event_Type_Cleaned = 'Assessment' THEN 1 ELSE 0 END AS is_assessment,
						-- Create assessment block ID
							-- When a row is not an assessment it has a value of 1. A runnign total is created, by summing togeter all these values for each previous row for this person in the event chronology.
							-- Therefore, with every non assessment row, the number increases. Consequently, consecutive assessments share the same running total, which becomes the assessment block ID.
					SUM(CASE WHEN base.Event_Type_Cleaned <> 'Assessment' THEN 1 ELSE 0 END)
						OVER (
							PARTITION BY base.LA_Code, base.Der_NHS_LA_Combined_Person_ID
							ORDER BY base.person_valid_event_order
							ROWS UNBOUNDED PRECEDING
						) AS assessment_block_id
				FROM #filtered_requests_assessments_and_services_3 base 
				),
			-- Determine presence of assessment progress to service in block
			determine_block_progress AS
				(
				SELECT blocks.*,
					-- For each person, assessment block detect presence of any of the  assessments (single assessment or consecutive assessments) having an outcome which indicates progress to service
						-- Note: if no assessments in block then max_flag_over_assessment_block outputs NULL
						MAX(CASE WHEN blocks.is_assessment = 1 THEN COALESCE(blocks.assessment_indicates_progress_to_service, 0) END) -- coalesce with 0 arguement to ensure non-null result where there is an assessmetn in a block
						OVER (PARTITION BY blocks.LA_Code, blocks.Der_NHS_LA_Combined_Person_ID, blocks.assessment_block_id) AS max_flag_over_assessment_block
	
				FROM create_assessment_blocks blocks
				)
			-- Create single indicator variable to be added to table which is only applied to assessment rows
			SELECT prog.*,
				CASE WHEN prog.is_assessment = 1 THEN prog.max_flag_over_assessment_block
					ELSE NULL
				END AS assessment_block_indicates_progress_to_service
			INTO #filtered_requests_assessments_and_services_4
			FROM determine_block_progress prog
			ORDER BY LA_Code, Der_NHS_LA_Combined_Person_ID, person_valid_event_order; 

			--- Drop variables
				ALTER TABLE #filtered_requests_assessments_and_services_4
				DROP COLUMN is_assessment, max_flag_over_assessment_block;

---- Stage 2.4 Link each request for a person to the first assessment and service following the request
	DROP TABLE IF EXISTS #requests_bind_1

	SELECT
		r.LA_Code,
		r.Der_NHS_LA_Combined_Person_ID,
		r.Der_Unique_Record_ID AS request_Der_Unique_Record_ID,
		r.Event_Start_Date AS request_start_date,
		r.Modified_Working_Age_Band AS request_Modified_Working_Age_Band,
		r.person_valid_event_order AS request_event_order,
		r.request_indicates_progress,
		r.request_indicates_progress_to_service,
		r.Der_Conversation AS request_Der_Conversation,

		-- First Assessment after the Request
		nextAsmt.person_valid_event_order AS next_assessment_order,
		nextAsmt.Event_Type_Cleaned AS next_assessment_event_type,
		nextAsmt.Assessment_Type_Cleaned as next_assessment_type,
		nextAsmt.Event_Start_Date AS next_assessment_start_date,
		nextAsmt.Modified_Working_Age_Band AS next_assessment_Modified_Working_Age_Band,
		nextAsmt.assessment_indicates_progress_to_service AS next_assessment_indicates_progress_to_service,
		nextAsmt.assessment_block_indicates_progress_to_service AS next_assessment_block_indicates_progress_to_service,
		nextAsmt.Der_Unique_Record_ID AS next_assessment_Der_Unique_Record_ID,

		-- First Service after the Request
		nextSrv.person_valid_event_order AS next_service_order,
		nextSrv.Event_Type_Cleaned AS next_service_event_type,
		nextSrv.Service_Type_Cleaned as next_service_type,
		nextSrv.Event_Start_Date AS next_service_start_date,
		nextSrv.Modified_Working_Age_Band AS next_service_Modified_Working_Age_Band,
		nextSrv.Der_Unique_Record_ID AS next_service_Der_Unique_Record_ID

	INTO #requests_bind_1
	FROM #filtered_requests_assessments_and_services_4 r

		-- First Assessment after request
		OUTER APPLY (
			SELECT TOP 1 x.*
			FROM #filtered_requests_assessments_and_services_4 x
			WHERE x.LA_Code = r.LA_Code
			  AND x.Der_NHS_LA_Combined_Person_ID = r.Der_NHS_LA_Combined_Person_ID
			  AND x.person_valid_event_order > r.person_valid_event_order
			  AND x.Event_Type_Cleaned = 'Assessment'
			ORDER BY x.person_valid_event_order
		) nextAsmt

		-- First Service after request
		OUTER APPLY (
			SELECT TOP 1 y.*
			FROM #filtered_requests_assessments_and_services_4 y
			WHERE y.LA_Code = r.LA_Code
			  AND y.Der_NHS_LA_Combined_Person_ID= r.Der_NHS_LA_Combined_Person_ID
			  AND y.person_valid_event_order > r.person_valid_event_order
			  AND y.Event_Type_Cleaned = 'Service'
			ORDER BY y.person_valid_event_order
		) nextSrv
	WHERE r.Event_Type_Cleaned = 'Request'
	ORDER BY r.LA_Code, r.Der_NHS_LA_Combined_Person_ID, r.person_valid_event_order;

	---- Temporarily save down table to cut re-processing time
	--DROP TABLE IF EXISTS ASC_Sandbox.Temp_Waiting_Times_S2_4
	--SELECT *
	--INTO ASC_Sandbox.Temp_Waiting_Times_S2_4
	--FROM #requests_bind_1;

	--- Temporarily re-instate table to cut re-processing time
	--DROP TABLE IF EXISTS #requests_bind_1;
	--SELECT *
	--INTO #requests_bind_1
	--FROM ASC_Sandbox.Temp_Waiting_Times_S2_4;


--###################################################################################
-- Stage 3 - Account for 3-conversations model, missing assessments and discount excess requests
--###################################################################################

---- Stage 3.1 Allow requests to act as an assessment when a conversation is flagged for release 1 requests
	DROP TABLE IF EXISTS  #requests_bind_1b

	SELECT *,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_event_order ELSE next_assessment_order END AS temp_next_assessment_order,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN 'Request' ELSE next_assessment_event_type END AS temp_next_assessment_event_type,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL ELSE next_assessment_type END AS temp_next_assessment_type,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_start_date ELSE next_assessment_start_date END AS temp_next_assessment_start_date,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_Modified_Working_Age_Band ELSE next_assessment_Modified_Working_Age_Band END AS temp_next_assessment_Modified_Working_Age_Band,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_Der_Unique_Record_ID ELSE next_assessment_Der_Unique_Record_ID END AS temp_next_assessment_Der_Unique_Record_ID,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL ELSE next_assessment_indicates_progress_to_service END AS temp_next_assessment_indicates_progress_to_service,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL ELSE next_assessment_block_indicates_progress_to_service END AS temp_next_assessment_block_indicates_progress_to_service
	INTO #requests_bind_1b
	FROM (	SELECT *,
				-- Create conversation 1 override flag
				CASE 
					WHEN ((request_start_date < next_assessment_start_date) OR next_assessment_start_date IS NULL)
						AND request_Der_Conversation = 1 
						AND request_indicates_progress = 1
					THEN 1 
					ELSE 0 
				END AS c1_request_override,
				-- Create spec flag
				CASE 
					 WHEN request_start_date < '2025-07-01' THEN 'R1' 
					 WHEN request_start_date >= '2025-07-01' THEN 'R2'
					 ELSE 'Other' 
				END AS request_spec
			FROM #requests_bind_1
		) a;

---- Stage 3.2 Allow services to be first response in part 1 metric
	DROP TABLE IF EXISTS #requests_bind_2

	SELECT *,
			CASE WHEN next_assessment_service_override = 1 THEN next_service_order ELSE temp_next_assessment_order END AS modified_next_assessment_order,
			CASE WHEN next_assessment_service_override = 1 THEN next_service_event_type ELSE temp_next_assessment_event_type END AS modified_next_assessment_event_type,
			CASE WHEN next_assessment_service_override = 1 THEN NULL ELSE temp_next_assessment_type END AS modified_next_assessment_type,
			CASE WHEN next_assessment_service_override = 1 THEN next_service_start_date ELSE temp_next_assessment_start_date END AS modified_next_assessment_start_date,
			CASE WHEN next_assessment_service_override = 1 THEN next_service_Modified_Working_Age_Band ELSE temp_next_assessment_Modified_Working_Age_Band END AS modified_next_assessment_Mod_Working_Age_Band,
			CASE WHEN next_assessment_service_override = 1 THEN next_service_Der_Unique_Record_ID ELSE temp_next_assessment_Der_Unique_Record_ID END AS modified_next_assessment_Der_Unique_Record_ID,
			CASE WHEN next_assessment_service_override = 1 THEN NULL ELSE temp_next_assessment_indicates_progress_to_service END AS modified_next_assessment_indicates_progress_to_service,
			CASE WHEN next_assessment_service_override = 1 THEN NULL ELSE temp_next_assessment_block_indicates_progress_to_service END AS modified_next_assessment_block_indicates_progress_to_service

	INTO #requests_bind_2
	FROM (
		SELECT *,
		CASE 
			WHEN temp_next_assessment_order > next_service_order THEN 1 
			WHEN temp_next_assessment_order IS NULL AND next_service_order IS NOT NULL THEN 1 
			ELSE 0 
		END AS next_assessment_service_override,
		-- further variable added for validation 
		CASE WHEN temp_next_assessment_start_date > next_service_start_date THEN 1 ELSE 0 END AS next_assessment_service_date_override
		FROM #requests_bind_1b) a;

	--- Drop temp variables
	ALTER TABLE #requests_bind_2
	DROP COLUMN temp_next_assessment_order, temp_next_assessment_event_type, temp_next_assessment_type, temp_next_assessment_start_date, temp_next_assessment_Modified_Working_Age_Band, temp_next_assessment_Der_Unique_Record_ID;

---- Stage 3.3 Exclude services from part 2 metric where an intervening assessment block does not indicate progress to service
	DROP TABLE IF EXISTS #requests_bind_3

	SELECT *,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_order END AS modified_next_service_order,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_event_type END AS modified_next_service_event_type,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_type END AS modified_next_service_type,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_start_date END AS modified_next_service_start_date,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_Modified_Working_Age_Band END AS modified_next_service_Mod_Working_Age_Band,
			CASE WHEN modified_next_assessment_event_type = 'Assessment' and modified_next_assessment_block_indicates_progress_to_service = 0 THEN NULL ELSE next_service_Der_Unique_Record_ID END AS modified_next_service_Der_Unique_Record_ID
	INTO #requests_bind_3
	FROM #requests_bind_2;


---- Stage 3.4 Exclude requests that overlap with ongoing activity linked to a previous request

	DROP TABLE IF EXISTS #requests_bind_4
	
	SELECT *,
			CASE
				WHEN LAG(GREATEST(modified_next_assessment_order, modified_next_service_order)) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order) > request_event_order
					THEN 1
				WHEN modified_next_assessment_order <= max_modified_next_assessment_order_prev 
					THEN 1
				WHEN modified_next_service_order <= max_modified_next_service_order_prev
					THEN 1
				ELSE 0
				END AS request_starts_before_previous_request_follow_up_ends

	INTO #requests_bind_4
	FROM (	SELECT *,
				-- For each row for a person, detected the highest previous first response event order
				-- Note: Coalesce, with the 0 value, is used to ensure the value is not NULL.
					COALESCE(
						MAX(modified_next_assessment_order) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order
							ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
						),
						0) AS max_modified_next_assessment_order_prev,
				-- For each row for a person, detected the highest previous service event order
				-- Note: Coalesce, with the 0 value, is used to ensure the value is not NULL.
					COALESCE(
						MAX(modified_next_service_order) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order
							ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
						),
						0) AS max_modified_next_service_order_prev
			FROM #requests_bind_3) a;


--###################################################################################
-- Stage 4  - Calculate waiting times
--###################################################################################

---- Stage 4.1 Calculate waiting times at an individual level and group waiting times by reporting period

	DROP TABLE IF EXISTS #wait_times_calc_1

	SELECT *,
		-- Create statistical reporting period end dates
		FORMAT(EOMONTH(stat_reporting_period_start_request, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_request,
		FORMAT(EOMONTH(stat_reporting_period_start_part1, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_part1,
		FORMAT(EOMONTH(stat_reporting_period_start_part2, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_part2,
		
		-- Calculate part 1 wait time
		CASE
			WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEDIFF(DAY, request_start_date, modified_next_assessment_start_date)
			ELSE NULL
		END AS part1_wait_time,
		-- Calculate part 2 wait time
		CASE
			WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEDIFF(DAY, request_start_date, modified_next_service_start_date)
			ELSE NULL
		END AS part2_wait_time
	INTO #wait_times_calc_1
	FROM (SELECT *,
			-- The start date of the statistical reporting period (quarter) the request starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN  DATEFROMPARTS(YEAR(request_start_date), ((DATEPART(QUARTER, request_start_date) - 1) * 3) + 1, 1) 
					ELSE NULL
			 END AS stat_reporting_period_start_request,
			
			-- The start date of the statistical reporting period (quarter) the part 1 event starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEFROMPARTS(YEAR(modified_next_assessment_start_date), ((DATEPART(QUARTER, modified_next_assessment_start_date) - 1) * 3) + 1, 1) 
				ELSE NULL
			  END AS stat_reporting_period_start_part1,

			-- The start date of the statistical reporting period (quarter) the part 2 event starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEFROMPARTS(YEAR(modified_next_service_start_date), ((DATEPART(QUARTER, modified_next_service_start_date ) - 1) * 3) + 1, 1) 
				ELSE NULL
			  END AS stat_reporting_period_start_part2
			FROM #requests_bind_4 
			) a;

---- Stage 4.2 Create local authority level median waiting times
		---- Exclude invalid data
		---- Created seperately for parts 1 and 2 and then appended together

	DROP TABLE IF EXISTS #wait_times_calc_la_both_parts
	SELECT DISTINCT
			LA_Code,
			stat_reporting_period_start_part1 AS Statistical_Reporting_Period_Start,
			stat_reporting_period_end_part1 AS Statistical_Reporting_Period_End,
			modified_next_assessment_Mod_Working_Age_Band AS Modified_Working_Age_Band,
			'Wait time to first response' AS Metric,
			-- Create LA level median
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY part1_wait_time)
				OVER (PARTITION BY LA_Code,
									stat_reporting_period_start_part1,
									modified_next_assessment_Mod_Working_Age_Band)
				AS Median_Waiting_Time,
			-- Create count of valid waiting times
			COUNT(*) OVER (
				PARTITION BY LA_Code,
								stat_reporting_period_start_part1,
								modified_next_assessment_Mod_Working_Age_Band)
				AS Number_Of_Waiting_Times_Identified
	INTO #wait_times_calc_la_both_parts
	FROM #wait_times_calc_1
	WHERE part1_wait_time IS NOT NULL
		AND modified_next_assessment_Mod_Working_Age_Band IN ('25 to 64', '65 and above') -- to be updated to 25 to 64
	UNION ALL
	SELECT DISTINCT
			LA_Code,
			stat_reporting_period_start_part2 AS Statistical_Reporting_Period_Start,
			stat_reporting_period_end_part2 AS Statistical_Reporting_Period_End,
			modified_next_service_Mod_Working_Age_Band AS Modified_Working_Age_Band,
			'Wait time to first service' AS Metric,
			-- Create LA level median
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY part2_wait_time)
				OVER (PARTITION BY LA_Code,
									stat_reporting_period_start_part2,
									modified_next_service_Mod_Working_Age_Band)
				AS Median_Waiting_Time,
			-- Create count of valid waiting times
			COUNT(*) OVER (
				PARTITION BY LA_Code,
								stat_reporting_period_start_part2,
								modified_next_service_Mod_Working_Age_Band)
				AS Number_Of_Waiting_Times_Identified
	FROM #wait_times_calc_1
	WHERE part2_wait_time IS NOT NULL
		AND modified_next_service_Mod_Working_Age_Band IN ('25 to 64', '65 and above'); -- to be updated to 25 to 64

--###################################################################################
-- Stage 5  - Create contextual information for Athena dashboard
--###################################################################################

---- Stage 5.1 Count number of events within cohort for each combination of LA, age band, statistical reporting period
	--- Requires output of Stage 2.1
	--- Note: Waiting times are calculated using the request, regardless of what statistical reporting period it is in. ...
	--- ... Hence the count of requests here is not as contextually useful for the metrics than the counts of assessments and services.
	DROP TABLE IF EXISTS #contextual_event_counts;
	SELECT calc.*,
			context.all_request_count,
			context.all_assessment_count,
			context.all_service_count
	INTO #contextual_event_counts
	FROM #wait_times_calc_la_both_parts AS calc
	LEFT JOIN ( -- Create summary information
			SELECT LA_Name, LA_Code, Modified_Working_Age_Band, Statistical_Reporting_Period_Start,
						SUM(CASE WHEN Event_Type_Cleaned = 'Request' THEN 1 ELSE 0 END) AS all_request_count,
						SUM(CASE WHEN Event_Type_Cleaned = 'Assessment' THEN 1 ELSE 0 END) AS all_assessment_count,
						SUM(CASE WHEN Event_Type_Cleaned = 'Service' THEN 1 ELSE 0 END) AS all_service_count
			FROM ( -- Create Statistical Reporting Period variable
					SELECT LA_Name, LA_Code, Event_Type_Cleaned, Modified_Working_Age_Band,
							DATEFROMPARTS(YEAR(Event_Start_Date), ((DATEPART(QUARTER, Event_Start_Date) - 1) * 3) + 1, 1) 
					 AS Statistical_Reporting_Period_Start
					FROM #filtered_requests_assessments_and_services
					WHERE Event_Start_Date >= '2023-04-01') a
			GROUP BY LA_Name, LA_Code, Modified_Working_Age_Band, Statistical_Reporting_Period_Start)
			AS context
		ON calc.LA_Code = context.LA_Code 
			AND calc.Modified_Working_Age_Band = context.Modified_Working_Age_Band
			AND calc.Statistical_Reporting_Period_Start= context.Statistical_Reporting_Period_Start;
	
---- Stage 5.2 Count of event types for first responses and assessment types where they are assessments included in waiting time metric

	SELECT base.*,
				ast.response_event_request,
				ast.response_event_assessment,
				ast.response_event_service,
				ast.at_short,
				ast.at_long,
				ast.at_invalid_and_not_mapped,
				ast.at_null
	INTO #contextual_part_1
	FROM #contextual_event_counts AS base
		LEFT JOIN (SELECT
						LA_Code,
						stat_reporting_period_start_part1,
						modified_next_assessment_Mod_Working_Age_Band,
						-- Create event type counts for responses
						SUM(CASE WHEN modified_next_assessment_event_type = 'Request' THEN 1 ELSE 0 END) AS response_event_request,
						SUM(CASE WHEN modified_next_assessment_event_type = 'Assessment' THEN 1 ELSE 0 END) AS response_event_assessment,
						SUM(CASE WHEN modified_next_assessment_event_type = 'Service' THEN 1 ELSE 0 END) AS response_event_service,
						-- Create assessment type counts when response is an assessment
						SUM(CASE WHEN modified_next_assessment_event_type = 'Assessment' AND modified_next_assessment_type = 'Short term assessment' THEN 1 ELSE 0 END) AS at_short,
						SUM(CASE WHEN modified_next_assessment_event_type = 'Assessment' AND modified_next_assessment_type = 'Long term assessment' THEN 1 ELSE 0 END) AS at_long,
						SUM(CASE WHEN modified_next_assessment_event_type = 'Assessment' AND modified_next_assessment_type = 'Invalid and not mapped' THEN 1 ELSE 0 END) AS at_invalid_and_not_mapped, -- Redundancy
						SUM(CASE WHEN modified_next_assessment_event_type = 'Assessment' AND modified_next_assessment_type IS NULL THEN 1 ELSE 0 END) AS at_null -- Redundancy
					FROM #wait_times_calc_1
					WHERE part1_wait_time IS NOT NULL
					AND modified_next_assessment_Mod_Working_Age_Band IN ('25 to 64', '65 and above')
					GROUP BY LA_Code,
							stat_reporting_period_start_part1,
							modified_next_assessment_Mod_Working_Age_Band
					) as ast
				ON base.LA_Code = ast.LA_Code
				AND base.Statistical_Reporting_Period_Start = ast.stat_reporting_period_start_part1
				AND base.Modified_Working_Age_Band = ast.modified_next_assessment_Mod_Working_Age_Band
	WHERE Metric = 'Wait time to first response';

---- Stage 5.3 Count of service types for services included in waiting time metric
	
	SELECT base.*,
			st.STS_ST_Max,
			st.LTS_Nursing,
			st.STS_Ongoing,
			st.LTS_Community,
			st.LTS_Residential,
			st.LTS_Prison,
			st.STS_Other,
			st.Invalid_and_not_mapped,
			st.[Null]
	INTO #contextual_part_2
	FROM #contextual_event_counts AS base
		LEFT JOIN (SELECT
						LA_Code,
						stat_reporting_period_start_part2,
						modified_next_service_Mod_Working_Age_Band,
						SUM(CASE WHEN modified_next_service_type = 'Short term support: ST-Max'            THEN 1 ELSE 0 END) AS [STS_ST_Max],
						SUM(CASE WHEN modified_next_service_type = 'Long term support: Nursing care'       THEN 1 ELSE 0 END) AS [LTS_Nursing],
						SUM(CASE WHEN modified_next_service_type = 'Short term support: Ongoing low level' THEN 1 ELSE 0 END) AS [STS_Ongoing],
						SUM(CASE WHEN modified_next_service_type = 'Long term support: Community'          THEN 1 ELSE 0 END) AS [LTS_Community],
						SUM(CASE WHEN modified_next_service_type = 'Long term support: Residential care'   THEN 1 ELSE 0 END) AS [LTS_Residential],
						SUM(CASE WHEN modified_next_service_type = 'Long term support: Prison'			   THEN 1 ELSE 0 END) AS [LTS_Prison],
						SUM(CASE WHEN modified_next_service_type = 'Short term support: Other short term'  THEN 1 ELSE 0 END) AS [STS_Other],
						SUM(CASE WHEN modified_next_service_type = 'Invalid and not mapped'				   THEN 1 ELSE 0 END) AS [Invalid_and_not_mapped], -- Redundancy
						SUM(CASE WHEN modified_next_service_type is NULL								   THEN 1 ELSE 0 END) AS [Null] -- Redundancy
					FROM #wait_times_calc_1
					WHERE part2_wait_time IS NOT NULL
					AND modified_next_service_Mod_Working_Age_Band IN ('25 to 64', '65 and above')
					GROUP BY LA_Code,
							stat_reporting_period_start_part2,
							modified_next_service_Mod_Working_Age_Band
					) as st
				ON base.LA_Code = st.LA_Code
				AND base.Statistical_Reporting_Period_Start = stat_reporting_period_start_part2
				AND base.Modified_Working_Age_Band = modified_next_service_Mod_Working_Age_Band
	WHERE Metric = 'Wait time to first service';

--###################################################################################
-- Stage 6  - Write outputs
--###################################################################################
	--- Athena dashboard outputs are resticted by specified date parameters

	--- Set quarter parameter
	DECLARE @Quarter AS VARCHAR(7) = 'Q4_2526';

	--- Set Athena dashboard inclusion date parameters
		-- Note: Statisitcal reporting periods are set to 1st day of finanical year quarters
		-- Upper limit will be included in filter
	DECLARE @Stat_RP_Start_Dashboard_Lower_Limit AS DATE = '2024-04-01';
	DECLARE @Stat_RP_Start_Dashboard_Upper_Limit AS DATE = '9999-01-01'; -- Date 9999-01-01 used as dummy to accept all statisitcal reporting periods after and including the lower limit
	

	--- Set table name parameter
	DECLARE @Metrics_Person_Level_DHSC	AS VARCHAR(256) = CONCAT('ASC_Sandbox.Waiting_Times_Metrics_Person_Level_', @Quarter, '_All_SRP');
	DECLARE @Metrics_LA_Level_DHSC		AS VARCHAR(256) = CONCAT('ASC_Sandbox.Waiting_Times_Metrics_LA_Level_', @Quarter, '_All_SRP');
	DECLARE @Metrics_LA_Level_Dashboard AS VARCHAR(256) = 'ASC_Sandbox.LA_PBI_Waiting_Times';
	DECLARE @Diag_Part1_DHSC			AS VARCHAR(256) = CONCAT('ASC_Sandbox.Waiting_Times_Diag_Part1_', @Quarter, '_All_SRP');
	DECLARE @Diag_Part1_Dashboard		AS VARCHAR(256) = 'ASC_Sandbox.LA_PBI_Waiting_Times_Diag_Part1';
	DECLARE @Diag_Part2_DHSC			AS VARCHAR(256) = CONCAT('ASC_Sandbox.Waiting_Times_Diag_Part2_', @Quarter, '_All_SRP');
	DECLARE @Diag_Part2_Dashboard		AS VARCHAR(256) = 'ASC_Sandbox.LA_PBI_Waiting_Times_Diag_Part2'

	DECLARE @QUERY NVARCHAR(MAX);
	SET @QUERY =
		--- Create ordinal variable and associated label text variable for Athena x axis
		'DROP TABLE IF EXISTS #PBI_Axis ;
		SELECT rp2.Statistical_Reporting_Period_Start,
			   rp2.Statistical_Reporting_Period_End,
			   rp2.PBI_Axis_Order,
			CASE WHEN rp2.Statistical_Reporting_Period_Start IS NULL OR rp2.Statistical_Reporting_Period_End IS NULL THEN NULL
				ELSE CONCAT(CONVERT(char(9), CAST(rp2.Statistical_Reporting_Period_Start AS date), 6),
							'' - '',
							CONVERT(char(9), CAST(rp2.Statistical_Reporting_Period_End AS date), 6)
								)
			END AS PBI_Axis_Name
		INTO #PBI_Axis
		FROM (
			SELECT rp.*,
				ROW_NUMBER() OVER (ORDER BY rp.Statistical_Reporting_Period_Start) as PBI_Axis_Order
			FROM (
				SELECT DISTINCT Statistical_Reporting_Period_Start, Statistical_Reporting_Period_End
				FROM #wait_times_calc_la_both_parts
				WHERE Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit
				) rp
			 )	rp2;
	
		--- Output person level waiting times for all Statistical Reporting Periods for DHSC
			DROP TABLE IF EXISTS '  + @Metrics_Person_Level_DHSC +';
			SELECT *
			INTO ' + @Metrics_Person_Level_DHSC +'
			FROM #wait_times_calc_1;
				
		--- LA level waiting times
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Metrics_LA_Level_DHSC +';
				SELECT *
				INTO ' + @Metrics_LA_Level_DHSC +'
				FROM #wait_times_calc_la_both_parts;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS '  + @Metrics_LA_Level_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Metrics_LA_Level_Dashboard +'
				FROM #wait_times_calc_la_both_parts base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit;


		--- Part 1 diagnositcs
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Diag_Part1_DHSC +';
				SELECT *
				INTO ' + @Diag_Part1_DHSC +'
				FROM #contextual_part_1;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS ' + @Diag_Part1_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Diag_Part1_Dashboard +'
				FROM #contextual_part_1 base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit ;
				
		--- Part 2 diagnositcs
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Diag_Part2_DHSC +';
				SELECT *
				INTO ' + @Diag_Part2_DHSC +'
				FROM #contextual_part_2;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS ' + @Diag_Part2_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Diag_Part2_Dashboard +'
				FROM #contextual_part_2 base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit ;';

	EXEC sp_executesql
		@stmt = @QUERY,
		@params = N'@LowerLimit DATE, @UpperLimit DATE',
		@LowerLimit = @Stat_RP_Start_Dashboard_Lower_Limit,
		@UpperLimit = @Stat_RP_Start_Dashboard_Upper_Limit;
