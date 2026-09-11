--###################################################################################
-- Preamble
--###################################################################################

-- DHSC waiting times method for calculating LA level median waiting times. Method is provisional and subject to change following local authority feedback.
-- Version 1.1.4, 1 September 2026

/* Change note: Updates from code version 1.1 to version 1.1.4 are do not impact the calculating of the waiting times.
	Changes relate to more consistent variable and table names with some addition annotations made.
	More efficient processing has been implemented for stage 2.4

  Methodology version 1.1 applies to this code version. See AGEM CLD website for assocaited methodology document */

/* Note: Some table and variable names reference the stage in which they were created. For example table "#s1_2_filtered_assessments_and_services" refers to the output table...
		from stage 1.2 where the output table contains the assessments and service which have met the filter critera. */

--###################################################################################
-- Stage 1 - Filter for events of interest using the CLD joined submission table
--###################################################################################

---- Stage 1.1 Create an initial table of CLD data
	--- For version 1.1.3 onwards Event_Type_Cleaned can be used from the joined submission table and no longer needs to be derived
	DROP TABLE IF EXISTS #s1_1_all_events

		SELECT *
		INTO #s1_1_all_events
		FROM DHSC_Reporting.CLD_230401_260630_JoinedSubmissions

---- Stage 1.2 Filter for relevant assessments and services
	DROP TABLE IF EXISTS #s1_2_filtered_assessments_and_services

	SELECT *
	INTO #s1_2_filtered_assessments_and_services
	FROM #s1_1_all_events
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
		DROP TABLE IF EXISTS #s1_3a_service_user_requests

		SELECT *
		INTO #s1_3a_service_user_requests
		FROM #s1_1_all_events
		WHERE Event_Type_Cleaned = 'Request'
		  AND Client_Type_Cleaned = 'Service user'
		  AND Event_Start_Date >= '2023-04-01'

	--- Stage 1.3b Filter requests by age at start of request
		-- Note: DHSC do not have access to date of birth for data privacy reasons. But DHSC are supplied derived fields including Der_Age_Event_Start_Date field. ...
		-- ... The Der_Working_Age_Band is created in the 'derived fields' stored procedure, which is part of the joined submission table creation process.
		-- Note: Requests are those aged below 25 are filtered out, therefore Der_Working_Age_Band label updated from 18-64 to 25-64.
		DROP TABLE IF EXISTS #s1_3b_service_user_all_requests_over_25

		SELECT 
			*,
			CASE
			WHEN Der_Working_Age_Band = '18 to 64' THEN '25 to 64'
			ELSE Der_Working_Age_Band
			END AS Age_Band
		INTO #s1_3b_service_user_all_requests_over_25
		FROM #s1_3a_service_user_requests
		WHERE Der_Age_Event_Start_Date >= 25;
	
	--- Stage 1.3c Filter out requests from existing clients
		DROP TABLE IF EXISTS #s1_3c_service_user_new_requests_over_25
		SELECT r.*
		INTO #s1_3c_service_user_new_requests_over_25
		FROM  #s1_3b_service_user_all_requests_over_25 r
		LEFT JOIN #s1_2_filtered_assessments_and_services s
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

		DROP TABLE IF EXISTS #s1_4_valid_requests

		SELECT *,
			ROW_NUMBER() OVER (PARTITION BY 
                              LA_Code,
                              Der_NHS_LA_Combined_Person_ID 
                            ORDER BY
                              Event_Start_Date,
							  ImportDate desc,
							  Der_Unique_Record_ID desc )
							as person_valid_request_order
		INTO #s1_4_valid_requests
		FROM #s1_3c_service_user_new_requests_over_25
		WHERE Event_Outcome_Grouped NOT IN ('NFA','Admitted to hospital');


--###################################################################################
-- Stage 2  - Link assessments and services to requests
--###################################################################################

---- Stage 2.1 Create single table of requests, assessments and services 
	--- Where requests have passed all previous criteria and assessments and services events where age at start of event is 25 or above. For each person, create chronological order of all of their events.
	--- Chronology logic is expanded form of request chronology where events have order of priorty as request, assessments, services.

	DROP TABLE IF EXISTS #s2_1_filtered_requests_assessments_and_services

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
	INTO #s2_1_filtered_requests_assessments_and_services
	FROM(
		SELECT *
		FROM #s1_4_valid_requests
		UNION ALL
		SELECT *, 
			CASE
				WHEN Der_Working_Age_Band = '18 to 64' THEN '25 to 64'
				ELSE Der_Working_Age_Band
			END AS Age_Band, 
			NULL as person_valid_request_order -- Create variable with NULL content as union requires inputs to have same variables
		FROM #s1_2_filtered_assessments_and_services
		WHERE Der_Age_Event_Start_Date >= 25
		) u

	---- Temporarily save down table to cut re-processing time
	--DROP TABLE IF EXISTS asc_sandbox.Temp_Waiting_Times_S2_1
	--SELECT *
	--INTO asc_sandbox.Temp_Waiting_Times_S2_1
	--FROM #s2_1_filtered_requests_assessments_and_services;

	-- Temporarily reinstate to cut re-processing time
	--DROP TABLE IF EXISTS #s2_1_filtered_requests_assessments_and_services
	--SELECT * 
	--INTO #s2_1_filtered_requests_assessments_and_services
	--FROM asc_sandbox.Temp_Waiting_Times_S2_1;

---- Stage 2.2 Remove clients who have assessments or services but no associated requests
	--- No valid assessment/service flag not filtered for as some requests may be proportionate assessments and therefore also act as the assessment. Flag retained for contextual data.
	DROP TABLE IF EXISTS #s2_2_filtered_requests_assessments_and_services;

	SELECT a.*
	INTO #s2_2_filtered_requests_assessments_and_services
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
		FROM #s2_1_filtered_requests_assessments_and_services) a
	where a.person_no_valid_requests = 0 ;
		--AND a.person_all_events_requests = 0; -- see note

---- Stage 2.3 Identify requests and assessments which indicate progress
	--- Stage 2.3a Identify requests and single assessments which indicate progress
		--- The source table for this analysis has had all data transformed to release 2 values. ...
		--- ... When Event_Outcome_Cleaned is 'Release 1 specification only: Not mapped' it is capturing the release 1 only values of 'Progress to financial assessment ' and 'Progress to End of Life Care'.

	DROP TABLE IF EXISTS #s2_3a_filtered_requests_assessments_and_services;

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

	INTO #s2_3a_filtered_requests_assessments_and_services
	FROM #s2_2_filtered_requests_assessments_and_services;

	--- Stage 2.3b Identify consecutive assessments where at least one of them indicates progress to service
		-- Use WITH function to build a temporary table to use in the next part of function. This instance chains two temporary tables (create_assessment_block and determine_block_progress) before the outputing the final table
		DROP TABLE IF EXISTS #s2_3b_filtered_requests_assessments_and_services;
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
				FROM #s2_3a_filtered_requests_assessments_and_services base 
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
			INTO #s2_3b_filtered_requests_assessments_and_services
			FROM determine_block_progress prog
			ORDER BY LA_Code, Der_NHS_LA_Combined_Person_ID, person_valid_event_order; 

			--- Drop variables
				ALTER TABLE #s2_3b_filtered_requests_assessments_and_services
				DROP COLUMN is_assessment, max_flag_over_assessment_block;

		-- Split output table into individual indexed tables for each event type
			-- Helps with efficieny of stage 2.4

			-- Requests
				DROP TABLE IF EXISTS #s2_3b_requests_indexed;

				SELECT *
				INTO #s2_3b_requests_indexed
				FROM #s2_3b_filtered_requests_assessments_and_services
				WHERE Event_Type_Cleaned = 'Request';

				CREATE CLUSTERED INDEX IX_requests
				ON #s2_3b_requests_indexed
				(
					LA_Code,
					Der_NHS_LA_Combined_Person_ID,
					person_valid_event_order
				);

			-- Assessments
				DROP TABLE IF EXISTS #s2_3b_assessments_indexed;

				SELECT *
				INTO #s2_3b_assessments_indexed
				FROM #s2_3b_filtered_requests_assessments_and_services
				WHERE Event_Type_Cleaned = 'Assessment';

				CREATE CLUSTERED INDEX IX_assessments
				ON #s2_3b_assessments_indexed
				(
					LA_Code,
					Der_NHS_LA_Combined_Person_ID,
					person_valid_event_order
				);

			-- Services
				DROP TABLE IF EXISTS #s2_3b_services_indexed;

				SELECT *
				INTO #s2_3b_services_indexed
				FROM #s2_3b_filtered_requests_assessments_and_services
				WHERE Event_Type_Cleaned = 'Service';

				CREATE CLUSTERED INDEX IX_services
				ON #s2_3b_services_indexed
				(
					LA_Code,
					Der_NHS_LA_Combined_Person_ID,
					person_valid_event_order
				);

---- Stage 2.4 Link each request for a person to the first assessment and service following the request
	-- Note: Variables related to the first assessment after the request are named as "response" variables as future stages will account for situations where the response could be a request or service, not just an assessment.
	DROP TABLE IF EXISTS #s2_4_linked_events

	SELECT
		r.LA_Code									,
		r.LA_Name									,
		r.Der_NHS_LA_Combined_Person_ID				,
		r.LA_Person_Unique_Identifier				,
		r.Der_Unique_Record_ID						AS request_Der_Unique_Record_ID,
		r.Event_Start_Date							AS request_start_date,
		r.Age_Band									AS request_age_band,
		r.person_valid_event_order					AS request_event_order,
		r.request_indicates_progress				,
		r.request_indicates_progress_to_service		,
		r.Der_Conversation							AS request_Der_Conversation,

		-- First Assessment after the Request
		nextAsmt.person_valid_event_order							AS s2_4_response_event_order,
		nextAsmt.Event_Type_Cleaned									AS s2_4_response_event_type,
		nextAsmt.Assessment_Type_Cleaned							AS s2_4_response_assessment_type,
		nextAsmt.Event_Start_Date									AS s2_4_response_start_date,
		nextAsmt.Age_Band											AS s2_4_response_age_band,
		nextAsmt.assessment_indicates_progress_to_service			AS s2_4_response_assessment_indicates_progress_to_service,
		nextAsmt.assessment_block_indicates_progress_to_service		AS s2_4_response_assessment_block_indicates_progress_to_service,
		nextAsmt.Der_Unique_Record_ID								AS s2_4_response_Der_Unique_Record_ID,

		-- First Service after the Request
		nextSrv.person_valid_event_order	AS s2_4_service_event_order,
		nextSrv.Event_Type_Cleaned			AS s2_4_service_event_type,
		nextSrv.Service_Type_Cleaned		AS s2_4_service_type,
		nextSrv.Event_Start_Date			AS s2_4_service_start_date,
		nextSrv.Age_Band					AS s2_4_service_age_band,
		nextSrv.Der_Unique_Record_ID		AS s2_4_service_Der_Unique_Record_ID

	INTO #s2_4_linked_events
	FROM #s2_3b_requests_indexed r

		-- First Assessment after request
		OUTER APPLY (
			SELECT TOP 1 x.*
			FROM #s2_3b_assessments_indexed x
			WHERE x.LA_Code = r.LA_Code
			  AND x.Der_NHS_LA_Combined_Person_ID = r.Der_NHS_LA_Combined_Person_ID
			  AND x.person_valid_event_order > r.person_valid_event_order
			ORDER BY x.person_valid_event_order
		) nextAsmt

		-- First Service after request
		OUTER APPLY (
			SELECT TOP 1 y.*
			FROM #s2_3b_services_indexed y
			WHERE y.LA_Code = r.LA_Code
			  AND y.Der_NHS_LA_Combined_Person_ID= r.Der_NHS_LA_Combined_Person_ID
			  AND y.person_valid_event_order > r.person_valid_event_order
			ORDER BY y.person_valid_event_order
		) nextSrv;


	---- Temporarily save down table to cut re-processing time
	--DROP TABLE IF EXISTS asc_sandbox.Temp_Waiting_Times_S2_4
	--SELECT *
	--INTO asc_sandbox.Temp_Waiting_Times_S2_4
	--FROM #s2_4_linked_events;

	---- Temporarily re-instate table to cut re-processing time
	--DROP TABLE IF EXISTS #s2_4_linked_events;
	--SELECT *
	--INTO #s2_4_linked_events
	--FROM asc_sandbox.Temp_Waiting_Times_S2_4;

--###################################################################################
-- Stage 3 - Account for 3-conversations model, missing assessments and discount excess requests
--###################################################################################

---- Stage 3.1 Allow requests to act as an assessment when a conversation is flagged for release 1 requests
	DROP TABLE IF EXISTS  #s3_1_linked_events_3Cs

	SELECT *,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_event_order			ELSE s2_4_response_event_order				END AS s3_1_response_event_order,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN 'Request'						ELSE s2_4_response_event_type				END AS s3_1_response_event_type,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL							ELSE s2_4_response_assessment_type			END AS s3_1_response_assessment_type,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_start_date				ELSE s2_4_response_start_date				END AS s3_1_response_start_date,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_age_band				ELSE s2_4_response_age_band					END AS s3_1_response_age_band,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN request_Der_Unique_Record_ID	ELSE s2_4_response_Der_Unique_Record_ID		END AS s3_1_response_Der_Unique_Record_ID,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL							ELSE s2_4_response_assessment_indicates_progress_to_service			END AS s3_1_response_indicates_progress_to_service,
			CASE WHEN (c1_request_override = 1 AND request_spec = 'R1') THEN NULL							ELSE s2_4_response_assessment_block_indicates_progress_to_service	END AS s3_1_response_block_indicates_progress_to_service
	INTO #s3_1_linked_events_3Cs
	FROM (	SELECT *,
				-- Create conversation 1 override flag
				CASE 
					WHEN ((request_start_date <  s2_4_response_start_date) OR  s2_4_response_start_date IS NULL)
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
			FROM #s2_4_linked_events
		) a;

---- Stage 3.2 Allow services to be first response in part 1 metric
	DROP TABLE IF EXISTS #s3_2_linked_events_service_as_response

	SELECT *,
			CASE WHEN next_assessment_service_override = 1 THEN s2_4_service_event_order			ELSE s3_1_response_event_order							END AS s3_2_response_event_order,
			CASE WHEN next_assessment_service_override = 1 THEN s2_4_service_event_type				ELSE s3_1_response_event_type							END AS s3_2_response_event_type,
			CASE WHEN next_assessment_service_override = 1 THEN NULL								ELSE s3_1_response_assessment_type						END AS s3_2_response_assessment_type,
			CASE WHEN next_assessment_service_override = 1 THEN s2_4_service_start_date				ELSE s3_1_response_start_date							END AS s3_2_response_start_date,
			CASE WHEN next_assessment_service_override = 1 THEN s2_4_service_age_band				ELSE s3_1_response_age_band								END AS s3_2_response_age_band,
			CASE WHEN next_assessment_service_override = 1 THEN s2_4_service_Der_Unique_Record_ID	ELSE s3_1_response_Der_Unique_Record_ID					END AS s3_2_response_Der_Unique_Record_ID,
			CASE WHEN next_assessment_service_override = 1 THEN NULL								ELSE s3_1_response_indicates_progress_to_service		END AS s3_2_response_indicates_progress_to_service,
			CASE WHEN next_assessment_service_override = 1 THEN NULL								ELSE s3_1_response_block_indicates_progress_to_service	END AS s3_2_response_block_indicates_progress_to_service

	INTO #s3_2_linked_events_service_as_response
	FROM (
		SELECT *,
		CASE 
			WHEN s3_1_response_event_order > s2_4_service_event_order THEN 1 
			WHEN s3_1_response_event_order IS NULL AND s2_4_service_event_order IS NOT NULL THEN 1 
			ELSE 0 
		END AS next_assessment_service_override,
		-- further variable added for validation 
		CASE WHEN s3_1_response_start_date > s2_4_service_start_date THEN 1 ELSE 0 END AS next_assessment_service_date_override
		FROM #s3_1_linked_events_3Cs) a;

	--- Drop temp variables
	ALTER TABLE #s3_2_linked_events_service_as_response
	DROP COLUMN s3_1_response_event_order, s3_1_response_event_type, s3_1_response_assessment_type, s3_1_response_start_date, s3_1_response_age_band, s3_1_response_Der_Unique_Record_ID;

---- Stage 3.3 Exclude services from part 2 metric where an intervening assessment block does not indicate progress to service
	DROP TABLE IF EXISTS #s3_3_linked_events_exclude_no_progress

	SELECT *,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_event_order				END AS s3_3_service_event_order,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_event_type				END AS s3_3_service_event_type,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_type					END AS s3_3_service_type,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_start_date				END AS s3_3_service_start_date,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_age_band				END AS s3_3_service_age_band,
			CASE WHEN s3_2_response_event_type = 'Assessment' and s3_2_response_block_indicates_progress_to_service = 0 THEN NULL ELSE s2_4_service_Der_Unique_Record_ID	END AS s3_3_service_Der_Unique_Record_ID
	INTO #s3_3_linked_events_exclude_no_progress
	FROM #s3_2_linked_events_service_as_response;


---- Stage 3.4 Exclude requests that overlap with ongoing activity linked to a previous request

	DROP TABLE IF EXISTS #s3_4_linked_events_exclude_overlap
	
	SELECT *,
			CASE
				WHEN LAG(GREATEST(s3_2_response_event_order, s3_3_service_event_order)) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order) > request_event_order
					THEN 1
				WHEN s3_2_response_event_order <= max_s3_2_response_event_order_prev 
					THEN 1
				WHEN s3_3_service_event_order <= max_s3_3_service_event_order_prev
					THEN 1
				ELSE 0
				END AS request_starts_before_previous_request_follow_up_ends

	INTO #s3_4_linked_events_exclude_overlap
	FROM (	SELECT *,
				-- For each row for a person, detected the highest previous first response event order
				-- Note: Coalesce, with the 0 value, is used to ensure the value is not NULL.
					COALESCE(
						MAX(s3_2_response_event_order) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order
							ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
						),
						0) AS max_s3_2_response_event_order_prev,
				-- For each row for a person, detected the highest previous service event order
				-- Note: Coalesce, with the 0 value, is used to ensure the value is not NULL.
					COALESCE(
						MAX(s3_3_service_event_order) OVER (
							PARTITION BY LA_Code, Der_NHS_LA_Combined_Person_ID
							ORDER BY request_event_order
							ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
						),
						0) AS max_s3_3_service_event_order_prev
			FROM #s3_3_linked_events_exclude_no_progress) a;


--###################################################################################
-- Stage 4  - Calculate waiting times
--###################################################################################

---- Stage 4.1 Calculate waiting times at an individual level and group waiting times by reporting period

	DROP TABLE IF EXISTS #s4_1_person_level_waiting_times

	SELECT *,
		-- Create statistical reporting period end dates
		FORMAT(EOMONTH(stat_reporting_period_start_request, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_request,
		FORMAT(EOMONTH(stat_reporting_period_start_part1, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_part1,
		FORMAT(EOMONTH(stat_reporting_period_start_part2, 2), 'yyyy-MM-dd') AS stat_reporting_period_end_part2,
		
		-- Calculate part 1 wait time
		CASE
			WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEDIFF(DAY, request_start_date, s3_2_response_start_date)
			ELSE NULL
		END AS part1_wait_time,
		-- Calculate part 2 wait time
		CASE
			WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEDIFF(DAY, request_start_date, s3_3_service_start_date)
			ELSE NULL
		END AS part2_wait_time
	INTO #s4_1_person_level_waiting_times
	FROM (SELECT *,
			-- The start date of the statistical reporting period (quarter) the request starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN  DATEFROMPARTS(YEAR(request_start_date), ((DATEPART(QUARTER, request_start_date) - 1) * 3) + 1, 1) 
					ELSE NULL
			 END AS stat_reporting_period_start_request,
			
			-- The start date of the statistical reporting period (quarter) the part 1 event starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEFROMPARTS(YEAR(s3_2_response_start_date), ((DATEPART(QUARTER, s3_2_response_start_date) - 1) * 3) + 1, 1) 
				ELSE NULL
			  END AS stat_reporting_period_start_part1,

			-- The start date of the statistical reporting period (quarter) the part 2 event starts in
			  CASE WHEN request_starts_before_previous_request_follow_up_ends = 0 THEN DATEFROMPARTS(YEAR(s3_3_service_start_date), ((DATEPART(QUARTER, s3_3_service_start_date ) - 1) * 3) + 1, 1) 
				ELSE NULL
			  END AS stat_reporting_period_start_part2
			FROM #s3_4_linked_events_exclude_overlap 
			) a;

---- Stage 4.2 Create local authority level median waiting times
		---- Exclude invalid data
		---- Created seperately for parts 1 and 2 and then appended together

	DROP TABLE IF EXISTS #s4_2_la_level_waiting_times
	SELECT DISTINCT
			LA_Code,
			LA_Name,
			stat_reporting_period_start_part1 AS Statistical_Reporting_Period_Start,
			stat_reporting_period_end_part1 AS Statistical_Reporting_Period_End,
			s3_2_response_age_band AS Age_Band,
			'Wait time to first response' AS Metric,
			-- Create LA level median
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY part1_wait_time)
				OVER (PARTITION BY LA_Code,
									stat_reporting_period_start_part1,
									s3_2_response_age_band)
				AS Median_Waiting_Time,
			-- Create count of valid waiting times
			COUNT(*) OVER (
				PARTITION BY LA_Code,
								stat_reporting_period_start_part1,
								s3_2_response_age_band)
				AS Number_Of_Waiting_Times_Identified
	INTO #s4_2_la_level_waiting_times
	FROM #s4_1_person_level_waiting_times
	WHERE part1_wait_time IS NOT NULL
		AND s3_2_response_age_band IN ('25 to 64', '65 and above') 
	UNION ALL
	SELECT DISTINCT
			LA_Code,
			LA_Name,
			stat_reporting_period_start_part2 AS Statistical_Reporting_Period_Start,
			stat_reporting_period_end_part2 AS Statistical_Reporting_Period_End,
			s3_3_service_age_band AS Age_Band,
			'Wait time to first service' AS Metric,
			-- Create LA level median
			PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY part2_wait_time)
				OVER (PARTITION BY LA_Code,
									stat_reporting_period_start_part2,
									s3_3_service_age_band)
				AS Median_Waiting_Time,
			-- Create count of valid waiting times
			COUNT(*) OVER (
				PARTITION BY LA_Code,
								stat_reporting_period_start_part2,
								s3_3_service_age_band)
				AS Number_Of_Waiting_Times_Identified
	FROM #s4_1_person_level_waiting_times
	WHERE part2_wait_time IS NOT NULL
		AND s3_3_service_age_band IN ('25 to 64', '65 and above');

--###################################################################################
-- Stage 5  - Create contextual information for Athena dashboard
--###################################################################################

---- Stage 5.1 Count number of events within cohort for each combination of LA, age band, statistical reporting period
	--- Requires output of Stage 2.1
	--- Note: Waiting times are calculated using the request, regardless of what statistical reporting period it is in. ...
	--- ... Hence the count of requests here is not as contextually useful for the metrics than the counts of assessments and services.
	DROP TABLE IF EXISTS #s5_1_contextual_event_counts;
	SELECT calc.*,
			context.all_request_count,
			context.all_assessment_count,
			context.all_service_count
	INTO #s5_1_contextual_event_counts
	FROM #s4_2_la_level_waiting_times AS calc
	LEFT JOIN ( -- Create summary information
			SELECT LA_Name, LA_Code, Age_Band, Statistical_Reporting_Period_Start,
						SUM(CASE WHEN Event_Type_Cleaned = 'Request' THEN 1 ELSE 0 END) AS all_request_count,
						SUM(CASE WHEN Event_Type_Cleaned = 'Assessment' THEN 1 ELSE 0 END) AS all_assessment_count,
						SUM(CASE WHEN Event_Type_Cleaned = 'Service' THEN 1 ELSE 0 END) AS all_service_count
			FROM ( -- Create Statistical Reporting Period variable
					SELECT LA_Name, LA_Code, Event_Type_Cleaned, Age_Band,
							DATEFROMPARTS(YEAR(Event_Start_Date), ((DATEPART(QUARTER, Event_Start_Date) - 1) * 3) + 1, 1) 
					 AS Statistical_Reporting_Period_Start
					FROM #s2_1_filtered_requests_assessments_and_services
					WHERE Event_Start_Date >= '2023-04-01') a
			GROUP BY LA_Name, LA_Code, Age_Band, Statistical_Reporting_Period_Start)
			AS context
		ON calc.LA_Code = context.LA_Code 
			AND calc.Age_Band = context.Age_Band
			AND calc.Statistical_Reporting_Period_Start= context.Statistical_Reporting_Period_Start;
	
---- Stage 5.2 Count of event types for first responses and assessment types where they are assessments included in waiting time metric
	DROP TABLE IF EXISTS #s5_2_contextual_part_1;
	SELECT base.*,
				ast.response_event_request,
				ast.response_event_assessment,
				ast.response_event_service,
				ast.at_short,
				ast.at_long,
				ast.at_invalid_and_not_mapped,
				ast.at_null
	INTO #s5_2_contextual_part_1
	FROM #s5_1_contextual_event_counts AS base
		LEFT JOIN (SELECT
						LA_Code,
						stat_reporting_period_start_part1,
						s3_2_response_age_band,
						-- Create event type counts for responses
						SUM(CASE WHEN s3_2_response_event_type = 'Request'		THEN 1 ELSE 0 END) AS response_event_request,
						SUM(CASE WHEN s3_2_response_event_type = 'Assessment'	THEN 1 ELSE 0 END) AS response_event_assessment,
						SUM(CASE WHEN s3_2_response_event_type = 'Service'		THEN 1 ELSE 0 END) AS response_event_service,
						-- Create assessment type counts when response is an assessment
						SUM(CASE WHEN s3_2_response_event_type = 'Assessment' AND s3_2_response_assessment_type = 'Short term assessment'	THEN 1 ELSE 0 END) AS at_short,
						SUM(CASE WHEN s3_2_response_event_type = 'Assessment' AND s3_2_response_assessment_type = 'Long term assessment'	THEN 1 ELSE 0 END) AS at_long,
						SUM(CASE WHEN s3_2_response_event_type = 'Assessment' AND s3_2_response_assessment_type = 'Invalid and not mapped'	THEN 1 ELSE 0 END) AS at_invalid_and_not_mapped, -- Redundancy
						SUM(CASE WHEN s3_2_response_event_type = 'Assessment' AND s3_2_response_assessment_type IS NULL						THEN 1 ELSE 0 END) AS at_null -- Redundancy
					FROM #s4_1_person_level_waiting_times
					WHERE part1_wait_time IS NOT NULL
					AND s3_2_response_age_band IN ('25 to 64', '65 and above')
					GROUP BY LA_Code,
							stat_reporting_period_start_part1,
							s3_2_response_age_band
					) as ast
				ON base.LA_Code = ast.LA_Code
				AND base.Statistical_Reporting_Period_Start = ast.stat_reporting_period_start_part1
				AND base.Age_Band = ast.s3_2_response_age_band
	WHERE Metric = 'Wait time to first response';

---- Stage 5.3 Count of service types for services included in waiting time metric
	
	DROP TABLE IF EXISTS #s5_3_contextual_part_2;
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
	INTO #s5_3_contextual_part_2
	FROM #s5_1_contextual_event_counts AS base
		LEFT JOIN (SELECT
						LA_Code,
						stat_reporting_period_start_part2,
						s3_3_service_age_band,
						SUM(CASE WHEN s3_3_service_type = 'Short term support: ST-Max'				THEN 1 ELSE 0 END) AS [STS_ST_Max],
						SUM(CASE WHEN s3_3_service_type = 'Long term support: Nursing care'			THEN 1 ELSE 0 END) AS [LTS_Nursing],
						SUM(CASE WHEN s3_3_service_type = 'Short term support: Ongoing low level'	THEN 1 ELSE 0 END) AS [STS_Ongoing],
						SUM(CASE WHEN s3_3_service_type = 'Long term support: Community'			THEN 1 ELSE 0 END) AS [LTS_Community],
						SUM(CASE WHEN s3_3_service_type = 'Long term support: Residential care'		THEN 1 ELSE 0 END) AS [LTS_Residential],
						SUM(CASE WHEN s3_3_service_type = 'Long term support: Prison'				THEN 1 ELSE 0 END) AS [LTS_Prison],
						SUM(CASE WHEN s3_3_service_type = 'Short term support: Other short term'	THEN 1 ELSE 0 END) AS [STS_Other],
						SUM(CASE WHEN s3_3_service_type = 'Invalid and not mapped'					THEN 1 ELSE 0 END) AS [Invalid_and_not_mapped], -- Redundancy
						SUM(CASE WHEN s3_3_service_type is NULL										THEN 1 ELSE 0 END) AS [Null] -- Redundancy
					FROM #s4_1_person_level_waiting_times
					WHERE part2_wait_time IS NOT NULL
					AND s3_3_service_age_band IN ('25 to 64', '65 and above')
					GROUP BY LA_Code,
							stat_reporting_period_start_part2,
							s3_3_service_age_band
					) as st
				ON base.LA_Code = st.LA_Code
				AND base.Statistical_Reporting_Period_Start = stat_reporting_period_start_part2
				AND base.Age_Band = s3_3_service_age_band
	WHERE Metric = 'Wait time to first service';

--###################################################################################
-- Stage 6  - Write outputs
--###################################################################################
	--- Athena dashboard outputs are resticted by specified date parameters

	--- Set quarter parameter
	DECLARE @Quarter AS VARCHAR(7) = 'Q1_2627';

	--- Set Athena dashboard inclusion date parameters
		-- Note: Statisitcal reporting periods are set to 1st day of finanical year quarters
		-- Upper limit will be included in filter
	DECLARE @Stat_RP_Start_Dashboard_Lower_Limit AS DATE = '2024-04-01';
	DECLARE @Stat_RP_Start_Dashboard_Upper_Limit AS DATE = '9999-01-01'; -- Date 9999-01-01 used as dummy to accept all statisitcal reporting periods after and including the lower limit
	

	--- Set table name parameter
	DECLARE @Metrics_Person_Level_DHSC	AS VARCHAR(256) = CONCAT('asc_sandbox.Waiting_Times_Metrics_Person_Level_', @Quarter, '_All_SRP');
	DECLARE @Metrics_LA_Level_DHSC		AS VARCHAR(256) = CONCAT('asc_sandbox.Waiting_Times_Metrics_LA_Level_', @Quarter, '_All_SRP');
	DECLARE @Metrics_LA_Level_Dashboard AS VARCHAR(256) = 'asc_sandbox.LA_PBI_Waiting_Times';
	DECLARE @Diag_Part1_DHSC			AS VARCHAR(256) = CONCAT('asc_sandbox.Waiting_Times_Diag_Part1_', @Quarter, '_All_SRP');
	DECLARE @Diag_Part1_Dashboard		AS VARCHAR(256) = 'asc_sandbox.LA_PBI_Waiting_Times_Diag_Part1';
	DECLARE @Diag_Part2_DHSC			AS VARCHAR(256) = CONCAT('asc_sandbox.Waiting_Times_Diag_Part2_', @Quarter, '_All_SRP');
	DECLARE @Diag_Part2_Dashboard		AS VARCHAR(256) = 'asc_sandbox.LA_PBI_Waiting_Times_Diag_Part2'

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
				FROM #s4_2_la_level_waiting_times
				WHERE Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit
				) rp
			 )	rp2;
	
		--- Output person level waiting times for all Statistical Reporting Periods for DHSC
			DROP TABLE IF EXISTS '  + @Metrics_Person_Level_DHSC +';
			SELECT *
			INTO ' + @Metrics_Person_Level_DHSC +'
			FROM #s4_1_person_level_waiting_times;
				
		--- LA level waiting times
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Metrics_LA_Level_DHSC +';
				SELECT *
				INTO ' + @Metrics_LA_Level_DHSC +'
				FROM #s4_2_la_level_waiting_times;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS '  + @Metrics_LA_Level_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Metrics_LA_Level_Dashboard +'
				FROM #s4_2_la_level_waiting_times base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit;


		--- Part 1 diagnositcs
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Diag_Part1_DHSC +';
				SELECT *
				INTO ' + @Diag_Part1_DHSC +'
				FROM #s5_2_contextual_part_1;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS ' + @Diag_Part1_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Diag_Part1_Dashboard +'
				FROM #s5_2_contextual_part_1 base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit ;
				
		--- Part 2 diagnositcs
			-- Output all Statistical Reporting Periods for DHSC
				DROP TABLE IF EXISTS '  + @Diag_Part2_DHSC +';
				SELECT *
				INTO ' + @Diag_Part2_DHSC +'
				FROM #s5_3_contextual_part_2;

			-- Output selected Statistical Reporting Periods for Athena dashboard
				DROP TABLE IF EXISTS ' + @Diag_Part2_Dashboard +';
				SELECT base.*,
					axis.PBI_Axis_Order,
					axis.PBI_Axis_Name
				INTO ' + @Diag_Part2_Dashboard +'
				FROM #s5_3_contextual_part_2 base
				LEFT JOIN #PBI_Axis AS axis
					ON base.Statistical_Reporting_Period_Start = axis.Statistical_Reporting_Period_Start
				WHERE base.Statistical_Reporting_Period_Start BETWEEN @LowerLimit AND @UpperLimit ;';

	EXEC sp_executesql
		@stmt = @QUERY,
		@params = N'@LowerLimit DATE, @UpperLimit DATE',
		@LowerLimit = @Stat_RP_Start_Dashboard_Lower_Limit,
		@UpperLimit = @Stat_RP_Start_Dashboard_Upper_Limit;
