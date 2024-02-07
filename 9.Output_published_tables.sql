-------------------------------
-- Output published tables ----
-------------------------------

--This code saves all the powerBI tables for the published dashboard separately
--This is to keep development tables separate from the live tables which feed the published dashboard

DROP TABLE IF EXISTS 
  DHSC_Reporting.LA_PBI_Master_Table,
  DHSC_Reporting.LA_PBI_Assessments_Fact,
  DHSC_Reporting.LA_PBI_Requests_Fact,
  DHSC_Reporting.LA_PBI_Reviews_Fact,
  DHSC_Reporting.LA_PBI_Services_Fact,
  DHSC_Reporting.LA_PBI_Costs_Fact,
  DHSC_Reporting.LA_PBI_Calendar_Dim,
  DHSC_Reporting.LA_PBI_Client_Type_Dim,
  DHSC_Reporting.LA_PBI_Working_Age_Dim,
  DHSC_Reporting.LA_PBI_DQ_Values_Aggregated,
  DHSC_Reporting.LA_PBI_Ethnicity_Dim,
  DHSC_Reporting.LA_PBI_Gender_Dim,
  DHSC_Reporting.LA_PBI_Geography_Dim,
  DHSC_Reporting.LA_PBI_PSR_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_Master_Table
FROM ASC_Sandbox.LA_PBI_Master_Table;

SELECT *
INTO DHSC_Reporting.LA_PBI_Assessments_Fact
FROM ASC_Sandbox.LA_PBI_Assessments_Fact;

SELECT *
INTO DHSC_Reporting.LA_PBI_Requests_Fact
FROM ASC_Sandbox.LA_PBI_Requests_Fact;

SELECT *
INTO DHSC_Reporting.LA_PBI_Reviews_Fact
FROM ASC_Sandbox.LA_PBI_Reviews_Fact;

SELECT *
INTO DHSC_Reporting.LA_PBI_Services_Fact
FROM ASC_Sandbox.LA_PBI_Services_Fact;

SELECT *
INTO DHSC_Reporting.LA_PBI_Costs_Fact
FROM ASC_Sandbox.LA_PBI_Costs_Fact;

SELECT *
INTO DHSC_Reporting.LA_PBI_Calendar_Dim
FROM ASC_Sandbox.LA_PBI_Calendar_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_Client_Type_Dim
FROM ASC_Sandbox.LA_PBI_Client_Type_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_Working_Age_Dim
FROM ASC_Sandbox.LA_PBI_Working_Age_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_DQ_Values_Aggregated
FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated;

SELECT *
INTO DHSC_Reporting.LA_PBI_Ethnicity_Dim
FROM ASC_Sandbox.LA_PBI_Ethnicity_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_Gender_Dim
FROM ASC_Sandbox.LA_PBI_Gender_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_Geography_Dim
FROM ASC_Sandbox.LA_PBI_Geography_Dim;

SELECT *
INTO DHSC_Reporting.LA_PBI_PSR_Dim
FROM ASC_Sandbox.LA_PBI_PSR_Dim;


