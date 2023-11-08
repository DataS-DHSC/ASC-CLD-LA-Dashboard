-------------------------------
-- Output published tables ----
-------------------------------

--This code saves all the powerBI tables for the published dashboard separately
--This is to keep development tables separate from the live tables which feed the published dashboard

DROP TABLE IF EXISTS 
  ASC_Sandbox.LA_PBI_Assessments_Fact_Published,
  ASC_Sandbox.LA_PBI_Requests_Fact_Published,
  ASC_Sandbox.LA_PBI_Reviews_Fact_Published,
  ASC_Sandbox.LA_PBI_Services_Fact_Published,
  ASC_Sandbox.LA_PBI_Costs_Fact_Published,
  ASC_Sandbox.LA_PBI_Calendar_Dim_Published,
  ASC_Sandbox.LA_PBI_Client_Type_Dim_Published,
  ASC_Sandbox.LA_PBI_Working_Age_Dim_Published,
  ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_Published,
  ASC_Sandbox.LA_PBI_Ethnicity_Dim_Published,
  ASC_Sandbox.LA_PBI_Gender_Dim_Published,
  ASC_Sandbox.LA_PBI_Geography_Dim_Published,
  ASC_Sandbox.LA_PBI_PSR_Dim_Published;

SELECT *
INTO ASC_Sandbox.LA_PBI_Assessments_Fact_Published
FROM ASC_Sandbox.LA_PBI_Assessments_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Requests_Fact_Published
FROM ASC_Sandbox.LA_PBI_Requests_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Reviews_Fact_Published
FROM ASC_Sandbox.LA_PBI_Reviews_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Services_Fact_Published
FROM ASC_Sandbox.LA_PBI_Services_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Costs_Fact_Published
FROM ASC_Sandbox.LA_PBI_Costs_Fact;

SELECT *
INTO ASC_Sandbox.LA_PBI_Calendar_Dim_Published
FROM ASC_Sandbox.LA_PBI_Calendar_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_Client_Type_Dim_Published
FROM ASC_Sandbox.LA_PBI_Client_Type_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_Working_Age_Dim_Published
FROM ASC_Sandbox.LA_PBI_Working_Age_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_DQ_Values_Aggregated_Published
FROM ASC_Sandbox.LA_PBI_DQ_Values_Aggregated;

SELECT *
INTO ASC_Sandbox.LA_PBI_Ethnicity_Dim_Published
FROM ASC_Sandbox.LA_PBI_Ethnicity_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_Gender_Dim_Published
FROM ASC_Sandbox.LA_PBI_Gender_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_Geography_Dim_Published
FROM ASC_Sandbox.LA_PBI_Geography_Dim;

SELECT *
INTO ASC_Sandbox.LA_PBI_PSR_Dim_Published
FROM ASC_Sandbox.LA_PBI_PSR_Dim;

