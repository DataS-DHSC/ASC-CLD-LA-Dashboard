------  COSTS FACT TABLE ------
--This code outputs the costs table from the previously created services fact table
--It restricts records to only the latest quarter (as LAs provide the latest cost this should only be applied to the latest events)
--It excludes any null costs and converts any negative costs or planned units to positive
--It also excludes any rows without planned units and where the units cannot be deduced from the cost frequency
--It derives new fields 'weeks_of_service', 'total_cost_period' and 'cost_per_week'

-------PRE-REQUISTIES--------
--1. Run create master table script
--2. Run services script

-----------------------------------------------------
---- Set reporting period dates -----
-----------------------------------------------------
DECLARE @ReportingPeriodStartDate AS DATE = '2024-04-01'
DECLARE @ReportingPeriodEndDate AS DATE = '2024-06-30'


-----------------------------------------------------
------ Filter to only the latest quarter  -----------
-----------------------------------------------------
DROP TABLE IF EXISTS #Costs_Latest_Quarter;

SELECT *
INTO #Costs_Latest_Quarter
FROM ASC_Sandbox.LA_PBI_Services_Fact
WHERE (Event_End_Date >= @ReportingPeriodStartDate OR Event_End_Date IS NULL)
  AND Event_Start_Date <= @ReportingPeriodEndDate;


-----------------------------------------------------
-- Prepare the cost fields  ------------
-----------------------------------------------------
--In general, the cost_frequency is trusted over the planned_units_per_week field 
--(as there are many instances where these two don't agree with each other)
-- and the cost frequency is converted into an average number of units per week (except for one-off costs)
-- Where cost frequency is less frequent than weekly, planned units per week is taken

--Any negative values for unit cost or planned units per week are made positive (absolute)
--All rows without a cost are excluded

DROP TABLE IF EXISTS #Costs_Clean;

SELECT *
INTO #Costs_Clean
FROM ( 
  SELECT *,
    ABS(Unit_Cost)  AS Unit_Cost_Abs,  --otherwise convert any negative costs to postive
    --where we don't have planned units use the cost frequency to deduce 
    -- can't deduce units per week when frequency is per session, hourly or daily and planned units is 0
    --numbers need to be decimal for sql to recognise and output a decimal
    CASE 
      WHEN  Cost_Frequency_Unit_Type LIKE 'week%' THEN 1.0       
      WHEN  Cost_Frequency_Unit_Type LIKE 'fortnight%' THEN 1.0 / 2.0  
      WHEN Cost_Frequency_Unit_Type LIKE '4-week%' THEN 1.0 / 4.0 
      WHEN Cost_Frequency_Unit_Type LIKE 'month%' THEN 12.0 / 52.0   
      WHEN Cost_Frequency_Unit_Type LIKE 'quarter%' THEN 4.0 / 52.0
      WHEN Cost_Frequency_Unit_Type LIKE 'annual%' THEN 1.0 /52.0
      ELSE ABS(Planned_Units_Per_Week)    --otherwise convert any negative planned units to positive
    END AS Planned_Units_Per_Week_Abs
  FROM #Costs_Latest_Quarter
) a
WHERE Unit_Cost_Abs IS NOT NULL AND Unit_Cost_Abs != 0   --remove rows without a cost
--remove rows where newly derived planned units per week is 0 or null unless the cost frequency is one-off
  AND (Cost_Frequency_Unit_Type = 'one-off' 
  OR (Planned_Units_Per_Week_Abs !=0 AND Planned_Units_Per_Week_Abs IS NOT NULL)) ;


		
-----------------------------------------------------
----Derive new fields based on row level calculations -----
-- weeks of service and total cost period are derived
-----------------------------------------------------
DROP TABLE IF EXISTS #Costs_Derived;

SELECT *,
--total cost in period is the cost per week times weeks of service (field calculated below)
--if cost frequency is one-off then the cost in period is only the unit cost
  CASE 
    WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' THEN Cost_Per_Week * Weeks_of_Service   
    WHEN ISNULL(Cost_Frequency_Unit_Type, '') = 'one-off' THEN Unit_Cost_Abs                       
    ELSE NULL
  END AS Total_Cost_Period
INTO #Costs_Derived
FROM (
  SELECT  *,
    --cost per week is only calculated when cost frequency isn't one off 
    CASE 
      WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' 
      THEN Unit_Cost_Abs * Planned_Units_Per_Week_Abs  
      ELSE NULL
    END AS Cost_Per_Week,
    --determine weeks of service based on start and end dates of the service
    CASE 
      WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' AND 
        Event_Start_Date >=  @ReportingPeriodStartDate AND Event_End_Date <=  @ReportingPeriodEndDate      
      THEN (DATEDIFF(DAY, Event_Start_Date, Event_End_Date)+1)/7.0
      --start and end dates are replaced with the reporting period start and end dates 
      --when they are outside the reporting period or ongoing services
      WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' AND 
        Event_Start_Date < @ReportingPeriodStartDate AND Event_End_Date <=  @ReportingPeriodEndDate 
      THEN (DATEDIFF(DAY, @ReportingPeriodStartDate, Event_End_Date)+1)/7.0              
      WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' AND 
        Event_Start_Date >= @ReportingPeriodStartDate AND 
        (Event_End_Date >  @ReportingPeriodEndDate OR event_end_date IS NULL)
      THEN (DATEDIFF(day, Event_Start_Date, @ReportingPeriodEndDate)+1)/7.0
      WHEN ISNULL(Cost_Frequency_Unit_Type, '') != 'one-off' AND 
        Event_Start_Date < @ReportingPeriodStartDate AND 
        (Event_End_Date >  @ReportingPeriodEndDate  OR event_end_date IS NULL)
      THEN (DATEDIFF(day, @ReportingPeriodStartDate, @ReportingPeriodEndDate)+1)/7.0
      ELSE NULL
    END AS Weeks_of_Service
  from #Costs_Clean
) a;

-----------------------------------------------------
----Select only required columns -----
-----------------------------------------------------
DROP TABLE IF EXISTS ASC_Sandbox.LA_PBI_Costs_Fact;

SELECT  
  LA_Code,
  LA_Name,
  Client_Type,
  Gender,
  Ethnicity,
  Der_Age_Band,
  Der_Working_Age_Band,
  Primary_Support_Reason,
  Event_Outcome,
  Event_Outcome_Grouped,
  Service_Type,
  Service_Type_Grouped,
  Service_Component,
  Der_NHS_LA_Combined_Person_ID,
  Cost_Per_Week,
  Weeks_of_Service,
  Total_Cost_Period
into ASC_Sandbox.LA_PBI_Costs_Fact
from #Costs_Derived;