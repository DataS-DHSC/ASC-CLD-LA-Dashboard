# Data cleaning, mapping and deriving new fields

> Processing steps applied in [`GetONSDeaths`](/Main_tables/stored_procedures/create_GetONSDeaths_procedure.sql) procedure and [`GetDerivedFields`](/Main_tables/stored_procedures/create_GetDerivedFields_procedure.sql) procedure.

[Back to Overview](/Main_tables/docs/methodology/0-overview.md)

## Cleaning and mapping

- **Release 1 values are mapped to Release 2 values** (prior to October 2025, the reverse mapping was applied).
- **Invalid values are mapped to the specification**, where possible, using manually maintained mapping tables. 

Cleaning and mapping is implemented by joining the data with mapping tables of the following form for each data field, where
- the `_Raw` field contains all values submitted by local authorities,
- the `_Cleaned` field is the equivalent release 2 value, and
- the `_Grouped` field is a higher-level grouping (where applicable).

E.g. `Ethnicity_Mapping`:

| `Ethnicity_Raw` | `Ethnicity_Cleaned` | `Ethnicity_Grouped` |
| --- | --- | --- |
| `Mixed: White and Asian` | `Mixed or multiple ethnic groups: White and Asian` | `Mixed or multiple ethnic groups` |
| `White` | `Invalid and not mapped` | `Unknown` |

Mapping tables are reviewed and updated quarterly if new invalid values are received. Where mapping is ambiguous:
- Widely used invalid values are investigated and mapped where possible.
- Rare or unclear values are mapped to **“invalid and not mapped”**.

## Derived fields

New fields are derived, including:

- [**Enhanced date of death**](/Main_tables/docs/methodology/dates-of-death.md) - `Der_Date_of_Death`
  <br> Populated with ONS mortality dataset date of death where an NHS number present, else CLD date of death. Derived early on in pipeline then used in place of CLD date of death.

- **Corrected event end dates** - `Der_Event_End_Date`
  <br> Populated with (enhanced) date of death where present and preceding the recorded end date. (See also [Deduplication step 2](/Main_tables/docs/methodology/6-deduplication.md#step-2--cropping-and-deduplicating-service-records).)

- **A combined person ID** - `Der_NHS_LA_Combined_Person_ID`
  <br> Traced NHS number if present, else LA-provided NHS number, else LA person ID. Row excluded if no person ID present.

- **Latest age and age band** - `Der_Latest_Age` / `Der_Age_Band` / `Der_Working_Age_Band`
  <br> Age and corresponding age band at latest point in an event - at event end date when populated, else at reporting period end date for ongoing services. Derived from birth year and month rather than exact date of birth (which is not available to DHSC), assuming birth on first day of month.

- **Event outcome hierarchy**

  Assigned as follows, in line with the hierarchy in the ASC CLD guidance:

  ```
    Event_Outcome_Hierarchy Event_Outcome_Spec
    1	Progress to reablement/ST-Max
    2	Progress to assessment, review or reassessment
    3	Release 1 specification only: Not mapped
    4	Progress to support planning or services
    5	Continuation of support or services
    6	Admitted to hospital
    7	NFA: Responsibility moved to another local authority
    8	NFA: Referral to NHS services or NHS funded social care
    9	NFA: Self-funded client or under 12wk disregard
    10	NFA: Information and advice or signposting
    11	NFA: Referral to other service within the local authority
    12	NFA: Support declined
    13	NFA: Deceased
    14	NFA: Support ended as planned
    15	NFA: Support ended for other reason
    16	NFA: No services offered for other reason
    17	NFA: Other
  ```

- **Higher‑level groupings**
  <br> Derived for ethnicity, event outcome, service type and review reason ("review type"). Logic and mapping tables for higher-level grouping derivations provided below.

  `Ethnicity_Grouped`
  ```
  Ethnicity_Cleaned	Ethnicity_Grouped
  Asian or Asian British: Any other Asian background	Asian or Asian British
  Asian or Asian British: Bangladeshi	Asian or Asian British
  Asian or Asian British: Chinese	Asian or Asian British
  Asian or Asian British: Indian	Asian or Asian British
  Asian or Asian British: Pakistani	Asian or Asian British
  Black, Black British, Caribbean or African: African	Black, Black British, Caribbean or African
  Black, Black British, Caribbean or African: Any other Black, Black British or Caribbean background	Black, Black British, Caribbean or African
  Black, Black British, Caribbean or African: Caribbean	Black, Black British, Caribbean or African
  Invalid and not mapped	Unknown
  Mixed or multiple ethnic groups: Any other Mixed or multiple ethnic background	Mixed or multiple ethnic groups
  Mixed or multiple ethnic groups: White and Asian	Mixed or multiple ethnic groups
  Mixed or multiple ethnic groups: White and Black African	Mixed or multiple ethnic groups
  Mixed or multiple ethnic groups: White and Black Caribbean	Mixed or multiple ethnic groups
  No data: Refused	No data
  No data: Undeclared or not known	No data
  Other ethnic group: Any other ethnic group	Other ethnic group
  Other ethnic group: Arab	Other ethnic group
  White: Any other White background	White
  White: English, Welsh, Scottish, Northern Irish or British	White
  White: Gypsy or Irish Traveller	White
  White: Irish	White
  White: Roma	White
  ```

  `Event_Outcome_Grouped`
  ```
  Event_Outcome_Cleaned	Event_Outcome_Grouped
  Admitted to hospital	Admitted to hospital
  Continuation of support or services	Continuation of support or services
  Invalid and not mapped	Invalid and not mapped
  NFA: Deceased	NFA
  NFA: Information and advice or signposting	NFA
  NFA: No services offered for other reason	NFA
  NFA: Other	NFA
  NFA: Referral to NHS services or NHS funded social care	NFA
  NFA: Referral to other service within the local authority	NFA
  NFA: Responsibility moved to another local authority	NFA
  NFA: Self-funded client or under 12wk disregard	NFA
  NFA: Support declined	NFA
  NFA: Support ended as planned	NFA
  NFA: Support ended for other reason	NFA
  Progress to assessment, review or reassessment	Progress to assessment, review or reassessment
  Progress to reablement/ST-Max	Progress to reablement/ST-Max
  Progress to support planning or services	Progress to support planning or services
  Release 1 specification only: Not mapped	Release 1 specification only: Not mapped
  ```

  `Service_Type_Grouped`
  ```
  Service_Type_Cleaned	Service_Type_Grouped
  Invalid and not mapped	Unknown
  Long term support: Community	Long term support
  Long term support: Nursing care	Long term support
  Long term support: Prison	Long term support
  Long term support: Residential care	Long term support
  Short term support: Ongoing low level	Short term support
  Short term support: Other short term	Short term support
  Short term support: ST-Max	Short term support
  Unpaid carer support: Direct to unpaid carer	Unpaid carer support
  Unpaid carer support: Support involving the person cared-for	Unpaid carer support
  ```
  
  `Review_Type`

  Derived for review events from the cleaned review reason field and grouped as 'Planned review of long term support', 'Unplanned', 'Review of short term support', or 'Review Type Unknown'. Non-review events are assigned NULL.

<br>

[Go to Deduplication](/Main_tables/docs/methodology/4-deduplication.md)