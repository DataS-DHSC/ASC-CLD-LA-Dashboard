# Enhancing date of death using ONS mortality data

> Processing applied in [`GetONSDeaths`](/Main_tables/stored_procedures/create_GetONSDeaths_procedure.sql) procedure.

## Method

Date of death information in submissions is known to be incomplete and, in some cases, inaccurate. ONS mortality data is used to derive an enhanced date of death for use throughout the main tables pipeline.

For each record, the NHS number used to link to ONS mortality data is:

1. traced NHS number, where present;
2. else LA-submitted NHS number.

An enhanced date of death is then derived as follows:

| | `Der_Date_of_Death` |
| --- | --- |
| NHS number present | ONS date of death |
| No NHS number present | CLD date of death |

N.B. Where an NHS number is present but there is no linked ONS death, `Der_Date_of_Death` is `NULL`. The CLD date of death is not used as a fallback.

The enhanced date is derived early in the pipeline and used in place of the CLD date of death in subsequent processing, including filtering events and deriving corrected event end dates.

## Selecting an ONS mortality record

Where there are multiple ONS mortality records for an NHS number, one record is selected using the following criteria:

| Data field | Sort order |
| --- | --- |
| 1. Death registration date | Descending, latest first |
| 2. Date of death | Descending, latest first |

The latest registration is prioritised because later registrations may reflect corrections. Where multiple dates of death have the same registration date, the latest date of death is selected so that an incorrect value is less likely to crop events prematurely.

Only the date of death and registration date are required from the mortality dataset. If further mortality fields are added in future, the deduplication method should be reviewed.

One pseudonymised NHS number associated with an unusually large number of death registrations is excluded, as it is considered likely to be invalid.

## Output fields

- `Date_of_Death_Raw`: date of death submitted in the CLD record.
- `Date_of_Death_ONS`: selected ONS date of death, where linked.
- `Death_Reg_Date`: registration date for the selected ONS mortality record.
- `Der_Date_of_Death`: enhanced date of death used in subsequent processing.

The registration date and a snapshot of the ONS mortality data is retained for auditability because the ONS mortality dataset is updated regularly.

## Notes

- The procedure can use a saved snapshot of ONS mortality records to support reproducibility.
- Input and output row counts are compared to identify join or deduplication issues.
- ONS mortality data can only be linked where a traced or LA-submitted NHS number is present.
- Local authorities cannot fully reproduce the method without access to the ONS mortality dataset.
- Deaths outside the coverage of the ONS mortality data (e.g. abroad) may not be identified.

<br>

[Back to Data Cleaning](/Main_tables/docs/methodology/3-data-cleaning.md)