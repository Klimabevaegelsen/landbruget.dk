# Pesticide disaggregation

This job creates estimated field-level allocations from SJI pesticide reports.
It does not identify the field or date of an observed spray event.

## Inputs and period

For pesticide year `YYYY`, the job reads the latest available copies of:

- `silver/pesticides/.../pesticiddata_YYYY_YYYY+1.parquet`
- `silver/fvm_marker_YYYY+1/.../data.parquet`

`YYYY` is the first year in the SJI agricultural reporting period. The public
export records the period explicitly as 1 August `YYYY` through 31 July
`YYYY+1`. The job deliberately uses FVM fields from `YYYY+1`; it does not
model changes in field geometry, field ID, or operator between the two years.

### Historical field-CVR provenance

The public FVM/LandbrugsGIS Marker WFS layers for the early years do not, by
themselves, provide the complete field-level CVR values required for the
CVR-and-crop match. For field years 2005, 2007, and 2008--2014, the FVM silver
job can instead complete incomplete CVRs from a separate historical
Fællesskema/IMK administrative extract. The original SAS-generated Excel files
(`MARKER_<year>.xlsx`) are retained in the
[Fields archive](https://drive.google.com/drive/folders/1RXaLAwJtAq_rJJJEgh7N3TMvnjlVE9Z0)
and are converted to `silver/fields/.../MARKER_<year>.parquet`.

The archive was received through *aktindsigt*. Its workbook metadata does not
identify a publisher or request reference, and it is not reproducible from the
public WFS/ZIP files alone. It supplies
`Cvrnr`, `MarkblokNr_c`, `Mark Nr`, crop, and area. The enrichment joins it to
FVM Marker records on mark-block number and field number, and only replaces
missing or incomplete CVRs. The archive does not cover field year 2015, so it
must not be cited as a source of a 2015 CVR match.

## Allocation rules

The job only allocates records that meet all of these conditions:

1. a numeric CVR is present in both SJI and FVM;
2. the crop code agrees after numeric normalisation; and
3. the reported treated area passes the relevant area rule.

For a normal multi-field match, the reported treated area is compared with the
total FVM area for the CVR and crop. The default tolerance is 2%. Quantity and
treated area are then split between fields in proportion to field area.

The implemented sequence is:

1. for CVR-and-crop groups containing organic fields, choose the eligible
   all-fields or non-organic-fields match with the smaller area discrepancy;
2. apply the all-fields area match to remaining records;
3. retry remaining records excluding organic fields; and
4. allocate to one matching field when the reported area is smaller than that
   field (partial-field allocation).

There is no broader-crop, geographic, cross-year, or spatial-cluster fallback
in the executed sequence. Unmatched records are not written as field
allocations. The spatial-clustering function remains in the codebase but the
job logs that it is not run.

## What the output can and cannot say

Each allocation row contains a source-row ID, `cvr_number`, field identifiers,
product and quantity fields, allocated treated area, allocation method,
confidence score, partial-field flag, and municipality. A source row may yield
several allocation rows.

For a multi-field allocation, every field receives the same period-average
reported rate. That is a modelling consequence of proportional allocation, not
evidence that each field was sprayed at the same rate or on the same date.

The public bulk export excludes CVR. Its quality resources count allocation
rows and distinct matched source rows, but do not contain an all-SJI-record
denominator, a quantity-weighted coverage metric, or an unmatched-quantity
breakdown.

## Verified production snapshot

The following values were read from the successful production matrix run on
1 July 2026 ([run 28498203785](https://github.com/Klimabevaegelsen/landbruget.dk/actions/runs/28498203785)).
`Eligible SJI rows` excludes explicit no-pesticide declarations; `allocated
source rows` is the number of distinct eligible source rows represented in the
output. The percentage is therefore a record-coverage measure for this run,
not a measure of allocated kg or litres.

| Pesticide year | Eligible SJI rows | Allocated source rows | Record coverage |
| --- | ---: | ---: | ---: |
| 2010 | 391,166 | 228,085 | 58.3% |
| 2011 | 407,366 | 262,128 | 64.3% |
| 2012 | 404,285 | 261,341 | 64.6% |
| 2013 | 422,790 | 288,331 | 68.2% |
| 2014 | 440,059 | 0 | 0.0% |
| 2015 | 423,483 | 354,201 | 83.6% |
| 2016 | 414,301 | 363,009 | 87.6% |
| 2017 | 338,845 | 295,290 | 87.1% |
| 2018 | 375,591 | 340,756 | 90.7% |
| 2019 | 347,595 | 319,132 | 91.8% |
| 2020 | 358,161 | 332,913 | 93.0% |
| 2021 | 342,303 | 315,771 | 92.2% |
| 2022 | 310,997 | 285,294 | 91.7% |
| 2023 | 313,317 | 284,042 | 90.7% |
| 2024 | 318,740 | 292,899 | 91.9% |

For 2014, the same run loaded 443,769 raw SJI rows and 741,882 FVM 2015
fields. The FVM table had zero distinct numeric CVR values, so no CVR-based
match was possible. This establishes why the current job cannot allocate 2014;
it does not establish that every conceivable reconstruction method has been
ruled out.

Run the validation again after any input or matching change. Do not treat the
table above as an invariant acceptance threshold.

## Run and validate

From `backend/pipelines/unified_pipeline/src`:

```bash
uv run python -m unified_pipeline run \
  --source pesticide_disaggregation \
  --stage gold \
  --pesticide-year 2021
```

Omit `--pesticide-year` to process every discoverable pesticide year. To check
available input and output paths from `backend`:

```bash
uv run python scripts/validate_disaggregation_robustness.py \
  --year 2021 --analysis all --verbose
```

## Validation behaviour

The job logs unmatched records and checks conservation of the dosage for
matched source rows. It warns about a conservation discrepancy above 1%; it
does not fail merely because coverage is below 92%.
