# Pesticide Data Analysis Project

## Project Goal
Disaggregate pesticide application data from company (CVR) level to field level by identifying the most reliable field-level dataset for each company.

## Data Sources
All data files are in Parquet format and located in the project directory:

1. Pesticide Data (`pesticiddata_2021_2022.parquet`)
   - Contains pesticide application records at company level
   - Key fields: CompanyRegistrationNumber, AcreageSize, PesticideName, PesticideRegistrationNumber
   - Total Records: 344,948

2. Marker Data (`marker_marker_2022.parquet`)
   - Contains field-level agricultural data
   - Key fields: CVR, Journalnr, IMK_areal, GBanmeldt
   - Total Records: 576,751

3. Jordbrugsanalyser Data (`jordbrugsanalyser_marker22.parquet`)
   - Contains agricultural analysis data at field level
   - Key fields: EjerNr, Ha (area)
   - Total Records: 576,544

4. Økologiske Arealer Data (`oekologiske_arealer_2022.parquet`)
   - Contains organic farming area data
   - Key fields: AutNR_Iden, FSjournal, Marknr
   - Total Records: 82,036

5. GKEA Data (`GKEA2022_Markplan_med_Gødningsoplysninger.parquet`)
   - Contains agricultural area data
   - Key fields: CVR, Areal, Harmoni Areal, Areal til rådighed for EA
   - Total Records: 559,970

## Table Descriptions

### MARKER Table
- Total Records: 576,751
- Key Fields:
  - id: Unique identifier
  - CVR: Company registration number
  - Journalnr: Journal number
  - Markblok: Field block identifier
  - Marknr: Field number
  - Afgkode: Crop code
  - Afgroede: Crop name
  - IMK_areal: Area measurement (IMK)
  - GBanmeldt: Reported pesticide application area
  - GB: Pesticide application indicator
  - geometry: Field geometry

### JORDBRUGSANALYSER Table
- Total Records: 576,544
- Key Fields:
  - id: Unique identifier
  - EjerNr: Owner number
  - MarkBlok: Field block identifier
  - MarkNr: Field number
  - AfgNr: Crop code
  - AfgKat: Crop category
  - AfgNavn: Crop name
  - Ha: Area measurement (Hectares)
  - HaIalt: Total area for the owner/crop
  - X, Y: Coordinates
  - geometry: Field geometry

### OEKOLOGISKE_AREALER Table
- Total Records: 82,036
- Key Fields:
  - id: Unique identifier
  - Marknr: Field number
  - AutNR_Iden: Authorization number identifier
  - Omlaegning: Conversion status
  - Afmeldings: De-registration date
  - FSjournal: Journal number (FS)
  - OML: Organic farming scheme
  - geometry: Field geometry

### PESTICIDE Table
- Total Records: 344,948
- Key Fields:
  - CompanyRegistrationNumber: CVR number
  - CompanyName: Company name
  - StreetName: Company address street
  - StreetBuildingIdentifier: Building identifier
  - FloorIdentifier: Floor identifier
  - PostCodeIdentifier: Postal code
  - City: City name
  - AcreageSize: Area size reported by company
  - AcreageUnit: Unit for AcreageSize
  - Name: Crop name (likely, associated with Code)
  - Code: Crop code
  - PesticideName: Name of the pesticide
  - PesticideRegistrationNumber: Registration number of the pesticide
  - DosageQuantity: Quantity of dosage
  - DosageUnit: Unit for DosageQuantity
  - NoPesticides: Indicator if no pesticides were used

### GKEA Table
- Total Records: 559,970
- Key Fields:
  - Journal Nummer: Journal number
  - CVR: Company registration number
  - Modtaget Dato: Received date
  - Marknummer: Field number
  - Areal: Area measurement
  - Fradrags Arealer: Deductible areas
  - Øvrige Fradrags Arealer: Other deductible areas
  - Harmoni Areal Indikator: Harmonized area indicator
  - Harmoni Areal: Harmonized area
  - Jordbundstype: Soil type
  - Jordbundstype Ændret: Soil type changed indicator
  - Vanding Indikator: Irrigation indicator
  - Hovedafgrøde: Main crop
  - Forfrugt: Previous crop
  - Udlæg: Catch crop/undersown crop
  - Øvrige Fradrag Uden EA: Other deductions without EA
  - Areal til rådighed for EA: Area available for Ecological Focus Area
  - Fosfortal: Phosphorus number
  - N Fradrag Forfrugt: Nitrogen deduction for previous crop
  - N Norm Afgrøde: Nitrogen norm for crop
  - N Norm Udlæg: Nitrogen norm for catch crop
  - N Korrektion: Nitrogen correction
  - Korrektion N Prognose: Nitrogen prognosis correction
  - N Kvote pr. Ha: Nitrogen quota per hectare
  - N Kvote Mark: Nitrogen quota for the field
- **Note on GKEA Geometry**: The `GKEA2022_Markplan_med_Gødningsoplysninger.parquet` file, as currently processed, does not contain a readily usable geometry column (e.g., a column named 'geometry' of a recognized spatial type like GEOMETRY or a WKB/WKT string). This currently prevents its direct use in spatial joins without further investigation or data transformation.

## Current Analysis Status

### Step 1: CVR Matching Analysis (Completed)
- Total unique pesticide CVRs: 19,094
- Unmatched pesticide CVRs (before disaggregation strategies): 875 (4.59%)
- Unique non-numeric CVRs in GKEA (excluded from matching): 682
- Area not covered by initially unmatched CVRs: 41,918.05 ha

#### Additional Notes:
- There are 22,102 records in the marker dataset with empty or null CVR values. This may impact matching and field-level disaggregation.
- An attempt was made to match CVRs using journal numbers between marker and GKEA, but this did not yield additional matches for records with missing CVRs.

### Step 2: Field Dataset Comparison (In Progress)
1. Marker vs Jordbrugsanalyser Comparison:
   - Field identification match rate: 99.98%
   - Crop code match rate: 99.63%
   - Area measurements:
     - IMK_areal vs Ha: 0.03% total difference (Note: script output `Average area difference: 95.98%` seems to refer to a different calculation, the detailed stats are more reliable here)
     - GBanmeldt vs Ha: 3.82% total difference
   - Detailed area statistics:
     - Records with IMK_areal vs Ha differences (>0.01 ha): 14,844 records (2.52%) (Note: script output `Records with differences: 79491` likely refers to `IMK_areal vs Ha` based on `FieldDatasetAnalyzer` class, but the existing breakdown is more granular. Keeping existing granular data if no direct mapping, or clarifying which 'differences' 79491 refers to.)
     - Records with GBanmeldt vs Ha differences (>0.01 ha): 75,258 records (12.77%)
     - Total absolute difference between IMK_areal and Ha: 34,741.12 ha (1.31% of total area)
     - Total absolute difference between GBanmeldt and Ha: 129,613.14 ha (4.88% of total area)
   - Field identification match rate (from script log): 99.98%
   - Records with differences (from script log, likely IMK_areal vs Ha): 79,491

### Step 3: Iterative Pesticide Disaggregation (In Progress)
- **Total Pesticide Records:** 344,948
- **Strategy 1: Marker (PesticideRowArea vs TotalFieldArea)**
    - Pesticide rows disaggregated: 316,175
    - Allocation method: `Marker_ApplicationAreaToTotalFieldArea_FieldProportional`
- **Strategy 2: GKEA (PesticideRowArea vs TotalFieldArea)**
    - Pesticide rows disaggregated (from remaining): 6,432
    - Allocation method: `GKEA_ApplicationAreaToTotalFieldArea_FieldProportional`
- **Strategy 3: Journal Number Based Matching (if feasible):**
    - [ ] Investigate if `Journalnr` can reliably link pesticide data (or CVRs) to specific `marker` fields for remaining pending rows.
    - [ ] Implement disaggregation if a reliable link is found.
- **Strategy 4: Marker Match (Non-Organic Fields - Spatial Approach):**
    - [X] Identify organic fields in `marker` by spatial intersection with `oekologiske_arealer` (89,386 marker fields found to intersect).
    - [X] For pending pesticide rows, attempt to match `AcreageSize` to the sum of *non-organic* `marker` field areas for the CVR/Crop.
    - [X] If a match is found (within tolerance), allocate pesticide application proportionally to these non-organic `marker` fields.
    - Pesticide rows disaggregated by this strategy: 67
    - Allocation method: `Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional`
- **Strategy 5: Subset Sum Match (Marker and GKEA Fields):**
    - [X] For pending pesticide rows, attempt to match `AcreageSize` to the sum of areas of a *subset* of available fields for the CVR/Crop.
    - [X] The strategy will check Marker fields first (all for the CVR/Crop), then GKEA fields.
    - [X] Use `_find_area_subsets_static` and `_get_closest_subset_static` helpers.
    - [X] **Optimization Note:** Implement pre-check: only run if pesticide `AcreageSize` < total area of available fields for the CVR/Crop/Source.
    - Pesticide rows disaggregated by this strategy: 3,446
    - Allocation method: `Marker_SubsetSum_Proportional` or `GKEA_SubsetSum_Proportional` (actual methods: `SubsetSum_Marker_FieldProportional`, `SubsetSum_GKEA_FieldProportional`)
- **Strategy 6: Geospatial Matching (Future):**
    - [ ] Explore using spatial relationships if pesticide locations can be determined.
- **Overall Disaggregation:**
    - **Note on Area Calculation Methods:** This report uses two main methods for pesticide area:
        - *Direct Sum of `AcreageSize`*: Sums all reported application areas. May include multiple treatments of the same area.
        - *Sum of MAX(`AcreageSize`) per CVR/Crop*: Takes the maximum reported area for each unique company/crop combination. This is a more conservative estimate of unique agricultural area.
    - Total pesticide rows disaggregated: 326,120 (by strategies: Marker: 316,175; GKEA: 6,432; Marker Non-Organic: 67; Subset Sum: 3,446)
    - Percentage of total rows disaggregated: ~94.54% (326,120 / 344,948)
    - **Original Total Pesticide Area:**
        - Direct Sum of `AcreageSize`: ~15,086,493.41 ha
        - Sum of MAX(`AcreageSize`) per CVR/Crop (unique area estimate): ~1,864,990.90 ha
    - **Disaggregated Area (based on Sum of MAX per CVR/Crop methodology):**
        - Total unique area disaggregated: ~1,758,288.37 ha
        - Percentage of unique area disaggregated: ~94.28% (1,758,288.37 / 1,864,990.90)
    - **Disaggregated Area (based on Direct Sum of `AcreageSize` methodology):**
        - Total area disaggregated (direct sum basis): ~14,414,123.51 ha
        - Percentage of area disaggregated (direct sum basis): ~95.54% (14,414,123.51 / 15,086,493.41)
    - Remaining pending pesticide rows: 18,828
    - Unallocated Area (Sum of MAX(`AcreageSize`) per CVR/Crop in pending rows): ~106,702.53 ha

## Key Findings

1. **CVR Coverage & Disaggregation**:
   - Initial unmatched pesticide CVRs (before disaggregation strategies): 875 (4.59% of unique CVRs).
   - Current disaggregation strategies (Marker, GKEA, Marker Non-Organic, Subset Sum) have successfully allocated:
     - ~94.54% of pesticide application rows (326,120 out of 344,948).
     - ~94.28% of the unique pesticide application area (estimated as ~1,758,288.37 ha out of ~1,864,990.90 ha, using the Sum of MAX(`AcreageSize`) per CVR/Crop method).
     - For comparison, using a direct sum of `AcreageSize`, ~95.54% of area is disaggregated (~14,414,123.51 ha out of ~15,086,493.41 ha).
   - Remaining pending pesticide rows: 18,828 (representing ~5.46% of rows).
   - Remaining unallocated unique area (Sum of MAX(`AcreageSize`) per CVR/Crop for pending rows): ~106,702.53 ha (representing ~5.72% of the original unique area).

2. **Area Measurements**:
   - IMK_areal in marker matches very closely with Ha in jordbrugsanalyser (0.03% total difference)
   - GBanmeldt is consistently lower than both IMK_areal and Ha (3.82% total difference)
   - While more records show differences in GBanmeldt (12.77% of records), the total area difference remains relatively small (4.88%)

3. **GKEA Matching Quality**:
   - High quality matches with very small area differences
   - All matches use the 'Areal' field, suggesting it's the most reliable
   - Multiple matches for some CVRs provide additional confidence in the matches
   - Geographic distribution shows matches across different regions

4. **Data Consistency**:
   - High consistency between marker and jordbrugsanalyser datasets
   - Field identification matches at 99.98%
   - Crop code matches at 99.63%
   - Area measurements show minimal differences
   - Records with differences (IMK_areal vs Ha, from script log): 79,491

5. **Dataset Matching Strategy**:
   - Marker and Jordbrugsanalyser datasets have almost exact mark and markblok combinations
   - Full join between these datasets is necessary to properly compare area and crop differences
   - Some pesticide CVRs remain unmatched due to empty CVR values in marker dataset
   - Alternative matching strategies to explore:
     - Field area-based matching
     - Crop-based matching
     - Journal number-based matching (already attempted but needs refinement)

## Next Steps

### 1. Field Dataset Quality Assessment
- [ ] Complete geospatial field overlap analysis
- [ ] Validate journal number matching across datasets
- [ ] Analyze area measurement consistency (re-evaluate the 95.98% average area diff calculation) - *Note: This specific average area difference was previously identified as skewed and not a primary metric.*
- [ ] Document dataset-specific advantages and limitations

### 2. Iterative Pesticide Disaggregation - Strategy Development
- [X] **Define `disaggregated_pesticide_applications` Table Schema:** Columns for original pesticide data, matched field info, allocated area, allocation method, confidence, etc.
- [X] **Establish `pending_pesticide_rows` Tracking:** Implemented logic to maintain a set of pesticide rows awaiting disaggregation.
- [X] **Strategy 1: Marker Match (Individual Application Area vs. Total Field Area for CVR/Crop):**
    - [X] Match individual pesticide row's `AcreageSize` to the total summed Marker field area for that CVR & Crop (within tolerance).
    - [X] Allocate pesticide application proportionally to individual Marker fields for the matched CVR/Crop.
    - Pesticide rows disaggregated: 316,175
- [X] **Strategy 2: GKEA Match (Individual Application Area vs. Total Field Area for CVR/Crop):**
    - [X] Match individual pesticide row's `AcreageSize` (from remaining pending rows) to the total summed GKEA field area for that CVR & Crop (within tolerance).
    - [X] Allocate pesticide application proportionally to individual GKEA fields for the matched CVR/Crop.
    - Pesticide rows disaggregated: 6,432
- [ ] **Strategy 3: Journal Number Based Matching (if feasible):**
    - [ ] Investigate if `Journalnr` can reliably link pesticide data (or CVRs) to specific `marker` fields for remaining pending rows.
    - [X] Identify organic fields in `marker` by spatial intersection with `oekologiske_arealer` (89,386 marker fields found to intersect).
    - [X] For pending pesticide rows, attempt to match `AcreageSize` to the sum of *non-organic* `marker` field areas for the CVR/Crop.
    - [X] If a match is found (within tolerance), allocate pesticide application proportionally to these non-organic `marker` fields.
    - Pesticide rows disaggregated by this strategy: 67
    - Allocation method: `Marker_NonOrganic_ApplicationAreaToTotalFieldArea_FieldProportional`
- [X] **Strategy 5: Subset Sum Match (Marker and GKEA Fields):**
    - [X] For pending pesticide rows, attempt to match `AcreageSize` to the sum of areas of a *subset* of available fields for the CVR/Crop.
    - [X] The strategy will check Marker fields first (all for the CVR/Crop), then GKEA fields.
    - [X] Use `_find_area_subsets_static` and `_get_closest_subset_static` helpers.
    - [X] **Optimization Note:** Implement pre-check: only run if pesticide `AcreageSize` < total area of available fields for the CVR/Crop/Source.
    - Pesticide rows disaggregated by this strategy: 3,446
    - Allocation method: `SubsetSum_Marker_FieldProportional`, `SubsetSum_GKEA_FieldProportional`
- [ ] **Strategy 6: Geospatial Matching (Future):**
    - [ ] Explore using spatial relationships if pesticide locations can be determined.
- [X] **Develop Confidence Scoring:** Basic confidence scoring based on area difference implemented.
- [ ] **Validate Results:** Validate disaggregated data against known cases or through expert review.

### 3. Iterative Pesticide Disaggregation - Implementation in `pesticide_analyzer`
- [X] Add `OriginalPesticideRowID` to `pesticide` table on load.
- [X] **Initialize Core Tables:**
    - [X] Create `disaggregated_pesticide_applications` table in DuckDB.
    - [X] Create initial `pending_pesticide_rows` table/view from all pesticide data.
- [X] **Implement Disaggregation Loop:**
    - [X] Sequentially apply implemented strategies (Marker, GKEA, Marker Non-Organic, Subset Sum) to `pending_pesticide_rows`.
    - [X] For each strategy:
        - [X] Insert successfully disaggregated rows into `disaggregated_pesticide_applications`.
        - [X] Update/remove processed rows from `pending_pesticide_rows`.
- [X] **Output Results:**
    - [X] Save the final `disaggregated_pesticide_applications` table to a Parquet file.
    - [X] Save the remaining (unallocated) `pending_pesticide_rows` to a Parquet file.
- [X] **Reporting:**
    - [X] Generate summary statistics on the number of rows disaggregated by each strategy and overall coverage (via logging).
    - [X] Generate summary statistics on the total area disaggregated by each strategy and overall area coverage (including Direct Sum and Sum(Max) per CVR/Crop methods, via logging).
    - [ ] Document the final methodology, including assumptions and limitations for each step in this markdown.

### 4. Analysis of Remaining Pending Rows (Completed Initial Investigation)
The root causes for the 18,828 pending pesticide rows (Total Direct Sum Area: ~672,369.90 ha; Total Sum(Max) Area: ~106,702.53 ha) have been investigated. The breakdown is as follows:

- **1. Unmatched CVRs:** CVRs in pesticide data with no corresponding CVR in `marker` or `GKEA`.
    - Row Count (distinct OriginalPesticideRowID): 1,932
    - Direct Sum Area (of these rows): ~41,914.55 ha
    - Sum(Max) Area (for these CVR/Crop groups): ~12,962.38 ha
    - Details saved to `debug_unmatched_cvr_details.csv`.

- **2. Unmatched CVR/Crop Combinations:** CVR is matched, but the specific CVR/Crop combination from the pesticide row is not found in `marker` or `GKEA` for that CVR.
    - Row Count (distinct OriginalPesticideRowID): 2,244
    - Direct Sum Area (of these rows): ~51,427.26 ha
    - Sum(Max) Area (for these CVR/Crop groups): ~19,740.70 ha
    - Details saved to `debug_unmatched_cvr_crop_details.csv`.

- **3. Pesticide Area Exceeds Marker Area:** For CVR/Crop combinations present in both pesticide and `marker` data, but the Max `AcreageSize` (from pending pesticide rows for that CVR/Crop) is greater than the total corresponding `marker` field area.
    - Row Count (distinct OriginalPesticideRowID belonging to such CVR/Crop groups): 7,181
    - Direct Sum Area (of these 7,181 rows): ~413,663.27 ha
    - Sum(Max) Area (for these CVR/Crop groups where MaxPesticideArea > TotalMarkerArea): ~49,915.24 ha
    - Detailed row-by-row analysis for *individual* pesticide rows where `AcreageSize > TotalMarkerArea` (including all original pesticide columns, total marker/GKEA areas, and differences) is saved to `debug_acreage_gt_marker_details.csv`.

- **4. Pesticide Area Exceeds GKEA Area:** For CVR/Crop combinations present in both pesticide and `GKEA` data, but the Max `AcreageSize` (from pending pesticide rows for that CVR/Crop) is greater than the total corresponding `GKEA` field area.
    - Row Count (distinct OriginalPesticideRowID belonging to such CVR/Crop groups): 7,357
    - Direct Sum Area (of these 7,357 rows): ~418,751.96 ha
    - Sum(Max) Area (for these CVR/Crop groups where MaxPesticideArea > TotalGKEAArea): ~50,668.75 ha
    - (GKEA area comparisons are also included in `debug_acreage_gt_marker_details.csv` where applicable).

**Note on Area Categories:** The Sum(Max) area for categories 3 and 4 specifically represents the sum of maximum pesticide application areas for CVR/Crop combinations where this maximum *itself* exceeds the corresponding total field area in Marker or GKEA. The row counts and direct sum areas in these categories refer to all pending pesticide rows belonging to these identified CVR/Crop groups.

## Technical Notes
- Using DuckDB and DuckDB-Spatial for all data processing
- All analysis code is in the `pesticide_analyzer` directory, with `main.py` as the entry point.
- Intermediate results (debug CSVs) are saved in `pesticide_analyzer/outputs/`.
- Current focus is on improving disaggregation strategies and coverage.

## Known Issues
1. Some CVRs have non-numeric formats in GKEA (handled by filtering).
2. Area measurements show small but consistent differences between datasets (Note: the script reported 95.98% "Average area difference" for Marker vs Jordbrugsanalyser, which needs clarification. The detailed % differences for IMK_areal vs Ha and GBanmeldt vs Ha are more specific).
3. Field identification has minor discrepancies between Marker and Jordbrugsanalyser
4. Some fields in the marker dataset have no CVR (22,102 records with empty/null CVR)
5. Some pesticide CVRs remain unmatched after all matching attempts (currently 18,828 rows pending disaggregation).

## Questions to Resolve
1. Which dataset provides the most reliable field boundaries?
2. How should we handle cases where field areas differ between datasets?
3. What confidence level should we assign to each type of disaggregation?
4. How should we handle organic fields in the analysis?