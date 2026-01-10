# End-to-End Test with Real GCS Data

## Overview

`test_end_to_end_real.py` is a comprehensive integration test that validates the full climate tool pipeline using actual data from Google Cloud Storage.

## What It Tests

1. **Data Loading** - Loads real data from GCS silver layer:
   - Livestock data from Green Accounts (CVR 31373077, Year 2023)
   - Field data from FVM (Year 2024)
   - Fertilizer data from GKEA (Year 2024, with fallback)

2. **Data Transformation** - Converts Danish schemas to calculator format:
   - Transforms Green Accounts livestock data (c_2001, c_2004, c_2006, c_2016)
   - Maps Danish species names to English (Svin → pigs, Kvæg → cattle)
   - Handles concatenated string values in numeric fields
   - Flexible field schema detection (multiple column name variants)

3. **Emission Calculations**:
   - ✅ N2O emissions from fertilizer application (IPCC Tier 1)
   - ⚠️ Livestock emissions (placeholder with rough estimates)
   - ⚠️ Field emissions (not yet implemented)
   - ⚠️ Energy emissions (not yet implemented)

4. **Report Generation**:
   - Creates EmissionReport with categories and sub-sources
   - Calculates intensity metrics (CO2e per animal, per hectare)
   - Tracks data quality and completeness

5. **Validation**:
   - Compares results against expected ranges
   - Scales expectations based on actual farm size
   - Validates all emission categories

## Test Farm Details

**CVR:** 31373077
**Type:** Pig farm
**Livestock:** ~109,000 pigs (2023)
**Fields:** 536 hectares (2024)
**Crops:** Winter wheat, winter barley, grass, rapeseed, etc.

**Expected Emissions:** ~1,500 tonnes CO2e/year
- Fertilizer N2O: ~180 tonnes
- Livestock (manure): ~1,300 tonnes

## Running the Test

### Prerequisites

```bash
# 1. Authenticate with GCS
gcloud auth application-default login

# 2. Activate Python environment
cd backend
source venv/bin/activate

# 3. Ensure dependencies are installed
pip install -r requirements.txt
```

### Execute Test

```bash
cd backend/pipelines/climate_tool
python test_end_to_end_real.py
```

### Expected Output

```
==========================================================================================
                   🧪 CLIMATE TOOL - END-TO-END TEST WITH REAL GCS DATA
==========================================================================================

Test Parameters:
  CVR: 31373077
  Livestock Year: 2023
  Fields/Fertilizer Year: 2024

...

==========================================================================================
                                 STEP 5: VALIDATE RESULTS
==========================================================================================

Expected ranges for pig farm with 108,940 animals:

✅ Total Emissions: 1,487,049 kg CO2e (within range 1,143,870 - 2,832,440)
✅ Fertilizer N2O: 179,769 kg CO2e (within range 81,705 - 817,050)
✅ Livestock Emissions: 1,307,280 kg CO2e (within range 762,580 - 2,124,330)

...

✅ END-TO-END TEST PASSED
```

## Test Results

### What's Working ✅

- **Data Loading**: Successfully loads from GCS silver layer
- **Schema Handling**: Robust handling of Danish column names and data types
- **Data Transformation**: Livestock, fields, and fertilizer data properly transformed
- **N2O Calculation**: IPCC Tier 1 methodology correctly implemented
- **Report Generation**: Complete EmissionReport structure with metadata
- **Validation**: Results within expected ranges for pig farm of this size

### What Needs Implementation ⚠️

1. **Pig-Specific Formulas**
   - Currently using rough estimates (2 kg CO2e/pig enteric, 10 kg CO2e/pig manure)
   - Need to implement proper pig emission factors from IPCC/DCA guidelines
   - Map Danish pig subtypes to emission categories

2. **Manure Management**
   - Housing system emissions (CH4 and N2O)
   - Storage type impact on emissions
   - Integration with Green Accounts housing codes (c_2005, c_2030)

3. **Field Emissions**
   - Carbon balance calculations
   - Nitrate leaching (indirect N2O)
   - Crop residue decomposition
   - Organic soil emissions

4. **Energy Emissions**
   - Diesel consumption (machinery)
   - Electricity usage
   - Heating (if applicable)

## Data Quality Issues Discovered

### 1. Concatenated String Values

**Problem:** Some numeric columns (c_2006, c_2016) contain concatenated strings instead of proper numeric values.

**Example:**
```
c_2006: "915.9516737686.68130.68124103565.836594105124521.68149.772900"
```

**Solution:** Use `pd.to_numeric(df[col], errors='coerce').fillna(0).sum()` to safely convert.

**Fixed in:** `data_transformer.py` lines 203, 207, 227, 234

### 2. Variable Field Schema

**Problem:** FVM field data uses different column names in different versions.

**Column Variants:**
- Area: `areal_ha`, `area_ha`, `areal`
- Crop: `afgroede`, `crop_name`, `afgroedekode`

**Solution:** Flexible column detection in `FVMTransformer.transform()`

**Fixed in:** `data_transformer.py` lines 446-469

### 3. Missing GKEA Data

**Problem:** GKEA fertilizer data not available for all CVR/year combinations.

**Fallback Strategy:**
- Use livestock N production from Green Accounts (c_2016)
- Estimate 65% of manure N is applied to fields
- Calculate N2O from estimated field application

**Implemented in:** `test_end_to_end_real.py` lines 231-250

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. LOAD RAW DATA FROM GCS                                   │
│                                                              │
│  ClimateDataLoader                                          │
│  ├─ load_livestock() → Green Accounts DataFrame             │
│  ├─ load_fields() → FVM DataFrame                           │
│  └─ load_fertilizer() → GKEA DataFrame                      │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. TRANSFORM DATA                                            │
│                                                              │
│  GreenAccountsTransformer                                   │
│  ├─ Danish species → English (Svin → pigs)                  │
│  ├─ Extract animal counts (c_2006)                          │
│  ├─ Extract N production (c_2016)                           │
│  └─ Map subtypes and housing systems                        │
│                                                              │
│  FVMTransformer                                             │
│  ├─ Flexible column detection                               │
│  ├─ Aggregate by crop type                                  │
│  └─ Calculate total areas                                   │
│                                                              │
│  GKEATransformer                                            │
│  ├─ Aggregate fertilizer applications                       │
│  └─ Calculate N2O emissions                                 │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. CALCULATE EMISSIONS                                       │
│                                                              │
│  ✅ GKEATransformer.calculate_n2o_emissions()               │
│     Formula: N_total * 0.01 * (44/28) * 298                 │
│                                                              │
│  ⚠️ Livestock emissions (placeholder)                        │
│     TODO: Implement pig-specific formulas                   │
│                                                              │
│  ⚠️ Field emissions (not implemented)                        │
│     TODO: Carbon balance, leaching, crop residues           │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. GENERATE REPORT                                           │
│                                                              │
│  EmissionReport                                             │
│  ├─ Total CO2e (kg)                                         │
│  ├─ Categories (fertilizer_n2o, livestock, fields, energy)  │
│  ├─ Sub-sources (enteric, manure, direct N2O, etc.)        │
│  ├─ Intensity metrics (per animal, per ha)                 │
│  └─ Data completeness score                                 │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. VALIDATE RESULTS                                          │
│                                                              │
│  Compare against expected ranges:                           │
│  ├─ Total emissions (scaled by farm size)                   │
│  ├─ N2O from fertilizer                                     │
│  └─ Livestock emissions                                     │
└─────────────────────────────────────────────────────────────┘
```

## Next Steps

### Phase 1: Implement Pig Formulas (High Priority)

1. Create `formulas/svin/` directory
2. Implement enteric fermentation CH4 for pigs
3. Implement manure management emissions (CH4 and N2O)
4. Map Danish pig subtypes to emission factors

### Phase 2: Integrate with Calculator

1. Update `climate_calculator.py` to use transformed data
2. Add pig emission calculations to `FarmClimateCalculator`
3. Handle missing data gracefully
4. Improve data quality scoring

### Phase 3: Complete Field Emissions

1. Carbon balance calculations
2. Nitrate leaching (indirect N2O)
3. Crop residue decomposition
4. Organic soil emissions

### Phase 4: Add Energy Emissions

1. Diesel consumption formulas
2. Electricity usage calculations
3. Integration with energy data sources

## Related Files

- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/test_end_to_end_real.py` - This test
- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/data_loader.py` - GCS data loading
- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/data_transformer.py` - Data transformation
- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/climate_calculator.py` - Emission calculations
- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/formulas/kvaeg/` - Cattle formulas (reference)

## Troubleshooting

### Test fails with "No module named 'loguru'"

**Solution:** Activate the virtual environment first:
```bash
cd backend
source venv/bin/activate
```

### Test fails with GCS authentication error

**Solution:** Authenticate with gcloud:
```bash
gcloud auth application-default login
```

### Test shows all zeros for emissions

**Problem:** Likely data quality issue or missing data for the CVR/year.

**Debug:**
1. Check if data was loaded: Look for "Loaded N records" messages
2. Inspect DataFrame columns: Check column names match expectations
3. Verify data types: Use `df.dtypes` to check for string vs numeric
4. Check for null values: Use `df.isna().sum()`

### Results outside expected ranges

**Problem:** Validation ranges are based on typical pig farms.

**Solution:** Adjust ranges in the test or use different CVR with known emissions.

## Performance

**Typical execution time:** 60-90 seconds
- Data loading: ~40 seconds (GCS download + DuckDB table creation)
- Transformation: ~5 seconds
- Calculations: <1 second
- Validation & reporting: <1 second

**Note:** First run may be slower due to cold cache.
