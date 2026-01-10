# Real GCS Data Analysis Results

**Test Date:** 2026-01-10
**Test CVR:** 31373077
**Test Year:** 2023

## Summary

Successfully loaded real data from GCS and analyzed actual schemas. The integration test confirms:
- ✅ **GCS authentication** works (using gcloud token)
- ✅ **Livestock data** loads correctly (11 records)
- ✅ **Field data** loads correctly (53 records)
- ⚠️ **Fertilizer data** not available for 2023

---

## 1. Livestock Data (Green Accounts)

**Source:** `gs://landbrugsdata-raw-data/silver/gr 2023/20260110_192737/V_4061GR_23_ISKV1_B_DYRERK_6B_1_pii_handled.parquet`

**Shape:** 11 rows × 45 columns

### Key Columns Identified

| Column | Description | Example Value |
|--------|-------------|---------------|
| `cvr_number` | Company CVR | 31373077 |
| `c_2001` | **Animal species** | Svin (Pigs), Kvæg (Cattle) |
| `c_2002` | Species code | 21454 |
| `c_2004` | **Animal category description** | "Antal producerede smågrise, fra 6,7 til 31 kg" |
| `c_2005` | **Housing system** | "Toklimastald, delvis spaltegulv" |
| `c_2006` | **Number of animals** | 21600 |
| `c_2008` | Animal weight start (kg) | 6.7 |
| `c_2009` | Animal weight end (kg) | 10 |
| `c_2015_1` | Manure type | Svinegylle (Pig slurry) |
| `c_2016` | **Total N production (tons)** | 915.95 |
| `c_2018` | N factor | 0.1113 |
| `c_2021` | N volatilization (kg) | 283.22 |
| `c_2029` | Emission factor group | 1511 |
| `c_2030` | Emission factor code | 151101 |

### Species Distribution

- **Svin (Pigs):** 11 categories
- **Kvæg (Cattle):** Not present for this CVR

### Data Completeness

✅ All required Green Accounts columns present:
- `c_2001` - Species
- `c_2004` - Animal category
- `c_2006` - Number of animals
- `c_2016` - Total N production

---

## 2. Field Data (FVM)

**Source:** `gs://landbrugsdata-raw-data/silver/fvm_marker_2023/20260110_221929/data.parquet`

**Shape:** 53 rows × 16 columns

### Key Columns Identified

| Column | Description | Example Value |
|--------|-------------|---------------|
| `field_id` | Field identifier | 103-1 |
| `area_ha` | **Field area in hectares** | 2.31 |
| `cvr_number` | Company CVR | 31373077 |
| `crop_code` | Crop type code | 22 |
| `crop_name` | **Crop name** | Vinterraps (Winter rape) |
| `grundbetaling_area_ha` | Subsidy eligible area | 2.31 |
| `journal_number` | Journal number | 23-0093232 |
| `block_id` | Block identifier | 551204-72 |
| `geometry` | **Field boundary geometry** | Binary WKB |
| `year` | Agricultural year | 2023 |
| `municipality` | Municipality | None |

### Data Completeness

✅ All required field columns present for N2O calculations:
- `area_ha` - Field area
- `crop_name` - Crop type
- `geometry` - Field boundaries

### Crop Types Present

The CVR has 53 fields with various crops including:
- Vinterraps (Winter rape)
- (Other crops in dataset)

---

## 3. Fertilizer Data (GKEA)

**Source:** Not found

**Status:** ⚠️ No GKEA fertilizer data available for year 2023

### Expected Path

- `gs://landbrugsdata-raw-data/silver/fertiliser/GKEA2023_*.parquet`

### Recommendation

- Check if fertilizer data is in a different location
- Verify if 2023 data exists at all
- Consider using 2024 data instead
- Or extract N data from Green Accounts (`c_2016` column)

---

## 4. Schema Mapping Requirements

### Current State vs. Expected

| Data Source | Current Schema | Calculator Expected | Status |
|-------------|----------------|---------------------|--------|
| **Livestock** | DataFrame with c_2001, c_2006, c_2016 | `livestock_data['cattle']` dict | ❌ Mismatch |
| **Fields** | DataFrame with area_ha, crop_name | DataFrame | ✅ Compatible |
| **Fertilizer** | Missing | DataFrame with total_n_kvote | ❌ Missing |

### Required Transformations

#### 1. Livestock Data Transformation

**From Green Accounts format:**
```python
{
    'c_2001': 'Svin',
    'c_2004': 'Antal producerede smågrise...',
    'c_2005': 'Toklimastald, delvis spaltegulv',
    'c_2006': 21600,
    'c_2016': 915.95
}
```

**To calculator format:**
```python
{
    'pigs': [
        {
            'category': 'weaner_pigs',
            'count': 21600,
            'housing_system': 'two_climate_barn_partial_slatted',
            'n_production_tons': 915.95
        }
    ]
}
```

#### 2. Species Code Mapping

| Danish Name | English | Calculator Key |
|-------------|---------|----------------|
| Svin | Pigs | `pigs` |
| Kvæg | Cattle | `cattle` |
| Fjerkræ | Poultry | `poultry` |
| Får | Sheep | `sheep` |
| Geder | Goats | `goats` |

#### 3. Housing System Mapping

| Danish (c_2005) | English | Emission Factor |
|-----------------|---------|-----------------|
| Toklimastald, delvis spaltegulv | Two-climate barn, partial slatted | TBD |
| Sengestald med spalter | Bedded barn with slats | TBD |
| Udendørs | Outdoor | TBD |

---

## 5. Next Steps

### Immediate Actions

1. **Create Data Transformer** (`data_transformer.py`)
   - Transform Green Accounts → calculator format
   - Map species codes (Svin → pigs)
   - Map housing systems
   - Extract N production values

2. **Update Climate Calculator**
   - Accept DataFrames directly
   - Handle missing fertilizer data
   - Use N from Green Accounts as fallback

3. **Create Emission Factor Mapping**
   - Map housing system codes to emission factors
   - Document source references (IPCC Tier 2)
   - Validate against Danish standards

### Data Quality Checks

- [ ] Verify c_2016 (N production) matches expected ranges
- [ ] Validate housing system codes are complete
- [ ] Check if animal counts (c_2006) are realistic
- [ ] Cross-reference field areas with CHR data

### Testing Strategy

1. **Unit Tests** - Test each transformation function
2. **Integration Tests** - Test complete pipeline with real CVR
3. **Validation Tests** - Compare results against known values
4. **Edge Cases** - Test missing data, multiple species, etc.

---

## 6. File References

### Created Files

- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/data_loader.py` - Loads GCS data
- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/climate_tool/test_integration_real_data.py` - Integration test

### Modified Files

- `/Users/martincollignon/conductor/landbruget.dk/.conductor/davis-v2/backend/pipelines/unified_pipeline/src/unified_pipeline/util/gcs_access.py` - Added gcloud token authentication

### Next Files to Create

- `data_transformer.py` - Transform GCS data → calculator format
- `emission_factor_mapping.py` - Housing system → emission factors
- `test_data_transformer.py` - Test transformations

---

## 7. Authentication Solution

**Problem:** GCS was using wrong credentials (`martin@plans.app`)

**Solution:** Modified `get_gcs_filesystem()` to use gcloud token:

```python
def get_gcs_filesystem() -> gcsfs.GCSFileSystem:
    """Get cached gcsfs filesystem instance using gcloud credentials."""
    import subprocess

    result = subprocess.run(
        ['gcloud', 'auth', 'print-access-token'],
        capture_output=True,
        text=True,
        check=True,
        timeout=10
    )
    token = result.stdout.strip()
    return gcsfs.GCSFileSystem(token=token)
```

This ensures we always use the active gcloud account (`danskcollignon@gmail.com`).
