# Data Transformer Module

## Overview

The `data_transformer.py` module bridges the GCS Danish schema (raw data from Danish government sources) to the English calculator schema expected by `climate_calculator.py`.

This is a critical data integration layer that:
- **Maps Danish field names to English**: `c_2001` → `species`, `c_2006` → `animal_count`
- **Translates Danish values to English**: `"Kvæg"` → `"cattle"`, `"Malkekøer"` → `"dairy_cows"`
- **Structures data for calculator**: Raw DataFrames → Typed objects (`LivestockSummary`, `FieldSummary`, etc.)
- **Handles missing data gracefully**: Empty DataFrames, null values, missing columns

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    GCS Raw Data (Danish Schema)                     │
│  - Green Accounts: c_2001, c_2004, c_2006, c_2016 (livestock)     │
│  - GKEA: total_n_kvote, faktisk_areal_ha (fertilizer)             │
│  - FVM: afgroede, areal_ha, bfe_nummer (fields)                   │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Data Transformers                           │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ GreenAccountsTransformer                                     │  │
│  │ - Maps Danish species → English (Kvæg → cattle)             │  │
│  │ - Extracts animal counts by subtype                         │  │
│  │ - Aggregates N production by species                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ GKEATransformer                                              │  │
│  │ - Aggregates total N applied across fields                  │  │
│  │ - Calculates N2O emissions (IPCC Tier 1)                    │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ FVMTransformer                                               │  │
│  │ - Maps Danish crop names → English                          │  │
│  │ - Aggregates area by crop type                              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ IntegratedFarmTransformer                                    │  │
│  │ - Orchestrates all transformers                             │  │
│  │ - Builds complete farm data structure                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                Structured Data (English Schema)                     │
│  - LivestockSummary: {species: cattle, total_count: 120, ...}     │
│  - FieldSummary: {crop_type: winter_wheat, total_area_ha: 25.5}   │
│  - FertilizerSummary: {total_n_kg: 5000.0, avg_n_kg_per_ha: 100}  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Climate Calculator                              │
│                   (climate_calculator.py)                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Input Schemas (GCS)

### Green Accounts (Livestock Data)

**Path**: `gs://landbrugsdata-raw-data/silver/gr {year}/`
**Format**: Parquet
**Years**: 2018-2023
**Records**: ~65,000 farms per year

**Key Columns**:
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `cvr_number` | string | Company CVR (8 digits) | `"31373077"` |
| `c_2001` | string | Species name (Danish) | `"Kvæg"`, `"Svin"`, `"Høns"` |
| `c_2004` | string | Type detail (Danish) | `"Malkekøer"`, `"Søer"` |
| `c_2006` | int | Animal count | `120` |
| `c_2016` | float | Total N production (kg) | `14400.0` |
| `c_2005` | string | Housing system type | `"Løsdrift"` |
| `c_2030` | int | Housing system code | `1`, `2`, `3` |

### GKEA (Fertilizer Data)

**Path**: `gs://landbrugsdata-raw-data/silver/fertiliser/`
**Format**: Parquet
**Years**: 2021-2024
**Records**: ~585,000 field records (2024)

**Key Columns**:
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `cvr_number` | string | Company CVR (8 digits) | `"31373077"` |
| `total_n_kvote` | float | **Total N applied (kg)** - PRIMARY FIELD | `2400.0` |
| `faktisk_areal_ha` | float | Actual field area (ha) | `20.0` |
| `marknummer` | string | Field number | `"M001"` |
| `year` | int | Agricultural year | `2024` |

### FVM (Field Data)

**Path**: `gs://landbrugsdata-raw-data/silver/fvm_marker_{year}/`
**Format**: Parquet
**Years**: 2008-2025

**Key Columns**:
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `cvr` | string | Company CVR (8 digits) | `"31373077"` |
| `bfe_nummer` | string | Cadastral field ID | `"BFE001"` |
| `afgroede` | string | Crop type (Danish) | `"Vinterhvede"`, `"Græs"` |
| `areal_ha` | float | Field area (hectares) | `25.5` |

---

## Output Data Structures

### LivestockSummary

```python
@dataclass
class LivestockSummary:
    species: str                          # English: 'cattle', 'pigs', 'poultry'
    total_count: int                      # Total animals
    total_n_production_kg: float          # Total N production
    subtypes: Dict[str, int]              # e.g., {'dairy_cows': 120}
    housing_systems: Dict[str, int]       # e.g., {'loose_housing': 100}
```

### FieldSummary

```python
@dataclass
class FieldSummary:
    crop_type: str                        # English: 'winter_wheat', 'grass'
    total_area_ha: float                  # Total area for this crop
    field_count: int                      # Number of fields
    avg_yield_kg_ha: Optional[float]      # Average yield (if available)
```

### FertilizerSummary

```python
@dataclass
class FertilizerSummary:
    total_n_kg: float                     # Total N applied
    total_area_ha: float                  # Total field area
    avg_n_kg_per_ha: float                # Average N application rate
    field_count: int                      # Number of fields
```

---

## Usage Examples

### Example 1: Transform Livestock Data

```python
from data_loader import ClimateDataLoader
from data_transformer import GreenAccountsTransformer

# Load raw data from GCS
loader = ClimateDataLoader()
livestock_df = loader.load_livestock(cvr="31373077", year=2023)

# Transform to structured format
livestock = GreenAccountsTransformer.transform(livestock_df)

# Access transformed data
for species, summary in livestock.items():
    print(f"{species}: {summary.total_count} animals")
    print(f"  Subtypes: {summary.subtypes}")
    print(f"  N production: {summary.total_n_production_kg:.1f} kg")

# Output:
# cattle: 163 animals
#   Subtypes: {'dairy_cows': 120, 'heifers': 25, 'calves': 18}
#   N production: 15760.0 kg
```

### Example 2: Transform Fertilizer Data and Calculate N2O

```python
from data_transformer import GKEATransformer

# Load raw fertilizer data
fert_df = loader.load_fertilizer(cvr="31373077", year=2024)

# Transform
fert = GKEATransformer.transform(fert_df)

print(f"Total N applied: {fert.total_n_kg:.1f} kg")
print(f"Average N/ha: {fert.avg_n_kg_per_ha:.1f} kg/ha")

# Calculate N2O emissions using IPCC Tier 1
n2o_co2e = GKEATransformer.calculate_n2o_emissions(fert)
print(f"N2O emissions: {n2o_co2e:.1f} kg CO2e")

# Formula: total_n_kg * 0.01 * (44/28) * 298 = kg CO2e
```

### Example 3: Transform Field Data

```python
from data_transformer import FVMTransformer

# Load raw field data
field_df = loader.load_fields(cvr="31373077", year=2024)

# Transform
fields = FVMTransformer.transform(field_df)

# Access crop breakdown
for field in fields:
    print(f"{field.crop_type}: {field.total_area_ha:.1f} ha ({field.field_count} fields)")

# Calculate total area
total_area = FVMTransformer.get_total_area(fields)
print(f"\nTotal farm area: {total_area:.1f} ha")

# Convert to DataFrame for analysis
crop_df = FVMTransformer.get_crop_breakdown(fields)
```

### Example 4: Integrated Farm Transformation

```python
from data_transformer import IntegratedFarmTransformer

# Load all data sources
livestock_df = loader.load_livestock(cvr="31373077", year=2023)
field_df = loader.load_fields(cvr="31373077", year=2024)
fert_df = loader.load_fertilizer(cvr="31373077", year=2024)

# Transform all at once
farm_data = IntegratedFarmTransformer.transform_all(
    livestock_df, field_df, fert_df
)

# Access integrated data
print(farm_data['metadata'])
# {
#   'has_livestock': True,
#   'has_fields': True,
#   'has_fertilizer': True,
#   'total_area_ha': 125.5,
#   'livestock_species': ['cattle', 'pigs'],
#   'crop_types': ['winter_wheat', 'grass', 'spring_barley']
# }

# Convert to FarmData object for calculator
farm_data_obj = IntegratedFarmTransformer.to_farm_data_object(farm_data)
```

---

## Mapping Tables

### Species Mapping (Danish → English)

| Danish | English |
|--------|---------|
| Kvæg / Kvaeg | cattle |
| Svin | pigs |
| Høns / Hoens | poultry |
| Får / Faar | sheep |
| Geder | goats |
| Heste | horses |
| Mink | mink |
| Kaniner | rabbits |

### Cattle Subtype Mapping

| Danish | English |
|--------|---------|
| Malkekøer / Malkekoer | dairy_cows |
| Ammekøer / Ammekoer | suckler_cows |
| Kvier | heifers |
| Kalve | calves |
| Ungtyre | young_bulls |
| Stude | steers |
| Tyre | bulls |

### Pig Subtype Mapping

| Danish | English |
|--------|---------|
| Søer / Soer | sows |
| Smågrise / Smaagrise | piglets |
| Slagtesvin | finishers |
| Polte | gilts |

### Crop Mapping

| Danish | English |
|--------|---------|
| Vinterhvede | winter_wheat |
| Vårhvede / Varhvede | spring_wheat |
| Vinterbyg | winter_barley |
| Vårbyg / Varbyg | spring_barley |
| Havre | oats |
| Rug | rye |
| Majs | maize |
| Raps | rapeseed |
| Kløver / Kloever | clover |
| Græs / Graes | grass |
| Lucerne | alfalfa |
| Sukkerroer | sugar_beet |
| Kartofler | potatoes |
| Ærter / Aerter | peas |
| Bønner / Boenner | beans |

---

## Data Quality Handling

### Missing Data

The transformers handle missing data gracefully:

```python
# Empty DataFrame
df = pd.DataFrame()
result = GreenAccountsTransformer.transform(df)
assert result == {}  # Returns empty dict, doesn't crash

# Missing columns
df = pd.DataFrame({'wrong_column': [1, 2, 3]})
result = GKEATransformer.transform(df)
assert result is None  # Returns None, logs error

# Null values
df = pd.DataFrame({
    'total_n_kvote': [1500.0, None, 2000.0],
    'faktisk_areal_ha': [10.0, 15.0, 20.0]
})
result = GKEATransformer.transform(df)
# Null values converted to 0, transformation continues
```

### Data Validation

Each transformer validates:
- ✓ Required columns present
- ✓ Numeric columns converted with error handling
- ✓ Danish values mapped to English (unmapped values logged)
- ✓ Empty groups filtered out

---

## Testing

### Run Unit Tests

```bash
cd backend
source venv/bin/activate
python -m pytest pipelines/climate_tool/test_data_transformer.py -v
```

### Run Validation Script

```bash
cd backend
source venv/bin/activate
python pipelines/climate_tool/validate_transformer.py
```

Expected output:
```
✓ ALL VALIDATIONS PASSED!

The data transformer is working correctly and ready to use.
It successfully bridges the Danish GCS schema to the calculator input schema.
```

### Run with Real GCS Data

```bash
cd backend
source venv/bin/activate
python pipelines/climate_tool/data_transformer.py
```

This will load real data for CVR `31373077` (test farm) and demonstrate all transformations.

---

## Integration with Climate Calculator

The transformers integrate seamlessly with `climate_calculator.py`:

```python
from data_loader import ClimateDataLoader
from data_transformer import IntegratedFarmTransformer
from climate_calculator import FarmClimateCalculator

# 1. Load raw data
loader = ClimateDataLoader()
livestock_df = loader.load_livestock(cvr="31373077", year=2023)
field_df = loader.load_fields(cvr="31373077", year=2024)
fert_df = loader.load_fertilizer(cvr="31373077", year=2024)

# 2. Transform data
farm_data = IntegratedFarmTransformer.transform_all(
    livestock_df, field_df, fert_df
)

# 3. Convert to FarmData object
farm_data_obj = IntegratedFarmTransformer.to_farm_data_object(farm_data)

# 4. Calculate emissions
calculator = FarmClimateCalculator(loader)
report = calculator.calculate_emissions(cvr="31373077", year=2024)

print(f"Total emissions: {report.total_co2e_kg:.1f} kg CO2e")
```

---

## Performance Considerations

### Memory Efficiency

- **Streaming**: Transformers process data in-memory (no disk writes)
- **Grouping**: Uses pandas groupby for efficient aggregation
- **No copies**: Minimal DataFrame copying (only when necessary for cleaning)

### Computational Complexity

| Transformer | Complexity | Notes |
|-------------|-----------|-------|
| GreenAccountsTransformer | O(n) | Groups by species (small number of groups) |
| GKEATransformer | O(n) | Single pass aggregation |
| FVMTransformer | O(n) | Groups by crop type |
| IntegratedFarmTransformer | O(n) | Calls each transformer once |

Where n = number of records in DataFrame.

For typical farm (CVR):
- Livestock: ~10-50 rows → <1ms
- Fields: ~20-100 rows → <1ms
- Fertilizer: ~20-100 rows → <1ms
- **Total**: <5ms per farm

---

## Error Handling

### Common Errors and Solutions

**Error**: `Missing required columns: ['c_2006']`
- **Cause**: GCS schema changed or wrong data source
- **Solution**: Verify GCS path and check column names in source data

**Error**: `Unknown species: Geder`
- **Cause**: Danish species name not in mapping table
- **Solution**: Add mapping to `SPECIES_MAPPING` dict in `data_transformer.py`

**Error**: `Invalid CVR format: 123`
- **Cause**: CVR not 8 digits
- **Solution**: Pad with zeros: `str(cvr).zfill(8)`

---

## Future Enhancements

### Planned Features

1. **Housing System Mapping**: Map Danish housing types to emission factors
2. **Yield Data**: Integrate actual yield data from harvest records
3. **Weather Integration**: Add climate data transformation
4. **Validation Rules**: Implement business logic validation (e.g., max animals per area)
5. **Data Quality Scores**: Calculate completeness and accuracy scores

### Extension Points

Add new transformers for additional data sources:

```python
class ARLATransformer:
    """Transform ARLA FarmAhead data."""

    @staticmethod
    def transform(df: pd.DataFrame) -> ARLASummary:
        # Transform ARLA milk production data
        pass

class CHRTransformer:
    """Transform CHR herd registry data."""

    @staticmethod
    def transform(df: pd.DataFrame) -> CHRSummary:
        # Transform CHR livestock movements
        pass
```

---

## References

### Related Files

- `/backend/pipelines/climate_tool/data_loader.py` - GCS data loading
- `/backend/pipelines/climate_tool/climate_calculator.py` - Emission calculations
- `/backend/pipelines/climate_tool/farm_data.py` - FarmData object definition
- `/docs/DATA_LINEAGE_COMPREHENSIVE.md` - Complete data lineage documentation

### Data Sources

- **Green Accounts**: Landbrugsstyrelsen gødningsregnskaber
- **GKEA**: Gødningskvoteberegning (fertilizer quota)
- **FVM**: Fælles Virkemiddelforvaltning (agricultural field boundaries)

### Standards

- **CVR Format**: 8 digits, zero-padded
- **IPCC Tier 1**: N2O emission factor = 1% of applied N
- **GWP Values**: CO2=1, CH4=25, N2O=298 (IPCC AR4)

---

## Contact

For questions or issues with the data transformer:
1. Check the validation script output
2. Review the test cases in `test_data_transformer.py`
3. Check GCS schema documentation in `data_loader.py`
4. Consult `docs/DATA_LINEAGE_COMPREHENSIVE.md` for data source details
