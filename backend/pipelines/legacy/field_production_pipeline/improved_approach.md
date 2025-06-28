# Improved Data-Driven Field Production Approach

## Current Problems

1. **Year-based assumptions**: Pipeline assumes DST data exists for every field year (2020-2025)
2. **No data availability checks**: Processes years without DST data, creating empty yield columns
3. **Wasteful storage**: Stores 800MB+ of duplicated field data with NULL yields
4. **Inflexible processing**: Can't handle irregular DST data availability

## Proposed Solution: Data-Driven Processing

### 1. Check DST Data Availability First

```python
def get_available_dst_years() -> Dict[str, List[int]]:
    """Check what DST data is actually available."""
    dst_tables = ["HST77", "GARTN1", "FRO", "HALM1"]
    availability = {}
    
    for table in dst_tables:
        table_path = f"data_cache/dst_pipeline/{table.lower()}_processed.parquet"
        if Path(table_path).exists():
            df = pd.read_parquet(table_path)
            available_years = sorted(df['year'].unique().tolist())
            availability[table] = available_years
            print(f"{table}: {available_years}")
        else:
            availability[table] = []
            print(f"{table}: No data available")
    
    return availability
```

### 2. Only Process Field-DST Combinations That Make Sense

```python
def get_viable_processing_combinations() -> List[Dict]:
    """Get combinations of field years and DST years that can produce yields."""
    
    # Check available field years
    field_years = []
    for year in range(2020, 2026):
        field_path = f"data_cache/agricultural_fields/agricultural_fields_{year}_data.parquet"
        if Path(field_path).exists():
            field_years.append(year)
    
    # Check available DST years
    dst_availability = get_available_dst_years()
    
    # Find viable combinations
    combinations = []
    for field_year in field_years:
        for dst_table, dst_years in dst_availability.items():
            if not dst_years:
                continue
                
            # Use the most recent DST data available (but not future years)
            viable_dst_years = [y for y in dst_years if y <= field_year]
            if viable_dst_years:
                most_recent_dst = max(viable_dst_years)
                combinations.append({
                    'field_year': field_year,
                    'dst_year': most_recent_dst,
                    'dst_table': dst_table,
                    'data_lag': field_year - most_recent_dst
                })
    
    return combinations
```

### 3. Process Only Viable Combinations

```python
def process_field_yields_data_driven():
    """Process field yields only for combinations where we have both field and DST data."""
    
    combinations = get_viable_processing_combinations()
    
    print(f"Found {len(combinations)} viable field-DST combinations:")
    for combo in combinations:
        print(f"  Fields {combo['field_year']} + DST {combo['dst_year']} "
              f"({combo['dst_table']}) - {combo['data_lag']} year lag")
    
    results = []
    for combo in combinations:
        try:
            # Load field data for specific year
            fields = load_agricultural_fields(combo['field_year'])
            
            # Load DST data for specific year and table
            dst_data = load_dst_data_for_year(combo['dst_table'], combo['dst_year'])
            
            # Calculate yields using the specific DST year data
            yields = calculate_yields(fields, dst_data, combo)
            
            # Store results with clear provenance
            output_file = f"field_yields_{combo['field_year']}_using_dst_{combo['dst_year']}.parquet"
            yields.to_parquet(output_file)
            
            results.append({
                'field_year': combo['field_year'],
                'dst_year': combo['dst_year'],
                'yields_calculated': len(yields),
                'output_file': output_file
            })
            
        except Exception as e:
            print(f"Failed to process {combo}: {e}")
    
    return results
```

### 4. Normalized Storage with Clear Provenance

```sql
-- field_yields table with clear data provenance
CREATE TABLE field_yields (
    -- JOIN KEYS
    field_id VARCHAR NOT NULL,
    block_id VARCHAR NOT NULL,
    field_year INTEGER NOT NULL,      -- Year of the field data
    dst_year INTEGER NOT NULL,        -- Year of the DST data used
    
    -- YIELD DATA
    yield_estimate_hkg_ha DOUBLE,
    production_estimate_hkg DOUBLE,
    
    -- DATA PROVENANCE
    dst_table VARCHAR,                -- Which DST table was used
    dst_category VARCHAR,             -- DST category mapping
    estimation_method VARCHAR,        -- How yield was calculated
    data_lag_years INTEGER,           -- field_year - dst_year
    
    -- METADATA
    created_at TIMESTAMP,
    
    PRIMARY KEY (field_id, block_id, field_year, dst_year)
);
```

### 5. Smart Processing Logic

```python
def should_process_combination(field_year: int, dst_year: int) -> bool:
    """Determine if a field-DST combination should be processed."""
    
    # Don't use DST data that's too old (>3 years lag)
    if field_year - dst_year > 3:
        return False
    
    # Don't use future DST data
    if dst_year > field_year:
        return False
    
    # Check if we already have results for this combination
    output_file = f"field_yields_{field_year}_using_dst_{dst_year}.parquet"
    if Path(output_file).exists():
        print(f"Skipping {field_year}-{dst_year}: already processed")
        return False
    
    return True
```

## Benefits of Data-Driven Approach

### 1. **No Wasted Processing**
- Only process combinations where both field and DST data exist
- Skip years like 2025 where no DST data is available

### 2. **Clear Data Provenance**
- Know exactly which DST year was used for each field year
- Track data lag (e.g., "2024 fields using 2022 DST data")

### 3. **Flexible DST Usage**
- Use most recent available DST data for each field year
- Handle irregular DST data availability gracefully

### 4. **Efficient Storage**
- Only store yield data where it actually exists
- No NULL-filled denormalized tables

### 5. **Better Debugging**
- Easy to see why yields are missing (no DST data vs calculation failure)
- Clear audit trail of data sources

## Example Output

```
DST Data Availability Check:
  HST77: [2020, 2021, 2022, 2023]
  GARTN1: [2020, 2021, 2022]
  FRO: [2020, 2021, 2022, 2023]
  HALM1: [2020, 2021]

Viable Processing Combinations:
  Fields 2023 + DST 2023 (HST77) - 0 year lag
  Fields 2023 + DST 2022 (GARTN1) - 1 year lag
  Fields 2024 + DST 2023 (HST77) - 1 year lag
  Fields 2024 + DST 2022 (GARTN1) - 2 year lag
  
Skipping:
  Fields 2025: No suitable DST data (would be >2 year lag)

Results:
  ✅ field_yields_2023_using_dst_2023.parquet: 245,678 yields calculated
  ✅ field_yields_2024_using_dst_2023.parquet: 198,432 yields calculated
  ⏭️  Fields 2025: Skipped (no recent DST data)
```

This approach eliminates the fundamental problem of trying to process data combinations that can't produce meaningful results. 