## NLES5 Pipeline Data Flow

This document shows how data is connected and joined throughout the NLES5 nitrogen estimation pipeline, and in what order operations occur.

```mermaid
flowchart TD
  subgraph Silver Inputs
    soil[soil_types]
    dmi_raw[dmi_data precip_and_evap]
    fertiliser[fertiliser silver dataset]
    gkea[field_plan_data GKEA]
    efter[catch_crops_data]
    fvm[fvm_marker_YYYY]
  end
  
  subgraph Farm Data Sources
    gr[gødningsregnskab farm data]
    gkea_prio[GKEA Gødningsoplysninger Priority 1]
    goereg_prio[Gødningsregnskaber Priority 2]
    fert_prio[fertilizer_accounts Priority 3]
  end

  subgraph "Phase 1-2: Data Loading & Climate Processing"
    A1[agricultural_fields_spatial]
    C1[climate_percolation]
  end

  subgraph "Phase 3-4: Spatial Tables & Nitrogen Prep"
    P1[NLES5 parameter tables]
    N1[fertilizer_history table]
    F1[fertilizer_distribution algorithm]
    FD1[farm_data_cache integrated]
  end

  subgraph "Phase 5: Target Year Processing"
    J1[fields x climate - SPATIAL_JOIN<br/>ST_Intersects + ST_Buffer 50-100km]
    J2[J1 left_join soil_types_prepared<br/>ST_Intersects]
    J3[J2 + crop classifications + field plan]
    J4[J3 + fertilizer distribution<br/>farm-level allocation by priority]
    K1[Percolation and soil effects]
    K2[Complete NLES5 calc Bt..Bg0 trend V Y5]
  end

  subgraph "Phase 6-7: Validation & Uncertainty"
    V1[Validation checks]
    U1[Uncertainty estimates]
  end

  subgraph "Phase 8-9: Export & Final Validation"
    O1[nles5_estimates_final_batched]
    O2[nles5_nitrogen_estimates_gold]
  end

  %% Phase 1-2: Data Loading
  fvm -->|load by year| A1
  dmi_raw -->|aggregate to percolation| C1
  soil --> P1
  
  %% Farm Data Sources Priority
  fertiliser -->|contains GKEA, Gødningsregnskaber, etc| gkea_prio
  fertiliser --> goereg_prio
  fertiliser --> fert_prio
  gr -->|farm-level data local/GCS| FD1
  
  %% Fertilizer Data Flow (Priority Order)
  gkea_prio -->|Priority 1| N1
  goereg_prio -->|Priority 2 fallback| N1
  fert_prio -->|Priority 3 fallback| N1
  gkea --> N1
  efter --> N1

  %% Phase 3-4: Preparation
  N1 --> F1
  FD1 -->|enhanced farm data| F1
  F1 -->|crop priority distribution| J4

  %% Phase 5: Core Processing
  A1 -->|ST_Intersects ST_Centroid + ST_Buffer| J1
  C1 -->|year equals target_year| J1
  J1 -->|ST_Intersects| J2
  P1 --> J2
  J2 --> J3
  J3 --> J4
  J4 --> K1
  K1 --> K2

  %% Phase 6-7: Validation & Uncertainty
  K2 --> V1
  V1 --> U1

  %% Phase 8-9: Outputs
  K2 --> O1
  U1 --> O1
  O1 --> O2
```

## Pipeline Phases (10 Phases Total)

1. **Phase 1**: Load required silver datasets (soil, climate, fertilizer, etc.)
2. **Phase 1.5**: Load agricultural fields data (FVM marker data)
3. **Phase 1.5.1**: Validate field IDs before processing
4. **Phase 1.6**: Load farm-level gødningsregnskab data for enhanced accuracy
5. **Phase 2**: Process climate data to calculate percolation
6. **Phase 3**: Create spatial tables and NLES5 parameter tables
7. **Phase 4**: Prepare nitrogen input tables (fertilizer history + distribution algorithm)
8. **Phase 5**: NLES5 target-year-by-target-year processing (main calculations)
9. **Phase 6**: Validate NLES5 estimates
10. **Phase 7**: Calculate uncertainty estimates
11. **Phase 8**: Final analysis and results export
12. **Phase 9**: Final validation of completed pipeline

## Key Technical Details

### Spatial Joins
- **Method**: `ST_Intersects(ST_Centroid(f.geom), ST_Buffer(c.geometry, distance))`
- **Buffer distances**: 50km for target-year processing, 100km for main spatial join
- **Year filtering**: `ABS(f.year - c.year) <= 2` (3-year window)

### Fertilizer Distribution
- **Algorithm**: Official Danish NLES5 methodology (N2023_62, Tabel 7)
- **Crop priorities**: 7-level priority system for organic fertilizer allocation
- **Distribution methods**: Proportional (organic > 50% quota) vs Priority-based (organic ≤ 50% quota)
- **Farm-level allocation**: Budgets distributed to fields based on N-quota requirements

### Fertilizer Data Sources (Priority Order)
- **Priority 1**: GKEA Gødningsoplysninger files (target year specific)
- **Priority 2**: Gødningsregnskaber files (fertilizer accounts)
- **Priority 3**: Generic fertilizer_accounts data (fallback)
- **Enhanced data**: Farm-level gødningsregnskab data for actual farm values (C_2016, C_2006, F_901, F_902, etc.)

### Farm Data Integration
- **Source**: Local files (data/In-depth/GR YYYY/) or future GCS parquet
- **Contents**: Animal production data (C_2016, C_2006) and detailed fertilizer applications (F_901, F_902, F_512, F_703_1, F_706_1, F_308_1)
- **Purpose**: Replace estimated values with actual farm-specific data for enhanced accuracy
- **Validation**: Comprehensive quality controls per N2023_62 Table 1 (21 validation rules)

### Output Tables
- **Intermediate**: `nles5_estimates_final_batched` (per batch/year)
- **Final**: `nles5_nitrogen_estimates_gold` (consolidated results)
- **Analysis tables**: `nles5_estimates_analysis`, `nles5_uncertainty_estimates`, etc.

## Notes
- Fields are spatially joined to climate per year using ST_Intersects with buffered climate points; then joined to soil polygons.
- Fertilizer data uses sophisticated distribution algorithm that allocates farm-level budgets to fields based on crop priorities and N-quota requirements.
- **Data source priority**: Pipeline prioritizes GKEA files over Gødningsregnskaber over generic fertilizer accounts for the most accurate data.
- **Enhanced accuracy**: Farm-level gødningsregnskab data (when available) replaces estimated values with actual farm-specific fertilizer applications and animal production data.
- **Validation**: Comprehensive quality controls ensure data integrity per Danish NLES5 methodology (N2023_62, Table 1).
- Pipeline processes data in target-year batches for memory efficiency.
- Final export table is `nles5_nitrogen_estimates_gold`.


