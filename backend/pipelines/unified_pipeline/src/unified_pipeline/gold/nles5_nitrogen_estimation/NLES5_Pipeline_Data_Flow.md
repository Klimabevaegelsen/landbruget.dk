## NLES5 Pipeline Data Flow

This document shows how data is connected and joined throughout the NLES5 nitrogen estimation pipeline, and in what order operations occur.

```mermaid
flowchart TD
  subgraph Silver Inputs
    soil[soil_types]
    dmi_raw[dmi_data precip_and_evap]
    fert[fertilizer_accounts]
    gkea[field_plan_data GKEA]
    efter[catch_crops_data]
    fvm[fvm_marker_YYYY]
  end

  subgraph "Phase 1-2: Data Loading & Climate Processing"
    A1[agricultural_fields_spatial]
    C1[climate_percolation]
  end

  subgraph "Phase 3-4: Spatial Tables & Nitrogen Prep"
    P1[NLES5 parameter tables]
    N1[fertilizer_history table]
    F1[fertilizer_distribution algorithm]
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
  fert --> N1
  gkea --> N1
  efter --> N1

  %% Phase 3-4: Preparation
  N1 --> F1
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

## Pipeline Phases (9 Phases Total)

1. **Phase 1**: Load required silver datasets (soil, climate, fertilizer, etc.)
2. **Phase 1.5**: Load agricultural fields data (FVM marker data)
3. **Phase 2**: Process climate data to calculate percolation
4. **Phase 3**: Create spatial tables and NLES5 parameter tables
5. **Phase 4**: Prepare nitrogen input tables (fertilizer history + distribution algorithm)
6. **Phase 5**: NLES5 target-year-by-target-year processing (main calculations)
7. **Phase 6**: Validate NLES5 estimates
8. **Phase 7**: Calculate uncertainty estimates
9. **Phase 8**: Final analysis and results export
10. **Phase 9**: Final validation of completed pipeline

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

### Output Tables
- **Intermediate**: `nles5_estimates_final_batched` (per batch/year)
- **Final**: `nles5_nitrogen_estimates_gold` (consolidated results)
- **Analysis tables**: `nles5_estimates_analysis`, `nles5_uncertainty_estimates`, etc.

## Notes
- Fields are spatially joined to climate per year using ST_Intersects with buffered climate points; then joined to soil polygons.
- Fertilizer data uses sophisticated distribution algorithm that allocates farm-level budgets to fields based on crop priorities and N-quota requirements.
- Pipeline processes data in target-year batches for memory efficiency.
- Final export table is `nles5_nitrogen_estimates_gold`.


