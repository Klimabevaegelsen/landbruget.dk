# Chr_Pipeline Schema Documentation

**Pipeline:** chr_pipeline
**Stage:** gold
**Generated:** 2025-06-28 12:57:51

This document contains schema information for all tables in this pipeline.

## Available Tables

- [animal_lifecycle_analysis](#animal-lifecycle-analysis)
- [movement_patterns](#movement-patterns)
- [antibiotic_usage_summary](#antibiotic-usage-summary)

## animal_lifecycle_analysis

```sql
-- Table: animal_lifecycle_analysis
-- Rows: 3
-- Columns: 6
--
-- Schema:
--   species: VARCHAR NULL
--   total_animals: BIGINT NULL
--   avg_age_days: DOUBLE NULL
--   avg_weight_kg: DOUBLE NULL
--   total_movements: BIGINT NULL
--   total_antibiotic_treatments: BIGINT NULL
```

## movement_patterns

```sql
-- Table: movement_patterns
-- Rows: 39
-- Columns: 6
--
-- Schema:
--   movement_type: VARCHAR NULL
--   movement_month: DATE NULL
--   movement_count: BIGINT NULL
--   unique_animals: BIGINT NULL
--   unique_source_farms: BIGINT NULL
--   unique_destination_farms: BIGINT NULL
```

## antibiotic_usage_summary

```sql
-- Table: antibiotic_usage_summary
-- Rows: 6
-- Columns: 6
--
-- Schema:
--   antibiotic_name: VARCHAR NULL
--   treatment_month: DATE NULL
--   treatment_count: BIGINT NULL
--   animals_treated: BIGINT NULL
--   total_dosage_ml: DECIMAL(38,2) NULL
--   avg_treatment_duration: DOUBLE NULL
```
