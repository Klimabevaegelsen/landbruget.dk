# Chr_Pipeline Schema Documentation

**Pipeline:** chr_pipeline
**Stage:** silver
**Generated:** 2025-06-28 12:57:51

This document contains schema information for all tables in this pipeline.

## Available Tables

- [chr_animals](#chr-animals)
- [chr_movements](#chr-movements)
- [chr_antibiotic_usage](#chr-antibiotic-usage)

## chr_animals

```sql
-- Table: chr_animals
-- Rows: 5,000
-- Columns: 6
--
-- Schema:
--   chr_number: VARCHAR NULL
--   species: VARCHAR NULL
--   age_days: INTEGER NULL
--   weight_kg: DOUBLE NULL
--   birth_date: DATE NULL
--   current_farm_chr: VARCHAR NULL
```

## chr_movements

```sql
-- Table: chr_movements
-- Rows: 2,000
-- Columns: 6
--
-- Schema:
--   movement_id: VARCHAR NULL
--   chr_number: VARCHAR NULL
--   movement_date: DATE NULL
--   from_farm_chr: VARCHAR NULL
--   to_farm_chr: VARCHAR NULL
--   movement_type: VARCHAR NULL
```

## chr_antibiotic_usage

```sql
-- Table: chr_antibiotic_usage
-- Rows: 1,500
-- Columns: 7
--
-- Schema:
--   usage_id: VARCHAR NULL
--   chr_number: VARCHAR NULL
--   treatment_date: DATE NULL
--   antibiotic_name: VARCHAR NULL
--   dosage_ml: DECIMAL(10,2) NULL
--   treatment_days: INTEGER NULL
--   prescribing_vet: VARCHAR NULL
```
