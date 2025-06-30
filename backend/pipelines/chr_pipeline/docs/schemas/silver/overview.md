# Chr_Pipeline Schema Documentation

**Pipeline:** chr_pipeline
**Stage:** silver
**Generated:** 2024-01-01 12:00:00

This document contains schema information for all tables in this pipeline.

## Available Tables

- [bes_list](#bes-list)
- [cattle_movements](#cattle-movements)
- [empty_antibiotic_usage](#empty-antibiotic-usage)

## bes_list

```sql
-- Table: bes_list
-- Rows: 1
-- Columns: 1
--
-- Schema:
--   test: VARCHAR NULL
```

## cattle_movements

```sql
-- Table: cattle_movements
-- Rows: 1
-- Columns: 1
--
-- Schema:
--   test: VARCHAR NULL
```

## empty_antibiotic_usage

```sql
-- Table: empty_antibiotic_usage
-- Rows: Unknown
-- Columns: 17
--
-- Schema:
--   entity_id: VARCHAR NULL
--   cvr_number: VARCHAR NULL
--   chr_number: BIGINT NULL
--   year: INTEGER NULL
--   month: INTEGER NULL
--   species_code: INTEGER NULL
--   age_group_code: INTEGER NULL
--   avg_usage_rolling_9m: DOUBLE NULL
--   avg_usage_rolling_12m: DOUBLE NULL
--   animal_days: DOUBLE NULL
--   animal_doses: DOUBLE NULL
--   add_per_100_dyr_per_dag: DOUBLE NULL
--   limit_value: DOUBLE NULL
--   municipality_code: INTEGER NULL
--   municipality_name: VARCHAR NULL
--   region_code: INTEGER NULL
--   region_name: VARCHAR NULL
```
