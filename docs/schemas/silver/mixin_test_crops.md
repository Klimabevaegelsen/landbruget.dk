# mixin_test_crops

**Pipeline:** test_mixin_pipeline
**Stage:** silver
**Generated:** 2025-06-28 12:55:46

## Table Overview

- **Row count:** 200
- **Columns:** 5

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| field_id | VARCHAR | ✓ |  |  |
| crop_type | VARCHAR | ✓ |  |  |
| area_hectares | DECIMAL(10,2) | ✓ |  |  |
| planting_date | DATE | ✓ |  |  |
| yield_kg_per_ha | DECIMAL(10,2) | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| field_id | FIELD_1 | FIELD_99 | 186 |  | 0.0% |
| crop_type | Barley | Wheat | 3 |  | 0.0% |
| area_hectares | 50.84 | 250.00 | 209 | 144.68 | 0.0% |
| planting_date | 2024-07-02 | 2025-06-24 | 147 | 2025-01-02 20:52:48 | 0.0% |
| yield_kg_per_ha | 27.99 | 9968.30 | 225 | 5502.00 | 0.0% |
