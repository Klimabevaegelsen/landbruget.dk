# movement_patterns

**Pipeline:** chr_pipeline
**Stage:** gold
**Generated:** 2025-06-28 12:57:51

## Table Overview

- **Row count:** 39
- **Columns:** 6

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| movement_type | VARCHAR | ✓ |  |  |
| movement_month | DATE | ✓ |  |  |
| movement_count | BIGINT | ✓ |  |  |
| unique_animals | BIGINT | ✓ |  |  |
| unique_source_farms | BIGINT | ✓ |  |  |
| unique_destination_farms | BIGINT | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| movement_type | Death | Transfer | 3 |  | 0.0% |
| movement_month | 2024-06-01 | 2025-06-01 | 14 | 2024-11-30 18:27:41.538462 | 0.0% |
| movement_count | 5 | 93 | 24 | 51.28 | 0.0% |
| unique_animals | 5 | 93 | 24 | 51.28 | 0.0% |
| unique_source_farms | 5 | 88 | 34 | 49.85 | 0.0% |
| unique_destination_farms | 5 | 91 | 26 | 50.00 | 0.0% |
