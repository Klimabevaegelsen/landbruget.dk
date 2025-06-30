# chr_movements

**Pipeline:** chr_pipeline
**Stage:** silver
**Generated:** 2025-06-28 12:57:51

## Table Overview

- **Row count:** 2,000
- **Columns:** 6

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| movement_id | VARCHAR | ✓ |  |  |
| chr_number | VARCHAR | ✓ |  |  |
| movement_date | DATE | ✓ |  |  |
| from_farm_chr | VARCHAR | ✓ |  |  |
| to_farm_chr | VARCHAR | ✓ |  |  |
| movement_type | VARCHAR | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| movement_id | MOV1 | MOV999 | 2047 |  | 0.0% |
| chr_number | DK1001204 | DK1999529 | 1599 |  | 0.0% |
| movement_date | 2024-06-28 | 2025-06-28 | 411 | 2024-12-26 05:53:31.2 | 0.0% |
| from_farm_chr | Farm_1 | Farm_999 | 1035 |  | 0.0% |
| to_farm_chr | Farm_0 | Farm_999 | 1014 |  | 0.0% |
| movement_type | Death | Transfer | 3 |  | 0.0% |
