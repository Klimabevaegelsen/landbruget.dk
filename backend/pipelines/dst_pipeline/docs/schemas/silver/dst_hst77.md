# dst_hst77

**Pipeline:** dst_pipeline
**Stage:** silver
**Generated:** 2025-06-29 11:21:12

## Table Overview

- **Row count:** 5,130
- **Columns:** 10

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| region | VARCHAR | ✓ |  |  |
| crop_type | VARCHAR | ✓ |  |  |
| measurement_unit | VARCHAR | ✓ |  |  |
| year | INTEGER | ✓ |  |  |
| value | DOUBLE | ✓ |  |  |
| contents_code | VARCHAR | ✓ |  |  |
| crop_category | VARCHAR | ✓ |  |  |
| table_source | VARCHAR | ✓ |  |  |
| processed_at | VARCHAR | ✓ |  |  |
| source_system | VARCHAR | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| region | Hele landet | Region Sjælland | 9 |  | 0.0% |
| crop_type | BÆLGSÆD I ALT | Vårraps | 32 |  | 0.0% |
| measurement_unit | Gennemsnitsudbytte, hkg pr. hektar | Gennemsnitsudbytte, hkg pr. hektar | 1 |  | 0.0% |
| year | 2006 | 2024 | 21 | 2015.00 | 0.0% |
| value | 0.0 | 954.0 | 2008 | 197.40 | 8.0% |
| contents_code | Høstresultat | Høstresultat | 1 |  | 0.0% |
| crop_category | Grains | Root vegetables | 5 |  | 0.0% |
| table_source | HST77 | HST77 | 1 |  | 0.0% |
| processed_at | 2025-06-29T11:21:12.695744 | 2025-06-29T11:21:12.695744 | 1 |  | 0.0% |
| source_system | Danmarks Statistik API | Danmarks Statistik API | 1 |  | 0.0% |
