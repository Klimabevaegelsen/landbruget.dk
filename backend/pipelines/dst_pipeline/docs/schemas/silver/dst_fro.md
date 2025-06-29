# dst_fro

**Pipeline:** dst_pipeline
**Stage:** silver
**Generated:** 2025-06-29 11:21:12

## Table Overview

- **Row count:** 2,376
- **Columns:** 9

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| region | VARCHAR | ✓ |  |  |
| crop_type | VARCHAR | ✓ |  |  |
| measurement_unit | VARCHAR | ✓ |  |  |
| year | INTEGER | ✓ |  |  |
| value | DOUBLE | ✓ |  |  |
| crop_category | VARCHAR | ✓ |  |  |
| table_source | VARCHAR | ✓ |  |  |
| processed_at | VARCHAR | ✓ |  |  |
| source_system | VARCHAR | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| region | National | National | 1 |  | 0.0% |
| crop_type | ANDRE PLANTER TIL FRØ | Westerwoldisk rajgræs | 23 |  | 0.0% |
| measurement_unit | Areal (1000 hektar) | Produktion, tons | 2 |  | 0.0% |
| year | 1989 | 2024 | 37 | 2006.50 | 0.0% |
| value | 0.0 | 163752.1 | 1067 | 4739.44 | 13.8% |
| crop_category | All seeds | Other | 4 |  | 0.0% |
| table_source | FRO | FRO | 1 |  | 0.0% |
| processed_at | 2025-06-29T11:21:12.695744 | 2025-06-29T11:21:12.695744 | 1 |  | 0.0% |
| source_system | Danmarks Statistik API | Danmarks Statistik API | 1 |  | 0.0% |
