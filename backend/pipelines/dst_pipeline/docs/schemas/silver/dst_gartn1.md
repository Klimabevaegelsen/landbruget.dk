# dst_gartn1

**Pipeline:** dst_pipeline
**Stage:** silver
**Generated:** 2025-06-29 11:21:12

## Table Overview

- **Row count:** 30,051
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
| region | Hele landet | Region Sjælland | 9 |  | 0.0% |
| crop_type | Agurker, væksthus | Ærter til konsum | 47 |  | 0.0% |
| measurement_unit | Dyrket areal, hektar | Produktion, tons | 3 |  | 0.0% |
| year | 2003 | 2023 | 21 | 2013.00 | 0.0% |
| value | 0.0 | 117550.0 | 2598 | 419.69 | 45.3% |
| crop_category | Cabbage varieties | Root and fruit vegetables | 5 |  | 0.0% |
| table_source | GARTN1 | GARTN1 | 1 |  | 0.0% |
| processed_at | 2025-06-29T11:21:12.695744 | 2025-06-29T11:21:12.695744 | 1 |  | 0.0% |
| source_system | Danmarks Statistik API | Danmarks Statistik API | 1 |  | 0.0% |
