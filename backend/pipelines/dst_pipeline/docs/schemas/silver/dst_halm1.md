# dst_halm1

**Pipeline:** dst_pipeline
**Stage:** silver
**Generated:** 2025-06-29 11:46:44

## Table Overview

- **Row count:** 27,360
- **Columns:** 10

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| region | VARCHAR | ✓ |  |  |
| crop_type | VARCHAR | ✓ |  |  |
| measurement_unit | VARCHAR | ✓ |  |  |
| usage_type | VARCHAR | ✓ |  |  |
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
| crop_type | Alle afgrøder | Vårraps | 16 |  | 0.0% |
| measurement_unit | Areal (1000 hektar) | Mængde (mio. kilo) | 2 |  | 0.0% |
| usage_type | Halm i alt | Til strøelse m.v. | 5 |  | 0.0% |
| year | 2006 | 2024 | 21 | 2015.00 | 0.0% |
| value | 0.0 | 6516.3 | 2993 | 68.44 | 12.8% |
| crop_category | All crops | Rapeseed | 5 |  | 0.0% |
| table_source | HALM1 | HALM1 | 1 |  | 0.0% |
| processed_at | 2025-06-29T11:46:44.054038 | 2025-06-29T11:46:44.054038 | 1 |  | 0.0% |
| source_system | Danmarks Statistik API | Danmarks Statistik API | 1 |  | 0.0% |
