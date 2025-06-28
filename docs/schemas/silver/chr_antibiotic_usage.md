# chr_antibiotic_usage

**Pipeline:** chr_pipeline
**Stage:** silver
**Generated:** 2025-06-28 12:57:51

## Table Overview

- **Row count:** 1,500
- **Columns:** 7

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| usage_id | VARCHAR | ✓ |  |  |
| chr_number | VARCHAR | ✓ |  |  |
| treatment_date | DATE | ✓ |  |  |
| antibiotic_name | VARCHAR | ✓ |  |  |
| dosage_ml | DECIMAL(10,2) | ✓ |  |  |
| treatment_days | INTEGER | ✓ |  |  |
| prescribing_vet | VARCHAR | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| usage_id | AB1 | AB999 | 1287 |  | 0.0% |
| chr_number | DK1000474 | DK1999777 | 1540 |  | 0.0% |
| treatment_date | 2025-05-29 | 2025-06-28 | 32 | 2025-06-12 18:08:38.4 | 0.0% |
| antibiotic_name | Amoxicillin | Tetracycline | 3 |  | 0.0% |
| dosage_ml | 0.04 | 99.94 | 1219 | 50.17 | 0.0% |
| treatment_days | 1 | 15 | 17 | 8.06 | 0.0% |
| prescribing_vet | VET_0 | VET_99 | 103 |  | 0.0% |
