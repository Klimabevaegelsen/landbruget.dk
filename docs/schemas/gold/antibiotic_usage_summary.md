# antibiotic_usage_summary

**Pipeline:** chr_pipeline
**Stage:** gold
**Generated:** 2025-06-28 12:57:51

## Table Overview

- **Row count:** 6
- **Columns:** 6

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| antibiotic_name | VARCHAR | ✓ |  |  |
| treatment_month | DATE | ✓ |  |  |
| treatment_count | BIGINT | ✓ |  |  |
| animals_treated | BIGINT | ✓ |  |  |
| total_dosage_ml | DECIMAL(38,2) | ✓ |  |  |
| avg_treatment_duration | DOUBLE | ✓ |  |  |

## Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| antibiotic_name | Amoxicillin | Tetracycline | 3 |  | 0.0% |
| treatment_month | 2025-05-01 | 2025-06-01 | 2 | 2025-05-16 12:00:00 | 0.0% |
| treatment_count | 30 | 596 | 6 | 250.00 | 0.0% |
| animals_treated | 30 | 595 | 6 | 249.83 | 0.0% |
| total_dosage_ml | 1230.46 | 29945.97 | 6 | 12541.47 | 0.0% |
| avg_treatment_duration | 7.472222222222222 | 8.966101694915254 | 6 | 8.03 | 0.0% |
