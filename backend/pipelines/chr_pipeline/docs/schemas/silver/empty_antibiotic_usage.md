# empty_antibiotic_usage

**Pipeline:** chr_pipeline
**Stage:** silver
**Generated:** 2024-01-01 12:00:00

## Table Overview

- **Row count:** Unknown
- **Columns:** 17

## Columns

| Column | Type | Nullable | Key | Default |
|--------|------|----------|-----|---------|
| entity_id | VARCHAR | ✓ |  |  |
| cvr_number | VARCHAR | ✓ |  |  |
| chr_number | BIGINT | ✓ |  |  |
| year | INTEGER | ✓ |  |  |
| month | INTEGER | ✓ |  |  |
| species_code | INTEGER | ✓ |  |  |
| age_group_code | INTEGER | ✓ |  |  |
| avg_usage_rolling_9m | DOUBLE | ✓ |  |  |
| avg_usage_rolling_12m | DOUBLE | ✓ |  |  |
| animal_days | DOUBLE | ✓ |  |  |
| animal_doses | DOUBLE | ✓ |  |  |
| add_per_100_dyr_per_dag | DOUBLE | ✓ |  |  |
| limit_value | DOUBLE | ✓ |  |  |
| municipality_code | INTEGER | ✓ |  |  |
| municipality_name | VARCHAR | ✓ |  |  |
| region_code | INTEGER | ✓ |  |  |
| region_name | VARCHAR | ✓ |  |  |
