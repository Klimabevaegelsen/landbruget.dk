---
name: data-catalog
description: |
  Activates when querying Danish agricultural data from R2/cloud storage.
  Use this skill for: data discovery, finding datasets, understanding schemas,
  querying parquet files, joining datasets on CVR/CHR/BFE identifiers.
  Keywords: data, catalog, datasets, R2, GCS, parquet, schema, query, DuckDB, rclone
---

# Data Catalog - R2 Storage (landbruget-data)

Data lake with 130+ datasets across bronze/silver/gold medallion layers in Cloudflare R2.

## Discovering Data

**Use `rclone` to browse R2 — never use `gsutil` or `google.cloud.storage`.**

```bash
# List top-level directories
rclone lsd r2:landbruget-data/

# List datasets in a layer
rclone lsd r2:landbruget-data/bronze/
rclone lsd r2:landbruget-data/silver/
rclone lsd r2:landbruget-data/gold/

# List snapshots (timestamped subdirectories) for a dataset
rclone lsd r2:landbruget-data/silver/subsidies/

# List files in a snapshot
rclone ls r2:landbruget-data/silver/subsidies/

# Find latest snapshot for a dataset
rclone lsd r2:landbruget-data/gold/field_production_2024/ | tail -1
```

**Important**: Some folder names contain spaces (e.g., `silver/animal welfare/`, `silver/work permits/`). Always quote paths when scripting.

## Reading Data with DuckDB

DuckDB with R2 auth is the primary way to query data. Use `StorageAccess` from `backend/common/storage/core.py`:

```python
from common.storage.core import StorageAccess

storage = StorageAccess()

# Read a parquet file into DuckDB
storage.create_table_from_storage_parquet("my_table", "landbruget-data/silver/subsidies/20260401_020000/data.parquet")

# Query it
result = storage.execute_query("SELECT cvr_number, COUNT(*) FROM my_table GROUP BY cvr_number")
```

Or use DuckDB directly after auth setup:

```python
import duckdb
from common.storage.filesystem import setup_duckdb_cloud_auth

conn = duckdb.connect()
setup_duckdb_cloud_auth(conn)

# Query directly from R2
result = conn.execute("""
    SELECT cvr_number, SUM(area_ha) as total_area
    FROM read_parquet('r2://landbruget-data/gold/field_production_2024/*/data.parquet')
    GROUP BY cvr_number
""").fetchdf()
```

## Environment Variables

```bash
# R2 credentials (required)
R2_ACCESS_KEY_ID=<access-key>
R2_SECRET_ACCESS_KEY=<secret-key>
R2_ACCOUNT_ID=<account-id>

# Bucket name (defaults to "landbruget-data")
R2_BUCKET=landbruget-data
# Or: STORAGE_BUCKET=landbruget-data
```

## Medallion Architecture

- `bronze/` — Raw data exactly as received (133 datasets)
- `silver/` — Cleaned, validated, standardized (126 datasets)
- `gold/` — Analysis-ready, joined datasets (86 datasets)

## Full Dataset Inventory

### Bronze (133 datasets)

| Category | Datasets |
|----------|----------|
| **Fields** | `agricultural_blocks_{2020-2024}`, `agricultural_fields_{2020-2025}`, `fields` |
| **FVM Marker** | `fvm_marker_{2008-2025}`, `fvm_marker_smaabiotoper_{2023-2025}` |
| **FVM Markblokke** | `fvm_markblokke_{2005-2026}` |
| **Organic** | `fvm_organic_areas_{2012-2024}`, `fvm_organic_subsidies_{2019-2024}` |
| **Subsidies** | `subsidies`, `fvm_environmental_subsidies_{2019-2023}`, `fvm_grassland_subsidies_{2019-2024}` |
| **Jordbrugsanalyser** | `jordbrugsanalyser_markers_{2012-2024}` |
| **Cadastral/Geo** | `cadastral`, `dagi_kommuner`, `dagi_landsdele`, `dagi_postnumre`, `dagi_regioner`, `bbr_buildings` |
| **Environment** | `bnbo_status`, `wetlands`, `soil_types`, `water_projects`, `water_typology_*` (3 datasets), `grukos_*` (2 datasets), `fertiliser` |
| **Pesticides** | `pesticides`, `bmd`, `geus_dataverse_pesticides`, `kemidata_surface_water_pesticides` |
| **Livestock** | `chr`, `animal_welfare`, `animal_mortality`, `animal_international_movements`, `pig_tail_cutting`, `slurry_leaks`, `stable_fires`, `transportation_accidents` |
| **Companies** | `cvr_raw_companies`, `dst`, `dmi` |
| **Workers** | `arbejdstilsynet_inspections`, `work_permits`, `worker_safety` |

### Silver (126 datasets)

| Category | Datasets |
|----------|----------|
| **Fields** | `fvm_marker_{2008-2025}`, `fvm_markblokke_{2005-2026}`, `fvm_smaabiotoper_{2023-2025}`, `fields` |
| **Organic** | `fvm_organic_areas_{2012-2023}`, `fvm_organic_subsidies_{2019-2024}` |
| **Subsidies** | `subsidies`, `fvm_environmental_subsidies_{2019-2023}`, `fvm_grassland_subsidies_{2019-2024}` |
| **Cadastral/Geo** | `cadastral`, `dagi_kommuner`, `dagi_landsdele`, `dagi_postnumre`, `dagi_regioner`, `bbr_buildings`, `dst_zone_mapping`, `dst_zone_mapping_reference` |
| **Environment** | `bnbo_status`, `bnbo_status_dissolved`, `wetlands` (implied), `grukos`, `grukos_*_dissolved` (2), `water_projects`, `water_projects_dissolved`, `fertiliser` |
| **Pesticides** | `pesticides`, `bmd`, `geus_dataverse_pesticides`, `geus_dataverse_pesticides_pfas` |
| **Livestock** | `chr`, `svineflytning`, `animal welfare`, `animal mortality`, `animal international movements`, `pig tail cutting`, `slurry leaks`, `stable fires`, `transportation accidents` |
| **Companies** | `cvr_companies`, `cvr_employment`, `cvr_persons`, `property_owners` |
| **Workers** | `arbejdstilsynet_inspections`, `work permits`, `worker safety` |
| **Legacy/Other** | `2016_*` (4 datasets), `gr {2015-2023}` (8 datasets), `fro_processed`, `gartn1_processed`, `halm1_processed`, `hst77_processed` |

### Gold (86 datasets)

| Category | Datasets |
|----------|----------|
| **Field Production** | `field_production_{2008-2025}` (18 years) |
| **Field Analysis** | `field_analysis_field_bnbo_intersections_{2024,2025}`, `field_analysis_field_bnbo_water_intersections_{2024,2025}`, `field_analysis_field_grukos_intersections_{2024,2025}`, `field_analysis_field_wetland_intersections_{2024,2025}`, `field_analysis_field_wetland_water_intersections_{2024,2025}`, `field_analysis_soil_intersections_{2024,2025}`, `field_analysis_water_projects_bnbo_intersections_{2024,2025}`, `field_analysis_water_projects_wetlands_intersections_{2024,2025}`, `field_analysis_wetland_water_coverage` |
| **Environmental Analysis** | `field_environmental_analysis_fields_{2024,2025}`, `field_environmental_analysis_properties_{2024,2025}` |
| **Pesticides** | `pesticide_disaggregation_{2010_2011 through 2023_2024}` (13 year-pairs), `pesticide_proximity_{2010_2011 through 2023_2024}` (13 year-pairs) |
| **Pre-computed stages** | `stage0_bnbo_filtered_{2024,2025}`, `stage0_grukos_filtered_{2024,2025}`, `stage0_properties_filtered_{2024,2025}`, `stage0_soil_types_filtered_{2024,2025}`, `stage0_water_projects_filtered_{2024,2025}`, `stage0_wetlands_filtered_{2024,2025}` |
| **Livestock** | `chr_timeline_summary`, `chr_veterinary_timeline` |
| **Companies** | `cvr_enrichment`, `cvr_enrichment_collection`, `cvr_enrichment_companies`, `cvr_enrichment_financial`, `cvr_enrichment_financial_statements`, `cvr_enrichment_pnumbers` |
| **Cadastral** | `property_cadastral_merged` |

### Other Top-Level

- `api/` — API-related data
- `cvr_collections/` — CVR collection data

## Key Identifiers

| Identifier | Format | Description | Validation |
|------------|--------|-------------|------------|
| **CVR** | 8 digits | Company registration number | `^\d{8}$` |
| **CHR** | 6 digits | Central Husbandry Register (herd ID) | `^\d{6}$` |
| **BFE** | Variable | Cadastral parcel number | varies |
| **field_id** | String | Field identifier from FVM | varies |
| **field_uuid** | UUID | Unique field identifier | UUID format |

## Dataset Quick Reference

### Okonomi (Finance)
| Dataset | Path | Rows | Key Columns |
|---------|------|------|-------------|
| Subsidies | `silver/subsidies/` | 554K | cvr_number, tilskudsberetigt |
| CVR Enrichment | `gold/cvr_enrichment/` | varies | cvr_number, company data |
| Property Owners | `silver/property_owners/` | 8.2M | CVRNummer, owner info |

### Landbrugsareal (Agricultural Land)
| Dataset | Path | Rows | Key Columns |
|---------|------|------|-------------|
| FVM Marker (fields) | `silver/fvm_marker_{year}/` | 617K/year | field_id, cvr_number, crop_code, geometry |
| Field Production | `gold/field_production_{year}/` | 617K/year | field_id, yield_estimate, crop_type |
| Cadastral | `silver/cadastral/` | 2.16M | bfe_number, geometry |

### Miljo (Environment)
| Dataset | Path | Rows | Key Columns |
|---------|------|------|-------------|
| Pesticide Disaggregation | `gold/pesticide_disaggregation_{year}/` | 1.52M | cvr_number, PesticideName, DosageQuantity |
| BNBO Status | `silver/bnbo_status/` | 5.4K | geometry, status_bnbo |
| Wetlands | `silver/wetlands/` (in bronze) | 1.7M | geometry, toerv_pct |

### Husdyr (Livestock)
| Dataset | Path | Rows | Key Columns |
|---------|------|------|-------------|
| Svineflytning | `silver/svineflytning/` | 1.27M | sender_chr_number, receiver_chr_number, total_animals |
| CHR Movements | `bronze/chr/` | 124K | reporting_herd_number, animal_count |
| Animal Welfare | `silver/animal welfare/` | varies | chr_number |

### Medarbejdere (Employees)
| Dataset | Path | Rows | Key Columns |
|---------|------|------|-------------|
| Arbejdstilsynet | `gold/arbejdstilsynet_inspections/` | 536 | cvr_number, decision, severity_score |
| Work Permits | `silver/work permits/` | varies | cvr_number |
| Worker Safety | `silver/worker safety/` | varies | cvr_number |

## Cross-Dataset Joins

### CVR-based joins (most common)
```python
# Join subsidies with pesticides on CVR using DuckDB
conn.execute("""
    SELECT s.cvr_number, s.tilskudsberetigt, p.PesticideName
    FROM read_parquet('r2://landbruget-data/silver/subsidies/*/data.parquet') s
    JOIN read_parquet('r2://landbruget-data/gold/pesticide_disaggregation_2023_2024/*/data.parquet') p
    ON s.cvr_number = p.cvr_number
""")
```

### Field-based joins
```python
# Join field production with environmental analysis
conn.execute("""
    SELECT fp.field_id, fp.yield_estimate, fe.bnbo_overlap_pct
    FROM read_parquet('r2://landbruget-data/gold/field_production_2024/*/data.parquet') fp
    JOIN read_parquet('r2://landbruget-data/gold/field_environmental_analysis_fields_2024/*/data.parquet') fe
    ON fp.field_id = fe.field_id
""")
```

## Data Update Schedule

| Layer | Frequency | Notes |
|-------|-----------|-------|
| Bronze | Weekly (Mondays 2AM UTC) | Immutable, timestamped |
| Silver | After bronze update | Cleaned, validated |
| Gold | After silver update | Analysis-ready |

## Related Skills

- **okonomi/** - Financial data: subsidies, property values
- **landbrugsareal/** - Field and crop data: FVM marker, production
- **miljo/** - Environmental data: pesticides, nitrogen, BNBO
- **husdyr/** - Livestock data: CHR, movements, welfare
- **medarbejdere/** - Employee data: inspections, safety

## Troubleshooting

### Check R2 access
```bash
rclone lsd r2:landbruget-data/
```

### If rclone fails
Check `~/.config/rclone/rclone.conf` has an `[r2]` section with:
- `type = s3`
- `provider = Cloudflare`
- `access_key_id` and `secret_access_key`
- `endpoint` pointing to your R2 account

### Large Files
Use DuckDB — never load large parquet files into Pandas:
```python
conn.execute("""
    SELECT cvr_number, SUM(area_ha) as total_area
    FROM read_parquet('r2://landbruget-data/gold/field_production_2024/*/data.parquet')
    GROUP BY cvr_number
""").fetchdf()
```

### CRS
All geometry stored in EPSG:4326 (WGS84) in Supabase. Bronze/Silver/Gold processing uses EPSG:25832 (UTM 32N).
