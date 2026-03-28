# Backend — Data Pipelines

Python-based data processing platform that transforms raw Danish government data into clean, analysis-ready datasets following a medallion architecture.

## Directory Structure

```
backend/
├── pipelines/                  # Individual data pipelines
│   ├── unified_pipeline/       # 18+ government data sources (Click CLI)
│   ├── chr_pipeline/           # Livestock registry + veterinary data
│   ├── svineflytning_pipeline/ # Pig movement tracking
│   ├── climate/                # Farm-level CO2e emissions
│   ├── bmd_scraper/            # Pesticide database scraper
│   ├── dma_scraper/            # Environmental company registry
│   ├── drive_data_pipeline/    # Google Drive regulatory docs
│   ├── bbr_buildings/          # Building registry
│   ├── arbejdstilsynet_inspections/ # Workplace safety inspections
│   ├── h3_pfas_exposure_pipeline/   # PFAS exposure mapping
│   ├── property_owners_sftp/   # Property ownership data
│   └── base/                   # Base classes for pipeline patterns
├── common/                     # Shared utilities (landbruget-common package)
├── api/                        # FastAPI endpoints (when needed)
├── scripts/                    # Backend utility scripts
├── migrations/                 # Data migration scripts
├── notebooks/                  # Jupyter notebooks for analysis
└── baselines/                  # Test baselines
```

## Quick Start

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run a pipeline
cd pipelines/chr_pipeline && python main.py --step bronze
```

## Stack

- **Python 3.11+** (< 3.13)
- **DuckDB** (>= 1.5.0) — primary processing engine, not Pandas
- **ibis-framework** — SQL abstraction
- **Pydantic** — data validation
- **Cloudflare R2** — cloud storage (S3-compatible, accessed via s3fs)
- **GitHub Actions** — pipeline orchestration

## Medallion Architecture

```
Bronze (raw, immutable) → Silver (cleaned, validated) → Gold (analysis-ready)
```

- **Bronze**: Preserve data exactly as received. Add `_fetch_timestamp`, `_source`, `_source_crs` metadata. Never transform or clean. Store in R2.
- **Silver**: Type coercion, format validation, deduplication. Keep EPSG:25832. Silver data should not depend on other data sources. Store in R2 as Parquet/GeoParquet.
- **Gold**: Join across datasets on CVR/CHR/BFE. Derive metrics (area, distance, buffers). Transform to EPSG:4326 only at final Supabase upload.

Bronze and Silver may run in the same pipeline. Gold runs in separate pipelines.

## Shared Utilities (`common/`)

All pipelines depend on `common/` as an editable package (`pip install -e`).

| Module | Purpose |
|--------|---------|
| `duckdb_processor.py` | `SharedDuckDBProcessor` base class with spatial extension |
| `crs_utils.py` | CRS constants (`DANISH_UTM`, `WGS84`), transform helpers |
| `storage/core.py` | `StorageAccess` class — R2 storage (read/write Parquet, CSV) |
| `storage/filesystem.py` | s3fs setup + DuckDB fsspec registration |
| `storage_interface.py` | `StoragePath` class (supports `gs://` and `r2://`) |
| `logging_utils.py` | `setup_pipeline_logger()` |
| `retry_utils.py` | Retry logic for transient failures |
| `schema_documentation.py` | Auto-generate schema docs from Parquet |
| `data_source_registry.py` | Data source metadata registry |
| `validation/` | Data validation utilities |

## Pipeline Patterns

Two patterns exist — follow whichever the pipeline already uses:

### Class-based (`unified_pipeline`)

```bash
python -m unified_pipeline bronze --source cadastral
```

Each source = one class inheriting from `BronzeBase`, `SilverBase`, or `GoldBase`. Config via Pydantic models.

### File-based (`chr_pipeline`, others)

```bash
python main.py --step bronze --processing-mode incremental
```

Separate modules per data source in `bronze/`, `silver/`, `gold/` directories.

## Creating a New Pipeline

1. Create a directory under `pipelines/` with `main.py` entry point
2. Add `bronze/`, `silver/`, `gold/` subdirectories as needed
3. Add `tests/` directory with `conftest.py`
4. Add `.env.example` with required environment variables
5. Add `README.md` following the [pipeline template](../docs/templates/PIPELINE_README_TEMPLATE.md)
6. Add `requirements.txt` or use shared deps
7. Create a GitHub Actions workflow in `.github/workflows/`

## CRS Strategy

**Process in EPSG:25832 (meters). Transform to EPSG:4326 once at Supabase upload.**

```python
from common.crs_utils import DANISH_UTM, WGS84
# Buffer/distance work directly in meters — no transform needed
conn.execute("SELECT ST_Buffer(geometry, 1000) FROM fields")  # 1000m buffer
# Transform only at final upload
conn.execute(f"SELECT ST_Transform(geometry, '{DANISH_UTM}', '{WGS84}') FROM fields")
```

## DuckDB Notes

DuckDB is the primary processor. Key DuckDB 1.5 considerations:

- Use `delim` parameter, not `DELIMITER` (breaking change)
- Wrap geometry operations with `TRY()` for error handling
- Spatial extension loads automatically via `SharedDuckDBProcessor`

## Testing

```bash
source venv/bin/activate
python -m pytest                      # All tests
python -m pytest -v -k test_name      # Specific test
```

- Each pipeline has its own `tests/` and `conftest.py`
- Common fixtures in `common/tests/conftest.py`: `mock_duckdb_connection`, `mock_gcs_filesystem`, `sample_danish_geometries`
- Markers: `pre_merge` (blocking), `cloud_required` (needs credentials)

## Linting

```bash
ruff check . && ruff format .
```

Per-pipeline ruff config in each `pyproject.toml`. Line length: 100, target: py311.

## Naming Conventions

- File names: lowercase with underscores, max 5 words
- Feature/column names: lowercase with underscores
- Geospatial fields: prefix with `geo_`
- Standard names: `cvr_number`, `chr_number`, `herd_number`, `municipality`, `species_name`, `year`, `field_id`, `field_block_id`

## Environment

Each pipeline loads its own env vars — no global config:

```python
from dotenv import load_dotenv
load_dotenv()
# StoragePath checks STORAGE_BUCKET first, then R2_BUCKET, then GCS_BUCKET as fallback
bucket = os.getenv("STORAGE_BUCKET") or os.getenv("R2_BUCKET") or os.getenv("GCS_BUCKET", "landbruget-data")
```

## Guidelines

- Use DuckDB SQL for data processing, not Pandas (for large files)
- Data should be processed locally in dev, stored on R2 in prod
- Do not share identifiers or credentials in commits
- If using LLMs in pipelines, use [OpenRouter](https://openrouter.ai/) and test output consistency
- Silver layer data should be expected to be publicly accessible — anonymize PII
- If cross-matching datasets uncovers anonymized identities, contact maintainers before committing
