# Backend — Landbruget.dk

Python 3.11+ (<3.13), DuckDB >=1.5.0, ibis-framework, Pydantic, s3fs/gcsfs. Package manager: **uv** (not pip).

## Commands

```bash
source venv/bin/activate                      # Always activate first
python -m pytest                              # Run all tests
python -m pytest -v -k test_name              # Run specific test
ruff check . && ruff format .                 # Lint and format
python -m unified_pipeline bronze --source X  # Run unified pipeline (Click CLI)
cd pipelines/chr_pipeline && python main.py   # Run CHR pipeline
```

## Processing Engine

**DuckDB is the primary processor, NOT Pandas.** All data processing uses DuckDB SQL with native GEOMETRY type:

```python
conn.execute("SELECT ST_Area(geometry) / 10000 AS area_ha FROM fields")
conn.execute("SELECT ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326') FROM fields")
```

- Base class: `SharedDuckDBProcessor` in `common/duckdb_processor.py`
- Spatial extension loaded automatically
- Use `TRY()` wrapper for geometry operations (DuckDB 1.5 requirement)
- Use `delim` parameter, not `DELIMITER` (DuckDB 1.5 breaking change)

## Pipeline Patterns

Two patterns exist — follow whichever the pipeline already uses:

### Class-based (unified_pipeline)
- Entry: `src/unified_pipeline/app.py` (Click CLI)
- Each source = one class inheriting from `BronzeBase`, `SilverBase`, or `GoldBase`
- Config via Pydantic models with `load_dotenv()` in `__init__`
- Run: `python -m unified_pipeline bronze --source cadastral`

### File-based (chr_pipeline, others)
- Entry: `main.py` with procedural orchestration
- Separate modules per data source in `bronze/`, `silver/`, `gold/`
- Run: `python main.py --processing-mode incremental`

## Shared Package: `landbruget-common`

Installed as editable via `uv` (hatchling build system, `common/pyproject.toml`). Key modules:

- `common/duckdb_processor.py` — `SharedDuckDBProcessor` + `PipelineProcessor` base classes
- `common/crs_utils.py` — CRS constants (`DANISH_UTM`, `WGS84`), SQL transform helpers
- `common/gcs/core.py` — `GCSDataAccess` class (read/write parquet to cloud)
- `common/gcs/filesystem.py` — s3fs/gcsfs setup + DuckDB fsspec registration via `setup_duckdb_cloud_auth()`
- `common/logging_utils.py` — `setup_pipeline_logger()`, `PipelineLogger`, `StageLogger`
- `common/storage_interface.py` — `StoragePath` class (supports `gs://` and `r2://`)
- `common/validation/` — CVR/CHR/BFE validators, area validators, baseline manager

## Environment Variables

Each pipeline loads its own env vars — no global config:

```python
from dotenv import load_dotenv
load_dotenv()
bucket = os.getenv("GCS_BUCKET", "landbrugsdata")
```

Cloud auth priority: `R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`/`R2_ACCOUNT_ID` → GCS HMAC → DuckDB native secrets.
Storage path resolution: `R2_BUCKET` → `GCS_BUCKET` → `"landbruget-data"` default.

## Testing

- Each pipeline has its own `tests/` and `conftest.py`
- Common fixtures in `common/tests/conftest.py`: `mock_duckdb_connection` (`:memory:` + spatial), `mock_gcs_filesystem`, `sample_danish_geometries`, `valid_cvr_numbers`, `valid_chr_numbers`
- Markers: `pre_merge` (blocking), `gcs_required` (needs credentials)
- `backend/conftest.py` manages `sys.path` for module resolution

## Common Mistakes to Avoid

- Using Pandas for large files — use DuckDB SQL instead
- Using `DELIMITER` in DuckDB — use `delim` (1.5+ breaking change)
- Bare geometry operations in DuckDB — wrap with `TRY()` for 1.5 compatibility
- Forgetting `source venv/bin/activate` before running anything
- Assuming global env loading — each pipeline manages its own `.env`
- Using WGS84 for buffer/distance — always process in EPSG:25832
- Referencing `common/gcs_utils.py` — actual path is `common/gcs/core.py` (`GCSDataAccess` class)
- Using pip — this project uses **uv** for Python package management
