# Common Library — Landbruget.dk Backend

Shared Python package (`landbruget-common`). Build system: hatchling. Lock file: `uv.lock`.
Dependencies: `pydantic>=2.0`, `s3fs>=2024.1.0`, `duckdb>=1.5.0`.

**Changes here affect every pipeline — test thoroughly.**

## Key Modules

### `duckdb_processor.py`
- `SharedDuckDBProcessor(db_path=":memory:", dataset_name="data")` — base class for all DuckDB ops
  - Auto-loads spatial + httpfs extensions
  - Methods: `table_exists()`, `create_table_from_parquet()`, `create_table_from_csv()`, `create_spatial_table()`, `create_table_from_storage_parquet()`, `save_table_to_storage_parquet()`, `get_row_count()`, `execute_query()`, `create_spatial_index()`
  - Context manager support (`with SharedDuckDBProcessor() as proc:`)
- `PipelineProcessor(SharedDuckDBProcessor)` — adds logging + `safe_execute()` + `process_with_memory_monitoring()`

### `crs_utils.py`
- Constants: `DANISH_UTM = "EPSG:25832"`, `WGS84 = "EPSG:4326"`, `WEB_MERCATOR = "EPSG:3857"`
- Aliases: `TARGET_CRS_PROCESSING = DANISH_UTM`, `TARGET_CRS_SUPABASE = WGS84`
- Bounds: `DENMARK_BOUNDS_WGS84`, `DENMARK_BOUNDS_UTM`
- Detection: `detect_crs_from_bounds()`, `is_utm_coordinate_range()`, `is_wgs84_coordinate_range()`
- Validation: `validate_crs_before_transform(conn, table, geom_col, expected_crs)`
- SQL helpers: `sql_transform_to_utm()`, `sql_transform_to_wgs84()`, `sql_buffer_meters()`, `sql_transform_for_supabase()`, `sql_transform_to_processing_crs()`

### `storage/core.py` — `StorageAccess`
- Initializes s3fs + DuckDB fsspec registration
- Two-tier cloud auth: native HMAC first, then s3fs fallback
- DuckDB config: 12GB memory limit, 4 threads
- `ResourceMonitor` for GitHub Actions memory tracking
- Retry logic via tenacity with exponential backoff

### `storage/filesystem.py`
- `get_r2_filesystem()` → `s3fs.S3FileSystem` for Cloudflare R2 (cached singleton)
- `setup_duckdb_cloud_auth(conn)` → configures DuckDB for cloud access (TYPE r2 secret, fallback to fsspec)
- Env vars: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`

### `storage_interface.py` — `StoragePath`
- `StoragePath(bucket=None)` — builds medallion-layer paths
- Methods: `.bronze(source, *parts)`, `.silver(source, *parts)`, `.gold(source, *parts)`, `.raw(*parts)`
- Bucket resolution: `STORAGE_BUCKET` → `R2_BUCKET` → `GCS_BUCKET` → `"landbruget-data"`
- `CloudStorage(bucket_name)` — wraps StorageAccess for `save_json()`, `save_parquet()`, `read_json()`
- `LocalStorage(base_dir)` — file-based alternative for dev

### `validation/`
- `identifier_validators.py` — CVR (`^\d{8}$`), CHR (`^\d{6}$`), BFE format validators + `CVRValidator`, `CHRValidator` classes
- `area_validator.py` — `FieldAreaValidator` compares field areas between pipeline stages
- `baseline_manager.py` — `BaselineManager` + `BaselineMetrics` for tracking data quality over time
- `report_generator.py` — `ValidationReportGenerator` for structured quality reports

### `logging_utils.py`
- `setup_pipeline_logger(name, level="INFO")` + `get_pipeline_logger(pipeline_name)`
- Context managers: `PipelineLogger`, `StageLogger` (with `log_progress()`)

## Testing

```bash
cd backend && source venv/bin/activate && python -m pytest common/
```

Fixtures in `common/tests/conftest.py`:
- `mock_duckdb_connection` — in-memory DuckDB with spatial extension
- `mock_cloud_filesystem` — mocked cloud storage
- `sample_danish_geometries` — test geometries in EPSG:25832
- `valid_cvr_numbers`, `valid_chr_numbers` — format-valid test identifiers
- `mock_supabase_client`, `cvr_validator`, `chr_validator`, `cvr_formatter`, `chr_formatter`

## Gotchas

- Module path is `common.storage.core` NOT `common.gcs_utils` (legacy name in some docs)
- DuckDB connection MUST load spatial extension before geometry operations (auto-loaded by `SharedDuckDBProcessor`)
- `StorageAccess` import fails gracefully if `unified_pipeline` modules not available
- Cloud auth tries native DuckDB R2 secret first, falls back to s3fs fsspec
- Package managed with **uv** (not pip) — `uv.lock` in this directory
