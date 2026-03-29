#!/usr/bin/env python3
"""
Generate baseline metrics for data quality validation.

This script generates baseline metrics from production data in cloud storage (R2),
which are then used for pre-merge validation to detect data regressions.

Usage:
    python generate_baseline_metrics.py --datasets all --output-path bucket/baselines/main/
    python generate_baseline_metrics.py --datasets fvm_marker,chr_herds --output-path ./baselines/

Run on main branch after significant data updates to refresh baselines.
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "common"))
sys.path.insert(0, str(Path(__file__).parent.parent / "pipelines" / "unified_pipeline" / "src"))

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Dataset configurations for baseline generation
# Organized by category for clarity
DATASET_CONFIGS = {
    # ==================== SILVER - Core Agricultural ====================
    "agricultural_fields_2024": {
        "storage_path": "silver/agricultural_fields_2024",
        "area_column": "area_ha",
        "identifier_columns": ["field_id", "journal_number", "cvr_number"],
        "geometry_column": "geometry",
    },
    "agricultural_fields_2025": {
        "storage_path": "silver/agricultural_fields_2025",
        "area_column": "area_ha",
        "identifier_columns": ["field_id", "journal_number", "cvr_number"],
        "geometry_column": "geometry",
    },
    "fvm_marker_2024": {
        "storage_path": "silver/fvm_marker_2024",
        "area_column": "area_ha",
        "identifier_columns": ["field_id", "cvr_number"],
        "geometry_column": "geometry",
    },
    "fvm_marker_2025": {
        "storage_path": "silver/fvm_marker_2025",
        "area_column": "area_ha",
        "identifier_columns": ["field_id", "cvr_number"],
        "geometry_column": "geometry",
    },
    # ==================== SILVER - Environment ====================
    "bnbo_status": {
        "storage_path": "silver/bnbo_status",
        "area_column": None,  # Will detect from schema
        "identifier_columns": [],
        "geometry_column": "geometry",
    },
    "wetlands": {
        "storage_path": "silver/wetlands",
        "area_column": None,
        "identifier_columns": [],
        "geometry_column": "geometry",
    },
    "grukos": {
        "storage_path": "silver/grukos",
        "area_column": None,
        "identifier_columns": [],
        "geometry_column": "geometry",
    },
    "water_projects": {
        "storage_path": "silver/water_projects",
        "area_column": None,
        "identifier_columns": [],
        "geometry_column": "geometry",
    },
    # ==================== SILVER - Property/Cadastral ====================
    "cadastral": {
        "storage_path": "silver/cadastral",
        "area_column": None,
        "identifier_columns": ["bfe_number"],
        "geometry_column": "geometry",
    },
    "property_owners": {
        "storage_path": "silver/property_owners",
        "area_column": None,
        "identifier_columns": ["bfe_nummer", "cvr_nummer"],
        "geometry_column": None,
    },
    # ==================== SILVER - Livestock/CHR ====================
    "svineflytning": {
        "storage_path": "silver/svineflytning",
        "area_column": None,
        "identifier_columns": ["chr_afsender", "chr_modtager"],
        "geometry_column": None,
    },
    # ==================== SILVER - Administrative ====================
    "dagi_kommuner": {
        "storage_path": "silver/dagi_kommuner",
        "area_column": None,
        "identifier_columns": ["kommunekode"],
        "geometry_column": "geometry",
    },
    # ==================== SILVER - Subsidies ====================
    "subsidies": {
        "storage_path": "silver/subsidies",
        "area_column": None,
        "identifier_columns": ["cvr"],
        "geometry_column": None,
    },
    # ==================== GOLD - CVR Enrichment ====================
    "cvr_enrichment": {
        "storage_path": "gold/cvr_enrichment",
        "area_column": None,
        "identifier_columns": ["cvr"],
        "geometry_column": None,
        "file_pattern": "*.json",  # Uses JSON not parquet
    },
    # ==================== GOLD - Field Analysis ====================
    "field_analysis_field_bnbo_2024": {
        "storage_path": "gold/field_analysis_field_bnbo_intersections_2024",
        "area_column": "field_area_m2",
        "identifier_columns": ["field_uuid"],
        "geometry_column": None,
    },
    "field_analysis_field_bnbo_2025": {
        "storage_path": "gold/field_analysis_field_bnbo_intersections_2025",
        "area_column": "field_area_m2",
        "identifier_columns": ["field_uuid"],
        "geometry_column": None,
    },
    "field_analysis_field_wetland_2024": {
        "storage_path": "gold/field_analysis_field_wetland_intersections_2024",
        "area_column": "field_area_m2",
        "identifier_columns": ["field_uuid"],
        "geometry_column": None,
    },
    "field_analysis_field_wetland_2025": {
        "storage_path": "gold/field_analysis_field_wetland_intersections_2025",
        "area_column": "field_area_m2",
        "identifier_columns": ["field_uuid"],
        "geometry_column": None,
    },
    # ==================== GOLD - Pesticides ====================
    "pesticide_compliance": {
        "storage_path": "gold/pesticide_compliance",
        "area_column": None,
        "identifier_columns": ["cvr"],
        "geometry_column": None,
    },
    # ==================== GOLD - Nitrogen ====================
    "nles5_nitrogen_estimation": {
        "storage_path": "gold/nles5_nitrogen_estimation_nitrogen_estimates",
        "area_column": "field_area_m2",
        "identifier_columns": ["field_uuid"],
        "geometry_column": None,
    },
    # ==================== GOLD - Worker Safety ====================
    "worker_safety": {
        "storage_path": "gold/worker_safety",
        "area_column": None,
        "identifier_columns": ["cvr"],
        "geometry_column": None,
    },
    "arbejdstilsynet_inspections": {
        "storage_path": "gold/arbejdstilsynet_inspections",
        "area_column": None,
        "identifier_columns": ["cvr"],
        "geometry_column": None,
    },
}


def get_latest_version(storage_path: str, storage_bucket: str) -> str | None:
    """Find the latest version directory in cloud storage."""
    from common.storage.filesystem import get_r2_filesystem

    fs = get_r2_filesystem()
    full_path = f"{storage_bucket}/{storage_path}"

    try:
        # List directories (versions are typically YYYYMMDD_HHMMSS format)
        dirs = fs.ls(full_path)
        dirs = [d for d in dirs if fs.isdir(d)]

        if not dirs:
            return None

        # Sort by name (which should be timestamp-based) and get latest
        dirs.sort(reverse=True)
        return dirs[0]
    except Exception as e:
        logger.warning(f"Could not list {full_path}: {e}")
        return None


def generate_metrics_for_dataset(
    dataset_name: str,
    config: dict,
    storage_bucket: str,
    conn: duckdb.DuckDBPyConnection,
) -> dict | None:
    """Generate baseline metrics for a single dataset."""
    import tempfile

    from common.storage.filesystem import get_r2_filesystem

    logger.info(f"Generating metrics for {dataset_name}...")

    # Find latest version
    latest_path = get_latest_version(config["storage_path"], storage_bucket)
    if not latest_path:
        logger.warning(f"No data found for {dataset_name}")
        return None

    fs = get_r2_filesystem()

    # Find parquet files
    parquet_files = [f for f in fs.ls(latest_path) if f.endswith(".parquet")]

    if not parquet_files:
        logger.warning(f"No parquet files found in {latest_path}")
        return None

    # Download to temp file and load into DuckDB
    storage_path = parquet_files[0]
    logger.info(f"Loading from {storage_path}")

    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
            fs.get(storage_path, tmp_path)

        # Clean dataset name for SQL (replace hyphens with underscores)
        sql_table_name = dataset_name.replace("-", "_")

        conn.execute(f"""
            CREATE OR REPLACE TABLE {sql_table_name} AS
            SELECT * FROM read_parquet('{tmp_path}')
        """)

        # Clean up temp file
        Path(tmp_path).unlink()
    except Exception as e:
        logger.error(f"Failed to load {dataset_name}: {e}")
        if "tmp_path" in locals() and Path(tmp_path).exists():
            Path(tmp_path).unlink()
        return None

    # Use sql_table_name for queries
    table_name = sql_table_name

    # Generate metrics
    metrics = {
        "dataset_name": dataset_name,
        "source_path": storage_path,
        "generated_at": datetime.utcnow().isoformat(),
    }

    # Record count
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    metrics["record_count"] = count
    logger.info(f"  Record count: {count:,}")

    # Area total
    if config.get("area_column"):
        area_col = config["area_column"]
        try:
            result = conn.execute(f"""
                SELECT COALESCE(SUM({area_col}), 0) FROM {table_name}
                WHERE {area_col} IS NOT NULL AND {area_col} > 0
            """).fetchone()
            metrics["total_area_m2"] = float(result[0])
            logger.info(f"  Total area: {metrics['total_area_m2']:,.0f} m²")
        except Exception as e:
            logger.warning(f"  Could not calculate area: {e}")
            metrics["total_area_m2"] = None

    # Identifier counts
    identifier_counts = {}
    for id_col in config.get("identifier_columns", []):
        try:
            result = conn.execute(f"""
                SELECT COUNT(DISTINCT {id_col}) FROM {table_name}
                WHERE {id_col} IS NOT NULL
            """).fetchone()
            identifier_counts[id_col] = int(result[0])
            logger.info(f"  Unique {id_col}: {identifier_counts[id_col]:,}")
        except Exception as e:
            logger.warning(f"  Could not count {id_col}: {e}")
    metrics["unique_identifiers"] = identifier_counts

    # Geometry bounds and CRS detection
    if config.get("geometry_column"):
        geom_col = config["geometry_column"]
        try:
            # First check if it's already a GEOMETRY type or needs casting from WKT
            col_type = conn.execute(f"""
                SELECT data_type FROM information_schema.columns
                WHERE table_name = '{table_name}' AND column_name = '{geom_col}'
            """).fetchone()

            # WKT string - need to cast to geometry if VARCHAR, else native geometry type
            geom_expr = f"ST_GeomFromText({geom_col})" if col_type and col_type[0] == "VARCHAR" else geom_col

            result = conn.execute(f"""
                SELECT
                    MIN(ST_XMin({geom_expr})) as min_x,
                    MAX(ST_XMax({geom_expr})) as max_x,
                    MIN(ST_YMin({geom_expr})) as min_y,
                    MAX(ST_YMax({geom_expr})) as max_y
                FROM {table_name}
                WHERE {geom_col} IS NOT NULL
            """).fetchone()

            if result[0] is not None:
                metrics["geometry_bounds"] = {
                    "min_x": float(result[0]),
                    "max_x": float(result[1]),
                    "min_y": float(result[2]),
                    "max_y": float(result[3]),
                }

                # Detect CRS
                from common.crs_utils import detect_crs_from_bounds

                detected_crs, _ = detect_crs_from_bounds(result[0], result[1], result[2], result[3])
                metrics["detected_crs"] = detected_crs
                logger.info(f"  CRS: {detected_crs}")
                logger.info(f"  Bounds: X=[{result[0]:.2f}, {result[1]:.2f}], Y=[{result[2]:.2f}, {result[3]:.2f}]")
        except Exception as e:
            logger.warning(f"  Could not extract geometry bounds: {e}")

    # Column null counts (sample)
    try:
        columns = conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()

        null_counts = {}
        for (col,) in columns[:20]:  # Limit to first 20 columns
            result = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE "{col}" IS NULL
            """).fetchone()
            null_counts[col] = int(result[0])
        metrics["column_null_counts"] = null_counts
    except Exception as e:
        logger.warning(f"  Could not count nulls: {e}")
        metrics["column_null_counts"] = {}

    return metrics


def save_metrics(metrics: dict, output_path: str) -> str:
    """Save metrics to local or cloud storage path."""
    dataset_name = metrics["dataset_name"]

    # Detect cloud paths: protocol prefix or bare bucket/path (not local filesystem)
    _is_local = output_path.startswith(("/", "."))
    _has_protocol = output_path.startswith(("r2://", "s3://"))
    if _has_protocol or (not _is_local and "/" in output_path):
        from common.storage.filesystem import get_r2_filesystem

        fs = get_r2_filesystem()
        # Strip legacy protocol prefix if present
        clean_path = output_path
        for prefix in ("r2://", "s3://"):
            if clean_path.startswith(prefix):
                clean_path = clean_path[len(prefix) :]
                break
        file_path = f"{clean_path}/{dataset_name}/metrics.json"

        with fs.open(file_path, "w") as f:
            json.dump(metrics, f, indent=2)
    else:
        local_path = Path(output_path) / dataset_name / "metrics.json"
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with local_path.open("w") as f:
            json.dump(metrics, f, indent=2)

        file_path = str(local_path)

    logger.info(f"Saved metrics to {file_path}")
    return file_path


def main():
    parser = argparse.ArgumentParser(description="Generate baseline metrics for data validation")
    parser.add_argument(
        "--datasets",
        type=str,
        default="all",
        help="Comma-separated list of datasets or 'all'",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="Output path (local or r2://...)",
    )
    parser.add_argument(
        "--storage-bucket",
        type=str,
        default="landbruget-data",
        help="Storage bucket name",
    )
    parser.add_argument(
        "--commit-sha",
        type=str,
        default=None,
        help="Git commit SHA to record in baseline",
    )
    args = parser.parse_args()

    # Determine which datasets to process
    datasets = list(DATASET_CONFIGS.keys()) if args.datasets == "all" else [d.strip() for d in args.datasets.split(",")]

    # Validate datasets
    for ds in datasets:
        if ds not in DATASET_CONFIGS:
            logger.error(f"Unknown dataset: {ds}")
            logger.info(f"Available datasets: {', '.join(DATASET_CONFIGS.keys())}")
            sys.exit(1)

    logger.info(f"Generating baselines for: {', '.join(datasets)}")
    logger.info(f"Output path: {args.output_path}")

    # Create DuckDB connection with spatial extension
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    # Set up cloud storage access
    conn.execute("SET s3_region='auto'")

    # Generate metrics for each dataset
    results = {}
    for dataset_name in datasets:
        config = DATASET_CONFIGS[dataset_name]
        metrics = generate_metrics_for_dataset(dataset_name, config, args.storage_bucket, conn)

        if metrics:
            # Add commit SHA if provided
            if args.commit_sha:
                metrics["source_commit"] = args.commit_sha

            # Save metrics
            save_metrics(metrics, args.output_path)
            results[dataset_name] = "success"
        else:
            results[dataset_name] = "failed"

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("BASELINE GENERATION SUMMARY")
    logger.info("=" * 50)

    success_count = sum(1 for v in results.values() if v == "success")
    fail_count = sum(1 for v in results.values() if v == "failed")

    for dataset, status in results.items():
        emoji = "✅" if status == "success" else "❌"
        logger.info(f"  {emoji} {dataset}: {status}")

    logger.info(f"\nTotal: {success_count} succeeded, {fail_count} failed")

    # Exit with error if any failed
    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
