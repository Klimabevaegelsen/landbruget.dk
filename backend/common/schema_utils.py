"""Shared schema documentation generation for pipelines.

Wraps SchemaDocumentationManager to eliminate boilerplate in each pipeline.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb


def generate_schema_docs(
    parquet_path: str | Path,
    pipeline_name: str,
    table_name: str,
    pipeline_start_time: datetime,
    stage: str = "silver",
    commit: bool = True,
    logger: logging.Logger | None = None,
) -> bool:
    """Generate and optionally commit schema documentation for a parquet file.

    Args:
        parquet_path: Path to the parquet file to document.
        pipeline_name: Name of the pipeline (e.g. "bmd_scraper").
        table_name: DuckDB table name to use (e.g. "bmd_processed").
        pipeline_start_time: When the pipeline run started.
        stage: Medallion stage ("bronze", "silver", "gold").
        commit: Whether to commit docs to GitHub.
        logger: Optional logger instance.

    Returns:
        True if docs were generated successfully, False otherwise.
    """
    log = logger or logging.getLogger(__name__)

    try:
        # Ensure project root is on sys.path for the import
        current_file = Path(__file__).resolve()
        for parent in current_file.parents:
            if (parent / "backend").is_dir():
                if str(parent) not in sys.path:
                    sys.path.insert(0, str(parent))
                break

        from backend.common.schema_documentation import SchemaDocumentationManager

        parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            log.warning(f"Parquet file not found: {parquet_path}")
            return False

        conn = duckdb.connect()
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")

        schema_manager = SchemaDocumentationManager(
            connection=conn,
            pipeline_name=pipeline_name,
            pipeline_start_time=pipeline_start_time,
            logger=logger,
        )

        schema_manager.generate_all_documentation([table_name], stage=stage)
        log.info(f"Generated schema documentation for {pipeline_name}")

        if commit:
            schema_manager.commit_to_github()
            log.info(f"{pipeline_name} schema documentation committed to GitHub")

        conn.close()
        return True

    except ImportError as e:
        log.warning(f"Schema documentation not available: {e}")
        return False
    except Exception as e:
        log.error(f"Failed to generate schema documentation: {e}", exc_info=True)
        return False
