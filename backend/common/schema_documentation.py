"""
Schema Documentation System for All Pipelines.

This module provides schema documentation capabilities that can be integrated
into any pipeline type (unified, legacy, or standalone). It leverages DuckDB's
native schema introspection to generate comprehensive documentation.
"""

import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb


class SchemaDocumentationManager:
    """Manages schema documentation generation and GitHub integration for any pipeline."""

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        pipeline_name: str,
        pipeline_start_time: Optional[datetime] = None,
        logger: Optional[Any] = None,
    ) -> None:
        """
        Initialize schema documentation manager.

        Args:
            connection: DuckDB connection
            pipeline_name: Name of the pipeline (e.g., 'chr_pipeline', 'bbr_buildings')
            pipeline_start_time: When the pipeline started (defaults to now)
            logger: Logger instance (optional)
        """
        self.conn = connection
        self.pipeline_name = pipeline_name
        self.pipeline_start_time = pipeline_start_time or datetime.now()
        self.logger = logger

        # Standardized path format for all pipelines
        self.schemas_base_dir = Path("docs/schemas")
        self.schemas_base_dir.mkdir(parents=True, exist_ok=True)

        if self.logger:
            self.logger.info(f"Schema documentation initialized for {pipeline_name}")

    def get_table_schema_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive schema information for a table using DuckDB native functions.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with schema information
        """
        try:
            schema_info = {
                "table_name": table_name,
                "generated_at": self.pipeline_start_time.isoformat(),
                "pipeline": self.pipeline_name,
            }

            # Basic schema using DESCRIBE
            describe_query = f"DESCRIBE {table_name}"
            describe_results = self.conn.execute(describe_query).fetchall()

            columns = []
            for row in describe_results:
                col_name, col_type, nullable, key, default, extra = row
                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "nullable": nullable == "YES",
                        "key": key,
                        "default": default,
                        "extra": extra,
                    }
                )

            schema_info["columns"] = columns

            # Row count
            try:
                count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                schema_info["row_count"] = count_result[0] if count_result else 0
            except Exception:
                schema_info["row_count"] = None

            # Table size (if available)
            try:
                size_query = f"SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))"
                size_result = self.conn.execute(size_query).fetchone()
                schema_info["table_size"] = size_result[0] if size_result else None
            except Exception:
                schema_info["table_size"] = None

            # Summary statistics using SUMMARIZE (if table has data)
            if schema_info["row_count"] and schema_info["row_count"] > 0:
                try:
                    summarize_query = f"SUMMARIZE {table_name}"
                    summarize_results = self.conn.execute(summarize_query).fetchall()

                    column_stats = {}
                    for row in summarize_results:
                        col_name = row[0]
                        column_stats[col_name] = {
                            "type": row[1],
                            "min": row[2],
                            "max": row[3],
                            "approx_unique": row[4],
                            "avg": row[5],
                            "std": row[6],
                            "q25": row[7],
                            "q50": row[8],
                            "q75": row[9],
                            "count": row[10],
                            "null_percentage": row[11],
                        }

                    schema_info["column_statistics"] = column_stats
                except Exception as e:
                    if self.logger:
                        self.logger.warning(f"Could not generate statistics for {table_name}: {e}")
                    schema_info["column_statistics"] = None

            return schema_info

        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to get schema info for {table_name}: {e}")
            return {
                "table_name": table_name,
                "generated_at": self.pipeline_start_time.isoformat(),
                "pipeline": self.pipeline_name,
                "error": str(e),
            }

    def generate_table_documentation(self, table_name: str, stage: str = "processed") -> str:
        """
        Generate markdown documentation for a single table.

        Args:
            table_name: Name of the table
            stage: Processing stage name

        Returns:
            Path to the generated markdown file
        """
        schema_info = self.get_table_schema_info(table_name)

        # Generate markdown content
        lines = [
            f"# {table_name}",
            "",
            f"**Pipeline:** {self.pipeline_name}",
            f"**Stage:** {stage}",
            f"**Generated:** {self.pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        if "error" in schema_info:
            lines.extend(["## Error", "", f"Could not generate schema documentation: {schema_info['error']}", ""])
        else:
            # Table overview
            lines.extend(
                [
                    "## Table Overview",
                    "",
                    f"- **Row count:** {schema_info['row_count']:,}"
                    if schema_info["row_count"]
                    else "- **Row count:** Unknown",
                    f"- **Columns:** {len(schema_info['columns'])}",
                ]
            )

            if schema_info.get("table_size"):
                lines.append(f"- **Size:** {schema_info['table_size']}")

            lines.append("")

            # Column definitions
            lines.extend(
                [
                    "## Columns",
                    "",
                    "| Column | Type | Nullable | Key | Default |",
                    "|--------|------|----------|-----|---------|",
                ]
            )

            for col in schema_info["columns"]:
                nullable = "✓" if col["nullable"] else "✗"
                key = col["key"] or ""
                default = col["default"] or ""
                lines.append(f"| {col['name']} | {col['type']} | {nullable} | {key} | {default} |")

            lines.append("")

            # Column statistics (if available)
            if schema_info.get("column_statistics"):
                lines.extend(
                    [
                        "## Column Statistics",
                        "",
                        "| Column | Min | Max | Unique | Avg | Null % |",
                        "|--------|-----|-----|--------|-----|--------|",
                    ]
                )

                for col_name, stats in schema_info["column_statistics"].items():
                    min_val = stats["min"] if stats["min"] is not None else ""
                    max_val = stats["max"] if stats["max"] is not None else ""
                    unique = stats["approx_unique"] if stats["approx_unique"] is not None else ""

                    # Handle avg formatting safely
                    try:
                        avg = f"{float(stats['avg']):.2f}" if stats["avg"] is not None else ""
                    except (ValueError, TypeError):
                        avg = str(stats["avg"]) if stats["avg"] is not None else ""

                    # Handle null percentage formatting safely
                    try:
                        null_pct = (
                            f"{float(stats['null_percentage']):.1f}%" if stats["null_percentage"] is not None else ""
                        )
                    except (ValueError, TypeError):
                        null_pct = str(stats["null_percentage"]) if stats["null_percentage"] is not None else ""

                    lines.append(f"| {col_name} | {min_val} | {max_val} | {unique} | {avg} | {null_pct} |")

                lines.append("")

                # Create stage directory
        stage_dir = self.schemas_base_dir / stage
        stage_dir.mkdir(parents=True, exist_ok=True)

        # Save the file with table name (latest schema only)
        doc_file = stage_dir / f"{table_name}.md"
        with open(doc_file, "w") as f:
            f.write("\n".join(lines))

        if self.logger:
            self.logger.info(f"Documentation generated for {table_name}: {doc_file}")

        return str(doc_file)

    def generate_overview_documentation(self, tables: List[str], stage: str = "processed") -> str:
        """
        Generate overview documentation with all table schemas.

        Args:
            tables: List of table names
            stage: Processing stage name

        Returns:
            Path to the generated overview file
        """
        lines = [
            f"# {self.pipeline_name.title()} Schema Documentation",
            "",
            f"**Pipeline:** {self.pipeline_name}",
            f"**Stage:** {stage}",
            f"**Generated:** {self.pipeline_start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This document contains schema information for all tables in this pipeline.",
            "",
            "## Available Tables",
            "",
        ]

        # Add table list with links
        for table in tables:
            lines.append(f"- [{table}](#{table.lower().replace('_', '-')})")

        lines.append("")

        # Add detailed schema for each table
        for table in tables:
            schema_info = self.get_table_schema_info(table)

            lines.extend([f"## {table}", ""])

            if "error" in schema_info:
                lines.extend([f"*Error retrieving schema: {schema_info['error']}*", ""])
                continue

            # Table summary
            row_count = schema_info["row_count"]
            row_count_str = f"{row_count:,}" if row_count else "Unknown"

            lines.extend(
                [
                    "```sql",
                    f"-- Table: {table}",
                    f"-- Rows: {row_count_str}",
                    f"-- Columns: {len(schema_info['columns'])}",
                    "--",
                    "-- Schema:",
                ]
            )

            for col in schema_info["columns"]:
                nullable_str = "NULL" if col["nullable"] else "NOT NULL"
                key_str = f" ({col['key']})" if col["key"] else ""
                default_str = f" DEFAULT {col['default']}" if col["default"] else ""

                lines.append(f"--   {col['name']}: {col['type']} {nullable_str}{key_str}{default_str}")

            lines.extend(["```", ""])

        # Create stage directory
        stage_dir = self.schemas_base_dir / stage
        stage_dir.mkdir(parents=True, exist_ok=True)

        # Save overview file
        overview_file = stage_dir / "overview.md"
        with open(overview_file, "w") as f:
            f.write("\n".join(lines))

        if self.logger:
            self.logger.info(f"Overview documentation generated: {overview_file}")

        return str(overview_file)

    def generate_all_documentation(self, tables: List[str], stage: str = "processed") -> List[str]:
        """
        Generate documentation for all tables and create overview.

        Args:
            tables: List of table names
            stage: Processing stage name (should NOT be 'bronze' due to memory constraints)

        Returns:
            List of paths to generated files

        Raises:
            ValueError: If stage is 'bronze' (to prevent memory issues with large raw datasets)
        """
        # Safeguard against bronze documentation
        if stage.lower() == "bronze":
            raise ValueError(
                "Schema documentation for 'bronze' stage is not supported due to memory constraints. "
                "Bronze layers often contain massive raw datasets that would cause memory issues. "
                "Use 'silver', 'gold', 'processed', or other stages instead."
            )
        generated_files = []

        # Generate individual table docs
        for table in tables:
            doc_file = self.generate_table_documentation(table, stage)
            generated_files.append(doc_file)

        # Generate overview with all table schemas
        overview_file = self.generate_overview_documentation(tables, stage)
        generated_files.append(overview_file)

        # Create stage directory
        stage_dir = self.schemas_base_dir / stage
        stage_dir.mkdir(parents=True, exist_ok=True)

        # Save JSON metadata
        metadata = {
            "pipeline": self.pipeline_name,
            "stage": stage,
            "generated_at": self.pipeline_start_time.isoformat(),
            "tables": tables,
            "files_generated": generated_files,
        }

        metadata_file = stage_dir / "metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        generated_files.append(str(metadata_file))

        if self.logger:
            self.logger.info(f"Generated {len(generated_files)} documentation files for {self.pipeline_name}")

        return generated_files

    def commit_to_github(self, commit_message: Optional[str] = None) -> bool:
        """
        Commit generated schema files to GitHub.

        Args:
            commit_message: Custom commit message

        Returns:
            True if successful, False otherwise
        """
        if not commit_message:
            timestamp = self.pipeline_start_time.strftime("%Y-%m-%d %H:%M:%S")
            commit_message = f"Update schema documentation for {self.pipeline_name} ({timestamp})"

        try:
            # Configure git identity if not already set
            try:
                result = subprocess.run(["git", "config", "user.name"], check=True, capture_output=True, text=True)
                user_name = result.stdout.strip()
                if not user_name:
                    raise subprocess.CalledProcessError(1, "git config user.name")
            except subprocess.CalledProcessError:
                # No user.name configured or empty, set appropriate identity
                import os

                if os.getenv("GITHUB_ACTIONS"):
                    # Running in GitHub Actions
                    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
                    subprocess.run(
                        ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True
                    )
                    if self.logger:
                        self.logger.info("Configured git identity for GitHub Actions")
                else:
                    # Running locally or elsewhere, use a generic identity
                    subprocess.run(["git", "config", "user.name", "Pipeline Bot"], check=True)
                    subprocess.run(["git", "config", "user.email", "pipeline-bot@landbruget.dk"], check=True)
                    if self.logger:
                        self.logger.info("Configured generic git identity for pipeline")

            # Add schema files to git
            subprocess.run(["git", "add", str(self.schemas_base_dir)], check=True)

            # Commit changes
            subprocess.run(["git", "commit", "-m", commit_message], check=True)

            # Push to GitHub
            subprocess.run(["git", "push"], check=True)

            if self.logger:
                self.logger.info(f"Schema documentation committed to GitHub: {commit_message}")

            return True

        except subprocess.CalledProcessError as e:
            if self.logger:
                self.logger.error(f"Failed to commit to GitHub: {e}")
            return False


class SchemaDocumentationMixin:
    """Mixin to add schema documentation to any pipeline class."""

    def init_schema_documentation(
        self, pipeline_name: str, pipeline_start_time: Optional[datetime] = None, enable_auto_commit: bool = False
    ) -> None:
        """
        Initialize schema documentation for this pipeline.

        Args:
            pipeline_name: Name of the pipeline
            pipeline_start_time: When the pipeline started
            enable_auto_commit: Whether to automatically commit to GitHub
        """
        self._schema_doc_manager = SchemaDocumentationManager(
            connection=self.conn,
            pipeline_name=pipeline_name,
            pipeline_start_time=pipeline_start_time,
            logger=getattr(self, "log", None),
        )
        self._auto_commit_schemas = enable_auto_commit

    def document_table_schema(self, table_name: str, stage: str = "processed") -> str:
        """Generate documentation for a single table."""
        if not hasattr(self, "_schema_doc_manager"):
            raise RuntimeError("Schema documentation not initialized. Call init_schema_documentation() first.")

        return self._schema_doc_manager.generate_table_documentation(table_name, stage)

    def document_all_schemas(self, tables: List[str], stage: str = "processed") -> List[str]:
        """Generate documentation for all tables."""
        if not hasattr(self, "_schema_doc_manager"):
            raise RuntimeError("Schema documentation not initialized. Call init_schema_documentation() first.")

        files = self._schema_doc_manager.generate_all_documentation(tables, stage)

        # Auto-commit if enabled
        if self._auto_commit_schemas:
            self._schema_doc_manager.commit_to_github()

        return files
