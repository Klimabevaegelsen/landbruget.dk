"""
Schema Documentation and GitHub Integration for Unified Pipeline.

This module automatically generates comprehensive schema documentation files
and commits them to the GitHub repository to provide up-to-date documentation
for all pipeline datasets.
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import duckdb

from unified_pipeline.common.native_schema_manager import NativeSchemaManager
from unified_pipeline.util.log_util import Logger


class SchemaToGitHub:
    """Manages automatic schema generation and GitHub commits."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, logger: Optional[Logger] = None):
        self.conn = connection
        self.logger = logger or Logger(__name__)
        self.schema_manager = NativeSchemaManager(connection, logger)

        # GitHub integration settings
        self.schemas_dir = Path("docs/schemas")
        self.schemas_dir.mkdir(parents=True, exist_ok=True)

    def generate_table_schema_doc(self, table_name: str, stage: str = "silver") -> str:
        """
        Generate a markdown documentation file for a table schema.

        Args:
            table_name: Name of the table
            stage: Processing stage (bronze, silver, gold)

        Returns:
            Markdown content as string
        """
        try:
            # Get comprehensive schema info
            schema_info = self.schema_manager.get_table_schema(table_name, include_summary=True)

            # Generate markdown content
            md_lines = [
                f"# {table_name} ({stage.title()} Layer)",
                "",
                f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "",
                "## Table Information",
                "",
                f"- **Table Name:** `{table_name}`",
                f"- **Stage:** {stage}",
                f"- **Estimated Rows:** {schema_info.get('basic_info', {}).get('estimated_size', 'Unknown'):,}",
                f"- **Column Count:** {schema_info.get('basic_info', {}).get('column_count', 'Unknown')}",
                "",
                "## Schema",
                "",
                "| Column | Type | Nullable | Default | Description |",
                "|--------|------|----------|---------|-------------|",
            ]

            # Add column information
            for col in schema_info.get("columns", []):
                nullable = "✓" if col.get("nullable") else "✗"
                default = col.get("default") or ""
                md_lines.append(
                    f"| `{col['name']}` | `{col['data_type']}` | {nullable} | {default} | |"
                )

            # Add data summary if available
            if "data_summary" in schema_info and schema_info["data_summary"]:
                md_lines.extend(
                    [
                        "",
                        "## Data Summary",
                        "",
                        "| Column | Min | Max | Unique Values | Null % |",
                        "|--------|-----|-----|---------------|--------|",
                    ]
                )

                for summary_row in schema_info["data_summary"]:
                    col_name = summary_row.get("column_name", "")
                    min_val = str(summary_row.get("min", ""))[:20] + (
                        "..." if len(str(summary_row.get("min", ""))) > 20 else ""
                    )
                    max_val = str(summary_row.get("max", ""))[:20] + (
                        "..." if len(str(summary_row.get("max", ""))) > 20 else ""
                    )
                    unique_count = summary_row.get("approx_unique", "")
                    null_pct = summary_row.get("null_percentage", "")

                    md_lines.append(
                        f"| `{col_name}` | {min_val} | {max_val} | {unique_count} | {null_pct} |"
                    )

            # Add constraints if any
            if schema_info.get("constraints"):
                md_lines.extend(["", "## Constraints", ""])
                for constraint in schema_info["constraints"]:
                    md_lines.append(f"- **{constraint['type']}:** {constraint['definition']}")

            # Add usage example
            md_lines.extend(
                [
                    "",
                    "## Usage Example",
                    "",
                    "```sql",
                    f"-- Query the {table_name} table",
                    f"SELECT * FROM {table_name}",
                    "LIMIT 10;",
                    "",
                    "-- Get row count",
                    f"SELECT COUNT(*) FROM {table_name};",
                    "",
                    "-- Basic statistics",
                    f"SUMMARIZE {table_name};",
                    "```",
                    "",
                    "---",
                    "*Schema documentation auto-generated for LLM context*",
                ]
            )

            return "\n".join(md_lines)

        except Exception as e:
            self.logger.error(f"Failed to generate schema doc for {table_name}: {e}")
            return f"# {table_name}\n\nError generating schema documentation: {e}"

    def generate_schema_overview_file(self, tables: List[str], stage: str = "silver") -> str:
        """
        Generate a comprehensive overview file with all table schemas.

        Args:
            tables: List of table names
            stage: Processing stage

        Returns:
            Path to the generated overview file
        """
        context_lines = [
            "# Pipeline Schema Documentation",
            "",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Stage:** {stage}",
            "",
            "This file contains schema information for all available tables in the pipeline.",
            "Use this as reference when building gold layer pipelines or analyzing data.",
            "",
            "## Available Tables",
            "",
        ]

        # Add table list
        for table in tables:
            context_lines.append(f"- [{table}](#{table.lower().replace('_', '-')})")

        context_lines.append("")

        # Add detailed schema for each table
        for table in tables:
            try:
                # Get basic schema using DuckDB's DESCRIBE
                query = f"DESCRIBE {table}"
                results = self.conn.execute(query).fetchall()

                context_lines.extend(
                    [f"## {table}", "", "```sql", f"-- Table: {table}", "-- Columns:"]
                )

                for row in results:
                    col_name, col_type, nullable, key, default, extra = row
                    nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                    key_str = f" ({key})" if key else ""
                    default_str = f" DEFAULT {default}" if default else ""

                    context_lines.append(
                        f"--   {col_name}: {col_type} {nullable_str}{key_str}{default_str}"
                    )

                # Add row count
                try:
                    count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    if count_result:
                        context_lines.append(f"-- Row count: {count_result[0]:,}")
                except:
                    pass

                context_lines.extend(["```", ""])

            except Exception as e:
                self.logger.warning(f"Could not get schema for {table}: {e}")
                context_lines.extend([f"## {table}", "", f"*Error retrieving schema: {e}*", ""])

        # Save the overview file
        overview_file = self.schemas_dir / f"{stage}_schemas_overview.md"
        with open(overview_file, "w") as f:
            f.write("\n".join(context_lines))

        self.logger.info(f"Schema overview file generated: {overview_file}")
        return str(overview_file)

    def commit_schemas_to_github(self, commit_message: Optional[str] = None) -> bool:
        """
        Commit schema files to GitHub repository.

        Args:
            commit_message: Custom commit message

        Returns:
            True if successful, False otherwise
        """
        try:
            if not commit_message:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                commit_message = f"Auto-update pipeline schemas - {timestamp}"

            # Change to repo root directory
            repo_root = self._find_repo_root()
            if not repo_root:
                self.logger.error("Could not find git repository root")
                return False

            original_cwd = os.getcwd()
            os.chdir(repo_root)

            try:
                # Add schema files
                subprocess.run(["git", "add", "docs/schemas/"], check=True, capture_output=True)

                # Check if there are changes to commit
                result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
                if result.returncode == 0:
                    self.logger.info("No schema changes to commit")
                    return True

                # Commit changes
                subprocess.run(
                    ["git", "commit", "-m", commit_message], check=True, capture_output=True
                )

                # Push to remote (if configured)
                try:
                    subprocess.run(["git", "push"], check=True, capture_output=True)
                    self.logger.info(
                        f"Schema files committed and pushed to GitHub: {commit_message}"
                    )
                except subprocess.CalledProcessError:
                    self.logger.warning("Committed locally but failed to push to remote")

                return True

            finally:
                os.chdir(original_cwd)

        except subprocess.CalledProcessError as e:
            self.logger.error(f"Git operation failed: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to commit schemas to GitHub: {e}")
            return False

    def _find_repo_root(self) -> Optional[str]:
        """Find the root directory of the git repository."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return None

    def generate_and_commit_all_schemas(
        self, stage: str = "silver", commit_to_github: bool = True
    ) -> List[str]:
        """
        Generate schema documentation for all tables and optionally commit to GitHub.

        Args:
            stage: Processing stage to document
            commit_to_github: Whether to commit to GitHub

        Returns:
            List of generated file paths
        """
        generated_files = []

        try:
            # Get all user tables
            query = """
            SELECT table_name 
            FROM duckdb_tables() 
            WHERE NOT internal 
            AND NOT temporary
            """

            tables = [row[0] for row in self.conn.execute(query).fetchall()]

            if not tables:
                self.logger.warning("No tables found to document")
                return generated_files

            # Generate individual schema docs
            for table in tables:
                try:
                    md_content = self.generate_table_schema_doc(table, stage)
                    file_path = self.schemas_dir / f"{table}_{stage}.md"

                    with open(file_path, "w") as f:
                        f.write(md_content)

                    generated_files.append(str(file_path))
                    self.logger.info(f"Generated schema doc: {file_path}")

                except Exception as e:
                    self.logger.error(f"Failed to generate doc for {table}: {e}")

            # Generate LLM context file
            context_file = self.generate_schema_overview_file(tables, stage)
            generated_files.append(context_file)

            # Commit to GitHub if requested
            if commit_to_github and generated_files:
                success = self.commit_schemas_to_github(
                    f"Update {stage} layer schemas - {len(tables)} tables documented"
                )
                if success:
                    self.logger.info("Schema files successfully committed to GitHub")
                else:
                    self.logger.warning("Failed to commit schema files to GitHub")

            return generated_files

        except Exception as e:
            self.logger.error(f"Failed to generate and commit schemas: {e}")
            return generated_files


class SchemaGitHubMixin:
    """Mixin to add GitHub schema integration to pipeline sources."""

    def generate_schemas_for_github(self, stage: str = "silver", commit: bool = True) -> List[str]:
        """
        Generate schema documentation and commit to GitHub.

        This method should be called after pipeline processing to document
        the output schemas for LLM consumption.

        Args:
            stage: Processing stage to document
            commit: Whether to commit to GitHub

        Returns:
            List of generated file paths
        """
        if not hasattr(self, "conn"):
            raise AttributeError(
                "DuckDB connection not found. Ensure pipeline is properly initialized."
            )

        schema_github = SchemaToGitHub(self.conn, getattr(self, "log", None))
        return schema_github.generate_and_commit_all_schemas(stage, commit)

    def get_llm_context_path(self, stage: str = "silver") -> str:
        """
        Get the path to the LLM context file for a specific stage.

        Returns:
            Path to the LLM context file
        """
        return f"docs/schemas/{stage}_schemas_overview.md"
