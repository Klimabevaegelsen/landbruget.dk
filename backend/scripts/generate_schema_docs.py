#!/usr/bin/env python3
"""
Generate schema documentation from Parquet files.

This script reads Parquet files from GCS (or local cache) and generates
comprehensive schema documentation in Markdown format, suitable for
Gemini File Search and human consumption.

Usage:
    python generate_schema_docs.py [--local-cache PATH] [--bucket BUCKET] [--output-dir DIR]

Environment Variables:
    GCS_BUCKET: Google Cloud Storage bucket name (default: landbruget-data)
    GCS_ACCESS_KEY_ID: HMAC access key for GCS
    GCS_SECRET_ACCESS_KEY: HMAC secret key for GCS
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow.parquet as pq


class SchemaDocumentationGenerator:
    """Generate schema documentation from Parquet files."""

    def __init__(
        self,
        output_dir: str = "schema",
        bucket_name: Optional[str] = None,
        local_cache: Optional[str] = None,
    ):
        """
        Initialize schema documentation generator.

        Args:
            output_dir: Directory for output markdown files
            bucket_name: GCS bucket name (uses GCS_BUCKET env var if not provided)
            local_cache: Local cache directory for Parquet files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET", "landbruget-data")
        self.local_cache = Path(local_cache) if local_cache else None

        # Initialize DuckDB connection
        self.conn = duckdb.connect(":memory:")
        self._setup_duckdb()

    def _setup_duckdb(self) -> None:
        """Setup DuckDB extensions and GCS authentication."""
        # Install required extensions
        try:
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            print("✅ DuckDB httpfs extension loaded")
        except Exception as e:
            print(f"Warning: Could not load httpfs extension: {e}")

        # Setup GCS authentication if credentials available
        gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
        gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

        if gcs_access_key and gcs_secret_key:
            try:
                self.conn.execute(f"""
                    CREATE OR REPLACE PERSISTENT SECRET gcs_hmac (
                        TYPE GCS,
                        KEY_ID '{gcs_access_key}',
                        SECRET '{gcs_secret_key}'
                    );
                """)
                print("✅ DuckDB GCS HMAC authentication configured")
            except Exception as e:
                print(f"Warning: Could not setup GCS authentication: {e}")
        else:
            print("ℹ️  No GCS HMAC credentials found")

    def discover_parquet_files(self, search_paths: List[str]) -> List[Tuple[str, str]]:
        """
        Discover Parquet files from GCS or local cache.

        Args:
            search_paths: List of GCS paths or local directories to search

        Returns:
            List of tuples (table_name, file_path)
        """
        discovered_files = []

        for search_path in search_paths:
            if search_path.startswith("gs://"):
                # GCS path
                discovered_files.extend(self._discover_gcs_files(search_path))
            else:
                # Local path
                discovered_files.extend(self._discover_local_files(search_path))

        return discovered_files

    def _discover_gcs_files(self, gcs_path: str) -> List[Tuple[str, str]]:
        """Discover Parquet files from GCS."""
        files = []

        try:
            # Use DuckDB to list files in GCS
            query = f"""
                SELECT DISTINCT file_path
                FROM glob('{gcs_path}/**/*.parquet')
            """
            result = self.conn.execute(query).fetchall()

            for row in result:
                file_path = row[0]
                # Extract table name from path
                table_name = self._extract_table_name(file_path)
                files.append((table_name, file_path))

            print(f"✅ Discovered {len(files)} Parquet files from {gcs_path}")

        except Exception as e:
            print(f"Warning: Could not discover GCS files at {gcs_path}: {e}")

        return files

    def _discover_local_files(self, local_path: str) -> List[Tuple[str, str]]:
        """Discover Parquet files from local directory."""
        files = []
        path = Path(local_path)

        if not path.exists():
            print(f"Warning: Local path does not exist: {local_path}")
            return files

        for parquet_file in path.rglob("*.parquet"):
            table_name = self._extract_table_name(str(parquet_file))
            files.append((table_name, str(parquet_file)))

        print(f"✅ Discovered {len(files)} Parquet files from {local_path}")
        return files

    def _extract_table_name(self, file_path: str) -> str:
        """Extract a meaningful table name from file path."""
        # Remove GCS prefix if present
        path = file_path.replace("gs://", "").replace(self.bucket_name + "/", "")

        # Get the filename without extension
        filename = Path(path).stem

        # If filename is generic (like 'data' or 'output'), use parent directory
        if filename in ["data", "output", "result", "export"]:
            parts = Path(path).parts
            if len(parts) >= 2:
                # Use second-to-last directory as table name
                filename = parts[-2]

        # Clean up the name
        table_name = filename.replace("-", "_").replace(" ", "_").lower()

        return table_name

    def get_parquet_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Get comprehensive schema information from Parquet file.

        Args:
            file_path: Path to Parquet file

        Returns:
            Dictionary with schema information
        """
        schema_info = {
            "file_path": file_path,
            "generated_at": datetime.now().isoformat(),
        }

        try:
            # Use PyArrow for detailed metadata
            parquet_file = pq.ParquetFile(file_path)
            schema = parquet_file.schema_arrow

            # Basic schema information
            schema_info["num_columns"] = len(schema)
            schema_info["num_row_groups"] = parquet_file.num_row_groups
            schema_info["row_count"] = parquet_file.metadata.num_rows

            # Column information
            columns = []
            for i, field in enumerate(schema):
                col_info = {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }

                # Try to get metadata if available
                if field.metadata:
                    col_info["metadata"] = {
                        k.decode() if isinstance(k, bytes) else k: v.decode()
                        if isinstance(v, bytes)
                        else v
                        for k, v in field.metadata.items()
                    }

                columns.append(col_info)

            schema_info["columns"] = columns

            # File metadata
            if parquet_file.metadata.metadata:
                schema_info["file_metadata"] = {
                    k.decode() if isinstance(k, bytes) else k: v.decode()
                    if isinstance(v, bytes)
                    else v
                    for k, v in parquet_file.metadata.metadata.items()
                }

            # Use DuckDB to get additional statistics
            try:
                # Create temporary view
                temp_table = f"temp_table_{hash(file_path) % 100000}"
                self.conn.execute(f"""
                    CREATE OR REPLACE TEMP VIEW {temp_table} AS
                    SELECT * FROM read_parquet('{file_path}')
                """)

                # Get column statistics using SUMMARIZE
                try:
                    summarize_query = f"SUMMARIZE {temp_table}"
                    summarize_results = self.conn.execute(summarize_query).fetchall()

                    column_stats = {}
                    for row in summarize_results:
                        col_name = row[0]
                        column_stats[col_name] = {
                            "type": row[1],
                            "min": str(row[2]) if row[2] is not None else None,
                            "max": str(row[3]) if row[3] is not None else None,
                            "approx_unique": row[4],
                            "avg": str(row[5]) if row[5] is not None else None,
                            "null_percentage": f"{row[11]:.1f}%"
                            if row[11] is not None
                            else None,
                        }

                    schema_info["column_statistics"] = column_stats
                except Exception:
                    # SUMMARIZE might fail for some data types
                    pass

            except Exception as e:
                print(f"Warning: Could not get DuckDB statistics for {file_path}: {e}")

        except Exception as e:
            schema_info["error"] = str(e)
            print(f"Error reading schema from {file_path}: {e}")

        return schema_info

    def generate_tables_md(self, files: List[Tuple[str, str]]) -> str:
        """
        Generate tables.md with list of all tables.

        Args:
            files: List of tuples (table_name, file_path)

        Returns:
            Path to generated file
        """
        output_file = self.output_dir / "tables.md"

        lines = [
            "# Landbruget.dk Data Tables",
            "",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Total Tables:** {len(files)}",
            "",
            "This document lists all available data tables in the Landbruget.dk data warehouse.",
            "",
            "## Table Index",
            "",
        ]

        # Get schema info for each file
        table_info = []
        for table_name, file_path in files:
            print(f"Processing {table_name}...")
            schema_info = self.get_parquet_schema(file_path)
            table_info.append((table_name, schema_info))

        # Sort by table name
        table_info.sort(key=lambda x: x[0])

        # Create summary table
        lines.extend(
            [
                "| Table Name | Row Count | Columns | Last Updated |",
                "|------------|-----------|---------|--------------|",
            ]
        )

        for table_name, info in table_info:
            row_count = (
                f"{info.get('row_count', 0):,}" if "row_count" in info else "N/A"
            )
            num_columns = info.get("num_columns", 0)
            # Try to extract date from file path
            last_updated = self._extract_date_from_path(info.get("file_path", ""))

            lines.append(
                f"| [{table_name}](#{table_name}) | {row_count} | {num_columns} | {last_updated} |"
            )

        lines.append("")
        lines.append("## Table Details")
        lines.append("")

        # Add detailed information for each table
        for table_name, info in table_info:
            lines.extend(
                [
                    f"### {table_name}",
                    "",
                ]
            )

            if "error" in info:
                lines.extend(
                    [
                        f"*Error: {info['error']}*",
                        "",
                        "---",
                        "",
                    ]
                )
                continue

            # Basic info
            lines.extend(
                [
                    f"**File:** `{info['file_path']}`",
                    f"**Row Count:** {info.get('row_count', 0):,}",
                    f"**Columns:** {info.get('num_columns', 0)}",
                ]
            )

            # Metadata if available
            if "file_metadata" in info and info["file_metadata"]:
                lines.append("")
                lines.append("**Metadata:**")
                for key, value in info["file_metadata"].items():
                    lines.append(f"- {key}: {value}")

            lines.extend(
                [
                    "",
                    "---",
                    "",
                ]
            )

        # Save to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"✅ Generated {output_file}")
        return str(output_file)

    def generate_columns_md(self, files: List[Tuple[str, str]]) -> str:
        """
        Generate columns.md with detailed column information.

        Args:
            files: List of tuples (table_name, file_path)

        Returns:
            Path to generated file
        """
        output_file = self.output_dir / "columns.md"

        lines = [
            "# Landbruget.dk Data Column Reference",
            "",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "This document provides detailed column-level information for all tables.",
            "",
        ]

        # Get schema info for each file
        table_info = []
        for table_name, file_path in files:
            schema_info = self.get_parquet_schema(file_path)
            table_info.append((table_name, schema_info))

        # Sort by table name
        table_info.sort(key=lambda x: x[0])

        # Add column details for each table
        for table_name, info in table_info:
            lines.extend(
                [
                    f"## {table_name}",
                    "",
                ]
            )

            if "error" in info:
                lines.extend(
                    [
                        f"*Error: {info['error']}*",
                        "",
                    ]
                )
                continue

            lines.extend(
                [
                    f"**Row Count:** {info.get('row_count', 0):,}",
                    "",
                ]
            )

            # Column table
            lines.extend(
                [
                    "| Column | Type | Nullable | Description |",
                    "|--------|------|----------|-------------|",
                ]
            )

            for col in info.get("columns", []):
                nullable = "Yes" if col.get("nullable", True) else "No"
                description = ""

                # Try to get description from metadata
                if "metadata" in col and col["metadata"]:
                    description = col["metadata"].get("description", "")

                lines.append(
                    f"| {col['name']} | {col['type']} | {nullable} | {description} |"
                )

            lines.append("")

            # Column statistics if available
            if "column_statistics" in info and info["column_statistics"]:
                lines.extend(
                    [
                        "### Column Statistics",
                        "",
                        "| Column | Min | Max | Unique | Avg | Null % |",
                        "|--------|-----|-----|--------|-----|--------|",
                    ]
                )

                for col_name, stats in info["column_statistics"].items():
                    min_val = stats.get("min", "")
                    max_val = stats.get("max", "")
                    unique = stats.get("approx_unique", "")
                    avg = stats.get("avg", "")
                    null_pct = stats.get("null_percentage", "")

                    lines.append(
                        f"| {col_name} | {min_val} | {max_val} | {unique} | {avg} | {null_pct} |"
                    )

                lines.append("")

            lines.extend(
                [
                    "---",
                    "",
                ]
            )

        # Save to file
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        print(f"✅ Generated {output_file}")
        return str(output_file)

    def _extract_date_from_path(self, file_path: str) -> str:
        """Extract date from file path if present."""
        import re

        # Look for date patterns like 20240101, 2024-01-01, 2024_01_01
        patterns = [
            r"(\d{4}[-_]?\d{2}[-_]?\d{2})",
            r"(\d{8})",
        ]

        for pattern in patterns:
            match = re.search(pattern, file_path)
            if match:
                date_str = match.group(1)
                # Normalize to YYYY-MM-DD
                date_str = date_str.replace("_", "-")
                if len(date_str) == 8:
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                return date_str

        return "N/A"

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate schema documentation from Parquet files"
    )
    parser.add_argument(
        "--local-cache",
        help="Local cache directory with Parquet files",
        default=None,
    )
    parser.add_argument(
        "--bucket",
        help="GCS bucket name (default: from GCS_BUCKET env var)",
        default=None,
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for markdown files",
        default="schema",
    )
    parser.add_argument(
        "--gcs-path",
        help="GCS path pattern to search (e.g., gs://bucket/gold/**)",
        action="append",
    )
    parser.add_argument(
        "--local-path",
        help="Local path to search for Parquet files",
        action="append",
    )

    args = parser.parse_args()

    # Initialize generator
    generator = SchemaDocumentationGenerator(
        output_dir=args.output_dir,
        bucket_name=args.bucket,
        local_cache=args.local_cache,
    )

    # Build search paths
    search_paths = []

    if args.gcs_path:
        search_paths.extend(args.gcs_path)
    elif args.local_cache:
        search_paths.append(args.local_cache)
    elif args.local_path:
        search_paths.extend(args.local_path)
    else:
        # Default: search in common locations
        bucket = args.bucket or os.getenv("GCS_BUCKET", "landbruget-data")
        default_paths = [
            f"gs://{bucket}/gold",
            f"gs://{bucket}/silver",
        ]
        search_paths.extend(default_paths)
        print(f"Using default search paths: {default_paths}")

    # Discover Parquet files
    print("\n🔍 Discovering Parquet files...")
    files = generator.discover_parquet_files(search_paths)

    if not files:
        print("❌ No Parquet files found")
        sys.exit(1)

    print(f"\n✅ Found {len(files)} tables to document\n")

    # Generate documentation
    print("📝 Generating tables.md...")
    generator.generate_tables_md(files)

    print("\n📝 Generating columns.md...")
    generator.generate_columns_md(files)

    print("\n✅ Schema documentation generation complete!")
    print(f"   Output directory: {args.output_dir}")

    # Cleanup
    generator.close()


if __name__ == "__main__":
    main()
