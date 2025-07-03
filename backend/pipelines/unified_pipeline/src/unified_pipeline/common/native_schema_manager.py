"""
Native DuckDB Schema Manager for Unified Pipeline.

This module leverages DuckDB's built-in schema introspection and metadata
capabilities instead of building custom schema generation. DuckDB provides
comprehensive native tools that are more efficient and reliable.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from unified_pipeline.util.log_util import Logger


class NativeSchemaManager:
    """Leverages DuckDB's native schema introspection capabilities."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, logger: Optional[Logger] = None):
        self.conn = connection
        self.logger = logger or Logger(__name__)

    def get_table_schema(self, table_name: str, include_summary: bool = True) -> Dict[str, Any]:
        """
        Get comprehensive schema information using DuckDB's native capabilities.

        Args:
            table_name: Name of the table to analyze
            include_summary: Whether to include SUMMARIZE statistics

        Returns:
            Dictionary with comprehensive schema information
        """
        schema_info = {
            "table_name": table_name,
            "generated_at": datetime.now().isoformat(),
            "basic_info": self._get_basic_table_info(table_name),
            "columns": self._get_column_details(table_name),
            "constraints": self._get_constraints(table_name),
            "storage_info": self._get_storage_info(table_name),
        }

        if include_summary:
            schema_info["data_summary"] = self._get_data_summary(table_name)

        return schema_info

    def _get_basic_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get basic table information using duckdb_tables()."""
        query = """
        SELECT 
            table_name,
            estimated_size,
            column_count,
            has_primary_key,
            temporary,
            sql
        FROM duckdb_tables() 
        WHERE table_name = ?
        """

        result = self.conn.execute(query, [table_name]).fetchone()
        if not result:
            return {}

        return {
            "table_name": result[0],
            "estimated_size": result[1],
            "column_count": result[2],
            "has_primary_key": result[3],
            "is_temporary": result[4],
            "create_sql": result[5],
        }

    def _get_column_details(self, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information using duckdb_columns()."""
        query = """
        SELECT 
            column_name,
            column_index,
            data_type,
            is_nullable,
            column_default,
            numeric_precision,
            numeric_scale,
            character_maximum_length,
            comment
        FROM duckdb_columns() 
        WHERE table_name = ?
        ORDER BY column_index
        """

        results = self.conn.execute(query, [table_name]).fetchall()

        columns = []
        for row in results:
            columns.append(
                {
                    "name": row[0],
                    "index": row[1],
                    "data_type": row[2],
                    "nullable": row[3],
                    "default": row[4],
                    "numeric_precision": row[5],
                    "numeric_scale": row[6],
                    "max_length": row[7],
                    "comment": row[8],
                }
            )

        return columns

    def _get_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get constraint information using duckdb_constraints()."""
        query = """
        SELECT 
            constraint_type,
            constraint_text,
            constraint_column_names
        FROM duckdb_constraints() 
        WHERE table_name = ?
        """

        results = self.conn.execute(query, [table_name]).fetchall()

        constraints = []
        for row in results:
            constraints.append({"type": row[0], "definition": row[1], "columns": row[2]})

        return constraints

    def _get_storage_info(self, table_name: str) -> Dict[str, Any]:
        """Get storage information using PRAGMA storage_info."""
        try:
            query = f"PRAGMA storage_info('{table_name}')"
            results = self.conn.execute(query).fetchall()

            storage_summary = {
                "total_segments": len(results),
                "compression_types": set(),
                "total_count": 0,
            }

            for row in results:
                if len(row) >= 8:  # Ensure we have enough columns
                    storage_summary["compression_types"].add(row[7])  # compression column
                    storage_summary["total_count"] += row[6] if row[6] else 0  # count column

            storage_summary["compression_types"] = list(storage_summary["compression_types"])
            return storage_summary

        except Exception as e:
            self.logger.warning(f"Could not get storage info for {table_name}: {e}")
            return {}

    def _get_data_summary(self, table_name: str) -> Dict[str, Any]:
        """Get data summary using DuckDB's SUMMARIZE command."""
        try:
            query = f"SUMMARIZE {table_name}"
            results = self.conn.execute(query).fetchall()

            # Get column names from the SUMMARIZE result
            columns = [desc[0] for desc in self.conn.description]

            summary = []
            for row in results:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    row_dict[col_name] = row[i]
                summary.append(row_dict)

            return summary

        except Exception as e:
            self.logger.warning(f"Could not get data summary for {table_name}: {e}")
            return {}

    def save_schema_to_gcs(
        self, table_name: str, schema_info: Dict[str, Any], bucket_path: str
    ) -> str:
        """Save schema information to GCS in JSON format."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_schema_{timestamp}.json"
        gcs_path = f"{bucket_path.rstrip('/')}/{filename}"

        try:
            from unified_pipeline.util.gcs_access import GCSDataAccess

            gcs_access = GCSDataAccess()
            gcs_access.upload_json(schema_info, gcs_path)
            self.logger.info(f"Schema saved to GCS: {gcs_path}")
            return gcs_path

        except Exception as e:
            self.logger.error(f"Failed to save schema to GCS: {e}")
            raise

    def save_schema_locally(
        self, table_name: str, schema_info: Dict[str, Any], output_dir: str = "schemas"
    ) -> str:
        """Save schema information locally in JSON format."""
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_schema_{timestamp}.json"
        filepath = Path(output_dir) / filename

        with open(filepath, "w") as f:
            json.dump(schema_info, f, indent=2, default=str)

        self.logger.info(f"Schema saved locally: {filepath}")
        return str(filepath)

    def get_all_tables_schemas(
        self, include_summary: bool = True, save_to_gcs: bool = False, gcs_bucket_path: str = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get schemas for all user tables in the database."""
        # Get all non-internal tables
        query = """
        SELECT table_name 
        FROM duckdb_tables() 
        WHERE NOT internal 
        AND NOT temporary
        """

        tables = self.conn.execute(query).fetchall()
        all_schemas = {}

        for (table_name,) in tables:
            self.logger.info(f"Generating schema for table: {table_name}")
            schema_info = self.get_table_schema(table_name, include_summary)
            all_schemas[table_name] = schema_info

            # Optionally save to GCS
            if save_to_gcs and gcs_bucket_path:
                self.save_schema_to_gcs(table_name, schema_info, gcs_bucket_path)

        return all_schemas


class SchemaMixin:
    """Mixin to add native schema generation to pipeline sources."""

    def generate_and_save_schema(
        self,
        table_name: str,
        include_summary: bool = True,
        save_to_gcs: bool = True,
        save_locally: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate and save schema using DuckDB's native capabilities.

        This should be called after data processing to document the output schema.
        """
        if not hasattr(self, "_duckdb_conn"):
            raise AttributeError(
                "DuckDB connection not found. Ensure pipeline is properly initialized."
            )

        schema_manager = NativeSchemaManager(self._duckdb_conn, getattr(self, "logger", None))
        schema_info = schema_manager.get_table_schema(table_name, include_summary)

        # Save to GCS if enabled
        if save_to_gcs:
            gcs_path = f"gs://landbrugsdata-raw-data/schemas/{self.__class__.__name__}"
            schema_manager.save_schema_to_gcs(table_name, schema_info, gcs_path)

        # Save locally if enabled
        if save_locally:
            schema_manager.save_schema_locally(table_name, schema_info)

        return schema_info

    def get_schema_for_llm(self, table_name: str) -> str:
        """
        Get a concise schema description optimized for LLM consumption.

        Returns a formatted string that LLMs can easily understand.
        """
        if not hasattr(self, "_duckdb_conn"):
            raise AttributeError("DuckDB connection not found.")

        # Use DuckDB's DESCRIBE for a clean, simple format
        query = f"DESCRIBE {table_name}"
        results = self.conn.execute(query).fetchall()

        schema_lines = [f"Table: {table_name}"]
        schema_lines.append("Columns:")

        for row in results:
            col_name, col_type, nullable, key, default, extra = row
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            key_str = f" ({key})" if key else ""
            default_str = f" DEFAULT {default}" if default else ""

            schema_lines.append(f"  - {col_name}: {col_type} {nullable_str}{key_str}{default_str}")

        return "\n".join(schema_lines)
