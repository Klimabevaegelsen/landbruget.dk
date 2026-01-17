"""
DST (Danmarks Statistik) Silver Layer Implementation

This module implements the silver layer data transformation for Danish Statistics data.
It processes raw bronze layer data from the DST API and transforms it into clean,
harmonized silver layer data following the medallion architecture.

The module contains:
- DSTSilverConfig: Configuration class for the DST silver transformation
- DSTSilver: Implementation class for transforming DST data using DuckDB

The data transformation includes cleaning, harmonization, and proper data typing
for multiple DST tables (HST77, GARTN1, FRO, HALM1).
"""

from datetime import datetime
from typing import Any, ClassVar

import duckdb
import pyarrow as pa

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import timed


class DSTSilverConfig(BaseJobConfig):
    """
    Configuration for DST Silver data processing.

    This configuration defines parameters for transforming DST data
    from raw (bronze) to structured (silver) format, including dataset names,
    storage parameters, and table configurations.

    Attributes:
        dataset (str): Primary dataset name for silver data collection
        bucket (str): GCS bucket name for storing processed data
        table_ids (List[str]): List of DST table IDs to process
    """

    dataset: str = "dst"
    bucket: str = "landbrugsdata-raw-data"
    table_ids: ClassVar[list[str]] = ["HST77", "GARTN1", "FRO", "HALM1"]


class DSTSilver(BaseSource[DSTSilverConfig], SilverJobInterface):
    """
    Silver layer processor for DST data using DuckDB.

    This class transforms raw DST data from the bronze layer into
    structured data using DuckDB for all data operations. It handles
    multiple table types with specific processing logic for each.

    The processing includes:
    1. Reading raw data from GCS or in-memory bronze data
    2. Parsing JSONSTAT format data using DuckDB
    3. Applying table-specific transformations
    4. Standardizing data types and column names
    5. Saving processed data to GCS
    """

    def __init__(self, config: DSTSilverConfig):
        """
        Initialize the DSTSilver processor.

        Args:
            config: Configuration for the silver processing job"""
        super().__init__(config)
        # Configure DuckDB connection for local processing
        self.duckdb_conn = duckdb.connect()

    async def _find_latest_bronze_data(
        self, table_id: str, bronze_data: dict | None = None
    ) -> dict[str, Any] | None:
        """
        Find and load the most recent bronze data for a table.

        Args:
            table_id (str): DST table identifier
            bronze_data (Optional[Dict]): In-memory bronze data from bronze stage

        Returns:
            Optional[Dict[str, Any]]: Table data and metadata, or None if not found
        """
        # If we have in-memory bronze data, use it directly
        if bronze_data and table_id in bronze_data:
            self.log.info(f"Using in-memory bronze data for table {table_id}")
            return bronze_data[table_id]

        # Fallback to storage if no in-memory data
        try:
            # List all bronze data files for this table
            pattern = f"gs://{self.config.bucket}/bronze/dst/*/{table_id}_data.json"
            bronze_files = self.gcs_access.list_files(pattern)

            if not bronze_files:
                self.log.warning(f"No bronze data files found for table {table_id}")
                return None

            # Sort by path (which includes date) and get the most recent
            bronze_files.sort(reverse=True)
            latest_file = bronze_files[0]

            self.log.info(f"Loading latest bronze data from GCS: {latest_file}")

            # Load the data
            table_data = self.gcs_access.download_json(latest_file)

            # Try to load metadata and table info
            metadata = None
            table_info = None

            try:
                metadata_file = latest_file.replace("_data.json", "_metadata.json")
                metadata = self.gcs_access.download_json(metadata_file)
            except Exception as e:
                self.log.warning(f"Could not load metadata for {table_id}: {e}")

            try:
                info_file = latest_file.replace("_data.json", "_tableinfo.json")
                table_info = self.gcs_access.download_json(info_file)
            except Exception as e:
                self.log.warning(f"Could not load table info for {table_id}: {e}")

            return {
                "table_id": table_id,
                "data": table_data,
                "table_info": table_info,
                "metadata": metadata,
            }

        except Exception as e:
            self.log.error(f"Failed to load bronze data from GCS for {table_id}: {e}")
            return None

    @timed(name="Loading DST JSON into DuckDB")
    def _load_dst_json_into_duckdb(self, json_data: dict[str, Any], table_name: str) -> bool:
        """
        Load DST JSONSTAT data into DuckDB.

        Args:
            json_data (Dict[str, Any]): Raw JSONSTAT data from DST API
            table_name (str): Name for the DuckDB table

        Returns:
            bool: True if loading succeeds, False otherwise
        """
        try:
            # Handle nested dataset structure from DST API
            # Data may be nested under 'dataset' key or in direct JSONSTAT format
            dataset = json_data.get("dataset", json_data)

            if not dataset or "value" not in dataset:
                self.log.warning(f"No value data found in JSON for {table_name}")
                return False

            # Extract dimensions and values from JSONSTAT format
            dimensions = dataset.get("dimension", {})
            values = dataset.get("value", [])

            if not dimensions or not values:
                self.log.warning(f"Missing dimensions or values in JSONSTAT data for {table_name}")
                return False

            # Get dimension info
            dim_info = {}
            for dim_id, dim_data in dimensions.items():
                if isinstance(dim_data, dict) and "category" in dim_data:
                    dim_info[dim_id] = {
                        "labels": dim_data["category"].get("label", {}),
                        "index": dim_data["category"].get("index", {}),
                        "size": len(dim_data["category"].get("index", {})),
                    }

            # Convert JSONSTAT to flat records
            records = []
            for i, value in enumerate(values):
                if value is not None:
                    record = {"value": value, "index": i}

                    # Calculate dimension indices
                    remaining_index = i
                    for dim_id in reversed(list(dim_info.keys())):
                        dim_size = dim_info[dim_id]["size"]
                        if dim_size > 0:
                            dim_index = remaining_index % dim_size
                            remaining_index = remaining_index // dim_size

                            # Get dimension value
                            index_to_key = {v: k for k, v in dim_info[dim_id]["index"].items()}
                            if dim_index in index_to_key:
                                key = index_to_key[dim_index]
                                label = dim_info[dim_id]["labels"].get(key, key)
                                record[dim_id] = label
                                record[f"{dim_id}_code"] = key

                    records.append(record)

            if not records:
                self.log.warning(f"No valid records extracted from JSONSTAT data for {table_name}")
                return False

            # Create DuckDB table from records
            # First, drop the table if it exists
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Convert records to PyArrow Table (DuckDB requires this format for registration)
            arrow_table = pa.Table.from_pylist(records)

            # Register the PyArrow table as a view that DuckDB can query
            self.duckdb_conn.register("temp_records", arrow_table)

            # Create table from the registered records
            self.duckdb_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_records")

            # Unregister the temporary view
            self.duckdb_conn.unregister("temp_records")

            self.log.info(f"Successfully loaded {len(records)} records into {table_name}")
            return True

        except Exception as e:
            self.log.error(f"Error loading JSONSTAT data into DuckDB for {table_name}: {e}")
            return False

    @timed(name="Processing HST77 data")
    def _process_hst77_data(
        self, json_data: dict[str, Any], metadata: dict[str, Any]
    ) -> str | None:
        """Process HST77 (harvest statistics) data using DuckDB SQL."""
        try:
            if not self._load_dst_json_into_duckdb(json_data, "hst77_raw"):
                return None

            processing_time = datetime.now().isoformat()
            output_table = "hst77_processed"

            # Apply HST77-specific transformations using SQL
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {output_table}")
            self.duckdb_conn.execute(f"""
                CREATE TABLE {output_table} AS
                SELECT
                    'HST77' AS table_id,
                    CAST("OMRÅDE_code" AS VARCHAR) AS area_code,
                    CAST("OMRÅDE" AS VARCHAR) AS area_name,
                    CAST("AFGRØDE_code" AS VARCHAR) AS crop_code,
                    CAST("AFGRØDE" AS VARCHAR) AS crop_name,
                    CAST("MÆNGDE4_code" AS VARCHAR) AS measure_code,
                    CAST("MÆNGDE4" AS VARCHAR) AS measure_name,
                    CAST("Tid_code" AS VARCHAR) AS time_period,
                    CAST("Tid" AS VARCHAR) AS time_label,
                    CAST(value AS DOUBLE) AS harvest_value,
                    '{processing_time}' AS processing_time
                FROM hst77_raw
                WHERE CAST(value AS DOUBLE) IS NOT NULL
            """)

            return output_table

        except Exception as e:
            self.log.error(f"Error processing HST77 data: {e}")
            return None

    @timed(name="Processing GARTN1 data")
    def _process_gartn1_data(
        self, json_data: dict[str, Any], metadata: dict[str, Any]
    ) -> str | None:
        """Process GARTN1 (horticulture) data using DuckDB SQL."""
        try:
            if not self._load_dst_json_into_duckdb(json_data, "gartn1_raw"):
                return None

            processing_time = datetime.now().isoformat()
            output_table = "gartn1_processed"

            # Apply GARTN1-specific transformations using SQL
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {output_table}")
            self.duckdb_conn.execute(f"""
                CREATE TABLE {output_table} AS
                SELECT
                    'GARTN1' AS table_id,
                    CAST("OMRÅDE_code" AS VARCHAR) AS area_code,
                    CAST("OMRÅDE" AS VARCHAR) AS area_name,
                    CAST("TAL_code" AS VARCHAR) AS measure_code,
                    CAST("TAL" AS VARCHAR) AS measure_name,
                    CAST("AFGRØDE_code" AS VARCHAR) AS crop_code,
                    CAST("AFGRØDE" AS VARCHAR) AS crop_name,
                    CAST("Tid_code" AS VARCHAR) AS time_period,
                    CAST("Tid" AS VARCHAR) AS time_label,
                    CAST(value AS DOUBLE) AS horticulture_value,
                    '{processing_time}' AS processing_time
                FROM gartn1_raw
                WHERE CAST(value AS DOUBLE) IS NOT NULL
            """)

            return output_table

        except Exception as e:
            self.log.error(f"Error processing GARTN1 data: {e}")
            return None

    @timed(name="Processing FRO data")
    def _process_fro_data(self, json_data: dict[str, Any], metadata: dict[str, Any]) -> str | None:
        """Process FRO (seed) data using DuckDB SQL."""
        try:
            if not self._load_dst_json_into_duckdb(json_data, "fro_raw"):
                return None

            processing_time = datetime.now().isoformat()
            output_table = "fro_processed"

            # Apply FRO-specific transformations using SQL
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {output_table}")
            self.duckdb_conn.execute(f"""
                CREATE TABLE {output_table} AS
                SELECT
                    'FRO' AS table_id,
                    CAST("AFGRØDE_code" AS VARCHAR) AS crop_code,
                    CAST("AFGRØDE" AS VARCHAR) AS crop_name,
                    CAST("MÆNGDE4_code" AS VARCHAR) AS measure_code,
                    CAST("MÆNGDE4" AS VARCHAR) AS measure_name,
                    CAST("Tid_code" AS VARCHAR) AS time_period,
                    CAST("Tid" AS VARCHAR) AS time_label,
                    CAST(value AS DOUBLE) AS seed_value,
                    '{processing_time}' AS processing_time
                FROM fro_raw
                WHERE CAST(value AS DOUBLE) IS NOT NULL
            """)

            return output_table

        except Exception as e:
            self.log.error(f"Error processing FRO data: {e}")
            return None

    @timed(name="Processing HALM1 data")
    def _process_halm1_data(
        self, json_data: dict[str, Any], metadata: dict[str, Any]
    ) -> str | None:
        """Process HALM1 (straw) data using DuckDB SQL."""
        try:
            if not self._load_dst_json_into_duckdb(json_data, "halm1_raw"):
                return None

            processing_time = datetime.now().isoformat()
            output_table = "halm1_processed"

            # Apply HALM1-specific transformations using SQL (HALM1 uses ANVENDELSE and ENHED, not MÆNGDE4)
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {output_table}")
            self.duckdb_conn.execute(f"""
                CREATE TABLE {output_table} AS
                SELECT
                    'HALM1' AS table_id,
                    CAST("OMRÅDE_code" AS VARCHAR) AS area_code,
                    CAST("OMRÅDE" AS VARCHAR) AS area_name,
                    CAST("AFGRØDE_code" AS VARCHAR) AS crop_code,
                    CAST("AFGRØDE" AS VARCHAR) AS crop_name,
                    CAST("ANVENDELSE_code" AS VARCHAR) AS usage_code,
                    CAST("ANVENDELSE" AS VARCHAR) AS usage_name,
                    CAST("ENHED_code" AS VARCHAR) AS unit_code,
                    CAST("ENHED" AS VARCHAR) AS unit_name,
                    CAST("Tid_code" AS VARCHAR) AS time_period,
                    CAST("Tid" AS VARCHAR) AS time_label,
                    CAST(value AS DOUBLE) AS straw_value,
                    '{processing_time}' AS processing_time
                FROM halm1_raw
                WHERE CAST(value AS DOUBLE) IS NOT NULL
            """)

            return output_table

        except Exception as e:
            self.log.error(f"Error processing HALM1 data: {e}")
            return None

    async def run(self, bronze_data: Any | None = None) -> Any | None:
        """
        Run silver processing for DST data.

        Args:
            bronze_data: Optional data from bronze stage. If None,
                        silver job will read from storage.

        Returns:
            Optional[Any]: Processed data that can be passed to gold stage,
                          or None if processing fails.
        """
        try:
            self.log.info("Starting DST silver processing")

            all_processed_data = {}

            for table_id in self.config.table_ids:
                self.log.info(f"Processing DST table {table_id}")

                # Get bronze data for this table
                table_bronze_data = await self._find_latest_bronze_data(table_id, bronze_data)
                if not table_bronze_data:
                    self.log.warning(f"No bronze data found for table {table_id}")
                    continue

                json_data = table_bronze_data.get("data")
                metadata = table_bronze_data.get("metadata", {})

                if not json_data:
                    self.log.warning(f"No JSON data found for table {table_id}")
                    continue

                # Process based on table type
                processed_table_name = None
                if table_id == "HST77":
                    processed_table_name = self._process_hst77_data(json_data, metadata)
                elif table_id == "GARTN1":
                    processed_table_name = self._process_gartn1_data(json_data, metadata)
                elif table_id == "FRO":
                    processed_table_name = self._process_fro_data(json_data, metadata)
                elif table_id == "HALM1":
                    processed_table_name = self._process_halm1_data(json_data, metadata)
                else:
                    self.log.warning(f"Unknown table type: {table_id}")
                    continue

                if processed_table_name is not None:
                    # Create the final table name for storage
                    final_table_name = f"dst_{table_id.lower()}_processed"

                    # Copy data from local DuckDB connection to base class connection
                    result = self.duckdb_conn.execute(
                        f"SELECT * FROM {processed_table_name}"
                    ).fetchall()
                    columns = [
                        desc[0]
                        for desc in self.duckdb_conn.execute(
                            f"DESCRIBE {processed_table_name}"
                        ).fetchall()
                    ]

                    # Create table in base class connection
                    if result:
                        # Drop existing table if it exists
                        self.conn.execute(f"DROP TABLE IF EXISTS {final_table_name}")

                        # Get column types from the local DuckDB connection
                        column_defs = []
                        for (
                            col_name,
                            col_type,
                            _nullable,
                            _key,
                            _default,
                            _extra,
                        ) in self.duckdb_conn.execute(
                            f"DESCRIBE {processed_table_name}"
                        ).fetchall():
                            column_defs.append(f'"{col_name}" {col_type}')

                        create_sql = f"CREATE TABLE {final_table_name} ({', '.join(column_defs)})"
                        self.conn.execute(create_sql)

                        # Insert data
                        placeholders = ", ".join(["?" for _ in columns])
                        insert_sql = f"INSERT INTO {final_table_name} VALUES ({placeholders})"
                        self.conn.executemany(insert_sql, result)

                    # Get record count for logging using the base class connection
                    record_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {final_table_name}"
                    ).fetchone()[0]

                    # Save using the base class method
                    self._save_data(
                        final_table_name,
                        f"{table_id.lower()}_processed",
                        self.config.bucket,
                        "silver",
                    )

                    # Store table name for gold stage processing
                    all_processed_data[table_id] = final_table_name
                    self.log.info(
                        f"Successfully processed {record_count:,} records for table {table_id}"
                    )

                    # Clean up the temporary tables from local DuckDB connection
                    self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {processed_table_name}")
                    self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_id.lower()}_raw")
                else:
                    self.log.warning(f"Failed to process data for table {table_id}")

            if not all_processed_data:
                self.log.error("No DST tables were successfully processed")
                return None

            self.log.info(f"Successfully processed {len(all_processed_data)} DST tables")
            return all_processed_data

        except Exception as e:
            self.log.error(f"Error in DST silver processing: {e}")
            return None
