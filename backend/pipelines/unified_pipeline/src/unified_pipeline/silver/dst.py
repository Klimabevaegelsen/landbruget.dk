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
from typing import Any, Dict, Optional

import ibis
from ibis import _

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.gcs_util import GCSUtil
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
    table_ids: list[str] = ["HST77", "GARTN1", "FRO", "HALM1"]


class DSTSilver(BaseSource[DSTSilverConfig], SilverJobInterface):
    """
    Silver layer processor for DST data using DuckDB and ibis.

    This class transforms raw DST data from the bronze layer into
    structured data using DuckDB for all data operations and ibis for
    transformations. It handles multiple table types with specific
    processing logic for each.

    The processing includes:
    1. Reading raw data from GCS or in-memory bronze data
    2. Parsing JSONSTAT format data using DuckDB
    3. Applying table-specific transformations
    4. Standardizing data types and column names
    5. Saving processed data to GCS
    """

    def __init__(self, config: DSTSilverConfig, gcs_util: GCSUtil):
        """
        Initialize the DSTSilver processor.

        Args:
            config: Configuration for the silver processing job
            gcs_util: Utility for GCS operations
        """
        super().__init__(config, gcs_util)
        # Configure ibis backend
        ibis.options.interactive = True
        self.ibis_con = ibis.duckdb.connect()

    async def _find_latest_bronze_data(
        self, table_id: str, bronze_data: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
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
            pattern = f"bronze/dst/*/{table_id}_data.json"
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
    def _load_dst_json_into_duckdb(self, json_data: Dict[str, Any], table_name: str):
        """
        Load DST JSONSTAT data into DuckDB using ibis.

        Args:
            json_data (Dict[str, Any]): Raw JSONSTAT data from DST API
            table_name (str): Name for the DuckDB table

        Returns:
            ibis table expression or None if loading fails
        """
        try:
            if not json_data or "value" not in json_data:
                self.log.warning(f"No value data found in JSON for {table_name}")
                return None

            # Extract dimensions and values from JSONSTAT format
            dimensions = json_data.get("dimension", {})
            values = json_data.get("value", [])

            if not dimensions or not values:
                self.log.warning(f"Missing dimensions or values in JSONSTAT data for {table_name}")
                return None

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
                return None

            # Create ibis table from records
            self.ibis_con.create_table(table_name, records, overwrite=True)
            table = self.ibis_con.table(table_name)

            self.log.info(f"Successfully loaded {len(records)} records into {table_name}")
            return table

        except Exception as e:
            self.log.error(f"Error loading JSONSTAT data into DuckDB for {table_name}: {e}")
            return None

    @timed(name="Processing HST77 data")
    def _process_hst77_data(
        self, json_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Optional[Any]:
        """Process HST77 (harvest statistics) data using ibis."""
        try:
            table = self._load_dst_json_into_duckdb(json_data, "hst77_raw")
            if table is None:
                return None

            # Apply HST77-specific transformations
            processed = (
                table.mutate(
                    area_code=_.OMRÅDE_code.cast("string"),
                    area_name=_.OMRÅDE.cast("string"),
                    crop_code=_.AFGRØDE_code.cast("string"),
                    crop_name=_.AFGRØDE.cast("string"),
                    measure_code=_.MÆNGDE4_code.cast("string"),
                    measure_name=_.MÆNGDE4.cast("string"),
                    time_period=_.Tid_code.cast("string"),
                    time_label=_.Tid.cast("string"),
                    harvest_value=_.value.cast("double"),
                    table_id=ibis.literal("HST77"),
                    processing_time=ibis.literal(datetime.now().isoformat()),
                )
                .select(
                    [
                        "table_id",
                        "area_code",
                        "area_name",
                        "crop_code",
                        "crop_name",
                        "measure_code",
                        "measure_name",
                        "time_period",
                        "time_label",
                        "harvest_value",
                        "processing_time",
                    ]
                )
                .filter(_.harvest_value.notnull())
            )

            return processed

        except Exception as e:
            self.log.error(f"Error processing HST77 data: {e}")
            return None

    @timed(name="Processing GARTN1 data")
    def _process_gartn1_data(
        self, json_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Optional[Any]:
        """Process GARTN1 (horticulture) data using ibis."""
        try:
            table = self._load_dst_json_into_duckdb(json_data, "gartn1_raw")
            if table is None:
                return None

            # Apply GARTN1-specific transformations
            processed = (
                table.mutate(
                    area_code=_.OMRÅDE_code.cast("string"),
                    area_name=_.OMRÅDE.cast("string"),
                    measure_code=_.TAL_code.cast("string"),
                    measure_name=_.TAL.cast("string"),
                    crop_code=_.AFGRØDE_code.cast("string"),
                    crop_name=_.AFGRØDE.cast("string"),
                    time_period=_.Tid_code.cast("string"),
                    time_label=_.Tid.cast("string"),
                    horticulture_value=_.value.cast("double"),
                    table_id=ibis.literal("GARTN1"),
                    processing_time=ibis.literal(datetime.now().isoformat()),
                )
                .select(
                    [
                        "table_id",
                        "area_code",
                        "area_name",
                        "measure_code",
                        "measure_name",
                        "crop_code",
                        "crop_name",
                        "time_period",
                        "time_label",
                        "horticulture_value",
                        "processing_time",
                    ]
                )
                .filter(_.horticulture_value.notnull())
            )

            return processed

        except Exception as e:
            self.log.error(f"Error processing GARTN1 data: {e}")
            return None

    @timed(name="Processing FRO data")
    def _process_fro_data(
        self, json_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Optional[Any]:
        """Process FRO (seed) data using ibis."""
        try:
            table = self._load_dst_json_into_duckdb(json_data, "fro_raw")
            if table is None:
                return None

            # Apply FRO-specific transformations
            processed = (
                table.mutate(
                    crop_code=_.AFGRØDE_code.cast("string"),
                    crop_name=_.AFGRØDE.cast("string"),
                    measure_code=_.MÆNGDE4_code.cast("string"),
                    measure_name=_.MÆNGDE4.cast("string"),
                    time_period=_.Tid_code.cast("string"),
                    time_label=_.Tid.cast("string"),
                    seed_value=_.value.cast("double"),
                    table_id=ibis.literal("FRO"),
                    processing_time=ibis.literal(datetime.now().isoformat()),
                )
                .select(
                    [
                        "table_id",
                        "crop_code",
                        "crop_name",
                        "measure_code",
                        "measure_name",
                        "time_period",
                        "time_label",
                        "seed_value",
                        "processing_time",
                    ]
                )
                .filter(_.seed_value.notnull())
            )

            return processed

        except Exception as e:
            self.log.error(f"Error processing FRO data: {e}")
            return None

    @timed(name="Processing HALM1 data")
    def _process_halm1_data(
        self, json_data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Optional[Any]:
        """Process HALM1 (straw) data using ibis."""
        try:
            table = self._load_dst_json_into_duckdb(json_data, "halm1_raw")
            if table is None:
                return None

            # Apply HALM1-specific transformations
            processed = (
                table.mutate(
                    crop_code=_.AFGRØDE_code.cast("string"),
                    crop_name=_.AFGRØDE.cast("string"),
                    measure_code=_.MÆNGDE4_code.cast("string"),
                    measure_name=_.MÆNGDE4.cast("string"),
                    time_period=_.Tid_code.cast("string"),
                    time_label=_.Tid.cast("string"),
                    straw_value=_.value.cast("double"),
                    table_id=ibis.literal("HALM1"),
                    processing_time=ibis.literal(datetime.now().isoformat()),
                )
                .select(
                    [
                        "table_id",
                        "crop_code",
                        "crop_name",
                        "measure_code",
                        "measure_name",
                        "time_period",
                        "time_label",
                        "straw_value",
                        "processing_time",
                    ]
                )
                .filter(_.straw_value.notnull())
            )

            return processed

        except Exception as e:
            self.log.error(f"Error processing HALM1 data: {e}")
            return None

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
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
                processed_data = None
                if table_id == "HST77":
                    processed_data = self._process_hst77_data(json_data, metadata)
                elif table_id == "GARTN1":
                    processed_data = self._process_gartn1_data(json_data, metadata)
                elif table_id == "FRO":
                    processed_data = self._process_fro_data(json_data, metadata)
                elif table_id == "HALM1":
                    processed_data = self._process_halm1_data(json_data, metadata)
                else:
                    self.log.warning(f"Unknown table type: {table_id}")
                    continue

                if processed_data is not None:
                    # ✅ OPTIMIZED: Save processed data directly without DataFrame conversion
                    # Create a DuckDB table from the Ibis expression
                    table_name = f"dst_{table_id.lower()}_processed"
                    processed_data.cache().name(table_name)

                    # Get record count for logging (without DataFrame conversion)
                    record_count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]

                    # Save directly using optimized method
                    gcs_path = self.save_data_direct(
                        table_name, f"dst_{table_id.lower()}", self.config.bucket, "silver"
                    )

                    # Store table name for gold stage processing (not DataFrame)
                    all_processed_data[table_id] = table_name
                    self.log.info(
                        f"Successfully processed {record_count:,} records for table {table_id}"
                    )
                    self.log.info(f"Saved to: {gcs_path}")

                    # Clean up the temporary table
                    self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
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
