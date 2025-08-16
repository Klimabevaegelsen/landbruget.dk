import logging
import os
from typing import Any, Optional

# ✅ MIGRATION: Removed pandas import - using DuckDB for data operations
from dotenv import load_dotenv
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface

logger = logging.getLogger(__name__)


class CadastralSilverConfig(BaseJobConfig):
    """Configuration for the Cadastral Silver source."""

    name: str = "Danish Cadastral"
    dataset: str = "cadastral"
    type: str = "wfs"
    description: str = "Cadastral parcels from WFS"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    load_dotenv()
    save_local: bool = os.getenv("SAVE_LOCAL", "False").lower() == "true"


class CadastralSilver(BaseSource[CadastralSilverConfig], SilverJobInterface):
    """Cadastral Silver source."""

    def __init__(self, config: CadastralSilverConfig) -> None:
        super().__init__(config)

    def _get_current_timestamp(self):
        """Get current timestamp using DuckDB."""
        import duckdb

        temp_conn = duckdb.connect()
        timestamp = temp_conn.execute("SELECT current_timestamp").fetchone()[0]
        temp_conn.close()
        return timestamp

    def _validate_and_transform(self, data: Any) -> str:
        """
        Validate and transform cadastral JSON features into a DuckDB table.

        This method takes the raw JSON features from bronze layer and transforms them
        into a structured DuckDB table with proper data types and geometry handling.

        Args:
            data: List of cadastral feature dictionaries from bronze layer

        Returns:
            str: Name of the DuckDB table containing the transformed data
        """
        if not isinstance(data, list):
            raise ValueError(f"Expected list of features, got {type(data)}")
            
        if not data:
            raise ValueError("No features provided for transformation")
            
        self.log.info(f"Transforming {len(data)} cadastral features to DuckDB table")
        
        # Create the cadastral features table
        table_name = "cadastral_features"
        
        # Drop table if it exists
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table with proper schema
        self.conn.execute(f"""
            CREATE TABLE {table_name} (
                bfe_number BIGINT,
                business_event VARCHAR,
                business_process VARCHAR,
                latest_case_id VARCHAR,
                id_local VARCHAR,
                id_namespace VARCHAR,
                registration_from TIMESTAMP,
                effect_from TIMESTAMP,
                authority VARCHAR,
                is_worker_housing BOOLEAN,
                is_common_lot BOOLEAN,
                has_owner_apartments BOOLEAN,
                is_separated_road BOOLEAN,
                agricultural_notation VARCHAR,
                geometry GEOMETRY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert features one by one with proper handling
        valid_features = 0
        for feature in data:
            try:
                # Extract values with defaults
                bfe_number = feature.get('bfe_number')
                if not bfe_number:
                    continue  # Skip features without BFE number
                    
                # Convert geometry from WKT to DuckDB geometry
                geometry_wkt = feature.get('geometry')
                if not geometry_wkt:
                    continue  # Skip features without geometry
                
                # Insert the feature
                self.conn.execute(f"""
                    INSERT INTO {table_name} (
                        bfe_number, business_event, business_process, latest_case_id,
                        id_local, id_namespace, registration_from, effect_from,
                        authority, is_worker_housing, is_common_lot, has_owner_apartments,
                        is_separated_road, agricultural_notation, geometry
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))
                """, [
                    bfe_number,
                    feature.get('business_event'),
                    feature.get('business_process'), 
                    feature.get('latest_case_id'),
                    feature.get('id_local'),
                    feature.get('id_namespace'),
                    feature.get('registration_from'),
                    feature.get('effect_from'),
                    feature.get('authority'),
                    feature.get('is_worker_housing', False),
                    feature.get('is_common_lot', False),
                    feature.get('has_owner_apartments', False),
                    feature.get('is_separated_road', False),
                    feature.get('agricultural_notation'),
                    geometry_wkt
                ])
                valid_features += 1
                
            except Exception as e:
                bfe_id = feature.get('bfe_number', 'unknown')
                self.log.warning(f"Failed to insert feature {bfe_id}: {e}")
                continue
        
        self.log.info(f"Successfully transformed {valid_features} out of {len(data)} features")
        
        if valid_features == 0:
            raise ValueError("No valid features could be transformed")
            
        return table_name

    def _create_dissolved_data(self, table_name: str) -> str:
        """
        Create a dissolved version of the cadastral data by merging geometries.

        This creates a simplified version where adjacent cadastral parcels with
        the same attributes are merged together for analysis purposes.

        Args:
            table_name: Name of the source DuckDB table

        Returns:
            str: Name of the DuckDB table containing dissolved data
        """
        try:
            dissolved_table_name = "cadastral_dissolved"
            
            # Drop table if it exists
            self.conn.execute(f"DROP TABLE IF EXISTS {dissolved_table_name}")
            
            self.log.info("Creating dissolved cadastral data")
            
            # Create dissolved data by grouping by key attributes and merging geometries
            self.conn.execute(f"""
                CREATE TABLE {dissolved_table_name} AS
                SELECT 
                    business_event,
                    business_process,
                    authority,
                    is_worker_housing,
                    is_common_lot,
                    has_owner_apartments,
                    agricultural_notation,
                    COUNT(*) as parcel_count,
                    MIN(bfe_number) as min_bfe_number,
                    MAX(bfe_number) as max_bfe_number,
                    ST_Union_Agg(geometry) as geometry,
                    MIN(created_at) as created_at
                FROM {table_name}
                WHERE geometry IS NOT NULL
                GROUP BY 
                    business_event, business_process, authority,
                    is_worker_housing, is_common_lot, has_owner_apartments,
                    agricultural_notation
                HAVING COUNT(*) >= 1
            """)
            
            # Get count for logging
            dissolved_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {dissolved_table_name}"
            ).fetchone()[0]
            original_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            
            self.log.info(
                f"Created dissolved data: {dissolved_count} dissolved features "
                f"from {original_count} original features"
            )
            
            return dissolved_table_name

        except Exception as e:
            self.log.error(f"Error creating dissolved data: {e}")
            # Return original table name as fallback
            return table_name

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[Any]:
        """
        Run the complete Cadastral silver layer processing job.

        This is the main entry point that orchestrates the entire process:
        1. Reads data from the bronze layer (either in-memory or from storage)
        2. Validates and transforms the data using DuckDB
        3. Creates a dissolved version of the data
        4. Saves both the original and dissolved data to GCS

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        Returns:
            Optional[Any]: Processed cadastral data for gold layer,
                          or None if processing fails.

        Raises:
            Exception: If there are issues at any step in the process.
        """
        self.log.info("Running Cadastral silver job")

        # Read data with support for in-memory passing
        if bronze_data is not None:
            self.log.info("Using bronze data from memory (in-memory data passing)")
            raw_data = bronze_data
        else:
            # Fallback to reading from storage
            self.log.info("Reading bronze data from storage (fallback)")
            raw_data = self._read_bronze_data_from_storage(self.config.dataset, self.config.bucket)
            if raw_data is None:
                self.log.error("Failed to read raw data from storage")
                return None

        if raw_data is None:
            self.log.warning("No data found in bronze layer")
            return None

        self.log.info("Processing data from bronze layer")

        # Validate and transform the data into DuckDB table
        processed_table = self._validate_and_transform(raw_data)

        if processed_table is None:
            self.log.warning("No valid data found after processing")
            return None

        # Create dissolved version
        dissolved_table = self._create_dissolved_data(processed_table)

        # Save both versions as parquet files using table names
        self._save_data(processed_table, self.config.dataset, self.config.bucket, "silver")
        self._save_data(
            dissolved_table, f"{self.config.dataset}_dissolved", self.config.bucket, "silver"
        )

        self.log.info("Cadastral silver job completed successfully")

        # Return processed table name for gold layer
        return processed_table
