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
        Validate and transform cadastral JSON features into a DuckDB table using bulk insert.

        This method takes the raw JSON features from bronze layer and transforms them
        into a structured DuckDB table with proper data types and geometry handling.
        Uses DuckDB's bulk insert capabilities for optimal performance with large datasets.

        Args:
            data: List of cadastral feature dictionaries from bronze layer

        Returns:
            str: Name of the DuckDB table containing the transformed data
        """
        if not isinstance(data, list):
            raise ValueError(f"Expected list of features, got {type(data)}")
            
        if not data:
            raise ValueError("No features provided for transformation")
            
        self.log.info(
            f"Transforming {len(data)} cadastral features to DuckDB table using bulk insert"
        )
        
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
        
        # Prepare data for bulk insert - filter and validate first
        valid_rows = []
        skipped_count = 0
        
        for feature in data:
            try:
                # Extract values with validation
                bfe_number = feature.get('bfe_number')
                if not bfe_number:
                    skipped_count += 1
                    continue  # Skip features without BFE number
                    
                # Convert geometry from WKT to DuckDB geometry
                geometry_wkt = feature.get('geometry')
                if not geometry_wkt:
                    skipped_count += 1
                    continue  # Skip features without geometry
                
                # Prepare row data
                row = [
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
                ]
                valid_rows.append(row)
                
            except Exception as e:
                bfe_id = feature.get('bfe_number', 'unknown')
                self.log.warning(f"Failed to prepare feature {bfe_id}: {e}")
                skipped_count += 1
                continue
        
        if not valid_rows:
            raise ValueError("No valid features could be prepared for transformation")
            
        self.log.info(
            f"Prepared {len(valid_rows)} valid rows for bulk insert (skipped {skipped_count})"
        )
        
        # Use DuckDB's ultra-fast bulk insert with VALUES clause
        try:
            self.log.info("Using DuckDB bulk insert with VALUES clause for maximum performance...")
            
            # Process in large batches to maximize DuckDB's performance
            batch_size = 10000  # Larger batches for better performance
            inserted_count = 0
            
            for i in range(0, len(valid_rows), batch_size):
                batch = valid_rows[i:i + batch_size]
                
                # Create VALUES clause with placeholders
                values_placeholders = ",".join([
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))"
                ] * len(batch))
                
                # Flatten the batch data for the query
                flattened_params = []
                for row in batch:
                    flattened_params.extend(row)
                
                # Execute bulk insert
                self.conn.execute(f"""
                    INSERT INTO {table_name} (
                        bfe_number, business_event, business_process, latest_case_id,
                        id_local, id_namespace, registration_from, effect_from,
                        authority, is_worker_housing, is_common_lot, has_owner_apartments,
                        is_separated_road, agricultural_notation, geometry
                    ) VALUES {values_placeholders}
                """, flattened_params)
                
                inserted_count += len(batch)
                
                # Log progress every 50k records
                if inserted_count % 50000 == 0 or i + batch_size >= len(valid_rows):
                    progress_pct = inserted_count / len(valid_rows) * 100
                    self.log.info(
                        f"Bulk insert progress: {inserted_count:,}/{len(valid_rows):,} "
                        f"features ({progress_pct:.1f}%)"
                    )
            
            self.log.info(
                f"Successfully bulk inserted {inserted_count:,} features using VALUES clause"
            )
            
        except Exception as e:
            self.log.error(f"Bulk VALUES insert failed: {e}")
            # Fallback to executemany if VALUES fails
            self.log.info("Attempting executemany as fallback...")
            
            try:
                self.conn.executemany(f"""
                    INSERT INTO {table_name} (
                        bfe_number, business_event, business_process, latest_case_id,
                        id_local, id_namespace, registration_from, effect_from,
                        authority, is_worker_housing, is_common_lot, has_owner_apartments,
                        is_separated_road, agricultural_notation, geometry
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))
                """, valid_rows)
                
                self.log.info(
                    f"Successfully inserted {len(valid_rows):,} features using executemany fallback"
                )
                
            except Exception as fallback_e:
                self.log.error(f"Executemany fallback also failed: {fallback_e}")
                # Final fallback to smaller batches
                self.log.info("Attempting small batch insert as final fallback...")
                
                batch_size = 1000
                inserted_count = 0
                
                for i in range(0, len(valid_rows), batch_size):
                    batch = valid_rows[i:i + batch_size]
                    try:
                        self.conn.executemany(f"""
                            INSERT INTO {table_name} (
                                bfe_number, business_event, business_process, latest_case_id,
                                id_local, id_namespace, registration_from, effect_from,
                                authority, is_worker_housing, is_common_lot, has_owner_apartments,
                                is_separated_road, agricultural_notation, geometry
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText(?))
                        """, batch)
                        inserted_count += len(batch)
                        
                        if i % (batch_size * 10) == 0:  # Log every 10k records
                            self.log.info(
                                f"Small batch insert progress: {inserted_count:,}/"
                                f"{len(valid_rows):,} features"
                            )
                            
                    except Exception as batch_e:
                        self.log.error(f"Small batch insert failed at batch {i}: {batch_e}")
                        continue
                
                self.log.info(
                    f"Small batch insert completed: {inserted_count:,}/{len(valid_rows):,} features"
                )
        
        # Verify final count
        final_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"Final table contains {final_count} features")
        
        if final_count == 0:
            raise ValueError("No features were successfully inserted")
            
        return table_name

    def _validate_and_transform_from_table(self, source_table_name: str) -> str:
        """
        Validate and transform cadastral data from an existing DuckDB table using bulk operations.

        This method takes a DuckDB table containing bronze data and transforms it
        into a structured DuckDB table with proper data types and geometry handling.
        Uses DuckDB's native bulk operations for optimal performance.

        Args:
            source_table_name: Name of the DuckDB table containing bronze data

        Returns:
            str: Name of the DuckDB table containing the transformed data
        """
        self.log.info(
            f"Transforming cadastral data from table {source_table_name} "
            f"using DuckDB bulk operations"
        )
        
        # Check if source table exists and get row count
        try:
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {source_table_name}").fetchone()[0]
            self.log.info(f"Source table {source_table_name} contains {row_count:,} rows")
        except Exception as e:
            self.log.error(f"Failed to access source table {source_table_name}: {e}")
            raise ValueError(f"Source table {source_table_name} is not accessible")
        
        if row_count == 0:
            raise ValueError(f"Source table {source_table_name} is empty")
        
        # Create the target cadastral features table
        target_table_name = "cadastral_features"
        
        # Drop table if it exists
        self.conn.execute(f"DROP TABLE IF EXISTS {target_table_name}")
        
        # Create and populate the target table using a single bulk SQL operation
        self.log.info("Creating target table with bulk transformation...")
        
        # First, inspect the source table structure to understand available columns
        try:
            columns_info = self.conn.execute(f"DESCRIBE {source_table_name}").fetchall()
            column_names = [col[0] for col in columns_info]
            self.log.info(f"Available columns in {source_table_name}: {column_names}")
        except Exception as e:
            self.log.warning(f"Could not describe source table: {e}")
            column_names = []
        
        try:
            # Use DuckDB's native bulk transformation - much faster than row-by-row processing
            # Build SELECT clause dynamically based on available columns
            select_clauses = []
            
            # Handle each expected column with fallbacks
            if 'bfe_number' in column_names:
                select_clauses.append("CAST(bfe_number AS BIGINT) as bfe_number")
            else:
                select_clauses.append("NULL as bfe_number")
                
            if 'business_event' in column_names:
                select_clauses.append("business_event")
            else:
                select_clauses.append("NULL as business_event")
                
            if 'business_process' in column_names:
                select_clauses.append("business_process")
            else:
                select_clauses.append("NULL as business_process")
                
            if 'latest_case_id' in column_names:
                select_clauses.append("latest_case_id")
            else:
                select_clauses.append("NULL as latest_case_id")
                
            if 'id_local' in column_names:
                select_clauses.append("id_local")
            else:
                select_clauses.append("NULL as id_local")
                
            if 'id_namespace' in column_names:
                select_clauses.append("id_namespace")
            else:
                select_clauses.append("NULL as id_namespace")
                
            if 'registration_from' in column_names:
                select_clauses.append("CAST(registration_from AS TIMESTAMP) as registration_from")
            else:
                select_clauses.append("NULL as registration_from")
                
            if 'effect_from' in column_names:
                select_clauses.append("CAST(effect_from AS TIMESTAMP) as effect_from")
            else:
                select_clauses.append("NULL as effect_from")
                
            if 'authority' in column_names:
                select_clauses.append("authority")
            else:
                select_clauses.append("NULL as authority")
                
            if 'is_worker_housing' in column_names:
                select_clauses.append("COALESCE(is_worker_housing, false) as is_worker_housing")
            else:
                select_clauses.append("false as is_worker_housing")
                
            if 'is_common_lot' in column_names:
                select_clauses.append("COALESCE(is_common_lot, false) as is_common_lot")
            else:
                select_clauses.append("false as is_common_lot")
                
            if 'has_owner_apartments' in column_names:
                select_clauses.append(
                    "COALESCE(has_owner_apartments, false) as has_owner_apartments"
                )
            else:
                select_clauses.append("false as has_owner_apartments")
                
            if 'is_separated_road' in column_names:
                select_clauses.append("COALESCE(is_separated_road, false) as is_separated_road")
            else:
                select_clauses.append("false as is_separated_road")
                
            if 'agricultural_notation' in column_names:
                select_clauses.append("agricultural_notation")
            else:
                select_clauses.append("NULL as agricultural_notation")
                
            if 'geometry' in column_names:
                select_clauses.append("ST_GeomFromText(geometry) as geometry")
            else:
                self.log.error("No geometry column found - cannot proceed")
                raise ValueError("Geometry column is required but not found")
                
            select_clauses.append("CURRENT_TIMESTAMP as created_at")
            
            # Build the final query
            select_clause = ",\n                    ".join(select_clauses)
            
            # Prepare conditions (avoid backslashes in f-strings)
            bfe_condition = 'bfe_number IS NOT NULL' if 'bfe_number' in column_names else 'TRUE'
            geometry_condition = "geometry IS NOT NULL AND geometry != ''" if 'geometry' in column_names else 'FALSE'
            
            self.conn.execute(f"""
                CREATE TABLE {target_table_name} AS
                SELECT 
                    {select_clause}
                FROM {source_table_name}
                WHERE ({bfe_condition})
                  AND ({geometry_condition})
            """)
            
            # Get final count for verification
            final_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {target_table_name}"
            ).fetchone()[0]
            skipped_count = row_count - final_count
            
            self.log.info("✅ Bulk transformation completed successfully!")
            self.log.info(
                f"📊 Processed {final_count:,} valid features (skipped {skipped_count:,} invalid)"
            )
            
            if final_count == 0:
                raise ValueError("No valid features were processed - all rows were filtered out")
                
            return target_table_name
            
        except Exception as e:
            self.log.error(f"Bulk transformation failed: {e}")
            raise

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

        # Handle different data formats from bronze layer
        if isinstance(raw_data, str):
            # raw_data is a table name from _read_bronze_data_from_storage
            self.log.info(f"Processing bronze data from DuckDB table: {raw_data}")
            processed_table = self._validate_and_transform_from_table(raw_data)
        elif isinstance(raw_data, list):
            # raw_data is a list of features from in-memory passing
            self.log.info("Processing bronze data from in-memory list")
            processed_table = self._validate_and_transform(raw_data)
        else:
            self.log.error(
                f"Unexpected raw_data type: {type(raw_data)}. "
                f"Expected str (table name) or list (features)"
            )
            return None

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
