"""
Silver layer transformation for Svineflytning (pig movement) data.

This module processes raw pig movement data from the bronze layer into clean,
structured data for analytical purposes. It handles JSON parsing, data cleaning,
standardization, and exports processed data to both local and GCS storage.
"""

import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from dotenv import load_dotenv
from google.cloud import storage

# Import DateTimeEncoder for JSON serialization

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Initialize storage configuration
GCS_BUCKET = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
USE_GCS = bool(GCS_BUCKET and GOOGLE_CLOUD_PROJECT)

# Initialize GCS client if available
gcs_client = None
if USE_GCS:
    try:
        gcs_client = storage.Client(project=GOOGLE_CLOUD_PROJECT)
        logger.debug(f"Initialized GCS client for project: {GOOGLE_CLOUD_PROJECT}")
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        logger.warning("Falling back to local storage")
        USE_GCS = False


class SvineflytningSilverProcessor:
    """
    Silver layer processor for pig movement data.

    This class transforms raw pig movement data from the bronze layer into
    structured, clean data using DuckDB for processing.
    """

    def __init__(self, output_dir: str = "/data/silver/svineflytning"):
        """
        Initialize the silver processor.

        Args:
            output_dir: Base directory for silver layer output
        """
        self.output_dir = Path(output_dir)
        self.conn = None
        self._setup_duckdb()

    def _setup_duckdb(self):
        """Setup DuckDB connection with necessary extensions."""
        try:
            self.conn = duckdb.connect(":memory:")
            # Install and load necessary extensions
            self.conn.execute("INSTALL json")
            self.conn.execute("LOAD json")
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            logger.info("✅ DuckDB initialized with JSON and HTTPFS extensions")
        except Exception as e:
            logger.error(f"Failed to setup DuckDB: {e}")
            raise

    def process_bronze_data(self, bronze_data_path: str, export_timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Process bronze pig movement data into silver layer.

        Args:
            bronze_data_path: Path to bronze data (local file or GCS path)
            export_timestamp: Timestamp for the export (defaults to current time)

        Returns:
            Dict containing processing results and metadata
        """
        if export_timestamp is None:
            export_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"Starting silver processing for: {bronze_data_path}")

        try:
            # Load and parse bronze data
            raw_data = self._load_bronze_data(bronze_data_path)
            if not raw_data:
                logger.warning("No data found in bronze layer")
                return {"success": False, "error": "No bronze data found"}

            # Process the data
            processed_count = self._transform_movements(raw_data, export_timestamp)

            # Export results
            export_result = self._export_silver_data(export_timestamp)

            result = {
                "success": True,
                "export_timestamp": export_timestamp,
                "processed_movements": processed_count,
                "output_path": export_result["destination"],
                "storage_type": export_result["storage_type"],
            }

            logger.info(f"Silver processing completed: {processed_count} movements processed")
            return result

        except Exception as e:
            logger.error(f"Error in silver processing: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _load_bronze_data(self, data_path: str) -> List[Dict[str, Any]]:
        """
        Load bronze data from local file or GCS.

        Args:
            data_path: Path to the bronze data file

        Returns:
            List of movement records from bronze data
        """
        logger.info(f"Loading bronze data from: {data_path}")

        try:
            if data_path.startswith("gs://"):
                # Load from GCS
                bucket_name = data_path.split("/")[2]
                blob_path = "/".join(data_path.split("/")[3:])
                bucket = gcs_client.bucket(bucket_name)
                blob = bucket.blob(blob_path)

                # Download to temporary file for processing
                with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp_file:
                    blob.download_to_filename(tmp_file.name)
                    with open(tmp_file.name, "r") as f:
                        data = json.load(f)
                os.unlink(tmp_file.name)
            else:
                # Load from local file
                with open(data_path, "r") as f:
                    data = json.load(f)

            logger.info(f"Loaded {len(data)} bronze records")
            return data

        except Exception as e:
            logger.error(f"Failed to load bronze data: {e}")
            raise

    def _transform_movements(self, raw_data: List[Dict[str, Any]], export_timestamp: str) -> int:
        """
        Transform raw movement data into structured silver tables.

        Args:
            raw_data: List of raw movement records from bronze
            export_timestamp: Timestamp for this processing run

        Returns:
            Number of movements processed
        """
        logger.info("Starting data transformation")

        # Extract all movements from all response chunks
        all_movements = []

        for chunk in raw_data:
            response = chunk.get("response", {}).get("Response", {})
            svineflytning_liste = response.get("SvineflytningListe", {})

            if isinstance(svineflytning_liste, dict) and "Svineflytning" in svineflytning_liste:
                movements = svineflytning_liste["Svineflytning"]
                if isinstance(movements, list):
                    for movement in movements:
                        # Add metadata from chunk
                        movement["_chunk_timestamp"] = chunk.get("timestamp")
                        movement["_chunk_start_date"] = chunk.get("start_date")
                        movement["_chunk_end_date"] = chunk.get("end_date")
                        all_movements.append(movement)

        logger.info(f"Extracted {len(all_movements)} total movements from bronze data")

        if not all_movements:
            logger.warning("No movements found in bronze data")
            return 0

        # Create DuckDB table from movements using direct data insertion
        self.conn.execute("DROP TABLE IF EXISTS raw_movements")

        # Create the raw_movements table with proper schema - capturing ALL fields
        self.conn.execute("""
            CREATE TABLE raw_movements (
                Id BIGINT,
                Oprindelse VARCHAR,
                Handling VARCHAR,
                FlytteTidspunkt_SvineflytDato DATE,
                FlytteTidspunkt_SvineflytTidspunkt INTEGER,
                FlytteTidspunkt_SvineflytRaekkefoelge INTEGER,
                Afsender_Landekode VARCHAR,
                Afsender_ChrNummer BIGINT,
                Afsender_BesaetningsNummer BIGINT,
                Afsender_Ejendom_Adresse VARCHAR,
                Afsender_Ejendom_ByNavn VARCHAR,
                Afsender_Ejendom_PostNummer INTEGER,
                Afsender_Ejendom_PostDistrikt VARCHAR,
                Afsender_Ejendom_KommuneNummer INTEGER,
                Afsender_Ejendom_KommuneNavn VARCHAR,
                Afsender_Ejendom_DatoOpret DATE,
                Afsender_Ejendom_DatoOpdatering DATE,
                Afsender_UdlandsEjendom VARCHAR,
                Modtager_Landekode VARCHAR,
                Modtager_ChrNummer BIGINT,
                Modtager_BesaetningsNummer BIGINT,
                Modtager_Ejendom_Adresse VARCHAR,
                Modtager_Ejendom_ByNavn VARCHAR,
                Modtager_Ejendom_PostNummer INTEGER,
                Modtager_Ejendom_PostDistrikt VARCHAR,
                Modtager_Ejendom_KommuneNummer INTEGER,
                Modtager_Ejendom_KommuneNavn VARCHAR,
                Modtager_Ejendom_DatoOpret DATE,
                Modtager_Ejendom_DatoOpdatering DATE,
                Modtager_UdlandsEjendom VARCHAR,
                AntalDyr_AntalDyrIAlt INTEGER,
                AntalDyr_AntalSoer INTEGER,
                AntalDyr_AntalSlagtesvin INTEGER,
                AntalDyr_Antal190LitersContainere INTEGER,
                AntalDyr_Antal240LitersContainere INTEGER,
                Koeretoej_Forvogn_Landekode VARCHAR,
                Koeretoej_Forvogn_RegNr VARCHAR,
                Koeretoej_Haenger_Landekode VARCHAR,
                Koeretoej_Haenger_RegNr VARCHAR,
                Omlaesser VARCHAR,
                TracesDokument VARCHAR,
                Sundhedscertifikat VARCHAR,
                IndberetterLogon VARCHAR,
                IndberetningForetaget TIMESTAMP,
                _chunk_timestamp VARCHAR,
                _chunk_start_date DATE,
                _chunk_end_date DATE
            )
        """)

        # Insert movements directly into DuckDB
        if all_movements:
            for movement in all_movements:
                # Extract nested data safely
                flyttetidspunkt = movement.get("FlytteTidspunkt") or {}
                afsender = movement.get("Afsender") or {}
                afsender_ejendom = afsender.get("Ejendom") or {}
                modtager = movement.get("Modtager") or {}
                modtager_ejendom = modtager.get("Ejendom") or {}
                antal_dyr = movement.get("AntalDyr") or {}
                koeretoej = movement.get("Koeretoej") or {}
                forvogn = koeretoej.get("Forvogn") or {}
                haenger = koeretoej.get("Haenger") or {}

                self.conn.execute(
                    """
                    INSERT INTO raw_movements VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                        ?, ?, ?, ?, ?, ?, ?
                    )
                """,
                    [
                        movement.get("Id"),
                        movement.get("Oprindelse"),
                        movement.get("Handling"),
                        flyttetidspunkt.get("SvineflytDato"),
                        flyttetidspunkt.get("SvineflytTidspunkt"),
                        flyttetidspunkt.get("SvineflytRaekkefoelge"),
                        afsender.get("Landekode"),
                        afsender.get("ChrNummer"),
                        afsender.get("BesaetningsNummer"),
                        afsender_ejendom.get("Adresse"),
                        afsender_ejendom.get("ByNavn"),
                        afsender_ejendom.get("PostNummer"),
                        afsender_ejendom.get("PostDistrikt"),
                        afsender_ejendom.get("KommuneNummer"),
                        afsender_ejendom.get("KommuneNavn"),
                        afsender_ejendom.get("DatoOpret"),
                        afsender_ejendom.get("DatoOpdatering"),
                        afsender.get("UdlandsEjendom"),
                        modtager.get("Landekode"),
                        modtager.get("ChrNummer"),
                        modtager.get("BesaetningsNummer"),
                        modtager_ejendom.get("Adresse"),
                        modtager_ejendom.get("ByNavn"),
                        modtager_ejendom.get("PostNummer"),
                        modtager_ejendom.get("PostDistrikt"),
                        modtager_ejendom.get("KommuneNummer"),
                        modtager_ejendom.get("KommuneNavn"),
                        modtager_ejendom.get("DatoOpret"),
                        modtager_ejendom.get("DatoOpdatering"),
                        modtager.get("UdlandsEjendom"),
                        antal_dyr.get("AntalDyrIAlt"),
                        antal_dyr.get("AntalSoer"),
                        antal_dyr.get("AntalSlagtesvin"),
                        antal_dyr.get("Antal190LitersContainere"),
                        antal_dyr.get("Antal240LitersContainere"),
                        forvogn.get("Landekode"),
                        forvogn.get("RegNr"),
                        haenger.get("Landekode") if haenger else None,
                        haenger.get("RegNr") if haenger else None,
                        movement.get("Omlaesser"),
                        movement.get("TracesDokument"),
                        movement.get("Sundhedscertifikat"),
                        movement.get("IndberetterLogon"),
                        movement.get("IndberetningForetaget"),
                        movement.get("_chunk_timestamp"),
                        movement.get("_chunk_start_date"),
                        movement.get("_chunk_end_date"),
                    ],
                )

        # Transform into structured silver tables
        self._create_movements_table(export_timestamp)
        self._create_properties_table(export_timestamp)
        self._create_vehicles_table(export_timestamp)

        # Get final count
        movement_count = self.conn.execute("SELECT COUNT(*) FROM silver_movements").fetchone()[0]
        logger.info(f"Transformed {movement_count} movements into silver tables")

        return movement_count

    def _create_movements_table(self, export_timestamp: str):
        """Create the main movements table with cleaned and standardized data."""
        logger.info("Creating silver movements table")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE silver_movements AS
            SELECT
                -- Movement identification
                Id as movement_id,
                Oprindelse as origin_system,
                Handling as action_type,
                
                -- Movement timing
                FlytteTidspunkt_SvineflytDato as movement_date,
                FlytteTidspunkt_SvineflytTidspunkt as movement_time,
                FlytteTidspunkt_SvineflytRaekkefoelge as movement_sequence,
                
                -- Sender information
                Afsender_Landekode as sender_country_code,
                Afsender_ChrNummer as sender_chr_number,
                Afsender_BesaetningsNummer as sender_herd_number,
                Afsender_Ejendom_Adresse as sender_address,
                Afsender_Ejendom_ByNavn as sender_city_name,
                Afsender_Ejendom_PostNummer as sender_postal_code,
                Afsender_Ejendom_PostDistrikt as sender_postal_district,
                Afsender_Ejendom_KommuneNummer as sender_municipality_code,
                Afsender_Ejendom_KommuneNavn as sender_municipality_name,
                Afsender_Ejendom_DatoOpret as sender_property_created,
                Afsender_Ejendom_DatoOpdatering as sender_property_updated,
                Afsender_UdlandsEjendom as sender_foreign_property,
                
                -- Receiver information
                Modtager_Landekode as receiver_country_code,
                Modtager_ChrNummer as receiver_chr_number,
                Modtager_BesaetningsNummer as receiver_herd_number,
                Modtager_Ejendom_Adresse as receiver_address,
                Modtager_Ejendom_ByNavn as receiver_city_name,
                Modtager_Ejendom_PostNummer as receiver_postal_code,
                Modtager_Ejendom_PostDistrikt as receiver_postal_district,
                Modtager_Ejendom_KommuneNummer as receiver_municipality_code,
                Modtager_Ejendom_KommuneNavn as receiver_municipality_name,
                Modtager_Ejendom_DatoOpret as receiver_property_created,
                Modtager_Ejendom_DatoOpdatering as receiver_property_updated,
                Modtager_UdlandsEjendom as receiver_foreign_property,
                
                -- Animal counts
                AntalDyr_AntalDyrIAlt as total_animals,
                AntalDyr_AntalSoer as sow_count,
                AntalDyr_AntalSlagtesvin as slaughter_pig_count,
                AntalDyr_Antal190LitersContainere as containers_190l,
                AntalDyr_Antal240LitersContainere as containers_240l,
                
                -- Transport information
                Koeretoej_Forvogn_Landekode as vehicle_country_code,
                Koeretoej_Forvogn_RegNr as vehicle_registration,
                Koeretoej_Haenger_Landekode as trailer_country_code,
                Koeretoej_Haenger_RegNr as trailer_registration,
                
                -- Additional transport and documentation
                Omlaesser as transshipment_info,
                TracesDokument as traces_document,
                Sundhedscertifikat as health_certificate,
                
                -- Administrative information
                IndberetterLogon as reporter_login,
                IndberetningForetaget as report_timestamp,
                
                -- Processing metadata
                '{export_timestamp}' as processed_timestamp,
                _chunk_timestamp as source_chunk_timestamp,
                _chunk_start_date as source_period_start,
                _chunk_end_date as source_period_end,
                
                -- Data quality flags
                CASE
                    WHEN Handling = 'slet' THEN true
                    ELSE false
                END as is_deleted,
                CASE
                    WHEN Id IS NULL THEN true
                    ELSE false
                END as is_invalid,
                CASE
                    WHEN AntalDyr_AntalDyrIAlt IS NULL OR AntalDyr_AntalDyrIAlt <= 0 THEN true
                    ELSE false
                END as missing_animal_count
                
            FROM raw_movements
            WHERE Id IS NOT NULL  -- Filter out completely invalid records
        """)

        # Log data quality metrics
        total_count = self.conn.execute("SELECT COUNT(*) FROM silver_movements").fetchone()[0]
        deleted_count = self.conn.execute("SELECT COUNT(*) FROM silver_movements WHERE is_deleted = true").fetchone()[0]
        invalid_count = self.conn.execute("SELECT COUNT(*) FROM silver_movements WHERE is_invalid = true").fetchone()[0]
        missing_animals = self.conn.execute(
            "SELECT COUNT(*) FROM silver_movements WHERE missing_animal_count = true"
        ).fetchone()[0]

        logger.info(
            f"Movements table created: {total_count} total, {deleted_count} deleted, "
            f"{invalid_count} invalid, {missing_animals} missing animal counts"
        )

    def _create_properties_table(self, export_timestamp: str):
        """Create a properties table with sender and receiver property information."""
        logger.info("Creating silver properties table")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE silver_properties AS
            WITH sender_properties AS (
                SELECT DISTINCT
                    Afsender_ChrNummer as chr_number,
                    Afsender_BesaetningsNummer as herd_number,
                    'sender' as property_role,
                    Afsender_Ejendom_Adresse as address,
                    Afsender_Ejendom_ByNavn as city_name,
                    Afsender_Ejendom_PostNummer as postal_code,
                    Afsender_Ejendom_PostDistrikt as postal_district,
                    Afsender_Ejendom_KommuneNummer as municipality_code,
                    Afsender_Ejendom_KommuneNavn as municipality_name,
                    Afsender_Ejendom_DatoOpret as date_created,
                    Afsender_Ejendom_DatoOpdatering as date_updated,
                    Afsender_UdlandsEjendom as foreign_property
                FROM raw_movements
                WHERE Afsender_ChrNummer IS NOT NULL
            ),
            receiver_properties AS (
                SELECT DISTINCT
                    Modtager_ChrNummer as chr_number,
                    Modtager_BesaetningsNummer as herd_number,
                    'receiver' as property_role,
                    Modtager_Ejendom_Adresse as address,
                    Modtager_Ejendom_ByNavn as city_name,
                    Modtager_Ejendom_PostNummer as postal_code,
                    Modtager_Ejendom_PostDistrikt as postal_district,
                    Modtager_Ejendom_KommuneNummer as municipality_code,
                    Modtager_Ejendom_KommuneNavn as municipality_name,
                    Modtager_Ejendom_DatoOpret as date_created,
                    Modtager_Ejendom_DatoOpdatering as date_updated,
                    Modtager_UdlandsEjendom as foreign_property
                FROM raw_movements
                WHERE Modtager_ChrNummer IS NOT NULL
            ),
            all_properties AS (
                SELECT * FROM sender_properties
                UNION ALL
                SELECT * FROM receiver_properties
            )
            SELECT
                chr_number,
                herd_number,
                address,
                city_name,
                postal_code,
                postal_district,
                municipality_code,
                municipality_name,
                date_created,
                date_updated,
                foreign_property,
                '{export_timestamp}' as processed_timestamp,
                COUNT(*) as occurrence_count,
                ARRAY_AGG(DISTINCT property_role) as roles
            FROM all_properties
            WHERE chr_number IS NOT NULL
            GROUP BY chr_number, herd_number, address, city_name, postal_code,
                     postal_district, municipality_code, municipality_name,
                     date_created, date_updated, foreign_property
        """)

        property_count = self.conn.execute("SELECT COUNT(*) FROM silver_properties").fetchone()[0]
        logger.info(f"Properties table created with {property_count} unique properties")

    def _create_vehicles_table(self, export_timestamp: str):
        """Create a vehicles table with transport information."""
        logger.info("Creating silver vehicles table")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE silver_vehicles AS
            SELECT DISTINCT
                Koeretoej_Forvogn_RegNr as vehicle_registration,
                Koeretoej_Forvogn_Landekode as vehicle_country_code,
                Koeretoej_Haenger_RegNr as trailer_registration,
                Koeretoej_Haenger_Landekode as trailer_country_code,
                '{export_timestamp}' as processed_timestamp,
                COUNT(*) as usage_count,
                MIN(FlytteTidspunkt_SvineflytDato) as first_movement_date,
                MAX(FlytteTidspunkt_SvineflytDato) as last_movement_date
            FROM raw_movements
            WHERE Koeretoej_Forvogn_RegNr IS NOT NULL
            GROUP BY Koeretoej_Forvogn_RegNr, Koeretoej_Forvogn_Landekode, 
                     Koeretoej_Haenger_RegNr, Koeretoej_Haenger_Landekode
        """)

        vehicle_count = self.conn.execute("SELECT COUNT(*) FROM silver_vehicles").fetchone()[0]
        logger.info(f"Vehicles table created with {vehicle_count} unique vehicles")

    def _export_silver_data(self, export_timestamp: str) -> Dict[str, Any]:
        """
        Export processed silver data to storage.

        Args:
            export_timestamp: Timestamp for this export

        Returns:
            Dict containing export metadata
        """
        logger.info("Exporting silver data")

        # Create local output directory first (always needed)
        local_destination = self.output_dir / export_timestamp
        local_destination.mkdir(parents=True, exist_ok=True)

        # 🚀 ENHANCED: Try native GCS export first if available
        gcs_export_success = False
        tables = ["silver_movements", "silver_properties", "silver_vehicles"]
        exported_files = []

        if USE_GCS:
            try:
                from unified_pipeline.util.gcs_access import GCSDataAccess

                gcs_access = GCSDataAccess()

                # Try native DuckDB HTTPFS GCS export for each table
                for table in tables:
                    filename = f"{table.replace('silver_', '')}.parquet"
                    gcs_path = f"gs://{GCS_BUCKET}/silver/svineflytning/{export_timestamp}/{filename}"

                    gcs_access.export_to_gcs_native(
                        connection=self.conn, table_name=table, gcs_path=gcs_path, compression="zstd"
                    )
                    exported_files.append(gcs_path)
                    logger.info(f"✅ Native GCS export successful: {gcs_path}")

                gcs_export_success = True
                destination_base = f"gs://{GCS_BUCKET}/silver/svineflytning/{export_timestamp}"
                storage_type = "gcs"

            except Exception as e:
                logger.warning(f"Native GCS export failed, using fallback: {e}")
                gcs_export_success = False

        if not gcs_export_success:
            # Fallback: Export each table to local files first
            for table in tables:
                filename = f"{table.replace('silver_', '')}.parquet"
                local_path = local_destination / filename

                # Export to local file using DuckDB COPY
                self.conn.execute(f"""
                    COPY {table} TO '{local_path}' (FORMAT PARQUET)
                """)
                logger.info(f"Exported {table} to local file: {local_path}")

        # If using GCS and native export didn't work, upload the local files
        if USE_GCS and not gcs_export_success:
            success = self._upload_silver_data_to_gcs(local_destination, export_timestamp)
            if success:
                # Build GCS paths for return
                for table in tables:
                    filename = f"{table.replace('silver_', '')}.parquet"
                    gcs_path = f"gs://{GCS_BUCKET}/silver/svineflytning/{export_timestamp}/{filename}"
                    exported_files.append(gcs_path)
                destination_base = f"gs://{GCS_BUCKET}/silver/svineflytning/{export_timestamp}"
                storage_type = "gcs"
            else:
                logger.warning("GCS upload failed, falling back to local files")
                for table in tables:
                    filename = f"{table.replace('silver_', '')}.parquet"
                    local_path = local_destination / filename
                    exported_files.append(str(local_path))
                destination_base = str(local_destination)
                storage_type = "local"
        elif not gcs_export_success:
            # Local storage only (populate files list if not done already)
            if not exported_files:
                for table in tables:
                    filename = f"{table.replace('silver_', '')}.parquet"
                    local_path = local_destination / filename
                    exported_files.append(str(local_path))
                destination_base = str(local_destination)
                storage_type = "local"

        return {
            "destination": destination_base,
            "storage_type": storage_type,
            "exported_files": exported_files,
            "export_timestamp": export_timestamp,
        }

    def _upload_silver_data_to_gcs(self, silver_dir: Path, export_timestamp: str) -> bool:
        """
        Upload all silver parquet files to GCS using streaming.

        Args:
            silver_dir: Local directory containing silver parquet files
            export_timestamp: Timestamp string for the export (YYYYMMDD_HHMMSS)

        Returns:
            True if upload successful, False otherwise
        """
        try:
            # Try to import GCS utilities
            from unified_pipeline.util.gcs_access import GCSDataAccess

            gcs_access = GCSDataAccess()

            # Find all parquet files in the silver directory
            parquet_files = list(silver_dir.glob("*.parquet"))

            if not parquet_files:
                logger.warning(f"No parquet files found in {silver_dir}")
                return False

            logger.info(f"Uploading {len(parquet_files)} silver files to GCS bucket '{GCS_BUCKET}'")

            uploaded_count = 0
            for parquet_file in parquet_files:
                try:
                    # Create GCS path: silver/svineflytning/{timestamp}/{filename}
                    gcs_path = f"gs://{GCS_BUCKET}/silver/svineflytning/{export_timestamp}/{parquet_file.name}"

                    # Upload file using streaming
                    with open(parquet_file, "rb") as src:
                        with gcs_access.fs.open(gcs_path, "wb") as dst:
                            import shutil

                            shutil.copyfileobj(src, dst)

                    logger.info(f"✅ Uploaded {parquet_file.name} to {gcs_path}")
                    uploaded_count += 1

                except Exception as e:
                    logger.error(f"❌ Failed to upload {parquet_file.name}: {e}")

            if uploaded_count == len(parquet_files):
                logger.info(f"✅ Successfully uploaded all {uploaded_count} silver files to GCS")
                return True
            else:
                logger.warning(f"⚠️ Only uploaded {uploaded_count}/{len(parquet_files)} silver files")
                return False

        except ImportError:
            logger.warning("GCS utilities not available - skipping silver data upload")
            return False
        except Exception as e:
            logger.error(f"❌ Error uploading silver data to GCS: {e}")
            return False


def process_svineflytning_silver(
    bronze_data_path: str, output_dir: str = "/data/silver/svineflytning", export_timestamp: Optional[str] = None
) -> Dict[str, Any]:
    """
    Main function to process svineflytning data from bronze to silver.

    Args:
        bronze_data_path: Path to bronze data (local or GCS)
        output_dir: Output directory for silver data
        export_timestamp: Timestamp for the export

    Returns:
        Dict containing processing results
    """
    processor = SvineflytningSilverProcessor(output_dir)
    return processor.process_bronze_data(bronze_data_path, export_timestamp)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process Svineflytning data to silver layer")
    parser.add_argument("bronze_data_path", help="Path to bronze data file or GCS path")
    parser.add_argument("--output-dir", default="/data/silver/svineflytning", help="Output directory")
    parser.add_argument("--export-timestamp", help="Export timestamp (defaults to current time)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Process data
    result = process_svineflytning_silver(args.bronze_data_path, args.output_dir, args.export_timestamp)

    if result["success"]:
        print("✅ Silver processing completed successfully")
        print(f"📊 Processed {result['processed_movements']} movements")
        print(f"💾 Output: {result['output_path']}")
    else:
        print(f"❌ Silver processing failed: {result['error']}")
        exit(1)
