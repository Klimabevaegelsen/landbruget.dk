#!/usr/bin/env python3

import json
import logging
import os
import sys
import traceback
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

import ijson
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import secretmanager, storage
from pyproj import Transformer
from shapely.geometry import shape

# Import schema documentation
try:
    import duckdb

    try:
        from backend.common.schema_documentation import SchemaDocumentationManager
    except ImportError as e:
        import warnings

        warnings.warn(f"Schema documentation not available: {e}")
        SchemaDocumentationManager = None
except ImportError:
    # Fallback for when running standalone
    duckdb = None
    SchemaDocumentationManager = None

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def flush_logs():
    """Ensure stdout and stderr are flushed."""
    sys.stdout.flush()
    sys.stderr.flush()


class PropertyDataProcessor:
    """Handles privacy transformations and Parquet conversion."""

    def __init__(self):
        self.cpr_to_uuid_mapping = {}  # Cache for consistent UUID mapping
        # Default assumption: Danish data is typically in EPSG:25832 (UTM Zone 32N)
        # We'll convert to EPSG:4326 as required by README
        self.source_crs = "EPSG:25832"  # Danish UTM Zone 32N (common for Danish official data)
        self.target_crs = "EPSG:4326"  # WGS84 (required by README for Silver layer)
        self.crs_transformer = None
        self.crs_detection_done = False
        self.pipeline_start_time = datetime.now()  # Track when processing started
        # ✅ MIGRATION: Create long-lived DuckDB connection
        self.conn = None
        try:
            self.conn = duckdb.connect()
            logger.info("✅ DuckDB connection established for property processing")
        except ImportError:
            logger.warning("DuckDB not available - schema documentation will be skipped")

    def __del__(self):
        """Clean up DuckDB connection."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

    def detect_and_setup_crs_transformer(self, sample_geometry):
        """Detect CRS from sample geometry and setup transformer if needed."""
        if self.crs_detection_done:
            return

        # Try to detect CRS from coordinate ranges
        if isinstance(sample_geometry, dict) and "coordinates" in sample_geometry:
            coords = sample_geometry["coordinates"]

            # Flatten coordinates to check ranges
            flat_coords = []

            def flatten_coords(obj):
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, (list, tuple)):
                            flatten_coords(item)
                        elif isinstance(item, (int, float)):
                            flat_coords.append(item)

            flatten_coords(coords)

            if len(flat_coords) >= 2:
                # Check x,y ranges to guess CRS
                x_coords = flat_coords[::2]  # Every even index (x coordinates)
                y_coords = flat_coords[1::2]  # Every odd index (y coordinates)

                if x_coords and y_coords:
                    min_x, max_x = min(x_coords), max(x_coords)
                    min_y, max_y = min(y_coords), max(y_coords)

                    # Check if coordinates look like they're already in WGS84
                    if (
                        (-180 <= min_x <= 180)
                        and (-90 <= min_y <= 90)
                        and (-180 <= max_x <= 180)
                        and (-90 <= max_y <= 90)
                    ):
                        logger.info("Detected coordinates appear to be in WGS84 (EPSG:4326) - no transformation needed")
                        self.source_crs = "EPSG:4326"
                    # Check if coordinates look like Danish UTM (typical ranges)
                    elif (400000 <= min_x <= 900000) and (6000000 <= min_y <= 6500000):
                        logger.info("Detected coordinates appear to be in Danish UTM Zone 32N (EPSG:25832)")
                        self.source_crs = "EPSG:25832"
                    else:
                        logger.warning(
                            f"Uncertain CRS detection. X range: {min_x:.0f}-{max_x:.0f}, Y range: {min_y:.0f}-{max_y:.0f}"
                        )
                        logger.warning("Assuming Danish UTM Zone 32N (EPSG:25832)")
                        self.source_crs = "EPSG:25832"

        # Setup transformer if conversion is needed
        if self.source_crs != self.target_crs:
            logger.info(f"Setting up CRS transformation: {self.source_crs} → {self.target_crs}")
            self.crs_transformer = Transformer.from_crs(self.source_crs, self.target_crs, always_xy=True)
        else:
            logger.info("No CRS transformation needed - data already in EPSG:4326")
            self.crs_transformer = None

        self.crs_detection_done = True

    def transform_geometry_to_4326(self, geometry):
        """Transform geometry to EPSG:4326 if needed."""
        if not geometry:
            return geometry

        # Detect CRS on first geometry
        if not self.crs_detection_done:
            self.detect_and_setup_crs_transformer(geometry)

        # If no transformation needed, return as-is
        if not self.crs_transformer:
            return geometry

        try:
            # Convert to shapely geometry if it's a dict
            if isinstance(geometry, dict):
                geom = shape(geometry)
            else:
                geom = geometry

            # Transform coordinates based on geometry type
            if geom.geom_type == "Point":
                x, y = self.crs_transformer.transform(geom.x, geom.y)
                return {"type": "Point", "coordinates": [x, y]}

            elif geom.geom_type == "Polygon":

                def transform_coord_list(coords):
                    return [list(self.crs_transformer.transform(x, y)) for x, y in coords]

                exterior = transform_coord_list(geom.exterior.coords)
                holes = [transform_coord_list(hole.coords) for hole in geom.interiors]

                if holes:
                    return {"type": "Polygon", "coordinates": [exterior] + holes}
                else:
                    return {"type": "Polygon", "coordinates": [exterior]}

            elif geom.geom_type == "MultiPolygon":
                transformed_coords = []
                for polygon in geom.geoms:
                    exterior = [list(self.crs_transformer.transform(x, y)) for x, y in polygon.exterior.coords]
                    holes = [
                        [list(self.crs_transformer.transform(x, y)) for x, y in hole.coords]
                        for hole in polygon.interiors
                    ]
                    if holes:
                        transformed_coords.append([exterior] + holes)
                    else:
                        transformed_coords.append([exterior])
                return {"type": "MultiPolygon", "coordinates": transformed_coords}

            else:
                logger.warning(f"Unsupported geometry type for transformation: {geom.geom_type}")
                return geometry

        except Exception as e:
            logger.warning(f"Failed to transform geometry: {e}")
            return geometry  # Return original if transformation fails

    def generate_uuid_for_cpr(self, cpr_id):
        """Generate consistent UUID for CPR numbers."""
        if not cpr_id:
            return None
        if cpr_id not in self.cpr_to_uuid_mapping:
            self.cpr_to_uuid_mapping[cpr_id] = str(uuid.uuid4())
        return self.cpr_to_uuid_mapping[cpr_id]

    def has_foreign_address(self, person_data):
        """Check if person has foreign address information."""
        if not person_data:
            return False
        udrejse_indrejse = person_data.get("UdrejseIndrejse", {})
        return bool(
            udrejse_indrejse.get("Udenlandsadresse")
            or udrejse_indrejse.get("cprLandUdrejse")
            or udrejse_indrejse.get("cprLandekodeUdrejse")
        )

    def transform_person_data(self, properties):
        """Apply privacy transformations to person ownership data."""
        if not properties or "ejendePerson" not in properties:
            return properties

        person_data = properties["ejendePerson"].get("Person", {})
        if not person_data:
            return properties

        # Create transformed copy
        transformed = properties.copy()
        transformed_person = person_data.copy()

        # 1. Remove gender field
        if "koen" in transformed_person:
            del transformed_person["koen"]

        # 2. Replace CPR ID with UUID
        if "id" in transformed_person:
            original_id = transformed_person["id"]
            transformed_person["id"] = self.generate_uuid_for_cpr(original_id)

        # 3. Remove ALL personal address information (including municipality)
        if "CprAdresse" in transformed_person:
            del transformed_person["CprAdresse"]

        # Remove detailed address information
        if "Adresseoplysninger" in transformed_person:
            del transformed_person["Adresseoplysninger"]

        # 4. Add abroad flag
        transformed_person["lives_abroad"] = self.has_foreign_address(person_data)

        # 5. Keep privacy protection notice (Beskyttelser) as is
        # No changes needed - this stays

        # 6. Remove foreign address details but keep the flag
        if "UdrejseIndrejse" in transformed_person:
            del transformed_person["UdrejseIndrejse"]

        # 7. Remove birth date (potential CPR info)
        if "foedselsdato" in transformed_person:
            del transformed_person["foedselsdato"]
        if "foedselsdatoUsikkerhedsmarkering" in transformed_person:
            del transformed_person["foedselsdatoUsikkerhedsmarkering"]

        # Update the transformed properties
        transformed["ejendePerson"]["Person"] = transformed_person

        return transformed

    def clean_empty_structures(self, obj):
        """Recursively remove empty dictionaries/structures that cause Parquet serialization issues."""
        if isinstance(obj, dict):
            cleaned = {}
            for key, value in obj.items():
                cleaned_value = self.clean_empty_structures(value)
                # Only keep non-empty values
                if cleaned_value or cleaned_value == 0 or not cleaned_value or cleaned_value == "":
                    # Keep primitive values and non-empty containers
                    if not isinstance(cleaned_value, (dict, list)) or cleaned_value:
                        cleaned[key] = cleaned_value
            return cleaned
        elif isinstance(obj, list):
            return [self.clean_empty_structures(item) for item in obj if item or item == 0 or not item or item == ""]
        else:
            return obj

    def normalize_schema_across_features(self, all_features):
        """Ensure consistent schema across all features."""
        logger.info("Analyzing schema across all features...")
        flush_logs()

        # Collect all possible keys from all features
        all_keys = set()
        for feature in all_features:
            if "properties" in feature:
                all_keys.update(self._get_all_nested_keys(feature["properties"]))

        logger.info(f"Found {len(all_keys)} unique property keys across all features")
        flush_logs()

        # Pad all features with missing keys (as None)
        normalized_features = []
        for feature in all_features:
            if "properties" in feature:
                normalized_props = self._pad_with_missing_keys(feature["properties"], all_keys)
                feature_copy = feature.copy()
                feature_copy["properties"] = normalized_props
                normalized_features.append(feature_copy)
            else:
                normalized_features.append(feature)

        logger.info("Schema normalization complete")
        flush_logs()
        return normalized_features

    def _get_all_nested_keys(self, obj, prefix=""):
        """Recursively get all nested keys from a dictionary."""
        keys = set()
        if isinstance(obj, dict):
            for key, value in obj.items():
                full_key = f"{prefix}.{key}" if prefix else key
                keys.add(full_key)
                if isinstance(value, dict):
                    keys.update(self._get_all_nested_keys(value, full_key))
        return keys

    def _pad_with_missing_keys(self, obj, all_keys, prefix=""):
        """Recursively pad object with missing keys."""
        if not isinstance(obj, dict):
            return obj

        # Create a padded copy
        padded = obj.copy()

        # Find missing keys at this level
        current_keys = set(obj.keys())
        for key in all_keys:
            if prefix:
                if key.startswith(f"{prefix}."):
                    relative_key = key[len(prefix) + 1 :]
                    if "." not in relative_key and relative_key not in current_keys:
                        padded[relative_key] = None
            else:
                if "." not in key and key not in current_keys:
                    padded[key] = None

        # Recursively pad nested dictionaries
        for key, value in padded.items():
            if isinstance(value, dict):
                new_prefix = f"{prefix}.{key}" if prefix else key
                padded[key] = self._pad_with_missing_keys(value, all_keys, new_prefix)

        return padded

    def process_json_to_parquet(self, json_file_path, output_parquet_path):
        """Process GeoJSON to Parquet with privacy transformations and batch processing."""
        logger.info(f"Starting JSON to Parquet processing: {json_file_path} -> {output_parquet_path}")
        flush_logs()

        batch_size = 500000  # Process in batches to manage memory
        batch_num = 0
        all_features = []

        with open(json_file_path, "rb") as f:
            # Use ijson to stream parse the large JSON file
            features = ijson.items(f, "features.item")

            current_batch = []
            for feature in features:
                # Apply privacy transformations
                if "properties" in feature:
                    feature["properties"] = self.transform_person_data(feature["properties"])
                    feature["properties"] = self.clean_empty_structures(feature["properties"])

                # Transform geometry to EPSG:4326
                if "geometry" in feature and feature["geometry"]:
                    feature["geometry"] = self.transform_geometry_to_4326(feature["geometry"])

                current_batch.append(feature)

                # Process batch when it reaches the limit
                if len(current_batch) >= batch_size:
                    batch_num += 1
                    logger.info(f"Processing batch {batch_num} with {len(current_batch)} features...")
                    flush_logs()

                    all_features.extend(current_batch)
                    current_batch = []

                    # Log memory usage periodically
                    if batch_num % 5 == 0:
                        logger.info(f"Processed {batch_num * batch_size} features so far...")
                        flush_logs()

            # Process remaining features
            if current_batch:
                batch_num += 1
                logger.info(f"Processing final batch {batch_num} with {len(current_batch)} features...")
                flush_logs()
                all_features.extend(current_batch)

        logger.info(f"Loaded {len(all_features)} features total from {batch_num} batches")
        flush_logs()

        # Normalize schema across all features
        all_features = self.normalize_schema_across_features(all_features)

        # Convert to DataFrame
        logger.info("Converting to DataFrame...")
        flush_logs()

        # Extract properties and geometry separately for easier DataFrame creation
        properties_list = []
        geometries = []

        for feature in all_features:
            properties_list.append(feature.get("properties", {}))
            geometries.append(json.dumps(feature.get("geometry")) if feature.get("geometry") else None)

        # Create DataFrame
        df = pa.Table.from_pylist(properties_list).to_pandas()
        df["geometry_json"] = geometries

        logger.info(f"Created DataFrame with shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        flush_logs()

        # Convert to Parquet
        logger.info("Converting to Parquet format...")
        flush_logs()

        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_parquet_path, compression="snappy")

        logger.info(f"Successfully saved Parquet file: {output_parquet_path}")
        logger.info(f"Final output size: {Path(output_parquet_path).stat().st_size / (1024**2):.1f} MB")
        flush_logs()

        # Generate schema documentation if available
        if SchemaDocumentationManager and duckdb:
            try:
                logger.info("Generating schema documentation for property owners data...")
                flush_logs()

                # ✅ MIGRATION: Use existing connection instead of creating new one
                if self.conn:
                    conn = self.conn
                    conn.execute(f"CREATE TABLE property_owners AS SELECT * FROM read_parquet('{output_parquet_path}')")
                else:
                    # Fallback if connection failed during init
                    conn = duckdb.connect()
                    conn.execute(f"CREATE TABLE property_owners AS SELECT * FROM read_parquet('{output_parquet_path}')")

                schema_manager = SchemaDocumentationManager(
                    connection=conn,
                    pipeline_name="property_owners_sftp",
                    pipeline_start_time=self.pipeline_start_time,
                    logger=logger,
                )

                # Generate documentation for the processed property data
                schema_manager.generate_all_documentation(["property_owners"], stage="silver")
                schema_manager.commit_to_github()

                logger.info("Schema documentation generated successfully")
                flush_logs()

            except Exception as e:
                logger.warning(f"Schema documentation failed (non-critical): {e}")
                flush_logs()


class LocalSFTPToGCSTransfer:
    def __init__(self):
        self.project_id = "landbrugsdata-1"
        self.bucket_name = "landbrugsdata-raw-data"
        self.storage_client = storage.Client()
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.processor = PropertyDataProcessor()
        logger.info("LocalSFTPToGCSTransfer initialized.")
        flush_logs()

    def get_secret(self, secret_name):
        logger.debug(f"Attempting to get secret: {secret_name}")
        flush_logs()
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        try:
            response = self.secret_client.access_secret_version(name=name)
            secret_value = response.payload.data.decode("UTF-8").strip()
            logger.debug(f"Successfully retrieved secret: {secret_name}")
            flush_logs()
            return secret_value
        except Exception as e:
            logger.error(f"Failed to retrieve secret: {secret_name}. Error: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def download_and_process_existing_file(self, gcs_zip_path, local_output_dir):
        """Download an existing ZIP file from GCS and process it locally."""
        logger.info(f"Downloading and processing: {gcs_zip_path}")
        flush_logs()

        # Create local directory
        os.makedirs(local_output_dir, exist_ok=True)

        # Download ZIP file
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_zip_path)

        local_zip_path = Path(local_output_dir) / "downloaded_property_data.zip"
        logger.info(f"Downloading ZIP to: {local_zip_path}")
        flush_logs()

        blob.download_to_filename(str(local_zip_path))
        logger.info(f"Download complete. File size: {local_zip_path.stat().st_size / (1024**3):.1f} GB")
        flush_logs()

        # Extract JSON from ZIP
        json_file_path = None
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            for file_info in zip_ref.filelist:
                if file_info.filename.endswith(".json") and not file_info.filename.endswith("_Metadata.json"):
                    json_file_path = Path(local_output_dir) / file_info.filename
                    logger.info(f"Extracting {file_info.filename}...")
                    flush_logs()
                    zip_ref.extract(file_info.filename, local_output_dir)
                    break

        if not json_file_path:
            raise FileNotFoundError("No JSON file found in ZIP")

        logger.info(f"Extracted JSON file: {json_file_path} ({json_file_path.stat().st_size / (1024**3):.1f} GB)")
        flush_logs()

        # Process JSON to Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_file_path = Path(local_output_dir) / f"local_property_owners_{timestamp}.parquet"
        logger.info("Starting JSON to Parquet processing with privacy transformations...")
        flush_logs()

        self.processor.process_json_to_parquet(str(json_file_path), str(parquet_file_path))

        logger.info(f"Processing complete. Parquet file: {parquet_file_path}")
        flush_logs()

        return str(parquet_file_path)


def main_local_test():
    """Main function for local testing."""
    logger.info("=== Starting Local Property Owners Processing Test ===")
    flush_logs()

    try:
        transfer_instance = LocalSFTPToGCSTransfer()

        # Use the latest bronze file we found earlier
        gcs_zip_path = "bronze/property_owners/20250602_031707_testigen_20250526120009.zip"
        local_output_dir = "./data/property_owners_analysis"

        parquet_file = transfer_instance.download_and_process_existing_file(gcs_zip_path, local_output_dir)

        logger.info(f"✅ Local processing successful! Output: {parquet_file}")
        flush_logs()

        # ✅ MIGRATION: Quick analysis using DuckDB instead of pandas
        logger.info("=== Quick Analysis ===")
        import duckdb

        conn = duckdb.connect()
        # Get basic statistics
        row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()[0]
        columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}')").fetchall()
        column_names = [col[0] for col in columns]

        logger.info(f"Processed {row_count} rows")
        logger.info(f"Columns: {column_names}")

        # Get file size instead of memory usage
        file_size_mb = Path(parquet_file).stat().st_size / 1024**2
        logger.info(f"File size: {file_size_mb:.1f} MB")

        # ✅ MIGRATION: Show sample of privacy transformations using DuckDB
        if "ejendePerson" in column_names:
            logger.info("Sample ejendePerson data (should show UUID instead of CPR):")
            sample_result = conn.execute(
                f"SELECT ejendePerson FROM read_parquet('{parquet_file}') WHERE ejendePerson IS NOT NULL LIMIT 1"
            ).fetchone()
            if sample_result and sample_result[0]:
                logger.info(f"Person data sample: {str(sample_result[0])[:100]}...")

        conn.close()

        flush_logs()
        return True

    except Exception as e:
        logger.error(f"Local processing failed: {e}")
        logger.error(traceback.format_exc())
        flush_logs()
        return False


if __name__ == "__main__":
    success = main_local_test()
    logger.info(f"Local test completed with success: {success}")
    flush_logs()
