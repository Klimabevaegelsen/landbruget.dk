#!/usr/bin/env python3
"""
Field Area Analysis Pipeline - Silver Layer Processor

This pipeline performs spatial analysis of agricultural fields against multiple datasets
and outputs only the essential spatial intersection results that can be joined with other data.

Outputs:
- field_id, block_id, cvr_number (identifiers)
- wetland_area_share (percentage coverage by wetlands)
- wetland_water_projects_share (percentage of wetland area covered by water projects)
- property_area_shares (by BFE number)
- soil_area_shares (by soil type)
- bnbo_area_shares (by status category)
- bnbo_water_projects_shares (by status category)
- water_projects_area_share (total water projects coverage)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb
import gcsfs
import pandas as pd


class FieldAnalysisSilver:
    """Silver layer processor for field spatial analysis"""

    def __init__(self, batch_size: int = 1000, memory_limit: str = "14GB", thread_count: int = 4):
        self.batch_size = batch_size
        self.memory_limit = memory_limit
        self.thread_count = thread_count
        self.conn = duckdb.connect()
        self.gcs = gcsfs.GCSFileSystem()
        self.setup_duckdb()

    def setup_duckdb(self):
        """Configure DuckDB for spatial joins"""
        self.conn.execute(f"SET memory_limit='{self.memory_limit}'")
        self.conn.execute(f"SET threads={self.thread_count}")
        self.conn.execute(f"SET max_memory='{self.memory_limit}'")

        # Install and load extensions for GCS access and spatial operations
        self.conn.execute("INSTALL httpfs")
        self.conn.execute("LOAD httpfs")
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

        print("✅ DuckDB Spatial and HTTPFS loaded - Field Area Analysis Silver Layer")
        print(
            f"   Memory: {self.memory_limit}, Threads: {self.thread_count}, Batch size: {self.batch_size:,}"
        )

    def find_latest_data_path(self, base_path: str, pattern: str = "data.parquet") -> str:
        """Find the most recent data file in a GCS directory structure"""
        try:
            # List all directories/files matching the pattern
            paths = self.gcs.glob(f"{base_path}/*/{pattern}")
            if not paths:
                # Try direct pattern match
                paths = self.gcs.glob(f"{base_path}/{pattern}")

            if not paths:
                raise FileNotFoundError(f"No files found matching pattern {base_path}/*/{pattern}")

            # Sort by path (timestamp directories sort chronologically)
            latest_path = sorted(paths)[-1]
            return f"gs://{latest_path}"

        except Exception as e:
            print(f"⚠️  Error finding latest data for {base_path}: {e}")
            raise

    def find_latest_property_data(self) -> str:
        """Find the most recent property cadastral data file"""
        try:
            base_path = "landbrugsdata-raw-data/silver/property_cadastral_merged"

            # First try the new timestamped directory structure: property_cadastral_merged/YYYYMMDD_HHMMSS/data.parquet
            timestamped_paths = self.gcs.glob(f"{base_path}/*/data.parquet")

            if timestamped_paths:
                # Sort by directory name (timestamp format sorts correctly)
                latest_path = sorted(timestamped_paths)[-1]
                return f"gs://{latest_path}"

            # Fallback to old structure: property_cadastral_merged/YYYY-MM-DD.parquet
            direct_paths = self.gcs.glob(f"{base_path}/*.parquet")

            if direct_paths:
                # Sort by filename (date format sorts correctly)
                latest_path = sorted(direct_paths)[-1]
                return f"gs://{latest_path}"

            raise FileNotFoundError(
                f"No property files found in {base_path} (tried both timestamped and direct structures)"
            )

        except Exception as e:
            print(f"⚠️  Error finding latest property data: {e}")
            raise

    def download_gcs_file(self, gcs_path: str, local_filename: str) -> str:
        """Download a GCS file to local storage and return the local path"""
        import tempfile
        from pathlib import Path

        # Create temp directory for this run
        temp_dir = Path(tempfile.gettempdir()) / "field_analysis_cache"
        temp_dir.mkdir(exist_ok=True)

        local_path = temp_dir / local_filename

        # Skip download if file already exists locally
        if local_path.exists():
            print(f"    📁 Using cached file: {local_path}")
            return str(local_path)

        print(f"    ⬇️  Downloading {gcs_path} to {local_path}")

        # Remove gs:// prefix for gcsfs
        gcs_file_path = gcs_path.replace("gs://", "")

        try:
            # Download using gcsfs
            self.gcs.get(gcs_file_path, str(local_path))
            print(
                f"    ✅ Downloaded {local_path.name} ({local_path.stat().st_size / 1024 / 1024:.1f} MB)"
            )
            return str(local_path)
        except Exception as e:
            print(f"    ❌ Failed to download {gcs_path}: {e}")
            raise

    def get_data_paths(self, year: str) -> dict[str, str]:
        """Get paths to all required data files, finding the most recent versions"""

        print("🔍 Discovering latest data files from GCS...")

        # Use environment variables if provided, otherwise discover latest
        fields_gcs_path = os.getenv(
            "FIELDS_DATA_PATH",
            self.find_latest_data_path(f"landbrugsdata-raw-data/silver/agricultural_fields_{year}"),
        )

        properties_gcs_path = os.getenv("PROPERTIES_DATA_PATH", self.find_latest_property_data())

        soil_gcs_path = os.getenv(
            "SOIL_DATA_PATH", self.find_latest_data_path("landbrugsdata-raw-data/silver/soil_types")
        )

        bnbo_gcs_path = os.getenv(
            "BNBO_DATA_PATH",
            self.find_latest_data_path("landbrugsdata-raw-data/silver/bnbo_status_dissolved"),
        )

        wetlands_gcs_path = os.getenv(
            "WETLANDS_DATA_PATH",
            self.find_latest_data_path("landbrugsdata-raw-data/silver/wetlands_dissolved"),
        )

        water_projects_gcs_path = os.getenv(
            "WATER_PROJECTS_DATA_PATH",
            self.find_latest_data_path("landbrugsdata-raw-data/silver/water_projects_dissolved"),
        )

        print("📂 Latest GCS paths discovered:")
        gcs_paths = {
            "fields": fields_gcs_path,
            "properties": properties_gcs_path,
            "soil_types": soil_gcs_path,
            "bnbo_status": bnbo_gcs_path,
            "wetlands": wetlands_gcs_path,
            "water_projects": water_projects_gcs_path,
        }

        for name, path in gcs_paths.items():
            print(f"   {name}: {path}")

        print("\n📥 Downloading files locally...")

        # Download all files locally
        local_paths = {
            "fields": self.download_gcs_file(
                fields_gcs_path, f"agricultural_fields_{year}.parquet"
            ),
            "properties": self.download_gcs_file(
                properties_gcs_path, "property_cadastral_merged.parquet"
            ),
            "soil_types": self.download_gcs_file(soil_gcs_path, "soil_types.parquet"),
            "bnbo_status": self.download_gcs_file(bnbo_gcs_path, "bnbo_status_dissolved.parquet"),
            "wetlands": self.download_gcs_file(wetlands_gcs_path, "wetlands_dissolved.parquet"),
            "water_projects": self.download_gcs_file(
                water_projects_gcs_path, "water_projects_dissolved.parquet"
            ),
        }

        print("\n📂 Local data paths ready:")
        for name, path in local_paths.items():
            print(f"   {name}: {path}")

        return local_paths

    def load_reference_data(self, paths: dict[str, str]):
        """Load reference datasets efficiently"""
        print("📊 Loading reference datasets from GCS...")

        # Load properties - only essential fields
        print("  🏠 Loading property cadastral data...")
        self.conn.execute(f"""
            CREATE TABLE properties AS
            SELECT 
                bestemtFastEjendomBFENr as bfe_number,
                geometry as geom
            FROM read_parquet('{paths["properties"]}')
        """)

        # Load soil types
        print("  🌱 Loading soil types...")
        self.conn.execute(f"""
            CREATE TABLE soil_types AS
            SELECT 
                soil_description,
                soil_code,
                geometry as geom
            FROM read_parquet('{paths["soil_types"]}')
        """)

        # Load BNBO status areas
        print("  🛡️ Loading BNBO status areas...")
        self.conn.execute(f"""
            CREATE TABLE bnbo_areas AS
            SELECT 
                status_category,
                geometry as geom
            FROM read_parquet('{paths["bnbo_status"]}')
        """)

        # Load wetlands
        print("  🌊 Loading wetlands...")
        self.conn.execute(f"""
            CREATE TABLE wetlands AS
            SELECT 
                wetland_id,
                geometry as geom
            FROM read_parquet('{paths["wetlands"]}')
        """)

        # Load water projects
        print("  💧 Loading water projects...")
        self.conn.execute(f"""
            CREATE TABLE water_projects AS
            SELECT 
                geometry as geom
            FROM read_parquet('{paths["water_projects"]}')
        """)

        # Get counts
        property_count = self.conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        soil_count = self.conn.execute("SELECT COUNT(*) FROM soil_types").fetchone()[0]
        bnbo_count = self.conn.execute("SELECT COUNT(*) FROM bnbo_areas").fetchone()[0]
        wetland_count = self.conn.execute("SELECT COUNT(*) FROM wetlands").fetchone()[0]
        water_count = self.conn.execute("SELECT COUNT(*) FROM water_projects").fetchone()[0]

        print(f"    ✅ Loaded {property_count:,} properties")
        print(f"    ✅ Loaded {soil_count:,} soil areas")
        print(f"    ✅ Loaded {bnbo_count:,} BNBO areas")
        print(f"    ✅ Loaded {wetland_count:,} wetlands")
        print(f"    ✅ Loaded {water_count:,} water projects")

    def process_field_batch(self, batch_start: int, paths: dict[str, str]) -> pd.DataFrame:
        """Process a batch of fields and return simplified spatial analysis results"""

        # Load the field batch - only essential fields
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE current_fields AS
            SELECT 
                field_id,
                block_id,
                cvr_number,
                geometry as geom
            FROM read_parquet('{paths["fields"]}')
            LIMIT {self.batch_size} OFFSET {batch_start}
        """)

        actual_fields = self.conn.execute("SELECT COUNT(*) FROM current_fields").fetchone()[0]
        print(f"    📋 Processing {actual_fields:,} fields...")

        # Property analysis - SINGLE spatial join condition only (DuckDB Spatial v1.2.2 limitation)
        print("    🔍 Analyzing property ownership...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                p.bfe_number,
                f.geom as field_geom,
                p.geom as property_geom
            FROM current_fields f
            JOIN properties p ON ST_Intersects(f.geom, p.geom)
        """)

        # Calculate area shares separately (not in JOIN due to single condition limitation)
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_property_shares AS
            SELECT 
                field_id,
                block_id,
                bfe_number,
                ST_Area(ST_Intersection(field_geom, property_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_property_intersections
            WHERE ST_Area(ST_Intersection(field_geom, property_geom)) / ST_Area(field_geom) > 0.01
        """)

        # Soil analysis - SINGLE spatial join condition only
        print("    🌱 Analyzing soil types...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                s.soil_code,
                s.soil_description,
                f.geom as field_geom,
                s.geom as soil_geom
            FROM current_fields f
            JOIN soil_types s ON ST_Intersects(f.geom, s.geom)
        """)

        # Calculate soil area shares separately
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_soil_shares AS
            SELECT 
                field_id,
                block_id,
                soil_code,
                soil_description,
                ST_Area(ST_Intersection(field_geom, soil_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_soil_intersections
            WHERE ST_Area(ST_Intersection(field_geom, soil_geom)) / ST_Area(field_geom) > 0.01
        """)

        # BNBO analysis - SINGLE spatial join condition only
        print("    🛡️ Analyzing BNBO status...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                b.status_category,
                f.geom as field_geom,
                b.geom as bnbo_geom
            FROM current_fields f
            JOIN bnbo_areas b ON ST_Intersects(f.geom, b.geom)
        """)

        # Calculate BNBO area shares separately
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_shares AS
            SELECT 
                field_id,
                block_id,
                status_category,
                ST_Area(ST_Intersection(field_geom, bnbo_geom)) / ST_Area(field_geom) * 100 as area_share
            FROM field_bnbo_intersections
            WHERE ST_Area(ST_Intersection(field_geom, bnbo_geom)) / ST_Area(field_geom) > 0.01
        """)

        # Wetlands analysis - SINGLE spatial join condition only
        print("    🌊 Analyzing wetlands...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                f.geom as field_geom,
                w.geom as wetland_geom
            FROM current_fields f
            JOIN wetlands w ON ST_Intersects(f.geom, w.geom)
        """)

        # Calculate wetland area shares with proper union aggregation
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_shares AS
            SELECT 
                field_id,
                block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(field_geom, wetland_geom))) / ST_Area(field_geom) * 100 as wetland_area_share
            FROM field_wetland_intersections
            GROUP BY field_id, block_id, field_geom
        """)

        # Water projects analysis - SINGLE spatial join condition only
        print("    💧 Analyzing water projects...")
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_water_intersections AS
            SELECT 
                f.field_id,
                f.block_id,
                f.geom as field_geom,
                wp.geom as water_geom
            FROM current_fields f
            JOIN water_projects wp ON ST_Intersects(f.geom, wp.geom)
        """)

        # Calculate water projects area shares with proper union aggregation
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_water_projects_shares AS
            SELECT 
                field_id,
                block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(field_geom, water_geom))) / ST_Area(field_geom) * 100 as water_projects_area_share
            FROM field_water_intersections
            GROUP BY field_id, block_id, field_geom
        """)

        # Wetland-water projects overlap analysis - TWO separate spatial joins (limitation compliance)
        print("    🌊💧 Analyzing wetland-water projects overlap...")

        # First: Get field-wetland intersections (already done above)
        # Second: Get those intersections that also intersect with water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE wetland_water_intersections AS
            SELECT 
                fwi.field_id,
                fwi.block_id,
                fwi.field_geom,
                ST_Intersection(fwi.field_geom, fwi.wetland_geom) as field_wetland_intersection,
                wp.geom as water_geom
            FROM field_wetland_intersections fwi
            JOIN water_projects wp ON ST_Intersects(ST_Intersection(fwi.field_geom, fwi.wetland_geom), wp.geom)
        """)

        # Calculate wetland-water overlap shares
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_wetland_water_overlap AS
            SELECT 
                wwi.field_id,
                wwi.block_id,
                ST_Area(ST_Union_Agg(ST_Intersection(wwi.field_wetland_intersection, wwi.water_geom))) / 
                ST_Area(ST_Union_Agg(wwi.field_wetland_intersection)) * 100 as wetland_water_projects_share
            FROM wetland_water_intersections wwi
            GROUP BY wwi.field_id, wwi.block_id
        """)

        # BNBO-water projects overlap analysis - TWO separate spatial joins
        print("    🛡️💧 Analyzing BNBO-water projects overlap...")

        # Get BNBO intersections that also intersect with water projects
        self.conn.execute("""
            CREATE OR REPLACE TABLE bnbo_water_intersections AS
            SELECT 
                fbi.field_id,
                fbi.block_id,
                fbi.status_category,
                fbi.field_geom,
                ST_Intersection(fbi.field_geom, fbi.bnbo_geom) as field_bnbo_intersection,
                wp.geom as water_geom
            FROM field_bnbo_intersections fbi
            JOIN water_projects wp ON ST_Intersects(ST_Intersection(fbi.field_geom, fbi.bnbo_geom), wp.geom)
        """)

        # Calculate BNBO-water overlap shares by status category
        self.conn.execute("""
            CREATE OR REPLACE TABLE field_bnbo_water_overlap AS
            SELECT 
                field_id,
                block_id,
                status_category,
                ST_Area(ST_Intersection(field_bnbo_intersection, water_geom)) / 
                ST_Area(field_bnbo_intersection) * 100 as bnbo_water_projects_share
            FROM bnbo_water_intersections
            WHERE ST_Area(field_bnbo_intersection) > 0
        """)

        # Combine all results into final table
        print("    🔗 Combining results...")
        results = self.conn.execute("""
            SELECT 
                f.field_id,
                f.block_id,
                f.cvr_number,
                COALESCE(fw.wetland_area_share, 0) as wetland_area_share,
                COALESCE(fwo.wetland_water_projects_share, 0) as wetland_water_projects_share,
                COALESCE(fwp.water_projects_area_share, 0) as water_projects_area_share
            FROM current_fields f
            LEFT JOIN field_wetland_shares fw ON f.field_id = fw.field_id AND f.block_id = fw.block_id
            LEFT JOIN field_wetland_water_overlap fwo ON f.field_id = fwo.field_id AND f.block_id = fwo.block_id
            LEFT JOIN field_water_projects_shares fwp ON f.field_id = fwp.field_id AND f.block_id = fwp.block_id
        """).df()

        # Get property shares as JSON - using pandas aggregation to avoid complex SQL
        property_shares = self.conn.execute("""
            SELECT field_id, block_id, bfe_number, area_share
            FROM field_property_shares
        """).df()

        # Aggregate property shares by field using pandas
        if not property_shares.empty:
            property_agg = (
                property_shares.groupby(["field_id", "block_id"])
                .apply(
                    lambda x: json.dumps(
                        dict(zip(x["bfe_number"].astype(str), x["area_share"], strict=False))
                    ),
                    include_groups=False,
                )
                .reset_index()
            )
            property_agg.columns = ["field_id", "block_id", "property_area_shares"]
            results = results.merge(property_agg, on=["field_id", "block_id"], how="left")
        else:
            results["property_area_shares"] = "{}"

        # Get soil shares as JSON - using pandas aggregation
        soil_shares = self.conn.execute("""
            SELECT field_id, block_id, soil_code, area_share
            FROM field_soil_shares
        """).df()

        if not soil_shares.empty:
            soil_agg = (
                soil_shares.groupby(["field_id", "block_id"])
                .apply(
                    lambda x: json.dumps(
                        dict(zip(x["soil_code"].astype(str), x["area_share"], strict=False))
                    ),
                    include_groups=False,
                )
                .reset_index()
            )
            soil_agg.columns = ["field_id", "block_id", "soil_area_shares"]
            results = results.merge(soil_agg, on=["field_id", "block_id"], how="left")
        else:
            results["soil_area_shares"] = "{}"

        # Get BNBO shares as JSON - using pandas aggregation
        bnbo_shares = self.conn.execute("""
            SELECT field_id, block_id, status_category, area_share
            FROM field_bnbo_shares
        """).df()

        if not bnbo_shares.empty:
            bnbo_agg = (
                bnbo_shares.groupby(["field_id", "block_id"])
                .apply(
                    lambda x: json.dumps(
                        dict(zip(x["status_category"], x["area_share"], strict=False))
                    ),
                    include_groups=False,
                )
                .reset_index()
            )
            bnbo_agg.columns = ["field_id", "block_id", "bnbo_area_shares"]
            results = results.merge(bnbo_agg, on=["field_id", "block_id"], how="left")
        else:
            results["bnbo_area_shares"] = "{}"

        # Get BNBO-water project overlap shares as JSON - using pandas aggregation
        bnbo_water_shares = self.conn.execute("""
            SELECT field_id, block_id, status_category, bnbo_water_projects_share
            FROM field_bnbo_water_overlap
        """).df()

        if not bnbo_water_shares.empty:
            bnbo_water_agg = (
                bnbo_water_shares.groupby(["field_id", "block_id"])
                .apply(
                    lambda x: json.dumps(
                        dict(
                            zip(x["status_category"], x["bnbo_water_projects_share"], strict=False)
                        )
                    ),
                    include_groups=False,
                )
                .reset_index()
            )
            bnbo_water_agg.columns = ["field_id", "block_id", "bnbo_water_projects_shares"]
            results = results.merge(bnbo_water_agg, on=["field_id", "block_id"], how="left")
        else:
            results["bnbo_water_projects_shares"] = "{}"

        # Fill null JSON fields with empty objects
        json_cols = [
            "property_area_shares",
            "soil_area_shares",
            "bnbo_area_shares",
            "bnbo_water_projects_shares",
        ]
        for col in json_cols:
            results[col] = results[col].fillna("{}")

        return results

    def run_analysis(self, year: str, max_batches: int | None = None) -> str:
        """Run the complete field spatial analysis"""
        start_time = time.time()

        print(f"🚀 Starting Field Analysis Silver Layer for {year}")
        print(f"   Configuration: {self.batch_size:,} fields per batch")
        if max_batches:
            print(f"   Limited to {max_batches:,} batches for testing")

        # Get data paths
        paths = self.get_data_paths(year)

        # Load reference datasets
        self.load_reference_data(paths)

        # Get total field count
        total_fields = self.conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{paths['fields']}')"
        ).fetchone()[0]
        print(f"📊 Total fields to analyze: {total_fields:,}")

        # Calculate batches
        total_batches = (total_fields + self.batch_size - 1) // self.batch_size
        if max_batches:
            total_batches = min(total_batches, max_batches)

        print(f"📦 Processing in {total_batches:,} batches")

        # Process batches and collect results
        all_results = []
        processed_fields = 0

        for batch_num in range(total_batches):
            batch_start = batch_num * self.batch_size

            batch_start_time = time.time()
            print(
                f"\n📦 Processing batch {batch_num + 1}/{total_batches} (starting at field {batch_start:,})"
            )

            # Process the batch
            batch_results = self.process_field_batch(batch_start, paths)
            all_results.append(batch_results)

            processed_fields += len(batch_results)
            batch_time = time.time() - batch_start_time

            # Progress update
            progress = (batch_num + 1) / total_batches * 100
            elapsed = time.time() - start_time
            if batch_num > 0:
                avg_batch_time = elapsed / (batch_num + 1)
                remaining_batches = total_batches - (batch_num + 1)
                eta_seconds = remaining_batches * avg_batch_time
                eta_minutes = eta_seconds / 60
                print(
                    f"    ⏱️ Batch completed in {batch_time:.1f}s | Progress: {progress:.1f}% | ETA: {eta_minutes:.1f} min"
                )
            else:
                print(f"    ⏱️ Batch completed in {batch_time:.1f}s | Progress: {progress:.1f}%")

        # Combine all results
        print(f"\n🔗 Combining {len(all_results)} batches...")
        final_results = pd.concat(all_results, ignore_index=True)

        # Save results locally first
        output_dir = Path("silver")
        output_dir.mkdir(exist_ok=True)

        timestamp = time.strftime("%Y%m%d_%H%M%S")
        local_output_file = output_dir / f"field_spatial_analysis_{year}_{timestamp}.parquet"

        print(f"\n💾 Saving results locally to {local_output_file}")
        final_results.to_parquet(local_output_file, index=False)

        # Upload to GCS if in production environment
        gcs_output_path = None
        if os.getenv("ENVIRONMENT") == "production":
            try:
                gcs_bucket = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
                gcs_path = f"silver/field_area_analysis/{timestamp}/field_spatial_analysis_{year}_{timestamp}.parquet"
                gcs_output_path = f"gs://{gcs_bucket}/{gcs_path}"

                print(f"🌐 Uploading results to GCS: {gcs_output_path}")

                # Upload using gcsfs
                full_gcs_path = f"{gcs_bucket}/{gcs_path}"
                self.gcs.put(str(local_output_file), full_gcs_path)

                print("✅ Successfully uploaded to GCS")

            except Exception as e:
                print(f"⚠️ Failed to upload to GCS: {e}")
                print("   Continuing with local file only")
        else:
            print("🏠 Running in development mode - skipping GCS upload")

        # Display summary
        self.display_results_summary(final_results, str(local_output_file))

        total_time = time.time() - start_time
        print(f"\n🎉 Analysis completed in {total_time / 60:.1f} minutes")
        print(f"   Processed {processed_fields:,} fields")
        print(f"   Results saved locally to: {local_output_file}")
        if gcs_output_path:
            print(f"   Results uploaded to GCS: {gcs_output_path}")

        return str(local_output_file)

    def display_results_summary(self, results: pd.DataFrame, output_file: str):
        """Display comprehensive results summary"""
        print("\n📊 Field Spatial Analysis Results Summary")
        print("=" * 60)

        total_fields = len(results)

        print("🎯 Analysis Results:")
        print(f"   Total fields analyzed: {total_fields:,}")
        print(f"   Average wetland coverage: {results['wetland_area_share'].mean():.1f}%")
        print(
            f"   Average water projects coverage: {results['water_projects_area_share'].mean():.1f}%"
        )
        print(
            f"   Fields with wetlands: {(results['wetland_area_share'] > 0).sum():,} ({(results['wetland_area_share'] > 0).mean() * 100:.1f}%)"
        )
        print(
            f"   Fields with water projects: {(results['water_projects_area_share'] > 0).sum():,} ({(results['water_projects_area_share'] > 0).mean() * 100:.1f}%)"
        )
        print(
            f"   Fields with property shares: {(results['property_area_shares'] != '{}').sum():,} ({(results['property_area_shares'] != '{}').mean() * 100:.1f}%)"
        )
        print(
            f"   Fields with soil data: {(results['soil_area_shares'] != '{}').sum():,} ({(results['soil_area_shares'] != '{}').mean() * 100:.1f}%)"
        )
        print(
            f"   Fields with BNBO status: {(results['bnbo_area_shares'] != '{}').sum():,} ({(results['bnbo_area_shares'] != '{}').mean() * 100:.1f}%)"
        )

        print("\n💾 Output:")
        print(f"   Results file: {output_file}")
        print(f"   File size: {Path(output_file).stat().st_size / 1024 / 1024:.1f} MB")
        print(f"   Columns: {list(results.columns)}")

    def find_latest_available_year(self) -> str:
        """Find the most recent year with available agricultural fields data"""
        try:
            # Look for agricultural fields directories
            base_path = "landbrugsdata-raw-data/silver"

            # List all directories that match the pattern agricultural_fields_YYYY
            paths = self.gcs.glob(f"{base_path}/agricultural_fields_*")

            if not paths:
                print("⚠️  No agricultural fields data found, defaulting to 2025")
                return "2025"

            # Extract years from directory names
            years = []
            for path in paths:
                dir_name = path.split("/")[-1]  # Get last part of path
                if dir_name.startswith("agricultural_fields_"):
                    year_str = dir_name.replace("agricultural_fields_", "")
                    try:
                        year = int(year_str)
                        years.append(year)
                    except ValueError:
                        continue

            if not years:
                print("⚠️  No valid years found in agricultural fields data, defaulting to 2025")
                return "2025"

            latest_year = str(max(years))
            print(f"🔍 Latest available year discovered: {latest_year}")
            return latest_year

        except Exception as e:
            print(f"⚠️  Error discovering latest year: {e}, defaulting to 2025")
            return "2025"


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Field Area Analysis Silver Layer Pipeline")
    parser.add_argument(
        "--year",
        type=str,
        default="latest",
        help="Analysis year (or 'latest' to auto-discover most recent)",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of fields per batch")
    parser.add_argument("--max-batches", type=int, help="Maximum number of batches to process")
    parser.add_argument("--memory-limit", type=str, default="14GB", help="Memory limit for DuckDB")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads for DuckDB")

    args = parser.parse_args()

    # Create pipeline instance for year discovery if needed
    if args.year == "latest":
        print("🔍 Auto-discovering latest available year...")
        temp_pipeline = FieldAnalysisSilver(batch_size=1)  # Minimal instance for discovery
        actual_year = temp_pipeline.find_latest_available_year()
        print(f"📅 Using year: {actual_year}")
    else:
        actual_year = args.year
        print(f"📅 Using specified year: {actual_year}")

    # Create the main pipeline instance
    try:
        pipeline = FieldAnalysisSilver(
            batch_size=args.batch_size,
            memory_limit=args.memory_limit,
            thread_count=args.threads,
        )
    except Exception as e:
        print(f"\n❌ Failed to initialize pipeline: {e}")
        sys.exit(1)

    print(f"🚀 Starting Field Area Analysis Silver Layer for {actual_year}")
    if args.max_batches:
        print(f"   Configuration: {args.batch_size:,} fields per batch")
        print(f"   Limited to {args.max_batches:,} batches for testing")
    else:
        print(f"   Configuration: {args.batch_size:,} fields per batch")

    try:
        output_file = pipeline.run_analysis(actual_year, args.max_batches)

        # Verify output file exists and has content
        if not Path(output_file).exists():
            print(f"\n❌ Pipeline reported success but output file not found: {output_file}")
            sys.exit(1)

        file_size = Path(output_file).stat().st_size
        if file_size == 0:
            print(f"\n❌ Pipeline reported success but output file is empty: {output_file}")
            sys.exit(1)

        print("\n✅ Field Area Analysis Silver Layer completed successfully!")
        print(f"   Output: {output_file}")
        print(f"   File size: {file_size / 1024 / 1024:.1f} MB")
    except ImportError as e:
        # Critical dependency errors should fail the pipeline
        print(f"\n❌ Critical dependency error: {e}")
        if "pyarrow" in str(e) or "parquet" in str(e):
            print("Missing pyarrow dependency - this is a critical error")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n❌ Required data not found: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
