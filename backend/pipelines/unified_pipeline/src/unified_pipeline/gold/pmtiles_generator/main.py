"""Main PMTiles Generator Pipeline."""

import argparse
import asyncio
import logging
import os
import sys

from unified_pipeline.common.base import BaseSource

from .buildings_generator import BuildingsProximityPMTilesGenerator
from .config import PMTilesGeneratorConfig
from .data_loader import PMTilesDataLoader
from .environmental_generator import EnvironmentalLayersPMTilesGenerator
from .field_analysis_generator import FieldAnalysisPMTilesGenerator
from .uploader import CloudflareR2Uploader
from .year_detector import DataSourceYearDetector

logger = logging.getLogger(__name__)


class PMTilesGeneratorPipeline(BaseSource[PMTilesGeneratorConfig]):
    """Main PMTiles Generator Pipeline."""

    def __init__(self, config: PMTilesGeneratorConfig):
        """Initialize the PMTiles generator pipeline.

        Args:
            config: PMTiles generator configuration
        """
        super().__init__(config)

        # Use inherited DuckDB connection and GCS access from BaseSource
        self.duckdb_conn = self.conn
        # self.gcs_access is already available from BaseSource
        self.year_detector = None
        self.data_loader = None
        self.field_generator = None
        self.environmental_generator = None
        self.buildings_generator = None
        self.uploader = None

    async def setup(self):
        """Setup pipeline components."""
        logger.info("Setting up PMTiles generator pipeline")

        # Ensure GCS credentials are properly configured for DuckDB
        self._ensure_gcs_credentials()

        # Initialize components
        self.year_detector = DataSourceYearDetector(self.config, self.gcs_access)
        self.data_loader = PMTilesDataLoader(self.config, self.gcs_access, self.duckdb_conn)
        self.field_generator = FieldAnalysisPMTilesGenerator(
            self.config, self.data_loader, self.duckdb_conn
        )
        self.environmental_generator = EnvironmentalLayersPMTilesGenerator(
            self.config, self.data_loader, self.duckdb_conn
        )
        self.buildings_generator = BuildingsProximityPMTilesGenerator(
            self.config, self.data_loader, self.duckdb_conn
        )
        self.uploader = CloudflareR2Uploader(self.config)

        # Create temp directory
        os.makedirs(self.config.temp_dir, exist_ok=True)

        logger.info("Pipeline setup completed")

    def _ensure_gcs_credentials(self):
        """Ensure GCS HMAC credentials are properly configured for DuckDB."""
        try:
            # Check for HMAC credentials in environment variables
            gcs_access_key = os.environ.get("GCS_ACCESS_KEY_ID")
            gcs_secret_key = os.environ.get("GCS_SECRET_ACCESS_KEY")

            if gcs_access_key and gcs_secret_key:
                logger.info("✅ Found GCS HMAC credentials, configuring DuckDB")

                # Install and load httpfs extension for GCS access
                self.duckdb_conn.execute("INSTALL httpfs")
                self.duckdb_conn.execute("LOAD httpfs")

                # Set correct GCS region (landbruget-data bucket is in EUROPE-WEST1)
                self.duckdb_conn.execute("SET s3_region = 'europe-west1'")

                # Escape single quotes in credentials to prevent SQL injection
                escaped_key = gcs_access_key.replace("'", "''")
                escaped_secret = gcs_secret_key.replace("'", "''")

                # Create persistent GCS secret for native access
                self.duckdb_conn.execute(f"""
                    CREATE OR REPLACE PERSISTENT SECRET gcs_hmac (
                        TYPE GCS,
                        KEY_ID '{escaped_key}',
                        SECRET '{escaped_secret}'
                    );
                """)
                logger.info("✅ DuckDB GCS HMAC authentication configured successfully")
            else:
                logger.warning("⚠️  GCS HMAC credentials not found in environment variables")
                logger.warning(
                    "    Set GCS_ACCESS_KEY_ID and GCS_SECRET_ACCESS_KEY for optimal performance"
                )
        except Exception as e:
            logger.error(f"❌ Failed to setup GCS HMAC authentication: {e}")
            # Don't raise - let it fall back to service account auth

    async def run(self, target_year: int | None = None) -> dict[str, any]:
        """Run the PMTiles generation pipeline.

        Args:
            target_year: Optional specific year to process

        Returns:
            Dictionary with pipeline results
        """
        logger.info("Starting PMTiles generation pipeline")

        try:
            await self.setup()

            # Detect available years
            logger.info("Detecting available years for data sources")
            available_years = await self.year_detector.detect_all_available_years()
            year_ranges = await self.year_detector.get_optimal_year_ranges()

            # Determine years to process
            if target_year:
                years_to_process = [target_year]
                logger.info(f"Processing specific year: {target_year}")
            else:
                years_to_process = self.year_detector.get_years_to_process(available_years)
                logger.info(f"Processing {len(years_to_process)} years: {years_to_process}")

            results = {
                "available_years": available_years,
                "year_ranges": year_ranges,
                "processed_years": years_to_process,
                "field_analysis_pmtiles": {},
                "environmental_pmtiles": {},
                "buildings_pmtiles": {},
                "upload_results": {},
            }

            # Initialize variables to prevent scope issues
            field_pmtiles = {}
            environmental_pmtiles = {}
            buildings_pmtiles = {}

            # Generate field analysis PMTiles for each year
            if years_to_process:
                logger.info("Generating field analysis PMTiles")
                field_pmtiles = await self.field_generator.generate_multiple_years(years_to_process)
                results["field_analysis_pmtiles"] = field_pmtiles
            else:
                logger.info("No years to process for field analysis")
                results["field_analysis_pmtiles"] = {}

            # Generate environmental layers PMTiles (year-independent)
            logger.info("Generating environmental layers PMTiles")
            environmental_pmtiles = (
                await self.environmental_generator.generate_all_environmental_pmtiles()
            )
            results["environmental_pmtiles"] = environmental_pmtiles

            # Generate buildings proximity PMTiles
            logger.info("Generating buildings proximity PMTiles")
            buildings_pmtiles = await self.buildings_generator.generate_all_building_pmtiles()
            results["buildings_pmtiles"] = buildings_pmtiles

            # Upload to Cloudflare R2
            if self._should_upload():
                logger.info("Uploading PMTiles to Cloudflare R2")
                upload_results = await self._upload_all_pmtiles(
                    field_pmtiles, environmental_pmtiles, buildings_pmtiles
                )
                results["upload_results"] = upload_results

            # Cleanup temporary files
            if self.config.cleanup_temp_files:
                await self._cleanup_temp_files()

            logger.info("PMTiles generation pipeline completed successfully")
            return results

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            if self.duckdb_conn:
                self.duckdb_conn.close()

    async def _upload_all_pmtiles(
        self,
        field_pmtiles: dict[int, str | None],
        environmental_pmtiles: dict[str, str | None],
        buildings_pmtiles: dict[str, str | None],
    ) -> dict[str, dict[str, str | None]]:
        """Upload all generated PMTiles to R2 with automatic cleanup.

        Args:
            field_pmtiles: Field analysis PMTiles by year
            environmental_pmtiles: Environmental layer PMTiles
            buildings_pmtiles: Buildings PMTiles

        Returns:
            Dictionary with upload results by category
        """
        upload_results = {"field_analysis": {}, "environmental": {}, "buildings": {}, "cleanup": {}}

        # Prepare all files for upload
        all_files = {}

        # Add field analysis PMTiles
        for year, file_path in field_pmtiles.items():
            if file_path and os.path.exists(file_path):
                r2_key = f"pmtiles/field_analysis_{year}.pmtiles"
                all_files[r2_key] = file_path

        # Add environmental PMTiles
        for layer_name, file_path in environmental_pmtiles.items():
            if file_path and os.path.exists(file_path):
                r2_key = f"pmtiles/{layer_name}.pmtiles"
                all_files[r2_key] = file_path

        # Add buildings PMTiles
        for layer_name, file_path in buildings_pmtiles.items():
            if file_path and os.path.exists(file_path):
                r2_key = f"pmtiles/{layer_name}.pmtiles"
                all_files[r2_key] = file_path

        if not all_files:
            logger.warning("No PMTiles files to upload")
            return upload_results

        # Upload with automatic cleanup
        logger.info(
            f"Uploading {len(all_files)} PMTiles files with cleanup enabled: {self.config.enable_r2_cleanup}"
        )

        if self.config.enable_r2_cleanup:
            # Upload with cleanup
            raw_results = await self.uploader.upload_with_cleanup(
                all_files, cleanup_old=True, keep_versions=self.config.keep_versions
            )
        else:
            # Upload without cleanup
            raw_results = await self.uploader.upload_multiple_pmtiles(all_files)

        # Organize results by category
        cleanup_results = raw_results.pop("_cleanup_results", {})
        if cleanup_results:
            upload_results["cleanup"] = cleanup_results

        for r2_key, public_url in raw_results.items():
            if r2_key.startswith("pmtiles/field_analysis_"):
                year_str = r2_key.replace("pmtiles/field_analysis_", "").replace(".pmtiles", "")
                upload_results["field_analysis"][f"field_analysis_{year_str}"] = public_url
            elif r2_key.startswith("pmtiles/"):
                layer_name = r2_key.replace("pmtiles/", "").replace(".pmtiles", "")
                if layer_name in environmental_pmtiles:
                    upload_results["environmental"][layer_name] = public_url
                elif layer_name in buildings_pmtiles:
                    upload_results["buildings"][layer_name] = public_url

        # Log upload summary
        total_successful = sum(
            len([url for url in category.values() if url])
            for category in [
                upload_results["field_analysis"],
                upload_results["environmental"],
                upload_results["buildings"],
            ]
        )
        total_files = len(all_files)
        logger.info(f"Upload completed: {total_successful}/{total_files} files successful")

        if cleanup_results:
            successful_deletions = sum(1 for success in cleanup_results.values() if success)
            total_deletions = len(cleanup_results)
            logger.info(
                f"Cleanup completed: {successful_deletions}/{total_deletions} old files deleted"
            )

        return upload_results

    def _should_upload(self) -> bool:
        """Determine if files should be uploaded to R2.

        Returns:
            True if upload should proceed, False otherwise
        """
        # Check if R2 credentials are available
        required_env_vars = ["R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT"]

        for var in required_env_vars:
            if not os.environ.get(var):
                logger.warning(f"R2 credential {var} not found, skipping upload")
                return False

        return True

    async def _cleanup_temp_files(self):
        """Clean up temporary files and directories."""
        try:
            import shutil

            if os.path.exists(self.config.temp_dir):
                shutil.rmtree(self.config.temp_dir)
                logger.info(f"Cleaned up temporary directory: {self.config.temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temporary files: {e}")

    async def generate_year_specific(self, year: int) -> dict[str, any]:
        """Generate PMTiles for a specific year only.

        Args:
            year: Target year

        Returns:
            Dictionary with generation results
        """
        logger.info(f"Generating PMTiles for year {year}")

        await self.setup()

        # Generate field analysis PMTiles for the specific year
        field_pmtiles_path = await self.field_generator.generate_field_analysis_pmtiles(year)

        results = {
            "year": year,
            "field_analysis_pmtiles": field_pmtiles_path,
            "success": field_pmtiles_path is not None,
        }

        # Upload if successful and credentials available
        if field_pmtiles_path and self._should_upload():
            r2_key = f"pmtiles/field_analysis_{year}.pmtiles"
            public_url = await self.uploader.upload_pmtiles(field_pmtiles_path, r2_key)
            results["public_url"] = public_url

        return results

    async def generate_environmental_only(self) -> dict[str, any]:
        """Generate only environmental layers PMTiles.

        Returns:
            Dictionary with generation results
        """
        logger.info("Generating environmental layers PMTiles only")

        await self.setup()

        # Generate environmental layers
        environmental_pmtiles = (
            await self.environmental_generator.generate_all_environmental_pmtiles()
        )
        buildings_pmtiles = await self.buildings_generator.generate_all_building_pmtiles()

        results = {
            "environmental_pmtiles": environmental_pmtiles,
            "buildings_pmtiles": buildings_pmtiles,
        }

        # Upload if credentials available
        if self._should_upload():
            upload_results = await self._upload_all_pmtiles(
                {}, environmental_pmtiles, buildings_pmtiles
            )
            results["upload_results"] = upload_results

        return results


async def main():
    """Main entry point for the PMTiles generator."""
    parser = argparse.ArgumentParser(description="PMTiles Generator Pipeline")
    parser.add_argument("--target-year", type=int, help="Specific year to process")
    parser.add_argument(
        "--environmental-only", action="store_true", help="Generate only environmental layers"
    )
    parser.add_argument("--no-upload", action="store_true", help="Skip upload to R2")
    parser.add_argument(
        "--no-cleanup", action="store_true", help="Skip cleanup of old PMTiles in R2"
    )
    parser.add_argument(
        "--keep-versions", type=int, help="Number of versions to keep for year-specific PMTiles"
    )
    parser.add_argument("--config-file", help="Path to configuration file")
    parser.add_argument("--temp-dir", help="Temporary directory for processing")
    parser.add_argument("--max-parallel-years", type=int, help="Maximum parallel years to process")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    try:
        # Load configuration
        config = PMTilesGeneratorConfig()

        # Override config with command line arguments
        if args.temp_dir:
            config.temp_dir = args.temp_dir
        if args.max_parallel_years:
            config.max_parallel_years = args.max_parallel_years
        if args.no_cleanup:
            config.enable_r2_cleanup = False
        if args.keep_versions:
            config.keep_versions = args.keep_versions

        # Initialize pipeline
        pipeline = PMTilesGeneratorPipeline(config)

        # Run pipeline based on arguments
        if args.environmental_only:
            results = await pipeline.generate_environmental_only()
        elif args.target_year:
            results = await pipeline.generate_year_specific(args.target_year)
        else:
            results = await pipeline.run()

        # Print summary
        logger.info("\n" + "=" * 50)
        logger.info("PMTiles Generation Summary")
        logger.info("=" * 50)

        if "processed_years" in results:
            logger.info(f"Processed years: {results['processed_years']}")

        if "field_analysis_pmtiles" in results:
            field_pmtiles = results["field_analysis_pmtiles"]
            if isinstance(field_pmtiles, dict):
                successful_years = sum(1 for path in field_pmtiles.values() if path)
                total_years = len(field_pmtiles)
                logger.info(f"Field analysis PMTiles: {successful_years}/{total_years} successful")
            else:
                # Single year result - just check if it exists
                success = field_pmtiles is not None
                logger.info(f"Field analysis PMTiles: {1 if success else 0}/1 successful")

        if "environmental_pmtiles" in results:
            env_pmtiles = results["environmental_pmtiles"]
            if isinstance(env_pmtiles, dict):
                successful_env = sum(1 for path in env_pmtiles.values() if path)
                total_env = len(env_pmtiles)
                logger.info(f"Environmental PMTiles: {successful_env}/{total_env} successful")
            else:
                # Single result
                success = env_pmtiles is not None
                logger.info(f"Environmental PMTiles: {1 if success else 0}/1 successful")

        if "buildings_pmtiles" in results:
            buildings_pmtiles = results["buildings_pmtiles"]
            if isinstance(buildings_pmtiles, dict):
                successful_buildings = sum(1 for path in buildings_pmtiles.values() if path)
                total_buildings = len(buildings_pmtiles)
                logger.info(
                    f"Buildings PMTiles: {successful_buildings}/{total_buildings} successful"
                )
            else:
                # Single result
                success = buildings_pmtiles is not None
                logger.info(f"Buildings PMTiles: {1 if success else 0}/1 successful")

        if "upload_results" in results:
            upload_results = results["upload_results"]
            if isinstance(upload_results, dict):
                total_uploads = 0
                successful_uploads = 0
                for category_results in upload_results.values():
                    if isinstance(category_results, dict):
                        for url in category_results.values():
                            total_uploads += 1
                            if url:
                                successful_uploads += 1
                    else:
                        # Single result
                        total_uploads += 1
                        if category_results:
                            successful_uploads += 1
            logger.info(f"Uploads: {successful_uploads}/{total_uploads} successful")

        logger.info("=" * 50)

        # Fail if no field analysis tiles were generated
        if "field_analysis_pmtiles" in results:
            field_pmtiles = results["field_analysis_pmtiles"]
            if isinstance(field_pmtiles, dict):
                successful = sum(1 for p in field_pmtiles.values() if p)
                if successful == 0 and len(field_pmtiles) > 0:
                    logger.error(
                        "No field analysis PMTiles were generated — check GCS data availability"
                    )
                    sys.exit(1)
            elif field_pmtiles is None:
                logger.error("Field analysis PMTiles generation failed")
                sys.exit(1)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
