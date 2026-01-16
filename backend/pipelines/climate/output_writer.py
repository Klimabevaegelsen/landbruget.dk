"""
Climate Output Writer Module

This module handles writing EmissionReport results to:
1. GCS gold layer (primary storage)
2. Supabase database (optional sync for web access)

The writer uses the GCSDataAccess pattern for optimal performance and
follows the medallion architecture gold layer standards.

Output Structure:
- GCS: gs://landbrugsdata-raw-data/gold/carbon_emissions/<YYYYMMDD_HHMMSS>/
  - emissions.parquet (main report data)
  - categories.parquet (emission categories breakdown)
  - metadata.json (run metadata)

Supabase Tables (optional):
- carbon_emissions (main report data)
- climate_emission_categories (categories breakdown)
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd

# Add unified_pipeline to path for GCSDataAccess import
unified_pipeline_path = Path(__file__).parent.parent / "unified_pipeline" / "src"
if str(unified_pipeline_path) not in sys.path:
    sys.path.insert(0, str(unified_pipeline_path))

from unified_pipeline.util.gcs_access import GCSDataAccess  # noqa: E402
from unified_pipeline.util.log_util import Logger  # noqa: E402

from climate_calculator import EmissionReport, EmissionCategory  # noqa: E402

logger = Logger.get_logger()


class ClimateOutputWriter:
    """
    Writes EmissionReport results to GCS gold layer and optionally syncs to Supabase.

    This writer:
    - Converts EmissionReport objects to parquet format
    - Writes to GCS gold/carbon_emissions/<timestamp>/
    - Includes comprehensive metadata
    - Optionally syncs to Supabase for web access
    """

    def __init__(self, bucket: str = None):
        """
        Initialize the climate output writer.

        Args:
            bucket: GCS bucket name. Defaults to GCS_BUCKET env var or 'landbrugsdata-raw-data'
        """
        self.bucket = bucket or os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
        self.gcs = GCSDataAccess()
        logger.info(f"✅ ClimateOutputWriter initialized with bucket: {self.bucket}")

    def write_to_gold_layer(
        self,
        reports: List[EmissionReport],
        timestamp: Optional[str] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Write EmissionReport results to GCS gold layer.

        This method:
        1. Converts EmissionReport objects to DataFrames
        2. Writes emissions.parquet (main report data)
        3. Writes categories.parquet (categories breakdown)
        4. Writes metadata.json (run metadata)

        Args:
            reports: List of EmissionReport objects to write
            timestamp: Optional timestamp string (YYYYMMDD_HHMMSS). If None, uses current time.
            run_metadata: Optional metadata about the run (pipeline version, data sources, etc.)

        Returns:
            GCS path to the output directory

        Example:
            >>> writer = ClimateOutputWriter()
            >>> reports = [calculator.calculate_emissions(cvr="12345678", year=2024)]
            >>> path = writer.write_to_gold_layer(reports)
            >>> print(f"Wrote {len(reports)} reports to {path}")
        """
        if not reports:
            logger.warning("No reports to write")
            return ""

        # Generate timestamp if not provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Build output path
        output_dir = f"gs://{self.bucket}/gold/carbon_emissions/{timestamp}"
        logger.info(f"Writing {len(reports)} emission reports to {output_dir}")

        try:
            # Convert reports to DataFrames
            emissions_df = self._reports_to_dataframe(reports)
            categories_df = self._categories_to_dataframe(reports)

            # Create DuckDB tables
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS emissions_temp")
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS categories_temp")

            # Register DataFrames as DuckDB tables
            self.gcs.duckdb_conn.register("emissions_temp", emissions_df)
            self.gcs.duckdb_conn.register("categories_temp", categories_df)

            # Write emissions.parquet
            emissions_path = f"{output_dir}/emissions.parquet"
            self.gcs.upload_from_duckdb_table(
                "emissions_temp",
                emissions_path,
                compression="zstd",
                row_group_size=10000,
            )
            logger.info(f"✅ Wrote {len(emissions_df)} emission records to {emissions_path}")

            # Write categories.parquet
            categories_path = f"{output_dir}/categories.parquet"
            self.gcs.upload_from_duckdb_table(
                "categories_temp",
                categories_path,
                compression="zstd",
                row_group_size=10000,
            )
            logger.info(f"✅ Wrote {len(categories_df)} category records to {categories_path}")

            # Write metadata.json
            metadata = self._build_metadata(reports, timestamp, run_metadata)
            metadata_path = f"{output_dir}/metadata.json"
            self.gcs.upload_json(metadata, metadata_path)
            logger.info(f"✅ Wrote metadata to {metadata_path}")

            # Cleanup temporary tables
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS emissions_temp")
            self.gcs.duckdb_conn.execute("DROP TABLE IF EXISTS categories_temp")

            logger.info(f"✅ Successfully wrote {len(reports)} reports to {output_dir}")
            return output_dir

        except Exception as e:
            logger.error(f"Failed to write reports to gold layer: {e}")
            raise

    def _reports_to_dataframe(self, reports: List[EmissionReport]) -> pd.DataFrame:
        """
        Convert EmissionReport objects to a DataFrame for emissions.parquet.

        Schema:
        - cvr (string): Company registration number
        - year (int): Calculation year
        - total_co2e_kg (float): Total emissions in kg CO2e
        - data_completeness (float): Data completeness score (0-1)
        - intensity_co2e_per_kg_milk (float, nullable): Emissions per kg milk
        - intensity_co2e_per_ha (float, nullable): Emissions per hectare
        - intensity_co2e_per_animal_unit (float, nullable): Emissions per animal unit
        - created_at (timestamp): Record creation timestamp

        Args:
            reports: List of EmissionReport objects

        Returns:
            DataFrame with emissions data
        """
        records = []

        for report in reports:
            record = {
                "cvr": report.cvr,
                "year": report.year,
                "total_co2e_kg": report.total_co2e_kg,
                "data_completeness": report.data_completeness,
                # Intensity metrics (nullable)
                "intensity_co2e_per_kg_milk": report.intensity_metrics.get("co2e_per_kg_milk"),
                "intensity_co2e_per_ha": report.intensity_metrics.get("co2e_per_ha"),
                "intensity_co2e_per_animal_unit": report.intensity_metrics.get("co2e_per_animal_unit"),
                "created_at": datetime.now(),
            }
            records.append(record)

        df = pd.DataFrame(records)

        # Ensure CVR is 8-digit string with leading zeros
        df["cvr"] = df["cvr"].astype(str).str.zfill(8)

        return df

    def _categories_to_dataframe(self, reports: List[EmissionReport]) -> pd.DataFrame:
        """
        Convert EmissionCategory data to a DataFrame for categories.parquet.

        Schema:
        - cvr (string): Company registration number
        - year (int): Calculation year
        - category_name (string): Category name (cattle, fields, energy, etc.)
        - co2e_kg (float): Emissions for this category in kg CO2e
        - data_quality (string): Quality indicator (complete, estimated, unavailable)
        - sub_sources (json/string): JSON string of sub-source breakdown
        - created_at (timestamp): Record creation timestamp

        Args:
            reports: List of EmissionReport objects

        Returns:
            DataFrame with category data
        """
        records = []

        for report in reports:
            for category in report.categories:
                record = {
                    "cvr": report.cvr,
                    "year": report.year,
                    "category_name": category.name,
                    "co2e_kg": category.co2e_kg,
                    "data_quality": category.data_quality,
                    # Store sub_sources as JSON string for parquet compatibility
                    "sub_sources": str(category.sub_sources) if category.sub_sources else "{}",
                    "created_at": datetime.now(),
                }
                records.append(record)

        df = pd.DataFrame(records)

        # Ensure CVR is 8-digit string with leading zeros
        df["cvr"] = df["cvr"].astype(str).str.zfill(8)

        return df

    def _build_metadata(
        self,
        reports: List[EmissionReport],
        timestamp: str,
        run_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build metadata JSON for the emission calculation run.

        Args:
            reports: List of EmissionReport objects
            timestamp: Timestamp of the run
            run_metadata: Optional additional metadata

        Returns:
            Metadata dictionary
        """
        metadata = {
            "timestamp": timestamp,
            "report_count": len(reports),
            "cvr_list": [report.cvr for report in reports],
            "year_range": {
                "min": min(report.year for report in reports),
                "max": max(report.year for report in reports),
            },
            "statistics": {
                "total_emissions_kg_co2e": sum(report.total_co2e_kg for report in reports),
                "avg_emissions_kg_co2e": sum(report.total_co2e_kg for report in reports) / len(reports),
                "avg_data_completeness": sum(report.data_completeness for report in reports) / len(reports),
            },
            "data_sources": {
                "chr": "silver/chr",
                "fvm_marker": "silver/fvm_marker_YYYY",
                "fertiliser": "silver/fertiliser",
            },
            "pipeline_version": "1.0.0",
            "created_at": datetime.now().isoformat(),
        }

        # Add custom run metadata if provided
        if run_metadata:
            metadata["run_metadata"] = run_metadata

        return metadata

    def sync_to_supabase(
        self,
        reports: List[EmissionReport],
        table_prefix: str = "climate_",
        upsert: bool = True,
    ) -> bool:
        """
        Sync EmissionReport results to Supabase database (optional).

        This method is optional and requires Supabase credentials to be configured.
        It will sync data to:
        - {table_prefix}emissions (main report data)
        - {table_prefix}emission_categories (categories breakdown)

        Args:
            reports: List of EmissionReport objects to sync
            table_prefix: Prefix for Supabase table names (default: "climate_")
            upsert: If True, upsert on conflict. If False, insert only.

        Returns:
            True if sync successful, False otherwise

        Note:
            This method requires supabase-py to be installed and SUPABASE_URL and
            SUPABASE_KEY environment variables to be set.

        Example:
            >>> writer = ClimateOutputWriter()
            >>> reports = [calculator.calculate_emissions(cvr="12345678", year=2024)]
            >>> success = writer.sync_to_supabase(reports)
            >>> if success:
            ...     print("Data synced to Supabase")
        """
        try:
            # Check if Supabase is configured
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")

            if not supabase_url or not supabase_key:
                logger.warning("Supabase sync skipped: SUPABASE_URL and SUPABASE_KEY environment variables not set")
                return False

            # Try to import supabase (optional dependency)
            try:
                from supabase import create_client
            except ImportError:
                logger.warning("Supabase sync skipped: supabase-py not installed")
                logger.info("To enable Supabase sync, install: pip install supabase")
                return False

            # Create Supabase client
            supabase = create_client(supabase_url, supabase_key)
            logger.info("✅ Connected to Supabase")

            # Sync emissions data
            emissions_df = self._reports_to_dataframe(reports)
            emissions_records = emissions_df.to_dict("records")

            emissions_table = f"{table_prefix}emissions"
            if upsert:
                # Upsert on (cvr, year) conflict
                supabase.table(emissions_table).upsert(emissions_records).execute()
            else:
                supabase.table(emissions_table).insert(emissions_records).execute()

            logger.info(f"✅ Synced {len(emissions_records)} emission records to {emissions_table}")

            # Sync categories data
            categories_df = self._categories_to_dataframe(reports)
            categories_records = categories_df.to_dict("records")

            categories_table = f"{table_prefix}emission_categories"
            if upsert:
                # Upsert on (cvr, year, category_name) conflict
                supabase.table(categories_table).upsert(categories_records).execute()
            else:
                supabase.table(categories_table).insert(categories_records).execute()

            logger.info(f"✅ Synced {len(categories_records)} category records to {categories_table}")

            return True

        except Exception as e:
            logger.error(f"Failed to sync to Supabase: {e}")
            return False

    def list_available_reports(self, pattern: str = None) -> List[str]:
        """
        List available emission reports in the gold layer.

        Args:
            pattern: Optional pattern to filter report directories
                    (e.g., "20240101_*" for specific date)

        Returns:
            List of GCS paths to report directories

        Example:
            >>> writer = ClimateOutputWriter()
            >>> reports = writer.list_available_reports()
            >>> print(f"Found {len(reports)} emission reports")
        """
        try:
            if pattern:
                search_pattern = f"gs://{self.bucket}/gold/carbon_emissions/{pattern}/metadata.json"
            else:
                search_pattern = f"gs://{self.bucket}/gold/carbon_emissions/*/metadata.json"

            metadata_files = self.gcs.list_files(search_pattern)

            # Extract directory paths
            report_dirs = ["/".join(path.split("/")[:-1]) for path in metadata_files]

            logger.info(f"Found {len(report_dirs)} emission report directories")
            return sorted(report_dirs, reverse=True)  # Most recent first

        except Exception as e:
            logger.error(f"Failed to list available reports: {e}")
            return []

    def read_report_metadata(self, report_path: str) -> Optional[Dict[str, Any]]:
        """
        Read metadata for a specific emission report.

        Args:
            report_path: GCS path to report directory (e.g., gs://bucket/gold/carbon_emissions/20240101_120000)

        Returns:
            Metadata dictionary or None if not found

        Example:
            >>> writer = ClimateOutputWriter()
            >>> metadata = writer.read_report_metadata("gs://bucket/gold/carbon_emissions/20240101_120000")
            >>> print(f"Report contains {metadata['report_count']} farms")
        """
        try:
            metadata_path = f"{report_path}/metadata.json"
            metadata = self.gcs.download_json(metadata_path)
            return metadata
        except Exception as e:
            logger.error(f"Failed to read metadata from {report_path}: {e}")
            return None


# Example usage
if __name__ == "__main__":
    # This example demonstrates the output writer usage
    # In practice, this would be integrated with the ClimateCalculator

    # Create writer
    writer = ClimateOutputWriter()

    # Example: Create mock report (in practice, this comes from ClimateCalculator)
    mock_report = EmissionReport(
        cvr="12345678",
        year=2024,
        total_co2e_kg=150000.0,
        categories=[
            EmissionCategory(
                name="cattle",
                co2e_kg=80000.0,
                data_quality="complete",
                sub_sources={
                    "enteric_methane": 60000.0,
                    "manure_storage": 20000.0,
                },
            ),
            EmissionCategory(
                name="fields",
                co2e_kg=70000.0,
                data_quality="complete",
                sub_sources={
                    "n2o_fertilizer": 40000.0,
                    "carbon_balance": 20000.0,
                    "nitrate_leaching": 10000.0,
                },
            ),
        ],
        intensity_metrics={
            "co2e_per_kg_milk": 1.2,
            "co2e_per_ha": 2500.0,
        },
        data_completeness=0.85,
    )

    # Write to gold layer
    output_path = writer.write_to_gold_layer([mock_report])
    print(f"✅ Wrote emission report to: {output_path}")

    # Optionally sync to Supabase
    success = writer.sync_to_supabase([mock_report])
    if success:
        print("✅ Synced to Supabase")
    else:
        print("⚠️  Supabase sync skipped (not configured)")

    # List available reports
    available_reports = writer.list_available_reports()
    print(f"\nAvailable reports: {len(available_reports)}")
    for report_dir in available_reports[:5]:  # Show first 5
        metadata = writer.read_report_metadata(report_dir)
        if metadata:
            print(f"  - {report_dir}: {metadata['report_count']} farms")
