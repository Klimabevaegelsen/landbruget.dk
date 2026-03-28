"""
Carbon Emissions Gold Layer

This module implements the gold layer processor for farm-level carbon emission
estimates. It integrates data from CHR (livestock), FVM (fields), and fertilizer
sources to compute CO2e emissions across multiple categories.

Datasets consumed (silver layer):
- gr {year}: Green Accounts livestock data (cvr_number, species, counts, N production)
- fvm_marker_{year}: FVM field boundaries (cvr, crop types, areas)
- fertiliser: GKEA fertilizer applications (cvr_number, total_n_kvote, area)

Output (gold layer):
- gold/carbon_emissions/{timestamp}/emissions.parquet
- gold/carbon_emissions/{timestamp}/categories.parquet
- gold/carbon_emissions/{timestamp}/metadata.json
"""

from datetime import datetime
from typing import Any

from unified_pipeline.common.base import BaseSource, GoldJobInterface
from unified_pipeline.util.log_util import Logger

from .climate_calculator import EmissionReport, FarmClimateCalculator
from .config import CarbonEmissionsGoldConfig
from .data_loader import ClimateDataLoader


class CarbonEmissionsGold(BaseSource[CarbonEmissionsGoldConfig], GoldJobInterface):
    """
    Gold layer processor for farm-level carbon emission estimates.

    This processor:
    - Discovers CVRs with agricultural data for the target year
    - Calculates emissions per CVR using livestock, field, and fertilizer data
    - Writes emission reports to gold layer as parquet files
    - Supports batch processing for GitHub Actions matrix jobs
    """

    def __init__(self, config: CarbonEmissionsGoldConfig):
        super().__init__(config)
        self.log = Logger.get_logger()

    async def run(self, silver_data: dict[str, Any] | None = None) -> None:
        """
        Run carbon emission calculations and write results to gold layer.

        Args:
            silver_data: Not used — this module reads directly from silver layer storage.
        """
        target_year = self.config.target_year
        if target_year is None:
            self.log.error("target_year is required. Use --target-year CLI flag.")
            raise ValueError("target_year is required for carbon emissions calculation")

        self.log.info(f"Starting carbon emissions calculation for year {target_year}")

        # Initialize data loader using the storage from BaseSource
        loader = ClimateDataLoader(storage=self.storage, bucket=self.config.bucket)
        calculator = FarmClimateCalculator(loader)

        # Discover CVRs with data
        cvr_list = self._discover_cvrs(loader, target_year)

        if not cvr_list:
            self.log.warning(f"No CVRs found with data for year {target_year}")
            return

        # Apply test limit if set
        if self.config.test_limit and self.config.test_limit > 0:
            cvr_list = cvr_list[: self.config.test_limit]
            self.log.info(f"Test limit applied: processing {len(cvr_list)} CVRs")

        # Apply batch slicing if set
        if self.config.batch_number is not None and self.config.total_batches:
            batch_size = self.config.batch_size
            start_idx = self.config.batch_number * batch_size
            end_idx = min(start_idx + batch_size, len(cvr_list))
            if start_idx >= len(cvr_list):
                self.log.warning(
                    f"Batch {self.config.batch_number} is out of range "
                    f"(total CVRs: {len(cvr_list)})"
                )
                return
            cvr_list = cvr_list[start_idx:end_idx]
            self.log.info(
                f"Batch {self.config.batch_number}: processing CVRs {start_idx}-{end_idx} "
                f"({len(cvr_list)} CVRs)"
            )

        # Process each CVR
        reports = []
        failed = 0

        self.log.info(f"Processing {len(cvr_list)} farm(s) for year {target_year}...")

        for i, cvr in enumerate(cvr_list, 1):
            try:
                report = calculator.calculate_emissions(cvr=cvr, year=target_year)
                reports.append(report)

                if i % 50 == 0:
                    self.log.info(f"  Processed {i}/{len(cvr_list)} CVRs...")
            except Exception as e:
                self.log.warning(f"Failed to process CVR {cvr}: {e}")
                failed += 1

        self.log.info(f"Calculation complete: {len(reports)} successful, {failed} failed")

        if not reports:
            self.log.warning("No reports generated — nothing to write")
            return

        # Write results to gold layer
        self._write_to_gold_layer(reports, target_year)

    def _discover_cvrs(self, loader: ClimateDataLoader, year: int) -> list[str]:
        """Discover all CVRs with agricultural data for a given year."""
        cvr_set = set()

        # Query Green Accounts for CVRs with livestock
        self.log.info(f"Discovering CVRs with livestock data (year {year})...")
        try:
            pattern = f"{loader.bucket}/silver/gr {year}/*/V_4061GR_*_DYRERK_*_pii_handled.parquet"
            files = loader.storage.list_files(pattern)
            if files:
                latest_file = sorted(files)[-1]
                table_name = "gr_discover_temp"
                loader.storage.create_table_from_storage(table_name, latest_file)
                result_df = loader.storage.duckdb_conn.execute(
                    f"SELECT DISTINCT cvr_number FROM {table_name} WHERE cvr_number IS NOT NULL"
                ).df()
                loader.storage.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                livestock_cvrs = set(result_df["cvr_number"].astype(str).str.zfill(8).tolist())
                cvr_set.update(livestock_cvrs)
                self.log.info(f"  Found {len(livestock_cvrs)} CVRs with livestock")
        except Exception as e:
            self.log.warning(f"Failed to query Green Accounts: {e}")

        # Query FVM for CVRs with fields
        self.log.info(f"Discovering CVRs with field data (year {year})...")
        try:
            pattern = f"{loader.bucket}/silver/fvm_marker_{year}/*/data.parquet"
            files = loader.storage.list_files(pattern)
            if files:
                latest_file = sorted(files)[-1]
                table_name = "fvm_discover_temp"
                loader.storage.create_table_from_storage(table_name, latest_file)
                sample = loader.storage.duckdb_conn.execute(
                    f"SELECT * FROM {table_name} LIMIT 0"
                ).df()
                cvr_col = "cvr_number" if "cvr_number" in sample.columns else "cvr"
                result_df = loader.storage.duckdb_conn.execute(
                    f"SELECT DISTINCT {cvr_col} as cvr_number FROM {table_name} "
                    f"WHERE {cvr_col} IS NOT NULL"
                ).df()
                loader.storage.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                field_cvrs = set(result_df["cvr_number"].astype(str).str.zfill(8).tolist())
                cvr_set.update(field_cvrs)
                self.log.info(f"  Found {len(field_cvrs)} CVRs with fields")
        except Exception as e:
            self.log.warning(f"Failed to query FVM: {e}")

        cvr_list = sorted(cvr_set)
        self.log.info(f"Discovery complete: {len(cvr_list)} unique CVRs")
        return cvr_list

    def _write_to_gold_layer(self, reports: list[EmissionReport], year: int) -> None:
        """Write emission reports to gold layer using StorageAccess."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"{self.config.bucket}/gold/carbon_emissions_{year}/{timestamp}"

        self.log.info(f"Writing {len(reports)} reports to {output_dir}")

        conn = self.storage.duckdb_conn

        # Create emissions table
        conn.execute("DROP TABLE IF EXISTS emissions_temp")
        conn.execute("""
            CREATE TABLE emissions_temp (
                cvr VARCHAR,
                year INTEGER,
                total_co2e_kg DOUBLE,
                data_completeness DOUBLE,
                intensity_co2e_per_kg_milk DOUBLE,
                intensity_co2e_per_ha DOUBLE,
                intensity_co2e_per_animal_unit DOUBLE,
                created_at TIMESTAMP
            )
        """)

        created_at = datetime.now()
        for report in reports:
            conn.execute(
                "INSERT INTO emissions_temp VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    str(report.cvr).zfill(8),
                    report.year,
                    report.total_co2e_kg,
                    report.data_completeness,
                    report.intensity_metrics.get("co2e_per_kg_milk"),
                    report.intensity_metrics.get("co2e_per_ha"),
                    report.intensity_metrics.get("co2e_per_animal_unit"),
                    created_at,
                ],
            )

        # Upload emissions parquet
        emissions_path = f"{output_dir}/emissions.parquet"
        self.storage.upload_from_duckdb_table(
            "emissions_temp", emissions_path, compression="zstd", row_group_size=10000
        )
        count = conn.execute("SELECT COUNT(*) FROM emissions_temp").fetchone()[0]
        self.log.info(f"Wrote {count} emission records to {emissions_path}")

        # Create categories table
        conn.execute("DROP TABLE IF EXISTS categories_temp")
        conn.execute("""
            CREATE TABLE categories_temp (
                cvr VARCHAR,
                year INTEGER,
                category_name VARCHAR,
                co2e_kg DOUBLE,
                data_quality VARCHAR,
                sub_sources VARCHAR,
                created_at TIMESTAMP
            )
        """)

        for report in reports:
            cvr = str(report.cvr).zfill(8)
            for category in report.categories:
                conn.execute(
                    "INSERT INTO categories_temp VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [
                        cvr,
                        report.year,
                        category.name,
                        category.co2e_kg,
                        category.data_quality,
                        str(category.sub_sources) if category.sub_sources else "{}",
                        created_at,
                    ],
                )

        # Upload categories parquet
        categories_path = f"{output_dir}/categories.parquet"
        self.storage.upload_from_duckdb_table(
            "categories_temp",
            categories_path,
            compression="zstd",
            row_group_size=10000,
        )
        cat_count = conn.execute("SELECT COUNT(*) FROM categories_temp").fetchone()[0]
        self.log.info(f"Wrote {cat_count} category records to {categories_path}")

        # Upload metadata JSON
        metadata = {
            "timestamp": timestamp,
            "year": year,
            "report_count": len(reports),
            "cvr_list": [report.cvr for report in reports],
            "statistics": {
                "total_emissions_kg_co2e": sum(r.total_co2e_kg for r in reports),
                "avg_emissions_kg_co2e": sum(r.total_co2e_kg for r in reports) / len(reports),
                "avg_data_completeness": sum(r.data_completeness for r in reports) / len(reports),
            },
            "data_sources": {
                "chr": "silver/chr",
                "fvm_marker": f"silver/fvm_marker_{year}",
                "fertiliser": "silver/fertiliser",
            },
            "pipeline_version": "2.0.0",
            "created_at": datetime.now().isoformat(),
        }
        metadata_path = f"{output_dir}/metadata.json"
        self.storage.upload_json(metadata, metadata_path)
        self.log.info(f"Wrote metadata to {metadata_path}")

        # Cleanup
        conn.execute("DROP TABLE IF EXISTS emissions_temp")
        conn.execute("DROP TABLE IF EXISTS categories_temp")

        self.log.info(f"Successfully wrote {len(reports)} reports to {output_dir}")
