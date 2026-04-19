"""
Batch Processor for Climate Pipeline

Handles batch processing of CVRs for GitHub Actions parallel execution.
Processes a subset of CVRs and writes results to parquet files.
"""

import json
from datetime import datetime
from pathlib import Path

import duckdb
from common.logging_utils import get_pipeline_logger

# Setup logging
logger = get_pipeline_logger(__name__)


class BatchProcessor:
    """
    Processes batches of CVRs for carbon emission calculations.

    Designed to work with GitHub Actions matrix jobs where each job
    processes a subset of CVRs independently.
    """

    def __init__(self, output_dir: str = "/tmp/climate_output"):
        """
        Initialize the batch processor.

        Args:
            output_dir: Directory to write batch output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Lazy-load heavy dependencies
        self._loader = None
        self._calculator = None

        # Create a DuckDB connection for batch processing
        self._duckdb_conn = duckdb.connect()

    @property
    def loader(self):
        """Lazy-load the data loader."""
        if self._loader is None:
            from .data_loader import ClimateDataLoader

            self._loader = ClimateDataLoader()
        return self._loader

    @property
    def calculator(self):
        """Lazy-load the calculator."""
        if self._calculator is None:
            from .climate_calculator import FarmClimateCalculator

            self._calculator = FarmClimateCalculator(self.loader)
        return self._calculator

    def process_batch(
        self,
        batch_number: int,
        batch_size: int,
        year: int,
        cvr_file: str,
    ) -> dict:
        """
        Process a batch of CVRs and write results to parquet.

        Args:
            batch_number: Batch index (0-based)
            batch_size: Number of CVRs per batch
            year: Year to calculate
            cvr_file: Path to JSON file with CVR list

        Returns:
            Summary dict with processing statistics
        """
        # Load CVR list
        with open(cvr_file) as f:
            discovery_data = json.load(f)

        cvr_list = discovery_data["cvr_numbers"]

        # Calculate batch slice
        start_idx = batch_number * batch_size
        end_idx = min(start_idx + batch_size, len(cvr_list))

        if start_idx >= len(cvr_list):
            logger.warning(f"Batch {batch_number} is out of range (total CVRs: {len(cvr_list)})")
            return {
                "batch_number": batch_number,
                "status": "skipped",
                "reason": "batch out of range",
            }

        batch_cvrs = cvr_list[start_idx:end_idx]
        logger.info(
            f"Processing batch {batch_number}: CVRs {start_idx}-{end_idx} ({len(batch_cvrs)} CVRs)"
        )

        # Process each CVR
        results = []
        errors = []
        start_time = datetime.utcnow()

        for i, cvr in enumerate(batch_cvrs):
            try:
                report = self.calculator.calculate_emissions(cvr=cvr, year=year)
                result = self._report_to_row(report)
                results.append(result)

                if (i + 1) % 50 == 0:
                    logger.info(f"  Processed {i + 1}/{len(batch_cvrs)} CVRs...")

            except Exception as e:
                logger.warning(f"Failed to process CVR {cvr}: {e}")
                errors.append(
                    {
                        "cvr_number": cvr,
                        "year": year,
                        "error_message": str(e),
                    }
                )
                # Add error row to results
                results.append(
                    {
                        "cvr_number": cvr,
                        "year": year,
                        "total_co2e_kg": None,
                        "cattle_emissions_kg": None,
                        "pig_emissions_kg": None,
                        "field_emissions_kg": None,
                        "nh3_emissions_kg": None,
                        "data_completeness_score": None,
                        "processing_timestamp": datetime.utcnow().isoformat(),
                        "error_message": str(e),
                    }
                )

        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        # Create DuckDB table from results and write to parquet
        output_file = self.output_dir / f"batch_{batch_number:03d}.parquet"
        self._write_results_to_parquet(results, output_file)
        logger.info(f"Wrote {len(results)} rows to {output_file}")

        # Return summary
        successful = len([r for r in results if r.get("error_message") is None])
        return {
            "batch_number": batch_number,
            "status": "completed",
            "cvr_count": len(batch_cvrs),
            "successful": successful,
            "failed": len(errors),
            "duration_seconds": duration_seconds,
            "output_file": str(output_file),
            "errors": errors[:10] if errors else [],  # Include first 10 errors
        }

    def _write_results_to_parquet(self, results: list[dict], output_file: Path) -> None:
        """
        Write results to parquet using DuckDB.

        Args:
            results: List of result dictionaries
            output_file: Path to output parquet file
        """
        if not results:
            # Write empty parquet with schema
            self._duckdb_conn.execute(f"""
                COPY (
                    SELECT
                        NULL::VARCHAR as cvr_number,
                        NULL::INTEGER as year,
                        NULL::DOUBLE as total_co2e_kg,
                        NULL::DOUBLE as cattle_emissions_kg,
                        NULL::DOUBLE as pig_emissions_kg,
                        NULL::DOUBLE as field_emissions_kg,
                        NULL::DOUBLE as nh3_emissions_kg,
                        NULL::DOUBLE as data_completeness_score,
                        NULL::VARCHAR as processing_timestamp,
                        NULL::VARCHAR as error_message
                    WHERE FALSE
                ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            return

        # Create a temporary table with the results
        self._duckdb_conn.execute("DROP TABLE IF EXISTS batch_results_temp")
        self._duckdb_conn.execute("""
            CREATE TABLE batch_results_temp (
                cvr_number VARCHAR,
                year INTEGER,
                total_co2e_kg DOUBLE,
                cattle_emissions_kg DOUBLE,
                pig_emissions_kg DOUBLE,
                field_emissions_kg DOUBLE,
                nh3_emissions_kg DOUBLE,
                data_completeness_score DOUBLE,
                processing_timestamp VARCHAR,
                error_message VARCHAR
            )
        """)

        # Insert results row by row
        for row in results:
            self._duckdb_conn.execute(
                """
                INSERT INTO batch_results_temp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    row.get("cvr_number"),
                    row.get("year"),
                    row.get("total_co2e_kg"),
                    row.get("cattle_emissions_kg"),
                    row.get("pig_emissions_kg"),
                    row.get("field_emissions_kg"),
                    row.get("nh3_emissions_kg"),
                    row.get("data_completeness_score"),
                    row.get("processing_timestamp"),
                    row.get("error_message"),
                ],
            )

        # Write to parquet
        self._duckdb_conn.execute(f"""
            COPY batch_results_temp TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Cleanup
        self._duckdb_conn.execute("DROP TABLE IF EXISTS batch_results_temp")

    def _report_to_row(self, report) -> dict:
        """
        Convert EmissionReport to flat dictionary for parquet.

        Args:
            report: EmissionReport object

        Returns:
            Flat dictionary suitable for DataFrame row
        """
        # Extract category emissions
        cattle_kg = 0.0
        pig_kg = 0.0
        field_kg = 0.0

        for cat in report.categories:
            if cat.name == "cattle":
                cattle_kg = cat.co2e_kg
            elif cat.name == "pigs":
                pig_kg = cat.co2e_kg
            elif cat.name == "fields":
                field_kg = cat.co2e_kg

        return {
            "cvr_number": report.cvr,
            "year": report.year,
            "total_co2e_kg": report.total_co2e_kg,
            "cattle_emissions_kg": cattle_kg,
            "pig_emissions_kg": pig_kg,
            "field_emissions_kg": field_kg,
            "nh3_emissions_kg": report.total_nh3_kg,
            "data_completeness_score": report.data_completeness,
            "processing_timestamp": datetime.utcnow().isoformat(),
            "error_message": None,
        }

    def consolidate_batches(
        self,
        year: int,
        batch_count: int,
        storage_output_path: str | None = None,
        cleanup: bool = True,
    ) -> dict:
        """
        Consolidate all batch parquet files into a single output.

        Args:
            year: Year being processed
            batch_count: Number of batches to consolidate
            storage_output_path: Optional storage path to upload consolidated file
            cleanup: Whether to delete batch files after consolidation

        Returns:
            Summary dict with consolidation statistics
        """
        batch_files = []
        missing_batches = []

        # Find all batch files
        for i in range(batch_count):
            batch_file = self.output_dir / f"batch_{i:03d}.parquet"
            if batch_file.exists():
                batch_files.append(batch_file)
            else:
                missing_batches.append(i)

        if missing_batches:
            logger.warning(f"Missing batch files: {missing_batches[:10]}...")

        if not batch_files:
            return {
                "status": "failed",
                "reason": "no batch files found",
            }

        # Read and concatenate all batch files using DuckDB
        logger.info(f"Consolidating {len(batch_files)} batch files...")

        # Build UNION ALL query for all batch files
        file_paths = [str(f) for f in batch_files]
        union_query = " UNION ALL ".join(
            [f"SELECT * FROM read_parquet('{fp}')" for fp in file_paths]
        )

        # Create consolidated table
        self._duckdb_conn.execute("DROP TABLE IF EXISTS consolidated_temp")
        self._duckdb_conn.execute(f"""
            CREATE TABLE consolidated_temp AS
            {union_query}
        """)

        # Get row count
        row_count = self._duckdb_conn.execute("SELECT COUNT(*) FROM consolidated_temp").fetchone()[
            0
        ]

        # Write consolidated file locally
        consolidated_file = self.output_dir / "data.parquet"
        self._duckdb_conn.execute(f"""
            COPY consolidated_temp TO '{consolidated_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        logger.info(f"Wrote consolidated file: {consolidated_file} ({row_count} rows)")

        # Generate summary statistics using DuckDB
        stats = self._duckdb_conn.execute("""
            SELECT
                COUNT(*) as total_cvrs,
                COUNT(CASE WHEN error_message IS NULL THEN 1 END) as successful,
                COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as failed,
                SUM(total_co2e_kg) as total_co2e,
                AVG(total_co2e_kg) as avg_co2e
            FROM consolidated_temp
        """).fetchone()

        total_cvrs = stats[0]
        successful_count = stats[1]
        failed_count = stats[2]
        total_co2e = stats[3] if stats[3] else 0.0
        avg_co2e = stats[4] if stats[4] else 0.0

        # Cleanup temp table
        self._duckdb_conn.execute("DROP TABLE IF EXISTS consolidated_temp")

        summary = {
            "year": year,
            "total_cvrs": total_cvrs,
            "successful": successful_count,
            "failed": failed_count,
            "total_co2e_kg": float(total_co2e),
            "total_co2e_tonnes": float(total_co2e / 1000) if total_co2e else 0.0,
            "avg_co2e_per_cvr_kg": float(avg_co2e) if successful_count > 0 else 0.0,
            "consolidated_at": datetime.utcnow().isoformat(),
        }

        # Write summary JSON
        summary_file = self.output_dir / "summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Wrote summary: {summary_file}")

        # Upload to cloud storage if path provided
        if storage_output_path:
            self._upload_to_storage(consolidated_file, storage_output_path, "data.parquet")
            self._upload_to_storage(summary_file, storage_output_path, "summary.json")

        # Cleanup batch files
        if cleanup:
            logger.info(f"Cleaning up {len(batch_files)} batch files...")
            for batch_file in batch_files:
                try:
                    batch_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete {batch_file}: {e}")

        return {
            "status": "completed",
            "summary": summary,
            "consolidated_file": str(consolidated_file),
            "storage_path": storage_output_path,
            "batches_consolidated": len(batch_files),
            "batches_missing": missing_batches,
        }

    def _upload_to_storage(self, local_file: Path, storage_base_path: str, filename: str):
        """
        Upload a file to cloud storage using StorageAccess.

        Args:
            local_file: Local file path
            storage_base_path: Storage path (bucket/prefix)
            filename: Target filename
        """
        from common.storage import StorageAccess

        storage = StorageAccess()
        storage_path = f"{storage_base_path.rstrip('/')}/{filename}"
        # Strip protocol prefix for s3fs
        clean_path = storage_path
        for prefix in ("r2://", "s3://"):
            if clean_path.startswith(prefix):
                clean_path = clean_path[len(prefix) :]
                break
        storage.fs.put(str(local_file), clean_path)
        logger.info(f"Uploaded {local_file} to {storage_path}")

    def __del__(self):
        """Cleanup DuckDB connection on deletion."""
        try:
            if hasattr(self, "_duckdb_conn") and self._duckdb_conn:
                self._duckdb_conn.close()
        except Exception:
            pass


def consolidate_main():
    """
    Entry point for consolidation job (called from GH Actions).

    Usage:
        python -c "from batch_processor import consolidate_main; consolidate_main()" \
            --year 2023 --batch-count 150 --output bucket/gold/carbon_emissions_2023/timestamp/
    """
    import argparse

    parser = argparse.ArgumentParser(description="Consolidate climate batch results")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--batch-count", type=int, required=True)
    parser.add_argument("--output-dir", type=str, default="/tmp/climate_output")
    parser.add_argument("--storage-output", type=str, help="cloud storage output path")
    parser.add_argument("--no-cleanup", action="store_true", help="Keep batch files")

    args = parser.parse_args()

    processor = BatchProcessor(output_dir=args.output_dir)
    result = processor.consolidate_batches(
        year=args.year,
        batch_count=args.batch_count,
        storage_output_path=args.storage_output,
        cleanup=not args.no_cleanup,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    consolidate_main()
