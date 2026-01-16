"""
Batch Processor for Climate Pipeline

Handles batch processing of CVRs for GitHub Actions parallel execution.
Processes a subset of CVRs and writes results to parquet files.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

# Setup logging
logger = logging.getLogger(__name__)

# Add current directory to path for local imports
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))


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

    @property
    def loader(self):
        """Lazy-load the data loader."""
        if self._loader is None:
            from data_loader import ClimateDataLoader
            self._loader = ClimateDataLoader()
        return self._loader

    @property
    def calculator(self):
        """Lazy-load the calculator."""
        if self._calculator is None:
            from climate_calculator import FarmClimateCalculator
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
        with open(cvr_file, "r") as f:
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
        logger.info(f"Processing batch {batch_number}: CVRs {start_idx}-{end_idx} ({len(batch_cvrs)} CVRs)")

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
                errors.append({
                    "cvr_number": cvr,
                    "year": year,
                    "error_message": str(e),
                })
                # Add error row to results
                results.append({
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
                })

        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        # Convert to DataFrame
        df = pd.DataFrame(results)

        # Write to parquet
        output_file = self.output_dir / f"batch_{batch_number:03d}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Wrote {len(df)} rows to {output_file}")

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
        gcs_output_path: Optional[str] = None,
        cleanup: bool = True,
    ) -> dict:
        """
        Consolidate all batch parquet files into a single output.

        Args:
            year: Year being processed
            batch_count: Number of batches to consolidate
            gcs_output_path: Optional GCS path to upload consolidated file
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

        # Read and concatenate all batch files
        logger.info(f"Consolidating {len(batch_files)} batch files...")
        dfs = [pd.read_parquet(f) for f in batch_files]
        consolidated_df = pd.concat(dfs, ignore_index=True)

        # Write consolidated file locally
        consolidated_file = self.output_dir / "data.parquet"
        consolidated_df.to_parquet(consolidated_file, index=False)
        logger.info(f"Wrote consolidated file: {consolidated_file} ({len(consolidated_df)} rows)")

        # Generate summary statistics
        successful_count = consolidated_df[consolidated_df["error_message"].isna()].shape[0]
        failed_count = consolidated_df[consolidated_df["error_message"].notna()].shape[0]
        total_co2e = consolidated_df["total_co2e_kg"].sum()

        summary = {
            "year": year,
            "total_cvrs": len(consolidated_df),
            "successful": successful_count,
            "failed": failed_count,
            "total_co2e_kg": float(total_co2e) if pd.notna(total_co2e) else 0.0,
            "total_co2e_tonnes": float(total_co2e / 1000) if pd.notna(total_co2e) else 0.0,
            "avg_co2e_per_cvr_kg": float(consolidated_df["total_co2e_kg"].mean()) if successful_count > 0 else 0.0,
            "consolidated_at": datetime.utcnow().isoformat(),
        }

        # Write summary JSON
        summary_file = self.output_dir / "summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Wrote summary: {summary_file}")

        # Upload to GCS if path provided
        if gcs_output_path:
            self._upload_to_gcs(consolidated_file, gcs_output_path, "data.parquet")
            self._upload_to_gcs(summary_file, gcs_output_path, "summary.json")

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
            "gcs_path": gcs_output_path,
            "batches_consolidated": len(batch_files),
            "batches_missing": missing_batches,
        }

    def _upload_to_gcs(self, local_file: Path, gcs_base_path: str, filename: str):
        """
        Upload a file to GCS.

        Args:
            local_file: Local file path
            gcs_base_path: GCS path (gs://bucket/prefix)
            filename: Target filename
        """
        try:
            # Use gcloud for upload
            import subprocess

            gcs_path = f"{gcs_base_path.rstrip('/')}/{filename}"
            cmd = ["gcloud", "storage", "cp", str(local_file), gcs_path]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Uploaded {local_file} to {gcs_path}")
        except Exception as e:
            logger.error(f"Failed to upload {local_file} to GCS: {e}")
            raise


def consolidate_main():
    """
    Entry point for consolidation job (called from GH Actions).

    Usage:
        python -c "from batch_processor import consolidate_main; consolidate_main()" \
            --year 2023 --batch-count 150 --gcs-output gs://bucket/gold/carbon_emissions_2023/timestamp/
    """
    import argparse

    parser = argparse.ArgumentParser(description="Consolidate climate batch results")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--batch-count", type=int, required=True)
    parser.add_argument("--output-dir", type=str, default="/tmp/climate_output")
    parser.add_argument("--gcs-output", type=str, help="GCS output path")
    parser.add_argument("--no-cleanup", action="store_true", help="Keep batch files")

    args = parser.parse_args()

    processor = BatchProcessor(output_dir=args.output_dir)
    result = processor.consolidate_batches(
        year=args.year,
        batch_count=args.batch_count,
        gcs_output_path=args.gcs_output,
        cleanup=not args.no_cleanup,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    consolidate_main()
