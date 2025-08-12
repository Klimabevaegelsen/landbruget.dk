"""Base classes and utilities for Field Area Analysis pipeline."""

import os
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from unified_pipeline.common.base import BaseJobConfig, BaseSource
from unified_pipeline.util.log_util import Logger

from .area_validation import FieldAreaValidator, ValidationException
from .config import CONFIG


class FieldAnalysisStageConfig(BaseJobConfig):
    """Base configuration for field analysis stages."""

    bucket: str = CONFIG.bucket
    batch_size: int = CONFIG.batch_size
    max_memory_gb: int = CONFIG.max_memory_gb
    max_threads: int = CONFIG.max_threads

    # Area validation settings (enabled by default for data integrity)
    enable_area_validation: bool = os.getenv("ENABLE_AREA_VALIDATION", "true").lower() == "true"
    area_validation_tolerance_pct: float = float(os.getenv("AREA_VALIDATION_TOLERANCE_PCT", "1.0"))
    fail_on_validation_error: bool = os.getenv("FAIL_ON_VALIDATION_ERROR", "true").lower() == "true"


class FieldAnalysisStageBase(BaseSource[FieldAnalysisStageConfig], ABC):
    """Base class for all field analysis pipeline stages."""

    def __init__(self, config: FieldAnalysisStageConfig, stage_name: str):
        super().__init__(config)
        self.log = Logger.get_logger()
        self.stage_name = stage_name
        self.start_time = None

        # Initialize area validator if validation is enabled
        self.area_validator = None
        if config.enable_area_validation:
            self.area_validator = FieldAreaValidator(
                conn=self.conn, log=self.log, tolerance_pct=config.area_validation_tolerance_pct
            )
            self.log.info(
                f"🔍 Area validation ENABLED by default for {stage_name} (tolerance: {config.area_validation_tolerance_pct}%)"
            )

        self.validation_config = config

    def _load_silver_dataset(self, dataset_name: str, table_name: str, where_clause: str = "1=1"):
        """
        Load a silver dataset into DuckDB using the standard BaseSource method.

        Args:
            dataset_name: Name of the dataset (e.g., 'fvm_marker_2024')
            table_name: Name to give the table in DuckDB
            where_clause: Optional WHERE clause for server-side filtering
        """
        self.log.info(f"Loading {dataset_name}...")

        # Use the standard BaseSource method with filtering
        if where_clause != "1=1":
            self.read_silver_data_with_filter_direct(dataset_name, where_clause, table_name)
        else:
            self.read_silver_data_direct(dataset_name, table_name)

        # Log table statistics
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(f"✅ Loaded {count:,} rows into table {table_name}")

    def _save_stage_output(self, table_name: str, output_key: str):
        """
        Save stage output to GCS using optimized export.

        Args:
            table_name: DuckDB table name to export
            output_key: Key in CONFIG.stage_outputs for the output dataset name
        """
        # Get year-aware output dataset name
        updated_outputs = CONFIG.update_outputs_for_year()
        output_dataset = updated_outputs[output_key]

        # Use the standard BaseSource method to save data
        self.save_data_direct(table_name, output_dataset, CONFIG.bucket, "gold")

        # Log export statistics
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        self.log.info(
            f"✅ Exported {count:,} rows to {output_dataset} (year: {CONFIG.agricultural_fields_year})"
        )

    def _get_latest_gold_path(self, dataset: str) -> str:
        """Get path to latest gold data file for a given dataset."""
        pattern = f"gs://{CONFIG.bucket}/gold/{dataset}/*/data.parquet"
        files = self.gcs_access.list_files(pattern)

        if not files:
            raise FileNotFoundError(f"No gold data found for {dataset}")

        return sorted(files)[-1]  # Latest by timestamp

    def _get_input_area_reference(self) -> Optional[Dict[str, Any]]:
        """
        Get reference area statistics from input data for validation.
        Should be overridden by stages that need validation.

        Returns:
            Dict with 'total_area' and 'field_count' keys, or None if not applicable
        """
        return None

    def _get_main_output_table(self) -> Optional[str]:
        """
        Get the name of the main output table for area validation.
        Should be overridden by stages that need validation.

        Returns:
            Name of the main output table to validate, or None if not applicable
        """
        return None

    def _should_validate_areas(self) -> bool:
        """Check if this stage should perform area validation."""
        # Skip validation for stages that intentionally filter/reduce the dataset
        if self.area_validator is None:
            return False

        # Stage 0: Pre-filtering doesn't preserve areas exactly
        if self.stage_name.startswith("Stage 0"):
            return False

        # Stage 2: Filtering stages intentionally reduce dataset size (only fields with intersections)
        if self.stage_name.startswith("Stage 2"):
            return False

        # All other stages should preserve area (Stage 1: enrichment, Stage 3: analysis, Stage 4: consolidation)
        return True

    def _validate_stage_areas(self) -> None:
        """Validate areas after stage processing if validation is enabled."""
        if not self._should_validate_areas():
            return

        # Get input reference and output table
        input_reference = self._get_input_area_reference()
        output_table = self._get_main_output_table()

        if not input_reference or not output_table:
            self.log.info(
                f"⚠️ Area validation skipped for {self.stage_name}: missing reference data or output table"
            )
            return

        # Perform validation
        try:
            validation_result = self.area_validator.validate_stage_with_input_reference(
                output_table=output_table,
                stage_name=self.stage_name,
                reference_total_area=input_reference["total_area"],
                reference_field_count=input_reference["field_count"],
            )

            # Handle validation failure
            if not validation_result.is_valid:
                if self.validation_config.fail_on_validation_error:
                    raise ValidationException(validation_result)
                else:
                    self.log.warning(
                        f"⚠️ Area validation failed but continuing: {validation_result.validation_message}"
                    )

        except ValidationException:
            raise  # Re-raise validation exceptions
        except Exception as e:
            error_msg = f"❌ Area validation error for {self.stage_name}: {str(e)}"
            if self.validation_config.fail_on_validation_error:
                raise RuntimeError(error_msg) from e
            else:
                self.log.warning(f"⚠️ {error_msg} - continuing anyway")

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute the stage processing."""
        self.start_time = time.time()
        self.log.info(f"🚀 Starting {self.stage_name}")

        try:
            # Load input data
            self._load_input_data()

            # Execute stage-specific processing
            result = await self._execute_stage_processing()

            # Save output data
            self._save_output_data(result)

            # Validate areas after processing (stages 1-4 only)
            self._validate_stage_areas()

            # Log performance
            total_time = time.time() - self.start_time
            self.log.info(f"✅ {self.stage_name} completed in {total_time:.1f} seconds")

            return {"stage": self.stage_name, "execution_time": total_time, "result": result}

        except Exception as e:
            total_time = time.time() - self.start_time if self.start_time else 0
            self.log.error(f"❌ {self.stage_name} failed after {total_time:.1f} seconds: {e}")
            raise

    @abstractmethod
    def _load_input_data(self):
        """Load input data for this stage."""
        pass

    @abstractmethod
    async def _execute_stage_processing(self) -> Dict[str, Any]:
        """Execute the main processing logic for this stage."""
        pass

    @abstractmethod
    def _save_output_data(self, result: Dict[str, Any]):
        """Save the output data from this stage."""
        pass
