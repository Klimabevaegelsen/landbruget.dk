"""
CVR Collection Step - Step 1 of CVR Enrichment Pipeline

This step collects CVR numbers from all other pipelines' CVR collection files,
deduplicates them, validates formats, and prepares them for the enrichment process.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.cvr_collection import CVRCollectionManager
from unified_pipeline.util.timing import timed

from .shared.batch_manager import CVRBatchManager
from .shared.config import CVREnrichmentSharedConfig, CVREnrichmentStep


class CVRCollectionConfig(BaseJobConfig):
    """Configuration for CVR collection step."""
    
    name: str = "CVR Collection"
    dataset: str = "cvr_enrichment_collection"
    type: str = "cvr_collection"
    description: str = "Collect and deduplicate CVR numbers from all pipelines"
    frequency: str = "monthly"
    bucket: str = "landbrugsdata-raw-data"
    
    # Shared configuration
    shared_config: CVREnrichmentSharedConfig = Field(
        default_factory=CVREnrichmentSharedConfig,
        description="Shared configuration for CVR enrichment pipeline"
    )
    
    # Collection-specific configuration
    validate_cvr_format: bool = Field(
        default=True,
        description="Whether to validate CVR number format before including"
    )
    
    save_invalid_cvrs: bool = Field(
        default=True,
        description="Whether to save invalid CVR numbers for analysis"
    )
    
    model_config = {"frozen": True}
    
    def apply_cli_filters(self, cli_config):
        """Apply CLI configuration filters to this config."""
        if cli_config.test_limit is not None:
            object.__setattr__(self, 'shared_config', 
                self.shared_config.model_copy(update={'test_limit': cli_config.test_limit}))


class CVRCollection(BaseSource[CVRCollectionConfig], GoldJobInterface):
    """
    CVR collection step implementation.
    
    This step:
    1. Collects CVR numbers from all pipeline CVR collections
    2. Deduplicates and validates CVR numbers
    3. Creates batches for parallel processing
    4. Saves collection results for subsequent steps
    """
    
    def __init__(self, config: CVRCollectionConfig):
        """
        Initialize CVR collection step.
        
        Args:
            config: Configuration for CVR collection
        """
        super().__init__(config)
        
        # Initialize CVR collection manager
        self.cvr_collection_manager = CVRCollectionManager(
            gcs_access=self.gcs_access, 
            bucket=self.config.bucket
        )
        
        # Initialize batch manager
        self.batch_manager = CVRBatchManager(
            batch_size=self.config.shared_config.default_batch_size
        )
        
        self.log.info("CVR collection step initialized")
        self.log.info("📋 Configuration:")
        self.log.info(f"   • Validate CVR format: {self.config.validate_cvr_format}")
        self.log.info(f"   • Save invalid CVRs: {self.config.save_invalid_cvrs}")
        self.log.info(f"   • Test limit: {self.config.shared_config.test_limit or 'none'}")
    
    @timed(name="CVR collection processing")
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Run the CVR collection process.
        
        Args:
            silver_data: Optional silver data (not used in this step)
            
        Returns:
            Table name containing collected CVR data
        """
        self.log.info("Starting CVR collection step")
        
        try:
            # Step 1: Collect all CVR numbers from pipeline collections
            self.log.info("📋 Step 1/4: Collecting CVR numbers from pipeline sources")
            collection_data = self._collect_cvr_numbers()
            
            # Step 2: Validate and filter CVR numbers
            self.log.info("✅ Step 2/4: Validating and filtering CVR numbers")
            validated_data = self._validate_cvr_numbers(collection_data)
            
            # Step 3: Create batches for parallel processing
            self.log.info("📦 Step 3/4: Creating processing batches")
            batch_data = self._create_processing_batches(validated_data)
            
            # Step 4: Save collection results
            self.log.info("💾 Step 4/4: Saving collection data")
            table_name = self._save_collection_data(batch_data)
            
            # Success summary
            total_cvrs = len(validated_data.get("valid_cvr_numbers", []))
            batches_created = len(batch_data.get("batches", []))
            
            self.log.info("=" * 60)
            self.log.info("✅ CVR COLLECTION COMPLETED SUCCESSFULLY")
            self.log.info("=" * 60)
            self.log.info("📊 SUMMARY:")
            self.log.info(f"   • Total CVR numbers collected: {total_cvrs:,}")
            self.log.info(f"   • Processing batches created: {batches_created}")
            self.log.info(f"   • Output table: {table_name}")
            self.log.info("   • Ready for next step: Company Fetching")
            self.log.info("=" * 60)
            
            return table_name
            
        except Exception as e:
            self.log.error("=" * 60)
            self.log.error("❌ CVR COLLECTION FAILED")
            self.log.error("=" * 60)
            self.log.error(f"💥 Error: {e}")
            self.log.error("🔍 Check the logs above for detailed error information")
            self.log.error("=" * 60)
            raise
    
    @timed(name="Collecting CVR numbers")
    def _collect_cvr_numbers(self) -> Dict[str, Any]:
        """
        Collect CVR numbers from all pipeline CVR collections.
        
        Returns:
            Dictionary containing collected CVR data
        """
        self.log.info("Collecting CVR numbers from all pipeline collections")
        
        # Use existing CVR collection manager
        collection_data = self.cvr_collection_manager.collect_all_cvr_numbers()
        
        unique_cvr_count = len(collection_data["all_cvr_numbers"])
        pipelines_count = collection_data["collection_summary"]["pipelines_processed"]
        
        self.log.info(
            f"Collected {unique_cvr_count} unique CVR numbers from {pipelines_count} pipelines"
        )
        
        # Add collection metadata
        collection_data["collection_timestamp"] = datetime.now().isoformat()
        collection_data["collection_step"] = CVREnrichmentStep.COLLECTION.value
        
        return collection_data
    
    @timed(name="Validating CVR numbers")
    def _validate_cvr_numbers(self, collection_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and filter CVR numbers.
        
        Args:
            collection_data: Raw collection data from pipelines
            
        Returns:
            Validated collection data
        """
        self.log.info("Validating CVR numbers")
        
        all_cvrs = collection_data["all_cvr_numbers"]
        pipeline_sources = collection_data["pipeline_sources"]
        
        valid_cvrs = set()
        invalid_cvrs = []
        
        for cvr in all_cvrs:
            if self._is_valid_cvr_format(cvr):
                valid_cvrs.add(cvr)
            else:
                invalid_cvrs.append({
                    "cvr_number": cvr,
                    "source_pipelines": pipeline_sources.get(cvr, []),
                    "validation_error": "Invalid format"
                })
        
        # Apply test limit if configured
        if self.config.shared_config.test_limit:
            original_count = len(valid_cvrs)
            valid_cvrs = set(sorted(list(valid_cvrs))[:self.config.shared_config.test_limit])
            self.log.info(
                f"🧪 Test mode: Limited CVR list from {original_count} to {len(valid_cvrs)} entries"
            )
        
        validation_summary = {
            "total_collected": len(all_cvrs),
            "valid_cvrs": len(valid_cvrs),
            "invalid_cvrs": len(invalid_cvrs),
            "validation_rate": len(valid_cvrs) / len(all_cvrs) if all_cvrs else 0,
            "test_limit_applied": self.config.shared_config.test_limit is not None
        }
        
        self.log.info(
            f"Validation completed: {validation_summary['valid_cvrs']} valid, "
            f"{validation_summary['invalid_cvrs']} invalid "
            f"({validation_summary['validation_rate']:.1%} valid rate)"
        )
        
        # Update collection data
        validated_data = collection_data.copy()
        validated_data["valid_cvr_numbers"] = valid_cvrs
        validated_data["invalid_cvr_numbers"] = invalid_cvrs
        validated_data["validation_summary"] = validation_summary
        validated_data["validation_timestamp"] = datetime.now().isoformat()
        
        return validated_data
    
    @timed(name="Creating processing batches")
    def _create_processing_batches(self, validated_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create batches for parallel processing.
        
        Args:
            validated_data: Validated collection data
            
        Returns:
            Collection data with batch information
        """
        self.log.info("Creating processing batches")
        
        valid_cvrs = validated_data["valid_cvr_numbers"]
        
        if not valid_cvrs:
            self.log.warning("No valid CVR numbers to batch")
            batches = []
        else:
            # Create batches using batch manager
            batches = self.batch_manager.create_cvr_batches(valid_cvrs)
        
        # Get batch summary
        batch_summary = self.batch_manager.get_batch_summary(batches)
        
        # Validate batch coverage
        if valid_cvrs:
            validation_result = self.batch_manager.validate_batch_coverage(valid_cvrs, batches)
            if not validation_result["is_valid"]:
                raise ValueError(f"Batch validation failed: {validation_result}")
        
        batch_data = validated_data.copy()
        batch_data["processing_batches"] = batches
        batch_data["batch_summary"] = batch_summary
        batch_data["batch_timestamp"] = datetime.now().isoformat()
        
        self.log.info(
            f"Created {batch_summary['total_batches']} processing batches "
            f"(avg size: {batch_summary['avg_batch_size']:.1f})"
        )
        
        return batch_data
    
    @timed(name="Saving collection data")
    def _save_collection_data(self, batch_data: Dict[str, Any]) -> str:
        """
        Save collection data to GCS.
        
        Args:
            batch_data: Collection data with batches
            
        Returns:
            Table name where data was saved
        """
        self.log.info("Saving CVR collection data")
        
        # Create DuckDB table with collection data
        table_name = "cvr_collection"
        
        # Prepare data for storage
        collection_summary = {
            "collection_summary": batch_data["collection_summary"],
            "validation_summary": batch_data["validation_summary"],
            "batch_summary": batch_data["batch_summary"],
            "collection_timestamp": batch_data["collection_timestamp"],
            "validation_timestamp": batch_data["validation_timestamp"],
            "batch_timestamp": batch_data["batch_timestamp"],
            "pipeline_run_id": self.date_pattern
        }
        
        # Save main collection data
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Convert valid CVR numbers to list for DuckDB
        valid_cvrs_list = sorted(list(batch_data["valid_cvr_numbers"]))
        
        if valid_cvrs_list:
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS
                SELECT 
                    unnest($1) as cvr_number,
                    $2 as collection_metadata
            """, [valid_cvrs_list, json.dumps(collection_summary)])
        else:
            # Create empty table with schema
            self.conn.execute(f"""
                CREATE TABLE {table_name} (
                    cvr_number VARCHAR,
                    collection_metadata VARCHAR
                )
            """)
        
        # Save to GCS
        self._save_data(
            data=table_name,
            dataset=self.config.dataset,
            bucket=self.config.bucket,
            stage="gold"
        )
        
        # Also save locally for GitHub Actions artifact sharing
        import os
        if os.getenv("GITHUB_ACTIONS") == "true":
            self.log.info("GitHub Actions detected - saving collection data locally for artifact sharing")
            local_path = "/tmp/cvr_collection_data.parquet"
            self.conn.execute(f"COPY {table_name} TO '{local_path}' (FORMAT PARQUET)")
            self.log.info(f"Saved collection data locally to {local_path}")
        
        # Save detailed batch information separately
        self._save_batch_details(batch_data)
        
        # Save invalid CVRs if configured
        if self.config.save_invalid_cvrs and batch_data["invalid_cvr_numbers"]:
            self._save_invalid_cvrs(batch_data["invalid_cvr_numbers"])
        
        # Save pipeline sources mapping
        self._save_pipeline_sources(batch_data["pipeline_sources"])
        
        collection_count = len(valid_cvrs_list)
        batch_count = batch_data["batch_summary"]["total_batches"]
        
        self.log.info(
            f"Saved {collection_count} valid CVR numbers in {batch_count} batches to {table_name}"
        )
        
        return table_name
    
    def _save_batch_details(self, batch_data: Dict[str, Any]) -> None:
        """Save detailed batch information for subsequent steps."""
        batch_details_path = f"gold/{self.config.dataset}/{self.date_pattern}/batch_details.json"
        
        batch_details = {
            "total_batches": batch_data["batch_summary"]["total_batches"],
            "batch_sizes": batch_data["batch_summary"]["batch_sizes"],
            "processing_batches": batch_data["processing_batches"],
            "created_timestamp": batch_data["batch_timestamp"]
        }
        
        self.gcs_access.upload_json(
            data=batch_details,
            gcs_path=f"gs://{self.config.bucket}/{batch_details_path}"
        )
        
        self.log.info(f"Saved batch details to {batch_details_path}")
    
    def _save_invalid_cvrs(self, invalid_cvrs: list) -> None:
        """Save invalid CVR numbers for analysis."""
        invalid_cvrs_path = f"gold/{self.config.dataset}/{self.date_pattern}/invalid_cvrs.json"
        
        self.gcs_access.upload_json(
            data=invalid_cvrs,
            gcs_path=f"gs://{self.config.bucket}/{invalid_cvrs_path}"
        )
        
        self.log.info(f"Saved {len(invalid_cvrs)} invalid CVR numbers to {invalid_cvrs_path}")
    
    def _save_pipeline_sources(self, pipeline_sources: Dict[str, list]) -> None:
        """Save pipeline sources mapping."""
        sources_path = f"gold/{self.config.dataset}/{self.date_pattern}/pipeline_sources.json"
        
        self.gcs_access.upload_json(
            data=pipeline_sources,
            gcs_path=f"gs://{self.config.bucket}/{sources_path}"
        )
        
        self.log.info(f"Saved pipeline sources mapping to {sources_path}")
    
    def _is_valid_cvr_format(self, cvr_number: str) -> bool:
        """
        Validate CVR number format.
        
        Args:
            cvr_number: CVR number to validate
            
        Returns:
            True if valid format, False otherwise
        """
        if not self.config.validate_cvr_format:
            return True
        
        if not cvr_number or not isinstance(cvr_number, str):
            return False
        
        # Remove any whitespace
        cvr_clean = cvr_number.strip()
        
        # Check if exactly 8 digits
        if len(cvr_clean) != 8:
            return False
        
        # Check if all characters are digits
        if not cvr_clean.isdigit():
            return False
        
        # CVR numbers shouldn't start with 0
        if cvr_clean.startswith("0"):
            return False
        
        return True
