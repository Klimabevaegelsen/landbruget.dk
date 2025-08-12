"""
Work Permits Gold Layer

This module implements the gold layer processor for agricultural work permits data.
It takes the cleaned silver layer work permits data from the drive pipeline and 
makes it available in the gold layer for database insertion.

The processor handles:
- Loading work permits data from drive pipeline silver output
- Final validation and formatting for database compatibility
- Clean structure: company_id, year, nationality, first_permits_count
"""

import os
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.timing import timed


class WorkPermitsGoldConfig(BaseJobConfig):
    """Configuration for Work Permits gold layer."""

    name: str = "Work Permits Gold"
    dataset: str = "work_permits"
    type: str = "gold"
    description: str = "Clean agricultural work permits data by company, year, and nationality"
    frequency: str = "monthly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver datasets from drive pipeline
    drive_data_dataset: str = "drive_data"  # Drive pipeline silver output
    
    # Processing configuration
    start_year: int = Field(default=2019, description="Start year for work permits data")
    end_year: int = Field(default=2025, description="End year for work permits data")
    
    # Data validation settings
    max_permits_per_record: int = Field(
        default=1000, 
        description="Maximum permits per company-nationality-year (for data validation)"
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class WorkPermitsGold(BaseSource[WorkPermitsGoldConfig], GoldJobInterface):
    """
    Gold layer processor for work permits data.
    
    Takes cleaned work permits data from drive pipeline silver layer and 
    makes it available for database insertion with structure:
    company_id, year, nationality, first_permits_count
    """

    def __init__(self, config: WorkPermitsGoldConfig):
        super().__init__(config)
        
        # Configure DuckDB for data processing
        self.conn.execute("SET memory_limit = '4GB'")
        self.conn.execute("SET threads = 2") 
        self.conn.execute("SET temp_directory = '/tmp'")

    @timed
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, str]]:
        """Execute work permits gold layer processing."""
        
        self.log.info("🚀 Starting Work Permits Gold processing")
        
        try:
            # Load work permits data from drive pipeline silver output
            await self._load_work_permits_data(silver_data)
            
            # Validate and clean the data
            await self._validate_work_permits_data()
            
            # Save gold layer outputs
            output_paths = await self._save_gold_data()
            
            self.log.info("✅ Work Permits Gold processing completed successfully")
            return output_paths
            
        except Exception as e:
            self.log.error(f"❌ Error in Work Permits Gold processing: {e}")
            raise

    async def _load_work_permits_data(self, silver_data: Optional[Dict[str, Any]] = None):
        """Load work permits data from drive pipeline silver output."""
        
        self.log.info("📥 Loading work permits data from drive pipeline silver layer")
        
        # Look for work permits parquet files in drive_data silver bucket
        # Pattern: work_permits_*.parquet
        try:
            # Try to find work permits files in the silver bucket
            work_permits_pattern = f"gs://{self.config.bucket}/silver/{self.config.drive_data_dataset}/**/work_permits_*.parquet"
            
            self.log.info(f"🔍 Looking for work permits files: {work_permits_pattern}")
            
            # Create work_permits table from silver parquet files
            create_table_sql = f"""
            CREATE OR REPLACE TABLE work_permits AS
            SELECT 
                company_id,
                year,
                nationality,
                first_permits_count,
                source_file,
                extracted_at,
                created_at,
                updated_at
            FROM read_parquet('{work_permits_pattern}')
            WHERE year BETWEEN {self.config.start_year} AND {self.config.end_year}
              AND first_permits_count > 0
              AND first_permits_count <= {self.config.max_permits_per_record}
            """
            
            self.conn.execute(create_table_sql)
            
            # Get count and basic stats
            count_result = self.conn.execute("SELECT COUNT(*) as count FROM work_permits").fetchone()
            record_count = count_result[0] if count_result else 0
            
            if record_count == 0:
                self.log.warning("⚠️ No work permits data found in silver layer")
                # Create empty table with correct schema
                self.conn.execute("""
                CREATE OR REPLACE TABLE work_permits (
                    company_id VARCHAR,
                    year INTEGER,
                    nationality VARCHAR,
                    first_permits_count INTEGER,
                    source_file VARCHAR,
                    extracted_at TIMESTAMP,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """)
                return
            
            # Log basic statistics
            stats_result = self.conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT company_id) as unique_companies,
                COUNT(DISTINCT nationality) as unique_nationalities,
                MIN(year) as min_year,
                MAX(year) as max_year,
                SUM(first_permits_count) as total_permits
            FROM work_permits
            """).fetchone()
            
            if stats_result:
                total_records, companies, nationalities, min_year, max_year, total_permits = stats_result
                self.log.info(f"📊 Loaded work permits data:")
                self.log.info(f"   • {total_records:,} records")
                self.log.info(f"   • {companies:,} unique companies")
                self.log.info(f"   • {nationalities} nationalities")
                self.log.info(f"   • Years: {min_year}-{max_year}")
                self.log.info(f"   • Total permits: {total_permits:,}")
            
        except Exception as e:
            self.log.error(f"❌ Failed to load work permits data: {e}")
            # Create empty table to prevent downstream errors
            self.conn.execute("""
            CREATE OR REPLACE TABLE work_permits (
                company_id VARCHAR,
                year INTEGER,
                nationality VARCHAR,
                first_permits_count INTEGER,
                source_file VARCHAR,
                extracted_at TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """)

    async def _validate_work_permits_data(self):
        """Validate and clean work permits data."""
        
        self.log.info("🔍 Validating work permits data")
        
        # Check for data quality issues
        quality_checks = self.conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN company_id IS NULL OR company_id = '' THEN 1 END) as missing_company_id,
            COUNT(CASE WHEN year IS NULL THEN 1 END) as missing_year,
            COUNT(CASE WHEN nationality IS NULL OR nationality = '' THEN 1 END) as missing_nationality,
            COUNT(CASE WHEN first_permits_count IS NULL OR first_permits_count <= 0 THEN 1 END) as invalid_permits,
            COUNT(CASE WHEN year < 2019 OR year > 2025 THEN 1 END) as invalid_year_range
        FROM work_permits
        """).fetchone()
        
        if quality_checks:
            total, missing_company, missing_year, missing_nationality, invalid_permits, invalid_years = quality_checks
            
            # Log quality issues
            if missing_company > 0:
                self.log.warning(f"⚠️ {missing_company} records with missing company_id")
            if missing_year > 0:
                self.log.warning(f"⚠️ {missing_year} records with missing year")
            if missing_nationality > 0:
                self.log.warning(f"⚠️ {missing_nationality} records with missing nationality")
            if invalid_permits > 0:
                self.log.warning(f"⚠️ {invalid_permits} records with invalid permit counts")
            if invalid_years > 0:
                self.log.warning(f"⚠️ {invalid_years} records with invalid year range")
            
            # Clean up invalid records
            if missing_company + missing_year + missing_nationality + invalid_permits + invalid_years > 0:
                self.log.info("🧹 Removing invalid records")
                self.conn.execute("""
                DELETE FROM work_permits 
                WHERE company_id IS NULL OR company_id = ''
                   OR year IS NULL
                   OR nationality IS NULL OR nationality = ''
                   OR first_permits_count IS NULL OR first_permits_count <= 0
                   OR year < 2019 OR year > 2025
                """)
                
                # Get final count
                final_count = self.conn.execute("SELECT COUNT(*) FROM work_permits").fetchone()[0]
                self.log.info(f"✅ Data validation complete: {final_count:,} clean records")

    async def _save_gold_data(self) -> Dict[str, str]:
        """Save gold layer work permits data."""
        
        self.log.info("💾 Saving work permits gold data")
        
        output_paths = {}
        timestamp = self.date_pattern
        
        try:
            # Create output directory
            output_dir = f"gold/{self.config.dataset}/{timestamp}"
            
            # Save main work permits dataset
            work_permits_path = f"{output_dir}/work_permits.parquet"
            await self._save_table_to_gcs("work_permits", work_permits_path)
            
            output_paths['work_permits'] = work_permits_path
            
            # Save summary statistics
            summary_path = f"{output_dir}/summary.parquet"
            
            # Create summary table first
            self.conn.execute(f"""
            CREATE OR REPLACE TABLE work_permits_summary AS
            SELECT 
                '{timestamp}' as processing_timestamp,
                COUNT(*) as total_records,
                COUNT(DISTINCT company_id) as unique_companies,
                COUNT(DISTINCT nationality) as unique_nationalities,
                MIN(year) as min_year,
                MAX(year) as max_year,
                SUM(first_permits_count) as total_permits,
                AVG(first_permits_count::DOUBLE) as avg_permits_per_record
            FROM work_permits
            """)
            
            await self._save_table_to_gcs("work_permits_summary", summary_path)
            output_paths['summary'] = summary_path
            
            return output_paths
            
        except Exception as e:
            self.log.error(f"❌ Failed to save gold data: {e}")
            raise

    async def _save_table_to_gcs(self, table_name: str, gcs_path: str) -> None:
        """Save a DuckDB table to GCS as parquet."""
        
        # Upload directly from DuckDB table to GCS (more efficient)
        full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"
        self.gcs_access.upload_from_duckdb_table(table_name, full_gcs_path)
        
        self.log.info(f"✅ Saved {table_name} to {full_gcs_path}")