#!/usr/bin/env python3
"""
Pesticide Proximity Analysis Gold Layer - Separate Pipeline

This pipeline performs spatial proximity analysis on disaggregated pesticide data.
It runs as a separate process after pesticide disaggregation to avoid DuckDB spatial crashes.

Key Features:
- Residential building proximity analysis (100m radius)
- Educational facility proximity analysis (100m radius) 
- Water feature proximity analysis (100m radius)
- Formatted string outputs with addresses and distances
- Matrix job approach for parallel processing by year
"""

import os
import time
from typing import Dict, Optional, Any
from loguru import logger
from pydantic import Field, ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface


class PesticideProximityGoldConfig(BaseJobConfig):
    """Configuration for pesticide proximity analysis gold processor."""

    name: str = "Pesticide Proximity Analysis Gold"
    dataset: str = "pesticide_proximity"
    type: str = "gold"
    description: str = "Spatial proximity analysis for disaggregated pesticide applications"
    frequency: str = "yearly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Distance thresholds for proximity analysis
    building_proximity_distance_m: float = Field(
        default=100.0,
        description="Distance threshold for building proximity analysis (meters)",
    )
    water_proximity_distance_m: float = Field(
        default=100.0,
        description="Maximum distance for water proximity analysis (meters)",
    )

    # Input datasets
    pesticide_disaggregation_dataset: str = "pesticide_disaggregation"
    agricultural_fields_dataset: str = "fvm_marker" 
    buildings_dataset: str = "bbr_buildings"
    water_typology_dataset: str = "water_typology"

    # Performance settings
    batch_size: int = Field(
        default=1000,
        description="Number of fields to process per batch for memory management",
    )

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class PesticideProximityGold(BaseSource[PesticideProximityGoldConfig], GoldJobInterface):
    """
    Pesticide Proximity Analysis Gold Layer Processor
    
    This processor performs spatial proximity analysis on disaggregated pesticide data,
    identifying nearby buildings and water features for environmental impact assessment.
    """

    def __init__(self, config: PesticideProximityGoldConfig):
        super().__init__(config)
        self.log = logger
        
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Main execution method for pesticide proximity analysis."""
        
        # Initialize DuckDB spatial extension
        self.log.info("🚀 Starting pesticide proximity analysis pipeline...")
        await self._setup_duckdb()
        
        # Load input datasets
        datasets = await self._load_datasets()
        
        # Get available years from disaggregated pesticide data
        years = await self._get_available_years(datasets['disaggregation'])
        self.log.info(f"📅 Found {len(years)} years to process: {years}")
        
        # Process each year
        for year in years:
            self.log.info(f"🔄 Processing proximity analysis for year {year}...")
            
            try:
                # Load year-specific disaggregated data
                await self._load_year_data(year, datasets)
                
                # Perform proximity analysis
                results_count = await self._perform_proximity_analysis(year)
                
                # Save results
                await self._save_year_results(year, results_count)
                
                self.log.info(f"✅ Year {year} completed: {results_count:,} records with proximity data")
                
            except Exception as e:
                self.log.error(f"❌ Year {year} failed: {e}")
                continue
        
        self.log.info("🏁 Pesticide proximity analysis completed successfully!")

    async def _setup_duckdb(self) -> None:
        """Initialize DuckDB with spatial extensions."""
        self.conn.execute('INSTALL spatial')
        self.conn.execute('LOAD spatial')
        self.log.info("✅ DuckDB spatial extension loaded")

    async def _load_datasets(self) -> Dict[str, Optional[str]]:
        """Load required datasets for proximity analysis."""
        datasets = {}
        
        self.log.info("📥 Loading input datasets...")
        
        # Load disaggregated pesticide data
        datasets['disaggregation'] = self._read_gold_data(self.config.pesticide_disaggregation_dataset)
        if not datasets['disaggregation']:
            raise ValueError(f"Pesticide disaggregation dataset not found: {self.config.pesticide_disaggregation_dataset}")
        
        # Load agricultural fields (marker data) 
        datasets['fields'] = self._read_silver_data(self.config.agricultural_fields_dataset)
        if not datasets['fields']:
            raise ValueError(f"Agricultural fields dataset not found: {self.config.agricultural_fields_dataset}")
            
        # Load buildings data
        datasets['buildings'] = self._read_silver_data(self.config.buildings_dataset)
        if not datasets['buildings']:
            self.log.warning(f"Buildings dataset not available: {self.config.buildings_dataset}")
            
        # Load water typology data
        datasets['water'] = self._read_silver_data(self.config.water_typology_dataset) 
        if not datasets['water']:
            self.log.warning(f"Water typology dataset not available: {self.config.water_typology_dataset}")
        
        self.log.info("✅ Input datasets loaded")
        return datasets

    async def _get_available_years(self, disaggregation_table: str) -> list:
        """Get list of available years from disaggregated pesticide data."""
        # Extract year from table name pattern: pesticide_disaggregation_{year}_{year+1}
        query = f"""
        SELECT DISTINCT 
            CAST(SPLIT_PART(table_name, '_', 3) AS INTEGER) as year
        FROM duckdb_tables()
        WHERE table_name LIKE '{disaggregation_table}_%'
        ORDER BY year
        """
        
        result = self.conn.execute(query).fetchall()
        years = [row[0] for row in result if row[0] is not None]
        
        if not years:
            # Fallback: check if there's a main table without year suffix
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if disaggregation_table in table_names:
                self.log.info("Using main disaggregation table (no year suffix)")
                return [2023]  # Default to 2023 if no year-specific tables found
        
        return years

    async def _load_year_data(self, year: int, datasets: Dict[str, str]) -> None:
        """Load data for a specific year."""
        
        # Load disaggregated data for this year
        year_table = f"{datasets['disaggregation']}_{year}_{year + 1}"
        
        try:
            # Check if year-specific table exists
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            
            if year_table in table_names:
                self.conn.execute(f"CREATE OR REPLACE VIEW current_disaggregation AS SELECT * FROM {year_table}")
            else:
                # Fallback to main table with year filter if available
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW current_disaggregation AS 
                    SELECT * FROM {datasets['disaggregation']} 
                    WHERE EXTRACT(year FROM application_date) = {year}
                """)
                
        except Exception as e:
            self.log.error(f"Failed to load disaggregation data for year {year}: {e}")
            raise

    async def _perform_proximity_analysis(self, year: int) -> int:
        """Perform spatial proximity analysis for the current year."""
        
        self.log.info(f"🌍 Performing spatial proximity analysis for year {year}...")
        
        # Get unique fields from current disaggregation data
        unique_fields = self.conn.execute("""
            SELECT COUNT(DISTINCT field_uuid) 
            FROM current_disaggregation 
            WHERE field_uuid IS NOT NULL
        """).fetchone()[0]
        
        self.log.info(f"📊 Processing {unique_fields:,} unique fields with pesticide applications")
        
        if unique_fields == 0:
            self.log.warning("No fields with valid field_uuid found")
            return 0
        
        # Create proximity analysis results table
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE proximity_results AS
            WITH fields_with_geometry AS (
                SELECT DISTINCT
                    cd.field_uuid,
                    ST_Transform(f.geometry, 'EPSG:4326', 'EPSG:25832') as field_geom_utm
                FROM current_disaggregation cd
                JOIN {datasets['fields']} f ON cd.field_uuid = f.field_uuid
                WHERE f.geometry IS NOT NULL
            ),
            residential_proximity AS (
                SELECT 
                    fg.field_uuid,
                    CASE 
                        WHEN COUNT(b.inspire_address) > 0 THEN
                            array_to_string(array_agg(
                                b.inspire_address || ':' || 
                                ROUND(ST_Distance(fg.field_geom_utm, ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')), 1) || 'm' 
                                ORDER BY ST_Distance(fg.field_geom_utm, ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'))
                            ), chr(10))
                        ELSE ''
                    END as residential_buildings_formatted
                FROM fields_with_geometry fg
                LEFT JOIN {datasets['buildings']} b ON ST_DWithin(
                    fg.field_geom_utm,
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                    {self.config.building_proximity_distance_m}
                ) AND b.inspire_category_group = 'residential'
                WHERE b.inspire_address IS NOT NULL
                GROUP BY fg.field_uuid
            ),
            educational_proximity AS (
                SELECT 
                    fg.field_uuid,
                    CASE 
                        WHEN COUNT(b.inspire_address) > 0 THEN
                            array_to_string(array_agg(
                                b.inspire_address || ':' || 
                                ROUND(ST_Distance(fg.field_geom_utm, ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832')), 1) || 'm'
                                ORDER BY ST_Distance(fg.field_geom_utm, ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'))
                            ), chr(10))
                        ELSE ''
                    END as educational_facilities_formatted
                FROM fields_with_geometry fg
                LEFT JOIN {datasets['buildings']} b ON ST_DWithin(
                    fg.field_geom_utm,
                    ST_Transform(b.geometry, 'EPSG:4326', 'EPSG:25832'),
                    {self.config.building_proximity_distance_m}
                ) AND b.inspire_category_group = 'publicServices'
                WHERE b.inspire_address IS NOT NULL
                GROUP BY fg.field_uuid
            ),
            water_proximity AS (
                SELECT 
                    fg.field_uuid,
                    CASE 
                        WHEN MIN(ST_Distance(fg.field_geom_utm, ST_Transform(w.geometry_spatial, 'EPSG:4326', 'EPSG:25832'))) IS NOT NULL
                        THEN ROUND(MIN(ST_Distance(fg.field_geom_utm, ST_Transform(w.geometry_spatial, 'EPSG:4326', 'EPSG:25832'))), 1) || 'm'
                        ELSE ''
                    END as water_distance_formatted
                FROM fields_with_geometry fg
                LEFT JOIN {datasets['water']} w ON ST_DWithin(
                    fg.field_geom_utm,
                    ST_Transform(w.geometry_spatial, 'EPSG:4326', 'EPSG:25832'),
                    {self.config.water_proximity_distance_m}
                )
                WHERE w.geometry_spatial IS NOT NULL
                GROUP BY fg.field_uuid
            )
            SELECT 
                cd.*,
                COALESCE(rp.residential_buildings_formatted, '') as residential_buildings_formatted,
                COALESCE(ep.educational_facilities_formatted, '') as educational_facilities_formatted,
                COALESCE(wp.water_distance_formatted, '') as water_distance_formatted
            FROM current_disaggregation cd
            LEFT JOIN residential_proximity rp ON cd.field_uuid = rp.field_uuid
            LEFT JOIN educational_proximity ep ON cd.field_uuid = ep.field_uuid
            LEFT JOIN water_proximity wp ON cd.field_uuid = wp.field_uuid
        """)
        
        # Get result count
        result_count = self.conn.execute("SELECT COUNT(*) FROM proximity_results").fetchone()[0]
        
        self.log.info(f"✅ Proximity analysis complete: {result_count:,} records with proximity data")
        return result_count

    async def _save_year_results(self, year: int, record_count: int) -> None:
        """Save proximity analysis results for the year."""
        
        output_path = f"gs://{self.config.bucket}/gold/{self.config.dataset}/{year}_{year + 1}"
        self.log.info(f"💾 Saving {record_count:,} proximity records to: {output_path}")
        
        # Export results to GCS
        self.gcs_access.upload_duckdb_table(
            "proximity_results",
            f"{self.config.dataset}/{year}_{year + 1}",
            create_folder_structure=True
        )
        
        self.log.info(f"✅ Year {year} results saved successfully")

    def get_schema_info(self) -> Dict[str, Any]:
        """Return schema information for the proximity analysis output."""
        return {
            "output_columns": [
                "All columns from disaggregated pesticide data",
                "residential_buildings_formatted: VARCHAR (newline-separated addresses with distances)",
                "educational_facilities_formatted: VARCHAR (newline-separated addresses with distances)", 
                "water_distance_formatted: VARCHAR (closest water distance in meters)"
            ],
            "spatial_analysis": {
                "building_radius_m": self.config.building_proximity_distance_m,
                "water_radius_m": self.config.water_proximity_distance_m,
                "coordinate_system": "EPSG:25832 (UTM32N)"
            }
        }
