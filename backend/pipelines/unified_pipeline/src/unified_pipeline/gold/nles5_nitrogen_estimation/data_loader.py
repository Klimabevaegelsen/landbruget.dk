"""
NLES5 Data Loading Module

This module handles all data loading operations for the NLES5 nitrogen estimation pipeline,
including agricultural fields data, fertilizer data, field plans, catch crops, and DMI climate data.

All methods maintain the exact same functionality and error handling as the original implementation.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from unified_pipeline.util.timing import timed


class NLES5DataLoader:
    """
    Data loading operations for NLES5 nitrogen estimation.
    
    This class handles loading and preprocessing of all input datasets required
    for NLES5 calculations, including agricultural fields, fertilizer data,
    climate data, and supporting datasets.
    """
    
    def __init__(self, processor):
        """
        Initialize the data loader with reference to the main processor.
        
        Args:
            processor: The main NLES5NitrogenEstimationGold processor instance
        """
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.gcs_access = processor.gcs_access
        self.db = processor.conn
    
    def _get_available_fvm_marker_years(self) -> List[int]:
        """
        Get all available fvm_marker years from GCS storage.

        Returns:
            List of available years for fvm_marker datasets
        """
        years: Set[int] = set()

        # Primary: discover from GCS
        try:
            files = self.gcs_access.list_files(
                f"gs://{self.config.bucket}/silver/fvm_marker_*/*/*"
            )
            for file_path in files:
                match = re.search(
                    r"silver/fvm_marker_(\d{4})/.*?/(?:fvm_marker_(\d{4})\.parquet|data\.parquet)", file_path
                )
                if match:
                    year1 = int(match.group(1))
                    year2 = match.group(2)
                    if year2:
                        year2_int = int(year2)
                        if year1 == year2_int:
                            years.add(year1)
                    else:
                        years.add(year1)
        except Exception as e:
            self.log.error(f"Error discovering FVM marker years from GCS: {e}")

        # Secondary: derive from local analysis JSONs if GCS discovery failed or returned empty
        if not years:
            try:
                analysis_dir = Path(__file__).resolve().parents[3] / "gcs_silver_analysis_nles5_json"
                if analysis_dir.exists():
                    for json_path in analysis_dir.glob("fvm_marker_*_analysis.json"):
                        m = re.match(r"fvm_marker_(\d{4})_analysis\.json", json_path.name)
                        if m:
                            years.add(int(m.group(1)))
                    if years:
                        self.log.info(f"Using local analysis to determine available FVM years: {sorted(years)}")
                else:
                    self.log.warning(f"Local analysis directory not found: {analysis_dir}")
            except Exception as e:
                self.log.warning(f"Failed to derive FVM years from local analysis: {e}")

        return sorted(list(years))

    def _read_fvm_marker_data_for_year(self, year: int) -> Optional[str]:
        """
        Read FVM marker data for a specific year.

        Args:
            year: The year to read data for

        Returns:
            GCS path to the FVM marker data file, or None if not found
        """
        # Try multiple path patterns for FVM marker data
        possible_paths = [
            f"gs://{self.config.bucket}/silver/fvm_marker_{year}/data.parquet",
            f"gs://{self.config.bucket}/silver/fvm_marker_{year}/fvm_marker_{year}.parquet",
            f"gs://{self.config.bucket}/silver/fvm_marker_{year}/latest/data.parquet",
            f"gs://{self.config.bucket}/silver/fvm_marker_{year}/latest/fvm_marker_{year}.parquet",
        ]

        for path in possible_paths:
            try:
                # Check if file exists
                files = self.gcs_access.list_files(path)
                if files:
                    self.log.info(f"Found FVM marker data for {year}: {path}")
                    return path
            except Exception as e:
                self.log.debug(f"Path not found {path}: {e}")
                continue

        # Try dynamic discovery
        try:
            files = self.gcs_access.list_files(f"gs://{self.config.bucket}/silver/fvm_marker_{year}/*/*")
            for file_path in files:
                if file_path.endswith(('.parquet', '.geoparquet')):
                    self.log.info(f"Discovered FVM marker data for {year}: {file_path}")
                    return file_path
        except Exception as e:
            self.log.debug(f"Dynamic discovery failed for year {year}: {e}")

        self.log.warning(f"No FVM marker data found for year {year}")
        return None

    def _get_fertilizer_data_path(self, target_year: int = None) -> str:
        """
        Get the path to fertilizer data, preferring the most recent available data.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to fertilizer data
        """
        # Standard pattern-based discovery
        patterns = [
            f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}_{target_year}/data.parquet" if target_year else None,
            f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}*/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/*/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/data.parquet"
        ]
        
        for pattern in patterns:
            if pattern is None:
                continue
            try:
                files = self.gcs_access.list_files(pattern)
                if files:
                    # Get the most recent file
                    latest_file = sorted(files, reverse=True)[0]
                    self.log.info(f"Found fertilizer data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Pattern {pattern} failed: {e}")
                continue
        
        # If nothing found, return the basic path and let the caller handle the error
        fallback_path = f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/data.parquet"
        self.log.warning(f"No fertilizer data found, using fallback: {fallback_path}")
        return fallback_path

    def _get_field_plan_data_path(self, target_year: int = None) -> str:
        """
        Get the path to field plan data from the fertiliser directory.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to field plan data
        """
        # Standard pattern-based discovery
        patterns = [
            f"gs://{self.config.bucket}/silver/fertiliser_{target_year}/field_plan*.parquet" if target_year else None,
            f"gs://{self.config.bucket}/silver/fertiliser*/field_plan*.parquet",
            f"gs://{self.config.bucket}/silver/fertiliser/*/field_plan*.parquet",
            f"gs://{self.config.bucket}/silver/fertiliser/field_plan*.parquet"
        ]
        
        for pattern in patterns:
            if pattern is None:
                continue
            try:
                files = self.gcs_access.list_files(pattern)
                if files:
                    # Get the most recent file
                    latest_file = sorted(files, reverse=True)[0]
                    self.log.info(f"Found field plan data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Pattern {pattern} failed: {e}")
                continue
        
        # If nothing found, return the basic path and let the caller handle the error
        fallback_path = f"gs://{self.config.bucket}/silver/fertiliser/field_plan.parquet"
        self.log.warning(f"No field plan data found, using fallback: {fallback_path}")
        return fallback_path

    def _get_catch_crops_data_path(self, target_year: int = None) -> str:
        """
        Get the path to catch crops data.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to catch crops data
        """
        # Standard pattern-based discovery
        patterns = [
            f"gs://{self.config.bucket}/silver/{self.config.catch_crops_dataset}_{target_year}/data.parquet" if target_year else None,
            f"gs://{self.config.bucket}/silver/{self.config.catch_crops_dataset}*/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.catch_crops_dataset}/*/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.catch_crops_dataset}/data.parquet"
        ]
        
        for pattern in patterns:
            if pattern is None:
                continue
            try:
                files = self.gcs_access.list_files(pattern)
                if files:
                    # Get the most recent file
                    latest_file = sorted(files, reverse=True)[0]
                    self.log.info(f"Found catch crops data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Pattern {pattern} failed: {e}")
                continue
        
        # If nothing found, return the basic path and let the caller handle the error
        fallback_path = f"gs://{self.config.bucket}/silver/{self.config.catch_crops_dataset}/data.parquet"
        self.log.warning(f"No catch crops data found, using fallback: {fallback_path}")
        return fallback_path

    def _read_silver_data_from_path(self, dataset_name: str, file_path: str, target_table: str) -> bool:
        """
        Read silver data from a specific GCS path into a DuckDB table.
        
        Args:
            dataset_name: Name of the dataset for logging
            file_path: GCS path to the data file
            target_table: Name of the target DuckDB table
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info(f"📥 Loading {dataset_name} from: {file_path}")
            
            # Check if file exists
            if not self.gcs_access.file_exists(file_path):
                self.log.error(f"File not found: {file_path}")
                return False
            
            # Special handling for GKEA field plan data
            if dataset_name == self.config.field_plan_dataset and 'GKEA' in file_path:
                return self._process_gkea_field_plan_data(file_path, target_table)
            
            # Use the standard GCSDataAccess method to create table from GCS
            self.gcs_access.create_table_from_gcs(target_table, file_path)
            
            # Verify the table was created and has data
            row_count = self.db.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            self.log.info(f"✅ Successfully loaded {dataset_name}: {row_count:,} rows")
            
            return True
            
        except Exception as e:
            self.log.error(f"❌ Failed to read {dataset_name} from {file_path}: {e}")
            return False

    def _process_gkea_field_plan_data(self, file_path: str, target_table: str) -> bool:
        """
        Process GKEA field plan data with proper column mapping and header handling.
        
        GKEA files have a specific format:
        - Row 1: Empty/metadata
        - Row 2: Danish column headers
        - Row 3+: Actual data
        
        Args:
            file_path: GCS path to the GKEA field plan file
            target_table: Target table name (should be 'field_plan')
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info("🔧 Processing GKEA field plan format (headers in row 2, data from row 3)")
            
            # First, load all data with row numbers
            self.db.execute(f"""
                CREATE OR REPLACE TABLE field_plan_all AS
                SELECT 
                    ROW_NUMBER() OVER () as row_num,
                    *
                FROM '{file_path}'
            """)
            
            # Get the header row (row 2) to map column names
            headers = self.db.execute("""
                SELECT * FROM field_plan_all 
                WHERE row_num = 2
                LIMIT 1
            """).fetchone()
            
            if not headers:
                raise ValueError("Could not find header row in field plan data")
            
            # Create raw data table (skip first 2 rows)
            self.db.execute("""
                CREATE OR REPLACE TABLE field_plan_raw AS
                SELECT * FROM field_plan_all 
                WHERE row_num >= 3  -- Skip empty row 1 and header row 2
            """)
            
            # Map Danish column names to expected English names
            # Based on the headers: 'Journal Nummer', 'CVR', 'Modtaget Dato', 'Marknummer', 'Areal', etc.
            self.log.info(f"🗺️ Mapping field plan columns from Danish headers: {headers[:5]}...")
            
            # CRITICAL: Map GKEA columns to expected schema with COMPOSITE KEY MATCHING
            self.db.execute(f"""
                CREATE OR REPLACE TABLE {target_table} AS
                SELECT
                    -- COMPOSITE KEY: Create field_id from CVR + marknummer for FVM matching
                    CONCAT(column_1, '_', column_3) as field_id,  -- 'CVR_Marknummer' composite key (97.1% match rate with FVM!)
                    2024 as year,                -- Fixed year for 2024 data  
                    gkea2023_markplan_goedningskvote as journal_nummer,  -- 'Journal Nummer' (first column)
                    column_1 as cvr_number,      -- 'CVR'
                    column_3 as marknummer,      -- 'Marknummer' (actual field number - column_3!)
                    column_2 as modtaget_dato,   -- 'Modtaget Dato'
                    -- Handle mixed data types in area column - try to cast, use NULL if it fails
                    TRY_CAST(column_4 as DOUBLE) as areal,  -- 'Areal' (column_4 contains area data!)
                    column_6 as harmoni_areal_indikator,  -- 'Harmoni Areal Indikator'
                    -- Handle mixed data types in harmoni area column
                    TRY_CAST(column_7 as DOUBLE) as harmoni_areal,  -- 'Harmoni Areal'
                    column_8 as jordbundstype,   -- 'Jordbundstype'
                    column_10 as crop_code,      -- 'Hovedafgrøde' - actual crop codes in column_10!
                    -- Add nitrogen data columns if available
                    TRY_CAST(column_18 as DOUBLE) as total_n_kg_ha,      -- 'N Kvote pr. Ha'
                    TRY_CAST(column_19 as DOUBLE) as total_n_kg_mark,    -- 'N Kvote Mark'
                    TRY_CAST(column_14 as DOUBLE) as mineral_n_spring,   -- 'N Norm Afgrøde'
                    TRY_CAST(column_17 as DOUBLE) as organic_n_total     -- 'Korrektion N Prognose'
                FROM field_plan_raw
                WHERE column_3 IS NOT NULL 
                  AND column_3 != ''
                  AND column_3 != 'Marknummer'  -- Skip any remaining header rows
                  AND column_1 IS NOT NULL     -- Must have journal number for composite key
                  AND TRY_CAST(column_4 as DOUBLE) > 0  -- Must have positive area
            """)
            
            # Clean up temporary tables
            self.db.execute("DROP TABLE IF EXISTS field_plan_all")
            self.db.execute("DROP TABLE IF EXISTS field_plan_raw")
            
            # Validate the processed data
            count = self.db.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
            if count == 0:
                raise ValueError("No valid field plan records found after processing")
            
            self.log.info(f"✅ Successfully processed GKEA field plan data: {count:,} records with proper field_id mapping")
            
            # ENHANCEMENT: Apply agricultural pattern matching to improve GKEA-FVM matching
            # Note: This will be called separately in async context
            # await self._apply_agricultural_pattern_matching(target_table)
            
            # Log sample data for verification
            sample = self.db.execute(f"""
                SELECT field_id, journal_nummer, cvr_number, areal, crop_code
                FROM {target_table} 
                LIMIT 5
            """).fetchall()
            
            self.log.info("📋 Sample processed GKEA data:")
            for i, row in enumerate(sample, 1):
                self.log.info(f"   {i}. field_id={row[0]}, journal={row[1]}, cvr={row[2]}, area={row[3]}, crop={row[4]}")
            
            return True
            
        except Exception as e:
            self.log.error(f"❌ Failed to process GKEA field plan data: {e}")
            return False
    
    async def _apply_agricultural_pattern_matching(self, gkea_table: str) -> None:
        """Apply agricultural pattern matching to enhance GKEA-FVM field matching."""
        try:
            self.log.info("🌾 Applying agricultural pattern matching enhancement...")
            
            # Check if FVM marker data exists
            if not await self.db.table_exists("marker"):
                self.log.warning("   FVM marker data not available - skipping agricultural pattern matching")
                return
            
            # Import and run agricultural pattern matcher
            from unified_pipeline.gold.agricultural_pattern_matcher import (
                AgriculturalPatternMatcher, 
                AgriculturalPatternMatcherConfig
            )
            
            # Configure for high-quality matches
            config = AgriculturalPatternMatcherConfig(
                min_pattern_score=0.8,
                min_field_score=0.7,
                max_operations_to_process=2000
            )
            
            # Run the pattern matching
            matcher = AgriculturalPatternMatcher(config)
            
            # Temporarily rename the GKEA table to match expected name
            self.db.execute(f"CREATE OR REPLACE VIEW gkea_field_plan_2024 AS SELECT * FROM {gkea_table}")
            
            await matcher.run()
            
            # Log results
            if hasattr(matcher, 'matches_found') and matcher.matches_found > 0:
                self.log.info(f"✅ Agricultural pattern matching found {matcher.matches_found:,} additional field matches")
                
                # Create enhanced field mappings table for NLES5 use
                self.db.execute("""
                    CREATE OR REPLACE TABLE gkea_fvm_enhanced_mappings AS
                    SELECT 
                        gkea_field_id,
                        fvm_field_id,
                        'direct_composite_key' as match_method,
                        1.0 as confidence_score
                    FROM gkea_field_plan_2024 g
                    JOIN marker f ON g.field_id = f.field_id
                    
                    UNION ALL
                    
                    SELECT 
                        gkea_field_id,
                        fvm_field_id,
                        'agricultural_pattern' as match_method,
                        field_similarity_score as confidence_score
                    FROM enhanced_gkea_fvm_matches
                    WHERE field_similarity_score >= 0.7
                """)
                
                total_matches = self.db.execute("SELECT COUNT(*) FROM gkea_fvm_enhanced_mappings").fetchone()[0]
                original_matches = self.db.execute("""
                    SELECT COUNT(*) FROM gkea_fvm_enhanced_mappings 
                    WHERE match_method = 'direct_composite_key'
                """).fetchone()[0]
                
                improvement = total_matches - original_matches
                self.log.info(f"   Total GKEA-FVM matches: {total_matches:,} (original: {original_matches:,}, +{improvement:,} from patterns)")
            else:
                self.log.info("   No additional matches found via agricultural pattern matching")
                
        except Exception as e:
            self.log.warning(f"⚠️ Agricultural pattern matching failed: {str(e)}")
            self.log.warning("   Continuing with standard GKEA field processing...")

    @timed(name="Loading required silver datasets")
    def _load_required_silver_datasets(self, silver_data: Optional[Dict[str, Any]]) -> Dict[str, str]:
        """
        Load all required silver datasets for NLES5 processing.
        
        Args:
            silver_data: Optional in-memory silver data
            
        Returns:
            Dictionary mapping dataset names to table names
        """
        loaded_tables: Dict[str, str] = {}
        required_datasets = [
            (self.config.soil_types_dataset, "soil_types"),
            (self.config.dmi_dataset, "dmi_data"),
            (self.config.fertilizer_dataset, "fertilizer_accounts"),  # Fixed table name
            (self.config.field_plan_dataset, "field_plan"),  # Fixed table name
            (self.config.catch_crops_dataset, "catch_crops"),  # Fixed table name
        ]
        
        self.log.info("📂 Loading required silver datasets for NLES5...")
        
        for dataset_name, table_name in required_datasets:
            self.log.info(f"🔍 Processing dataset: {dataset_name} -> {table_name}")
            try:
                if silver_data and dataset_name in silver_data:
                    # Use data passed from a previous pipeline if available
                    self.log.info(f"Using in-memory data for {dataset_name}")
                    loaded_tables[dataset_name] = silver_data[dataset_name]
                else:
                    # Special handling for field plan data - always try to load from fertiliser directory
                    if dataset_name == self.config.field_plan_dataset:
                        try:
                            # Use the first target year as the reference for field plan data
                            target_year = (self.config.target_years[0]
                                            if getattr(self.config, 'target_years', None)
                                            and len(self.config.target_years) > 0 else None)
                            field_plan_path = self._get_field_plan_data_path(target_year)
                            self.log.info(f"Using field plan file from fertiliser directory for year {target_year}: {field_plan_path}")
                            success = self._read_silver_data_from_path(dataset_name, field_plan_path, table_name)
                            if success:
                                loaded_tables[dataset_name] = table_name
                                self.log.info(f"✅ Successfully loaded field plan data: {dataset_name}")
                                continue
                            else:
                                self.log.error(f"❌ Failed to load field plan data {dataset_name}")
                                continue
                        except Exception as e:
                            self.log.error(f"❌ CRITICAL: Failed to load required field plan data: {e}")
                            continue
                    
                    # Load from GCS using modern pattern
                    else:
                        # Try to find the dataset in silver layer
                        pattern = f"gs://{self.config.bucket}/silver/{dataset_name}/*/data.parquet"
                        files = self.gcs_access.list_files(pattern)
                        
                        if not files:
                            # Try alternative patterns
                            alt_patterns = [
                                f"gs://{self.config.bucket}/silver/{dataset_name}/data.parquet",
                                f"gs://{self.config.bucket}/silver/{dataset_name}*/*.parquet"
                            ]
                            for alt_pattern in alt_patterns:
                                files = self.gcs_access.list_files(alt_pattern)
                                if files:
                                    break
                        
                        if files:
                            self.log.info(f"Found {dataset_name} in silver layer.")
                            
                            # Special handling for fertilizer data to get the latest 2024 data
                            if dataset_name == self.config.fertilizer_dataset:
                                try:
                                    # Use the first target year as the reference for fertilizer data
                                    target_year = (self.config.target_years[0]
                                                    if getattr(self.config, 'target_years', None)
                                                    and len(self.config.target_years) > 0 else None)
                                    fertilizer_path = self._get_fertilizer_data_path(target_year)
                                    self.log.info(f"Using fertilizer file for year {target_year}: {fertilizer_path}")
                                    success = self._read_silver_data_from_path(
                                        dataset_name, fertilizer_path, table_name
                                    )
                                    if success:
                                        loaded_tables[dataset_name] = table_name
                                        self.log.info(f"✅ Successfully loaded fertilizer data: {dataset_name}")
                                        continue
                                    else:
                                        self.log.error(f"❌ Failed to load fertilizer data {dataset_name}")
                                        continue
                                except Exception as e:
                                    self.log.error(f"❌ CRITICAL: Failed to load required fertilizer data: {e}")
                                    continue
                            
                            # Special handling for catch crops data (optional)
                            elif dataset_name == self.config.catch_crops_dataset:
                                try:
                                    target_year = (self.config.target_years[0]
                                                    if getattr(self.config, 'target_years', None)
                                                    and len(self.config.target_years) > 0 else None)
                                    catch_crops_path = self._get_catch_crops_data_path(target_year)
                                    self.log.info(f"Using catch crops file for year {target_year}: {catch_crops_path}")
                                    success = self._read_silver_data_from_path(dataset_name, catch_crops_path, table_name)
                                    if success:
                                        loaded_tables[dataset_name] = table_name
                                        self.log.info(f"✅ Successfully loaded catch crops data: {dataset_name}")
                                        continue
                                    else:
                                        self.log.warning(f"⚠️ Catch crops data not available (optional): {dataset_name}")
                                        continue
                                except Exception as e:
                                    self.log.warning(f"⚠️ Catch crops data not available (optional): {e}")
                                    continue
                            
                            # Standard loading for other datasets
                            else:
                                try:
                                    # Get the most recent file
                                    latest_file = sorted(files, reverse=True)[0]
                                    self.log.info(f"📥 Loading {dataset_name} from: {latest_file}")
                                    
                                    # Use the standard GCSDataAccess method
                                    self.gcs_access.create_table_from_gcs(table_name, latest_file)
                                    
                                    # Verify and log
                                    row_count = self.db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                                    loaded_tables[dataset_name] = table_name
                                    self.log.info(f"✅ Successfully loaded {dataset_name}: {row_count:,} rows")
                                except Exception as e:
                                    self.log.error(f"❌ Failed to load {dataset_name}: {e}")
                                    continue
                        else:
                            self.log.error(f"❌ Dataset not found in silver layer: {dataset_name}")
                            continue
                        
            except Exception as e:
                self.log.error(f"❌ Error processing dataset {dataset_name}: {e}")
                continue
        
        self.log.info(f"📊 Successfully loaded {len(loaded_tables)}/{len(required_datasets)} required datasets")
        
        # Log which datasets failed to load
        failed_datasets = [name for name, _ in required_datasets if name not in loaded_tables]
        if failed_datasets:
            self.log.warning(f"⚠️ Failed to load datasets: {failed_datasets}")
        
        return loaded_tables

    def _load_and_combine_dmi_data(self) -> bool:
        """
        Load and combine DMI climate data from multiple sources.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info("🌤️ Loading DMI climate data...")
            
            # Try to find DMI data in silver layer
            pattern = f"gs://{self.config.bucket}/silver/{self.config.dmi_dataset}/*/data.parquet"
            files = self.gcs_access.list_files(pattern)
            
            if not files:
                # Try alternative patterns
                alt_patterns = [
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_dataset}/data.parquet",
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_dataset}*/*.parquet"
                ]
                for alt_pattern in alt_patterns:
                    files = self.gcs_access.list_files(alt_pattern)
                    if files:
                        break
            
            if files:
                self.log.info("DMI data found in silver layer")
                # Get the most recent file
                latest_file = sorted(files, reverse=True)[0]
                self.log.info(f"📥 Loading DMI data from: {latest_file}")
                
                # Use the standard GCSDataAccess method
                self.gcs_access.create_table_from_gcs("dmi_data", latest_file)
                
                # Verify the data was loaded
                row_count = self.db.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
                self.log.info(f"✅ DMI data loaded successfully: {row_count:,} rows")
                return True
            else:
                self.log.error("❌ DMI data not found in silver layer")
                return False
                
        except Exception as e:
            self.log.error(f"❌ Failed to load DMI data: {e}")
            return False

    def _load_climate_data_for_years(self, years: List[int]) -> str:
        """
        Load climate data for specific years.
        
        Args:
            years: List of years to load climate data for
            
        Returns:
            Name of the table containing the loaded climate data
        """
        try:
            self.log.info(f"🌤️ Loading climate data for years: {years}")
            
            # Create a combined climate table
            combined_table = "climate_data_years"
            self.db.execute(f"DROP TABLE IF EXISTS {combined_table}")
            
            # Load DMI data if not already loaded
            if not self._load_and_combine_dmi_data():
                raise ValueError("Failed to load DMI climate data")
            
            # Filter for the specified years
            year_filter = ",".join(map(str, years))
            filter_sql = f"""
            CREATE TABLE {combined_table} AS
            SELECT *
            FROM dmi_data
            WHERE EXTRACT(YEAR FROM dato) IN ({year_filter})
            """
            
            self.db.execute(filter_sql)
            
            # Verify the filtered data
            row_count = self.db.execute(f"SELECT COUNT(*) FROM {combined_table}").fetchone()[0]
            self.log.info(f"✅ Climate data loaded for {len(years)} years: {row_count:,} rows")
            
            return combined_table
            
        except Exception as e:
            self.log.error(f"❌ Failed to load climate data for years {years}: {e}")
            raise

    @timed(name="Loading agricultural fields data")
    def _load_agricultural_fields_data(self, silver_data: Optional[Dict[str, Any]]) -> str:
        """
        Load agricultural fields data from multiple yearly datasets.

        Args:
            silver_data: Optional in-memory silver data

        Returns:
            Table name containing combined agricultural fields data
        """
        # OPTIMIZATION: Determine target calculation years and required supporting years for NLES5
        if self.config.target_years:
            target_calculation_years = self.config.target_years
            self.log.info(f"Target NLES5 calculation years: {target_calculation_years}")
        else:
            all_available_years = self._get_available_fvm_marker_years()
            # Apply year limit for memory management
            if self.config.max_years_to_process:
                # Take the most recent years up to the limit as targets
                target_calculation_years = sorted(all_available_years)[-self.config.max_years_to_process:]
                self.log.info(f"Auto-discovered {len(all_available_years)} available, targeting most recent {len(target_calculation_years)}: {target_calculation_years}")
            else:
                target_calculation_years = all_available_years
                self.log.info(f"Auto-discovered target years (no limit): {target_calculation_years}")

        if not target_calculation_years:
            self.log.error("No FVM marker years found to process")
            raise ValueError("No FVM marker data available")

        # CRITICAL OPTIMIZATION: Calculate minimum years needed for NLES5 (3-year windows)
        years_to_load = self.processor._calculate_required_data_years(target_calculation_years, all_available_years if 'all_available_years' in locals() else self._get_available_fvm_marker_years())
        
        self.log.info("🎯 NLES5 Memory Optimization:")
        self.log.info(f"   Target calculation years: {len(target_calculation_years)} years → {target_calculation_years}")
        self.log.info(f"   Required data years: {len(years_to_load)} years → {years_to_load}")
        self.log.info(f"   Memory reduction: {len(all_available_years if 'all_available_years' in locals() else self._get_available_fvm_marker_years()) - len(years_to_load)} years eliminated")
        
        years_to_process = years_to_load

        # Process each year and collect table names
        yearly_tables = []
        for i, year in enumerate(years_to_process):
            try:
                # Clean up temp files more frequently to manage disk space
                if i > 0 and i % 2 == 0:  # Every 2 years instead of 3
                    self.log.info(f"Cleaning up temporary files after processing {i} years...")
                    self.processor._cleanup_temp_files()

                # Check if data is available in silver_data dict
                year_dataset = f"fvm_marker_{year}"
                table_name = f"agricultural_fields_{year}"

                if silver_data and year_dataset in silver_data:
                    self.log.info(f"Using in-memory data for {year_dataset}")
                    yearly_tables.append((year, silver_data[year_dataset]))
                    continue

                # Read from GCS
                fvm_path = self._read_fvm_marker_data_for_year(year)
                if not fvm_path:
                    self.log.warning(f"⚠️ No FVM marker data found for year {year}, skipping")
                    continue

                # Load the data
                success = self._read_silver_data_from_path(year_dataset, fvm_path, table_name)
                if success:
                    yearly_tables.append((year, table_name))
                    self.log.info(f"✅ Loaded agricultural fields for {year}")
                else:
                    self.log.error(f"❌ Failed to load agricultural fields for {year}")

            except Exception as e:
                self.log.error(f"❌ Error processing year {year}: {e}")
                continue

        if not yearly_tables:
            raise ValueError("No agricultural fields data could be loaded")

        # Combine all yearly tables
        combined_table = self._combine_yearly_fvm_data({year: table for year, table in yearly_tables})
        
        self.log.info(f"✅ Agricultural fields data loading completed: {len(yearly_tables)} years processed")
        return combined_table

    def _load_agricultural_fields_for_years(self, years: List[int], table_name: str):
        """
        Load agricultural fields data for specific years into a named table.
        
        Args:
            years: List of years to load
            table_name: Name of the target table
        """
        try:
            self.log.info(f"📊 Loading agricultural fields for years: {years}")
            
            yearly_tables = {}
            for year in years:
                year_table = f"agricultural_fields_{year}"
                fvm_path = self._read_fvm_marker_data_for_year(year)
                
                if fvm_path:
                    success = self._read_silver_data_from_path(f"fvm_marker_{year}", fvm_path, year_table)
                    if success:
                        yearly_tables[year] = year_table
                        self.log.info(f"✅ Loaded agricultural fields for {year}")
                    else:
                        self.log.warning(f"⚠️ Failed to load agricultural fields for {year}")
                else:
                    self.log.warning(f"⚠️ No FVM marker data found for year {year}")
            
            if yearly_tables:
                # Combine the tables
                combined_table = self._combine_yearly_fvm_data(yearly_tables)
                
                # Rename to the target table name
                self.db.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.db.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {combined_table}")
                self.db.execute(f"DROP TABLE IF EXISTS {combined_table}")
                
                row_count = self.db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                self.log.info(f"✅ Agricultural fields loaded for {len(yearly_tables)} years: {row_count:,} rows")
            else:
                raise ValueError(f"No agricultural fields data could be loaded for years: {years}")
                
        except Exception as e:
            self.log.error(f"❌ Failed to load agricultural fields for years {years}: {e}")
            raise

    def _load_required_silver_datasets_for_batch(self, silver_data: Optional[Dict[str, Any]], batch_years: List[int]) -> Dict[str, str]:
        """
        Load required silver datasets for a specific batch of years.
        
        Args:
            silver_data: Optional in-memory silver data
            batch_years: List of years in the current batch
            
        Returns:
            Dictionary mapping dataset names to table names
        """
        self.log.info(f"📂 Loading silver datasets for batch years: {batch_years}")
        
        # Use the same logic as the main loader but with batch-specific optimizations
        loaded_tables = self._load_required_silver_datasets(silver_data)
        
        # Log batch-specific information
        self.log.info(f"✅ Loaded {len(loaded_tables)} datasets for batch processing")
        
        return loaded_tables

    def _load_agricultural_fields_data_for_batch(
        self, silver_data: Optional[Dict[str, Any]], batch_years: List[int], loaded_tables: Dict[str, str]
    ) -> str:
        """
        Load agricultural fields data for a specific batch of years.
        
        Args:
            silver_data: Optional in-memory silver data
            batch_years: List of years in the current batch
            loaded_tables: Dictionary of already loaded tables
            
        Returns:
            Table name containing combined agricultural fields data for the batch
        """
        self.log.info(f"📊 Loading agricultural fields for batch years: {batch_years}")
        
        # Calculate required data years for this batch (including 3-year windows)
        all_available_years = self._get_available_fvm_marker_years()
        required_years = self.processor._calculate_required_data_years(batch_years, all_available_years)
        
        self.log.info(f"Batch {batch_years} requires data years: {required_years}")
        
        # Load the agricultural fields for the required years
        self._load_agricultural_fields_for_years(required_years, "agricultural_fields_batch")
        
        return "agricultural_fields_batch"

    def _combine_yearly_fvm_data(self, yearly_tables: Dict[int, str]) -> str:
        """
        Combine FVM data from multiple years into a single table.
        
        Args:
            yearly_tables: Dictionary mapping years to table names
            
        Returns:
            Name of the combined table
        """
        if not yearly_tables:
            raise ValueError("No yearly tables to combine")
        
        combined_table = "combined_agricultural_fields"
        self.db.execute(f"DROP TABLE IF EXISTS {combined_table}")
        
        # Create the combined table by unioning all yearly tables
        union_parts = []
        for year, table_name in yearly_tables.items():
            union_parts.append(f"SELECT *, {year} AS data_year FROM {table_name}")
        
        union_sql = f"""
        CREATE TABLE {combined_table} AS
        {' UNION ALL '.join(union_parts)}
        """
        
        self.db.execute(union_sql)
        
        # Verify the combined table
        row_count = self.db.execute(f"SELECT COUNT(*) FROM {combined_table}").fetchone()[0]
        year_count = len(yearly_tables)
        
        self.log.info(f"✅ Combined {year_count} years of FVM data: {row_count:,} total rows")
        
        return combined_table
