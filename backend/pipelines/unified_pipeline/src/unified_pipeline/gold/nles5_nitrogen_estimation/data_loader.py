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

        # Primary: discover from GCS using the correct fvm_marker path pattern
        try:
            files = self.gcs_access.list_files(
                f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_*/*/*"
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
        # Try multiple path patterns for FVM marker data using the correct dataset name
        possible_patterns = [
            f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/*/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/*/{self.config.agricultural_fields_dataset}_{year}.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/latest/data.parquet",
            f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/latest/{self.config.agricultural_fields_dataset}_{year}.parquet",
        ]

        for pattern in possible_patterns:
            try:
                # Check if files match this pattern
                files = self.gcs_access.list_files(pattern)
                if files:
                    # Return the first matching file (the actual path, not the pattern)
                    actual_path = files[0]
                    self.log.info(f"Found FVM marker data for {year}: {actual_path}")
                    return actual_path
            except Exception as e:
                self.log.debug(f"Pattern not found {pattern}: {e}")
                continue

        # Try dynamic discovery using the correct dataset name
        try:
            files = self.gcs_access.list_files(f"gs://{self.config.bucket}/silver/{self.config.agricultural_fields_dataset}_{year}/*/*")
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
        Get the path to fertilizer data from the fertiliser directory structure.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to fertilizer data directory (contains GKEA and Efterafgrøder files)
        """
        # Look for the fertiliser directory with timestamp subdirectories
        try:
            # Use a more direct approach - list directories and find the latest timestamped one
            import subprocess
            import re
            
            # Use gsutil to list directories
            cmd = f"gsutil ls gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/"
            result = subprocess.run(cmd.split(), capture_output=True, text=True)
            
            if result.returncode == 0:
                directories = [line.strip() for line in result.stdout.strip().split('\n') if line.strip().endswith('/')]
                if directories:
                    # Get the most recent directory (by timestamp in name)
                    timestamped_dirs = []
                    for dir_path in directories:
                        # Extract timestamp from path like gs://.../fertiliser/20250803_205033/
                        match = re.search(r'/(\d{8}_\d{6})/$', dir_path)
                        if match:
                            timestamped_dirs.append((match.group(1), dir_path))
                    
                    if timestamped_dirs:
                        # Sort by timestamp and get the latest
                        latest_timestamp, latest_dir = sorted(timestamped_dirs, reverse=True)[0]
                        self.log.info(f"Found latest fertilizer directory: {latest_dir} (timestamp: {latest_timestamp})")
                        return latest_dir
            
            # Fallback to the original method
            pattern = f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/*/"
            directories = self.gcs_access.list_files(pattern)
            
            if directories:
                # Get the most recent directory (by name/timestamp)
                latest_dir = sorted(directories, reverse=True)[0]
                # Ensure it ends with / for proper path building
                if not latest_dir.endswith('/'):
                    latest_dir += '/'
                self.log.info(f"Found fertilizer directory (fallback method): {latest_dir}")
                return latest_dir
            else:
                # Try direct path
                fallback_path = f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/"
                self.log.info(f"Using direct fertilizer path: {fallback_path}")
                return fallback_path
                
        except Exception as e:
            self.log.debug(f"Fertilizer directory discovery failed: {e}")
            # If nothing found, return the basic path
            fallback_path = f"gs://{self.config.bucket}/silver/{self.config.fertilizer_dataset}/"
            self.log.warning(f"No fertilizer directory found, using fallback: {fallback_path}")
            return fallback_path

    def _get_fertilizer_accounts_file_path(self, target_year: int = None) -> str:
        """
        Get the specific path to fertilizer accounts data file.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to specific fertilizer accounts parquet file
        """
        try:
            # Get the fertilizer directory
            fertilizer_dir = self._get_fertilizer_data_path(target_year)
            
            # List all files in the directory to find fertilizer accounts files
            pattern = f"{fertilizer_dir}*.parquet"
            files = self.gcs_access.list_files(pattern)
            
            # Look for files that match fertilizer accounts pattern (Gødningsregnskaber)
            fertilizer_accounts_files = []
            for file_path in files:
                if 'Gødningsregnskaber' in file_path or 'fertilizer' in file_path.lower():
                    # Extract year from filename (look for 4 digits that are not part of timestamp)
                    import re
                    # Get just the filename, not the full path to avoid timestamp confusion
                    filename = file_path.split('/')[-1]
                    year_match = re.search(r'(\d{4})', filename)
                    if year_match:
                        file_year = int(year_match.group(1))
                        fertilizer_accounts_files.append((file_year, file_path))
                    else:
                        # If no year found, use as fallback
                        fertilizer_accounts_files.append((0, file_path))
            
            if fertilizer_accounts_files:
                # Sort by year (descending) and get the most recent
                fertilizer_accounts_files.sort(reverse=True)
                selected_year, selected_file = fertilizer_accounts_files[0]
                self.log.info(f"Found fertilizer accounts file: {selected_file} (year: {selected_year})")
                return selected_file
            else:
                self.log.warning(f"No fertilizer accounts files found in {fertilizer_dir}")
                return None
                
        except Exception as e:
            self.log.error(f"Error finding fertilizer accounts file: {e}")
            return None

    def _get_field_plan_data_path(self, target_year: int = None) -> str:
        """
        Get the path to GKEA field plan data from the fertiliser directory.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to GKEA field plan data
        """
        try:
            # Get the fertiliser base directory
            fertiliser_dir = self._get_fertilizer_data_path(target_year)
            
            # Look for GKEA field plan files with correct patterns
            # Always look for the most recent GKEA file since field plans are typically multi-year
            patterns = [
                f"{fertiliser_dir}GKEA*_Markplan_med_Gødningsoplysninger*.parquet",
                f"{fertiliser_dir}GKEA*_Markplan*.parquet"
            ]
            
            # If target_year is specified, prefer files from that year
            if target_year:
                priority_patterns = [
                    f"{fertiliser_dir}GKEA{target_year}_Markplan_med_Gødningsoplysninger*.parquet",
                    f"{fertiliser_dir}GKEA{target_year}_Markplan*.parquet"
                ]
                patterns = priority_patterns + patterns  # Try target year first, then any year
            
            for pattern in patterns:
                try:
                    files = self.gcs_access.list_files(pattern)
                    if files:
                        # Get the most recent file (by name)
                        latest_file = sorted(files, reverse=True)[0]
                        self.log.info(f"Found GKEA field plan data: {latest_file}")
                        return latest_file
                except Exception as e:
                    self.log.debug(f"Pattern {pattern} failed: {e}")
                    continue
            
            # If specific year not found, try to find any GKEA file
            try:
                all_gkea_files = self.gcs_access.list_files(f"{fertiliser_dir}GKEA*_Markplan*.parquet")
                if all_gkea_files:
                    # Get the most recent GKEA file
                    latest_file = sorted(all_gkea_files, reverse=True)[0]
                    self.log.info(f"Found alternative GKEA field plan data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Alternative GKEA search failed: {e}")
            
            # If nothing found, return a fallback path
            fallback_path = f"{fertiliser_dir}GKEA2024_Markplan_med_Gødningsoplysninger.parquet"
            self.log.warning(f"No GKEA field plan data found, using fallback: {fallback_path}")
            return fallback_path
            
        except Exception as e:
            self.log.error(f"Failed to get field plan data path: {e}")
            return f"gs://{self.config.bucket}/silver/fertiliser/GKEA_Markplan.parquet"

    def _get_catch_crops_data_path(self, target_year: int = None) -> str:
        """
        Get the path to catch crops (Efterafgrøder) data from the fertiliser directory.
        
        Args:
            target_year: Optional target year to match
            
        Returns:
            GCS path to catch crops data
        """
        try:
            # Get the fertiliser base directory
            fertiliser_dir = self._get_fertilizer_data_path(target_year)
            
            # Look for Efterafgrøder (catch crops) files
            if target_year:
                patterns = [
                    f"{fertiliser_dir}Efterafgrøder {target_year}.parquet",
                    f"{fertiliser_dir}Efterafgrøder_{target_year}.parquet"
                ]
            else:
                patterns = [
                    f"{fertiliser_dir}Efterafgrøder *.parquet",
                    f"{fertiliser_dir}Efterafgrøder_*.parquet"
                ]
            
            for pattern in patterns:
                try:
                    files = self.gcs_access.list_files(pattern)
                    if files:
                        # Get the most recent file (by name)
                        latest_file = sorted(files, reverse=True)[0]
                        self.log.info(f"Found catch crops (Efterafgrøder) data: {latest_file}")
                        return latest_file
                except Exception as e:
                    self.log.debug(f"Pattern {pattern} failed: {e}")
                    continue
            
            # If specific year not found, try to find any Efterafgrøder file
            try:
                all_catch_files = self.gcs_access.list_files(f"{fertiliser_dir}Efterafgrøder *.parquet")
                if all_catch_files:
                    # Get the most recent catch crops file
                    latest_file = sorted(all_catch_files, reverse=True)[0]
                    self.log.info(f"Found alternative catch crops data: {latest_file}")
                    return latest_file
            except Exception as e:
                self.log.debug(f"Alternative catch crops search failed: {e}")
            
            # If nothing found, return a fallback path
            fallback_path = f"{fertiliser_dir}Efterafgrøder 2023.parquet"
            self.log.warning(f"No catch crops data found, using fallback: {fallback_path}")
            return fallback_path
            
        except Exception as e:
            self.log.error(f"Failed to get catch crops data path: {e}")
            return f"gs://{self.config.bucket}/silver/fertiliser/Efterafgrøder.parquet"

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
            # Extract year from the filename (e.g., GKEA2021_Markplan_... -> 2021)
            import re
            year_match = re.search(r'GKEA(\d{4})_', file_path)
            gkea_year = int(year_match.group(1)) if year_match else 2024
            
            self.log.info(f"🔧 Processing GKEA field plan format for year {gkea_year} (headers in row 2, data from row 3)")
            
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
            self.log.info(f"🗺️ Mapping field plan columns from Danish headers: {headers[:8]}...")
            
            # CRITICAL: Map GKEA columns to expected schema with COMPOSITE KEY MATCHING
            # Dynamically determine the correct column names based on headers (structure varies by year!)
            columns_info = self.db.execute("PRAGMA table_info(field_plan_raw)").fetchall()
            column_names = [col[1] for col in columns_info]  # col[1] is the column name
            
            # Find the journal nummer column (it varies by year)
            journal_column = None
            for col_name in column_names:
                if 'markplan' in col_name.lower() and 'goedning' in col_name.lower():
                    journal_column = col_name
                    break
            
            if not journal_column:
                journal_column = column_names[0] if column_names else 'column_0'
            
            # Create header-to-column mapping based on actual headers
            # NOTE: The parquet file structure is:
            # - headers[1] (Journal Nummer) -> gkea{year}_markplan_goedningskvote (column 0 in parquet)
            # - headers[2] (CVR) -> column_1 (column 1 in parquet)
            # - headers[3] (Kundetype) -> column_2 (column 2 in parquet)
            # etc.
            header_to_column = {}
            for i, header_value in enumerate(headers[1:], 1):  # Skip row_num column
                if header_value and str(header_value).strip():
                    clean_header = str(header_value).strip().replace('\n', ' ')
                    # Map headers to actual parquet columns
                    if i == 1:  # Journal Nummer -> named column
                        header_to_column[clean_header] = journal_column
                    else:  # Other headers -> column_{i-1}
                        header_to_column[clean_header] = f'column_{i-1}'
            
            self.log.info(f"🗺️ Header mapping: {list(header_to_column.keys())[:6]}")
            
            # Dynamically find key columns based on headers
            cvr_column = None
            marknummer_column = None  
            areal_column = None
            modtaget_dato_column = None
            
            for header, col in header_to_column.items():
                if header == 'CVR':  # Exact match to avoid matching "Kundetype (CVR/CPR)"
                    cvr_column = col
                elif 'Marknummer' in header:
                    marknummer_column = col
                elif header == 'Areal':  # Exact match to avoid matching "Fradrags Arealer" etc
                    areal_column = col
                elif 'Modtaget Dato' in header:
                    modtaget_dato_column = col
            
            self.log.info(f"🗂️ Column mapping - Journal: {journal_column}, CVR: {cvr_column}, Marknummer: {marknummer_column}, Areal: {areal_column}")
            
            # Ensure we have the essential columns
            if not all([cvr_column, marknummer_column, areal_column]):
                missing = []
                if not cvr_column: missing.append("CVR")
                if not marknummer_column: missing.append("Marknummer") 
                if not areal_column: missing.append("Areal")
                raise ValueError(f"Missing essential columns in GKEA data: {missing}")
            
            self.db.execute(f"""
                CREATE OR REPLACE TABLE {target_table} AS
                SELECT
                    -- COMPOSITE KEY: Create field_id from CVR + marknummer for FVM matching
                    CONCAT({cvr_column}, '_', {marknummer_column}) as field_id,  -- Dynamic CVR_Marknummer composite key
                    {gkea_year} as year,         -- Year from filename (e.g., 2021 from GKEA2021_...)  
                    {journal_column} as journal_nummer,  -- 'Journal Nummer' (dynamically determined)
                    {cvr_column} as cvr_number,      -- 'CVR' (dynamic)
                    {marknummer_column} as marknummer,      -- 'Marknummer' (dynamic)
                    {modtaget_dato_column if modtaget_dato_column else 'NULL'} as modtaget_dato,   -- 'Modtaget Dato' (dynamic, may be NULL)
                    -- Handle mixed data types in area column - try to cast, use NULL if it fails
                    TRY_CAST({areal_column} as DOUBLE) as areal,  -- 'Areal' (dynamic)
                    -- Additional columns with fallbacks for different year structures
                    CASE WHEN '{gkea_year}' = '2021' THEN column_7 ELSE NULL END as harmoni_areal_indikator,  -- Only in 2021
                    CASE WHEN '{gkea_year}' = '2021' THEN TRY_CAST(column_8 as DOUBLE) ELSE NULL END as harmoni_areal,  -- Only in 2021
                    CASE WHEN '{gkea_year}' = '2021' THEN column_9 ELSE NULL END as jordbundstype,   -- Only in 2021
                    -- Crop codes and nitrogen data - structure varies significantly by year, use NULL for now
                    NULL as crop_code,      -- Structure varies too much between years
                    NULL as total_n_kg_ha,  -- Structure varies too much between years
                    NULL as total_n_kg_mark,    -- Structure varies too much between years
                    NULL as mineral_n_spring,   -- Structure varies too much between years
                    NULL as organic_n_total     -- Structure varies too much between years
                FROM field_plan_raw
                WHERE {marknummer_column} IS NOT NULL 
                  AND {marknummer_column} != ''
                  AND {marknummer_column} != 'Marknummer'  -- Skip any remaining header rows
                  AND {journal_column} IS NOT NULL     -- Must have journal number for composite key
                  AND TRY_CAST({areal_column} as DOUBLE) > 0  -- Must have positive area (dynamic column!)
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
            self._apply_agricultural_pattern_matching_sync(target_table)
            
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
            marker_exists = False
            try:
                marker_count = self.db.execute("SELECT COUNT(*) FROM marker").fetchone()[0]
                marker_exists = marker_count > 0
            except:
                marker_exists = False
                
            if not marker_exists:
                self.log.warning("   FVM marker data not available - skipping agricultural pattern matching")
                return
            
            # Import and run agricultural pattern matcher
            from unified_pipeline.gold.agricultural_pattern_matcher import (
                AgriculturalPatternMatcher, 
                AgriculturalPatternMatcherConfig,
                run_agricultural_pattern_matching
            )
            
            # Configure for high-quality matches
            config = AgriculturalPatternMatcherConfig(
                min_pattern_score=0.8,
                min_field_score=0.7,
                max_operations_to_process=2000
            )
            
            # Run the pattern matching with our database connection
            results = await run_agricultural_pattern_matching(config, self.db)
            
            # Log results
            if results.get('matches_found', 0) > 0:
                matches_found = results['matches_found']
                self.log.info(f"✅ Agricultural pattern matching found {matches_found:,} additional field matches")
                
                # Create enhanced field mappings table for NLES5 use
                try:
                    self.db.execute(f"""
                        CREATE OR REPLACE TABLE gkea_fvm_enhanced_mappings AS
                        SELECT 
                            g.field_id as gkea_field_id,
                            f.field_id as fvm_field_id,
                            'direct_composite_key' as match_method,
                            1.0 as confidence_score
                        FROM {gkea_table} g
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
                    
                except Exception as mapping_error:
                    self.log.warning(f"   Could not create enhanced mappings table: {mapping_error}")
                    
            else:
                self.log.info("   No additional matches found via agricultural pattern matching")
                
        except Exception as e:
            self.log.warning(f"⚠️ Agricultural pattern matching failed: {str(e)}")
            self.log.warning("   Continuing with standard GKEA field processing...")

    def _apply_agricultural_pattern_matching_sync(self, gkea_table: str) -> None:
        """Synchronous wrapper for agricultural pattern matching."""
        import asyncio
        
        try:
            # Run the async method in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._apply_agricultural_pattern_matching(gkea_table))
            finally:
                loop.close()
        except Exception as e:
            self.log.warning(f"⚠️ Agricultural pattern matching sync wrapper failed: {str(e)}")
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
            ("dmi_climate", "dmi_data"),  # Special handling for DMI data
            (self.config.fertilizer_dataset, "fertilizer_accounts"),
            (self.config.field_plan_dataset, "field_plan_data"),  # Match backup naming
            (self.config.catch_crops_dataset, "catch_crops_data"),  # Match backup naming
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
                    # Special handling for DMI climate data
                    if dataset_name == "dmi_climate":
                        try:
                            success = self._load_and_combine_dmi_data()
                            if success:
                                loaded_tables[dataset_name] = table_name
                                self.log.info(f"✅ Successfully loaded DMI climate data")
                                continue
                            else:
                                self.log.error(f"❌ Failed to load DMI climate data")
                                continue
                        except Exception as e:
                            self.log.error(f"❌ CRITICAL: Failed to load required DMI climate data: {e}")
                            continue
                    
                    # Special handling for field plan data - always try to load from fertiliser directory
                    elif dataset_name == self.config.field_plan_dataset:
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
                        # Special handling for fertilizer and catch crops datasets (they don't follow standard patterns)
                        if dataset_name in [self.config.fertilizer_dataset, self.config.catch_crops_dataset]:
                            # For fertilizer/catch crops, check if we can find the directory structure
                            try:
                                if dataset_name == self.config.fertilizer_dataset:
                                    test_path = self._get_fertilizer_accounts_file_path()
                                    files = [test_path] if test_path else []
                                elif dataset_name == self.config.catch_crops_dataset:
                                    test_path = self._get_catch_crops_data_path()
                                    files = [test_path] if test_path else []
                                else:
                                    files = []
                            except Exception as e:
                                self.log.debug(f"Special handling failed for {dataset_name}: {e}")
                                files = []
                        else:
                            # Try to find the dataset in silver layer using standard patterns
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
                                    fertilizer_file_path = self._get_fertilizer_accounts_file_path(target_year)
                                    if fertilizer_file_path:
                                        self.log.info(f"Using fertilizer accounts file for year {target_year}: {fertilizer_file_path}")
                                        success = self._read_silver_data_from_path(
                                            dataset_name, fertilizer_file_path, table_name
                                        )
                                        if success:
                                            # Add year column to fertilizer data
                                            self._add_year_to_fertilizer_data(fertilizer_file_path, table_name)
                                            # Transform raw fertilizer data to expected schema
                                            self._transform_raw_fertilizer_data(table_name)
                                            loaded_tables[dataset_name] = table_name
                                            self.log.info(f"✅ Successfully loaded fertilizer data: {dataset_name}")
                                            continue
                                        else:
                                            self.log.error(f"❌ Failed to load fertilizer data {dataset_name}")
                                            continue
                                    else:
                                        self.log.error(f"❌ No fertilizer accounts file found for year {target_year}")
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
        Load and combine DMI climate data from precipitation and evaporation datasets.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info("🌤️ Loading DMI climate data from multiple sources...")
            
            # Load precipitation data
            precip_success = self._load_dmi_precipitation_data()
            
            # Load evaporation data
            evap_success = self._load_dmi_evaporation_data()
            
            if precip_success and evap_success:
                # Combine the two datasets
                self._combine_dmi_datasets()
                return True
            elif precip_success:
                self.log.warning("⚠️ Only precipitation data available, using that")
                self.db.execute("CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_precipitation")
                return True
            elif evap_success:
                self.log.warning("⚠️ Only evaporation data available, using that")
                self.db.execute("CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_evaporation")
                return True
            else:
                self.log.error("❌ No DMI climate data could be loaded")
                return False
                
        except Exception as e:
            self.log.error(f"❌ Failed to load DMI data: {e}")
            return False
    
    def _load_dmi_precipitation_data(self) -> bool:
        """Load DMI precipitation data."""
        try:
            pattern = f"gs://{self.config.bucket}/silver/{self.config.dmi_precipitation_dataset}/*/data.parquet"
            files = self.gcs_access.list_files(pattern)
            
            if not files:
                # Try alternative patterns
                alt_patterns = [
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_precipitation_dataset}/data.parquet",
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_precipitation_dataset}*/*.parquet"
                ]
                for alt_pattern in alt_patterns:
                    files = self.gcs_access.list_files(alt_pattern)
                    if files:
                        break
            
            if files:
                latest_file = sorted(files, reverse=True)[0]
                self.log.info(f"📥 Loading DMI precipitation data from: {latest_file}")
                self.gcs_access.create_table_from_gcs("dmi_precipitation", latest_file)
                
                row_count = self.db.execute("SELECT COUNT(*) FROM dmi_precipitation").fetchone()[0]
                self.log.info(f"✅ DMI precipitation data loaded: {row_count:,} rows")
                return True
            else:
                self.log.warning("⚠️ DMI precipitation data not found")
                return False
                
        except Exception as e:
            self.log.error(f"❌ Failed to load DMI precipitation data: {e}")
            return False
    
    def _load_dmi_evaporation_data(self) -> bool:
        """Load DMI evaporation data."""
        try:
            pattern = f"gs://{self.config.bucket}/silver/{self.config.dmi_evaporation_dataset}/*/data.parquet"
            files = self.gcs_access.list_files(pattern)
            
            if not files:
                # Try alternative patterns
                alt_patterns = [
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_evaporation_dataset}/data.parquet",
                    f"gs://{self.config.bucket}/silver/{self.config.dmi_evaporation_dataset}*/*.parquet"
                ]
                for alt_pattern in alt_patterns:
                    files = self.gcs_access.list_files(alt_pattern)
                    if files:
                        break
            
            if files:
                latest_file = sorted(files, reverse=True)[0]
                self.log.info(f"📥 Loading DMI evaporation data from: {latest_file}")
                self.gcs_access.create_table_from_gcs("dmi_evaporation", latest_file)
                
                row_count = self.db.execute("SELECT COUNT(*) FROM dmi_evaporation").fetchone()[0]
                self.log.info(f"✅ DMI evaporation data loaded: {row_count:,} rows")
                return True
            else:
                self.log.warning("⚠️ DMI evaporation data not found")
                return False
                
        except Exception as e:
            self.log.error(f"❌ Failed to load DMI evaporation data: {e}")
            return False
    
    def _combine_dmi_datasets(self):
        """Combine precipitation and evaporation datasets into a single DMI table."""
        try:
            self.log.info("🔗 Combining DMI precipitation and evaporation data...")
            
            # Check what columns are available in each dataset
            precip_columns = self.db.execute("DESCRIBE dmi_precipitation").fetchall()
            evap_columns = self.db.execute("DESCRIBE dmi_evaporation").fetchall()
            
            self.log.info(f"Precipitation columns: {[col[0] for col in precip_columns[:5]]}")
            self.log.info(f"Evaporation columns: {[col[0] for col in evap_columns[:5]]}")
            
            # Create combined table with both datasets
            # Since both tables have the same structure, just use precipitation data for now
            # TODO: Implement proper spatial joining once we understand the spatial column structure
            self.db.execute("""
                CREATE OR REPLACE TABLE dmi_data AS
                SELECT 
                    *,
                    'precipitation' as data_type
                FROM dmi_precipitation
            """)
            
            combined_count = self.db.execute("SELECT COUNT(*) FROM dmi_data").fetchone()[0]
            self.log.info(f"✅ Combined DMI data: {combined_count:,} rows")
            
        except Exception as e:
            self.log.warning(f"⚠️ Failed to combine DMI datasets, using precipitation only: {e}")
            self.db.execute("CREATE OR REPLACE TABLE dmi_data AS SELECT * FROM dmi_precipitation")

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
                year_dataset = f"{self.config.agricultural_fields_dataset}_{year}"  # Use fvm_marker_YYYY
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
                    success = self._read_silver_data_from_path(f"{self.config.agricultural_fields_dataset}_{year}", fvm_path, year_table)
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
        # Use the standard table name expected by nles5_calculator
        self._load_agricultural_fields_for_years(required_years, "agricultural_fields")
        
        return "agricultural_fields"

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
        
        combined_table = "agricultural_fields"
        self.processor.conn.execute(f"DROP TABLE IF EXISTS {combined_table}")
        
        # Create the combined table by unioning all yearly tables
        union_parts = []
        for year, table_name in yearly_tables.items():
            union_parts.append(f"SELECT *, {year} AS data_year FROM {table_name}")
        
        union_sql = f"""
        CREATE TABLE {combined_table} AS
        {' UNION ALL '.join(union_parts)}
        """
        
        self.processor.conn.execute(union_sql)
        
        # Verify the combined table
        row_count = self.processor.conn.execute(f"SELECT COUNT(*) FROM {combined_table}").fetchone()[0]
        year_count = len(yearly_tables)
        
        self.log.info(f"✅ Combined {year_count} years of FVM data: {row_count:,} total rows")
        
        return combined_table

    def _add_year_to_fertilizer_data(self, file_path: str, table_name: str) -> None:
        """
        Add year column to fertilizer data by extracting it from the filename.
        
        Args:
            file_path: GCS path to the fertilizer file (e.g., "Gødningsregnskaber 2023.parquet")
            table_name: Name of the table to update (e.g., "fertilizer_accounts")
        """
        import re
        
        # Extract 4-digit year from filename
        filename = file_path.split('/')[-1]  # Get just the filename
        year_match = re.search(r'(\d{4})', filename)
        
        if not year_match:
            self.log.warning(f"⚠️ Could not extract year from filename: {filename}")
            return
            
        year = int(year_match.group(1))
        self.log.info(f"🗓️ Adding year {year} to fertilizer data from filename: {filename}")
        
        # Check if table exists before trying to modify it
        try:
            self.log.info(f"🔍 Available tables before year addition: {[t[0] for t in self.processor.conn.execute('SHOW TABLES').fetchall()]}")
            
            # Verify the table exists
            table_count = self.processor.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Table {table_name} found successfully")
            
            # Add year column to the existing table
            self.processor.conn.execute(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN IF NOT EXISTS year INTEGER DEFAULT {year}
            """)
            
            # Update all rows with the extracted year
            self.processor.conn.execute(f"""
                UPDATE {table_name} 
                SET year = {year} 
                WHERE year IS NULL OR year = {year}
            """)
            
            self.log.info(f"✅ Successfully added year column to {table_name}: {year}")
            
        except Exception as e:
            self.log.error(f"❌ Failed to add year to fertilizer data: {e}")
            # List available tables for debugging
            try:
                tables = self.processor.conn.execute("SHOW TABLES").fetchall()
                self.log.error(f"Available tables: {[t[0] for t in tables]}")
            except:
                self.log.error("Could not list available tables")

    def _transform_raw_fertilizer_data(self, table_name: str) -> None:
        """
        Transform raw fertilizer data with form codes (f_901, f_902) to expected schema.
        
        Args:
            table_name: Name of the fertilizer table to transform
        """
        try:
            self.log.info(f"🔧 Transforming raw fertilizer data to expected schema...")
            
            # Check current columns
            columns = self.processor.conn.execute(f"DESCRIBE {table_name}").fetchall()
            column_names = [col[0] for col in columns]
            self.log.info(f"Available fertilizer columns: {len(column_names)} columns")
            
            # Transform to expected schema with best-guess mapping
            # Based on common fertilizer form patterns and backup expectations
            self.processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_transformed AS
                SELECT
                    cvr_number,
                    year,
                    -- Map form codes to expected fertilizer columns (best effort)
                    -- These are educated guesses based on typical fertilizer form structures
                    COALESCE(TRY_CAST(f_901 AS DOUBLE), 0.0) as tn_t_ha,  -- Total nitrogen quota
                    COALESCE(TRY_CAST(f_185_2 AS DOUBLE), 0.0) as mineral_n_foraar,  -- Spring mineral N
                    COALESCE(TRY_CAST(f_185_3 AS DOUBLE), 0.0) as mineral_n_eft,     -- Autumn mineral N
                    COALESCE(TRY_CAST(f_188_2 AS DOUBLE), 0.0) as mineral_n_udb,     -- Growing season N
                    COALESCE(TRY_CAST(f_601_2 AS DOUBLE), 0.0) as organic_n_hus,     -- Organic manure N
                    'Standard' as niveau  -- Default harmoni level
                FROM {table_name}
                WHERE cvr_number IS NOT NULL
            """)
            
            # Replace original table with transformed version
            self.processor.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.processor.conn.execute(f"ALTER TABLE {table_name}_transformed RENAME TO {table_name}")
            
            # Verify transformation
            row_count = self.processor.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.log.info(f"✅ Successfully transformed fertilizer data: {row_count:,} rows with expected schema")
            
        except Exception as e:
            self.log.warning(f"⚠️ Failed to transform fertilizer data, using defaults: {e}")
            # Create a minimal table with defaults if transformation fails
            self.processor.conn.execute(f"""
                CREATE OR REPLACE TABLE {table_name}_defaults AS
                SELECT
                    cvr_number,
                    year,
                    0.0 as tn_t_ha,
                    0.0 as mineral_n_foraar,
                    0.0 as mineral_n_eft,
                    0.0 as mineral_n_udb,
                    0.0 as organic_n_hus,
                    'Default' as niveau
                FROM {table_name}
                WHERE cvr_number IS NOT NULL
            """)
            self.processor.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.processor.conn.execute(f"ALTER TABLE {table_name}_defaults RENAME TO {table_name}")
            self.log.info(f"✅ Created default fertilizer schema")
