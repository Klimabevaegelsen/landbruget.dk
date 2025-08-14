"""
Silver layer data processing for Fertiliser data.

This module handles the processing of fertiliser parquet files from the bronze layer and
converts them into harmonized tables with standardized schemas across all file types.

The module contains:
- FertiliserSilverConfig: Configuration class for the silver processing
- FertiliserSilver: Implementation class for harmonizing fertiliser data

The data is processed from raw parquet files into standardized tables
with consistent column names, data types, and unified schemas.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

import duckdb

from ..base.silver_base import SilverJobInterface  
from ..common.base import BaseJobConfig, ConnectionManager

logger = logging.getLogger(__name__)


class FertiliserSilverConfig(BaseJobConfig):
    """
    Configuration for the Fertiliser Silver processing.

    This class defines all configuration parameters needed for processing
    raw fertiliser data from the bronze layer into clean, harmonized
    data for the silver layer.

    Attributes:
        name (str): Human-readable name of the data processing
        type (str): Type of the data processing (parquet)
        description (str): Brief description of the processing
        dataset (str): Name of the dataset in storage
        input_path (str): Path to input fertiliser parquet files
        bucket (str): GCS bucket name for data storage
    """

    name: str = "Danish Fertiliser Data Silver"
    type: str = "parquet"
    description: str = "Harmonizes fertiliser data (Efterafgrøder, GKEA, Gødningsregnskaber)"
    dataset: str = "fertiliser"
    input_path: str = "data/fertiliser"
    bucket: str = "landbrugsdata-raw-data"


class FertiliserSilver(SilverJobInterface):
    """Processor for harmonizing and standardizing fertiliser data."""
    
    def __init__(self, config: FertiliserSilverConfig):
        """Initialize the fertiliser processor."""
        super().__init__(config)
        self.config = config
        self.connection_manager = ConnectionManager()
        self.table_name = "fertiliser_harmonized"
        self.schema_name = "silver"
    
    async def run(self, bronze_data: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Run the silver processing for fertiliser data.
        
        Args:
            bronze_data: Optional bronze data containing file information
            
        Returns:
            Table name if successful, None if failed
        """
        logger.info("Starting fertiliser data harmonization")
        
        try:
            # Use bronze data if available, otherwise default path
            input_path = (
                bronze_data.get("input_path", self.config.input_path) 
                if bronze_data else self.config.input_path
            )
            
            # Create main harmonized table
            harmonized_table = await self.harmonize_fertiliser_data(input_path)
            
            logger.info(f"Fertiliser data harmonization completed. Created table: {harmonized_table}")
            return harmonized_table
            
        except Exception as e:
            logger.error(f"Fertiliser silver processing failed: {str(e)}")
            return None
    
    async def harmonize_fertiliser_data(self, input_data_path: str) -> str:
        """Harmonize all fertiliser data sources."""
        
        with self.connection_manager.get_connection() as conn:
            # Create schema if not exists
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            
            # Process each category of fertiliser data
            efterafgroeder_table = self._process_efterafgroeder_files(conn, input_data_path)
            gkea_table = self._process_gkea_files(conn, input_data_path)
            goedning_table = self._process_goedningsregnskaber_files(conn, input_data_path)
            
            # Create unified harmonized table combining all sources
            harmonized_table = f"{self.schema_name}.{self.table_name}"
            
            conn.execute(f"DROP TABLE IF EXISTS {harmonized_table}")
            
            # Create unified structure with common fields and source identification
            create_sql = f"""
            CREATE TABLE {harmonized_table} AS
            SELECT 
                'efterafgroeder' as data_source,
                prod_aar as year,
                cvr_number,
                capnumber,
                markbloknummer,
                marknummer,
                indberet_alternativ,
                faktisk_areal_ha,
                omregnet_areal_ha,
                CAST(NULL as VARCHAR) as journal_nummer,
                CAST(NULL as DOUBLE) as total_n_kvote,
                CAST(NULL as DOUBLE) as fosfortal,
                'Efterafgrøder' as data_type,
                data_source_file
            FROM {efterafgroeder_table}
            
            UNION ALL
            
            SELECT 
                'gkea' as data_source,
                CAST(regexp_extract(data_source_file, 'GKEA(\\d{{4}})', 1) as VARCHAR) as year,
                cvr_number,
                CAST(NULL as VARCHAR) as capnumber,
                CAST(NULL as VARCHAR) as markbloknummer,
                marknummer,
                hovedafgroede as indberet_alternativ,
                areal_ha as faktisk_areal_ha,
                harmoni_areal_ha as omregnet_areal_ha,
                journal_nummer,
                n_kvote_mark as total_n_kvote,
                fosfortal,
                'GKEA Markplan' as data_type,
                data_source_file
            FROM {gkea_table}
            
            UNION ALL
            
            SELECT 
                'goedningsregnskaber' as data_source,
                planaar as year,
                cvr_number,
                CAST(NULL as VARCHAR) as capnumber,
                CAST(NULL as VARCHAR) as markbloknummer,
                CAST(NULL as VARCHAR) as marknummer,
                CAST(NULL as VARCHAR) as indberet_alternativ,
                CAST(NULL as DOUBLE) as faktisk_areal_ha,
                CAST(NULL as DOUBLE) as omregnet_areal_ha,
                CAST(NULL as VARCHAR) as journal_nummer,
                CAST(NULL as DOUBLE) as total_n_kvote,
                CAST(NULL as DOUBLE) as fosfortal,
                'Gødningsregnskaber' as data_type,
                data_source_file
            FROM {goedning_table}
            """
            
            conn.execute(create_sql)
            
            # Add indexes for performance
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_year ON {harmonized_table}(year)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_cvr ON {harmonized_table}(cvr_number)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_source ON {harmonized_table}(data_source)")
            
            # Log summary statistics
            summary = conn.execute(f"""
                SELECT 
                    data_source, 
                    data_type,
                    COUNT(*) as record_count,
                    MIN(year) as min_year,
                    MAX(year) as max_year,
                    COUNT(DISTINCT cvr_number) as unique_companies
                FROM {harmonized_table}
                GROUP BY data_source, data_type
                ORDER BY data_source
            """).fetchdf()
            
            logger.info("Harmonized data summary:")
            logger.info(f"\n{summary.to_string()}")
            
            return harmonized_table
    
    def _process_efterafgroeder_files(self, conn: duckdb.DuckDBPyConnection, input_data_path: str) -> str:
        """Process and harmonize Efterafgrøder (cover crops) files."""
        logger.info("Processing Efterafgrøder files")
        
        table_name = f"{self.schema_name}.efterafgroeder_harmonized"
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Define standardized schema for Efterafgrøder
        create_sql = f"""
        CREATE TABLE {table_name} (
            prod_aar VARCHAR,
            cvr_number VARCHAR,
            capnumber VARCHAR,
            markbloknummer VARCHAR,
            marknummer VARCHAR,
            indberet_alternativ VARCHAR,
            faktisk_areal_ha DOUBLE,
            omregnet_areal_ha DOUBLE,
            data_source_file VARCHAR
        )
        """
        conn.execute(create_sql)
        
        # Map the different column naming patterns across years
        file_mappings = [
            {
                'pattern': 'Efterafgrøder 2020',
                'columns': {
                    'a18_indberetefterafgalternativ': 'indberet_alternativ',
                    'a19_faktiskhaudlagteaalternativ': 'faktisk_areal_ha', 
                    'a20_omregnethamedea': 'omregnet_areal_ha'
                }
            },
            {
                'pattern': 'Efterafgrøder 2021', 
                'columns': {
                    'a19_indberetefterafgalternativ': 'indberet_alternativ',
                    'a20_faktiskhaudlagteaalternativ': 'faktisk_areal_ha',
                    'a21_omregnethamedea': 'omregnet_areal_ha'
                }
            },
            {
                'pattern': 'Efterafgrøder 2022',
                'columns': {
                    'a20_indberetefterafgalternativ': 'indberet_alternativ', 
                    'a21_faktiskhaudlagteaalternativ': 'faktisk_areal_ha',
                    'a24_omregnethamedea': 'omregnet_areal_ha'
                }
            },
            {
                'pattern': 'Efterafgrøder 2023',
                'columns': {
                    'a19_indberetefterafgalternativ': 'indberet_alternativ',
                    'a20_faktiskhaudlagteaalternativ': 'faktisk_areal_ha', 
                    'a23_omregnethamedea': 'omregnet_areal_ha'
                }
            }
        ]
        
        base_path = input_data_path
        
        for mapping in file_mappings:
            file_pattern = f"{base_path}/{mapping['pattern']}.parquet"
            
            try:
                # Check if file exists
                df_check = conn.execute(f"SELECT COUNT(*) as cnt FROM read_parquet('{file_pattern}')").fetchone()
                if df_check[0] == 0:
                    logger.warning(f"No data found in {file_pattern}")
                    continue
                    
                # Build dynamic column selection
                column_mappings = mapping['columns']
                select_columns = []
                
                # Standard columns that should exist in all files
                select_columns.extend([
                    'prod_aar',
                    'cvr_number', 
                    'capnumber',
                    'markbloknummer'
                ])
                
                # Add marknummer if available (2022, 2023)
                column_names = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_pattern}')").fetchdf()['column_name'].values
                if 'marknummer' in column_names:
                    select_columns.append('marknummer')
                else:
                    select_columns.append("CAST(NULL as VARCHAR) as marknummer")
                
                # Add mapped columns  
                for original_col, standard_col in column_mappings.items():
                    if standard_col == 'faktisk_areal_ha' or standard_col == 'omregnet_areal_ha':
                        select_columns.append(f"CAST(REPLACE({original_col}, ',', '.') as DOUBLE) as {standard_col}")
                    else:
                        select_columns.append(f"{original_col} as {standard_col}")
                
                select_columns.append(f"'{mapping['pattern']}' as data_source_file")
                
                insert_sql = f"""
                INSERT INTO {table_name}
                SELECT {', '.join(select_columns)}
                FROM read_parquet('{file_pattern}')
                WHERE prod_aar IS NOT NULL AND cvr_number IS NOT NULL
                """
                
                conn.execute(insert_sql)
                logger.info(f"Processed {mapping['pattern']}")
                
            except Exception as e:
                logger.error(f"Failed to process {mapping['pattern']}: {str(e)}")
        
        return table_name
    
    def _process_gkea_files(self, conn: duckdb.DuckDBPyConnection, input_data_path: str) -> str:
        """Process and harmonize GKEA files with proper column naming."""
        logger.info("Processing GKEA files")
        
        table_name = f"{self.schema_name}.gkea_harmonized" 
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        create_sql = f"""
        CREATE TABLE {table_name} (
            journal_nummer VARCHAR,
            cvr_number VARCHAR,
            marknummer VARCHAR,
            areal_ha DOUBLE,
            harmoni_areal_ha DOUBLE,
            hovedafgroede VARCHAR,
            n_kvote_mark DOUBLE,
            fosfortal DOUBLE,
            data_source_file VARCHAR
        )
        """
        conn.execute(create_sql)
        
        # Define GKEA file structures based on CORRECT header validation
        gkea_files = [
            {
                'pattern': 'GKEA2021_Markplan_med_Gødningsoplysninger',
                'journal_col': 'gkea2021_markplan_goedningskvote',  # CORRECTED: First column is journal
                'columns': {
                    'column_1': 'cvr_number',        # CVR (8-digit numbers)
                    'column_5': 'marknummer',        # Marknummer (CORRECTED: was column_6)
                    'column_6': 'areal_ha',          # Areal (CORRECTED: was column_7)
                    'column_10': 'harmoni_areal_ha', # Harmoni Areal
                    'column_14': 'hovedafgroede',    # Hovedafgrøde (CORRECTED: was column_15)
                    'column_19': 'fosfortal'         # Fosfortal (often empty)
                }
            },
            {
                'pattern': 'GKEA2022_Markplan_med_Gødningsoplysninger',
                'journal_col': 'gkea2022_markplan_goedningskvote',  # CORRECTED: First column is journal
                'columns': {
                    'column_1': 'cvr_number',        # CVR (8-digit numbers)
                    'column_3': 'marknummer',        # Marknummer (CORRECTED: was column_4)
                    'column_4': 'areal_ha',          # Areal (CORRECTED: was column_5)
                    'column_8': 'harmoni_areal_ha',  # Harmoni Areal
                    'column_12': 'hovedafgroede',    # Hovedafgrøde
                    'column_17': 'fosfortal'         # Fosfortal (often empty)
                }
            },
            {
                'pattern': 'GKEA2023_Markplan_med_Gødningsoplysninger_Aktindsigt',
                'journal_col': 'gkea2023_markplan_goedningskvote',  # CORRECTED: First column is journal
                'columns': {
                    'column_1': 'cvr_number',        # CVR (8-digit numbers)
                    'column_3': 'marknummer',        # Marknummer (CORRECTED: was column_4)
                    'column_4': 'areal_ha',          # Areal (CORRECTED: was column_5)
                    'column_6': 'harmoni_areal_ha',  # Harmoni Areal (CORRECTED: was column_7)
                    'column_10': 'hovedafgroede',    # Hovedafgrøde (CORRECTED: was column_11)
                    'column_19': 'n_kvote_mark'      # N Kvote Mark
                }
            },
            {
                'pattern': 'GKEA2024_Markplan_Efterafgrøder',
                'journal_col': 'gkea2024_markplan_efterafgroeder',  # Different naming pattern
                'columns': {
                    'column_1': 'cvr_number',        # CVR
                    'column_4': 'marknummer',        # Marknummer
                    'column_5': 'areal_ha',          # Areal (CORRECTED: was column_6)
                    'column_6': 'harmoni_areal_ha',  # Areal Omregnet Til EA (CORRECTED: was column_14)
                    'column_14': 'hovedafgroede'     # Hoved afgrøde (CORRECTED: was column_5)
                }
            },
            {
                'pattern': 'GKEA2024_Markplan_med_Gødningsoplysninger',
                'journal_col': 'gkea2024_markplan_med_goedningsoplysninger',  # CORRECTED: First column is journal
                'columns': {
                    'column_1': 'cvr_number',        # CVR (8-digit numbers)
                    'column_3': 'marknummer',        # Marknummer (CORRECTED: was column_4)
                    'column_4': 'areal_ha',          # Areal (CORRECTED: was column_5)
                    'column_6': 'harmoni_areal_ha',  # Harmoni Areal (CORRECTED: was column_7)
                    'column_10': 'hovedafgroede',    # Hovedafgrøde (CORRECTED: was column_11)
                    'column_19': 'n_kvote_mark'      # N Kvote Mark
                }
            }
        ]
        
        base_path = input_data_path
        
        for file_info in gkea_files:
            file_pattern = f"{base_path}/{file_info['pattern']}.parquet"
            
            try:
                # Skip header rows (first 2 rows contain metadata)
                select_columns = []
                column_mappings = file_info['columns']
                journal_col = file_info['journal_col']
                
                # Add journal column first
                select_columns.append(f"NULLIF(TRIM({journal_col}), '') as journal_nummer")
                
                # Add mapped columns
                for original_col, standard_col in column_mappings.items():
                    if standard_col in ['areal_ha', 'harmoni_areal_ha', 'n_kvote_mark', 'fosfortal']:
                        select_columns.append(f"CAST(NULLIF(REPLACE({original_col}, ',', '.'), '') as DOUBLE) as {standard_col}")
                    else:
                        select_columns.append(f"NULLIF(TRIM({original_col}), '') as {standard_col}")
                
                select_columns.append(f"'{file_info['pattern']}' as data_source_file")
                
                insert_sql = f"""
                INSERT INTO {table_name}
                SELECT {', '.join(select_columns)}
                FROM (
                    SELECT *, ROW_NUMBER() OVER () as row_num
                    FROM read_parquet('{file_pattern}')
                )
                WHERE row_num > 2  -- Skip header rows
                AND {journal_col} IS NOT NULL 
                AND {journal_col} != ''
                AND {journal_col} NOT LIKE 'Journal%'  -- Skip any remaining header content
                """
                
                conn.execute(insert_sql)
                logger.info(f"Processed {file_info['pattern']}")
                
            except Exception as e:
                logger.error(f"Failed to process {file_info['pattern']}: {str(e)}")
        
        return table_name
    
    def _process_goedningsregnskaber_files(self, conn: duckdb.DuckDBPyConnection, input_data_path: str) -> str:
        """Process Gødningsregnskaber (fertilizer accounts) files.""" 
        logger.info("Processing Gødningsregnskaber files")
        
        table_name = f"{self.schema_name}.goedningsregnskaber_harmonized"
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        create_sql = f"""
        CREATE TABLE {table_name} (
            cvr_number VARCHAR,
            planaar VARCHAR,
            kommune VARCHAR,
            virksomhed_navn VARCHAR,
            data_source_file VARCHAR
        )
        """
        conn.execute(create_sql)
        
        base_path = input_data_path
        
        # Process main data files
        goedning_files = [
            'Gødningsregnskaber 2022_data.parquet',
            'Gødningsregnskaber 2023.parquet'
        ]
        
        for file_name in goedning_files:
            file_pattern = f"{base_path}/{file_name}"
            
            try:
                # Use f_planaar column from the actual data instead of hardcoded values
                # Check which schema the file has
                if '2022' in file_name:
                    insert_sql = f"""
                    INSERT INTO {table_name}
                    SELECT 
                        cvr_number,
                        CAST(f_planaar as VARCHAR) as planaar,  -- Use actual f_planaar column
                        kommune,
                        vir_navn as virksomhed_navn,
                        '{file_name}' as data_source_file
                    FROM read_parquet('{file_pattern}')
                    WHERE cvr_number IS NOT NULL AND f_planaar IS NOT NULL
                    """
                else:  # 2023 - missing kommune column
                    insert_sql = f"""
                    INSERT INTO {table_name}
                    SELECT 
                        cvr_number,
                        CAST(f_planaar as VARCHAR) as planaar,  -- Use actual f_planaar column
                        CAST(NULL as VARCHAR) as kommune,  -- 2023 file missing kommune
                        vir_navn as virksomhed_navn,
                        '{file_name}' as data_source_file
                    FROM read_parquet('{file_pattern}')
                    WHERE cvr_number IS NOT NULL AND f_planaar IS NOT NULL
                    """
                
                conn.execute(insert_sql)
                logger.info(f"Processed {file_name}")
                
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {str(e)}")
        
        return table_name