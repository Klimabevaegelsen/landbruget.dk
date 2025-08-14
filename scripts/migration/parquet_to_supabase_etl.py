#!/usr/bin/env python3
"""
Parquet to Supabase ETL Pipeline
High-performance migration system for landbruget.dk data

Based on parquet_to_supabase_schema_analysis.md findings
"""

import os
import sys
import time
import logging
import asyncio
import pandas as pd
import duckdb
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from psycopg2.pool import ThreadedConnectionPool
import numpy as np
from datetime import datetime, date
import json
import uuid
from io import StringIO

# Add project root to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

@dataclass
class MigrationConfig:
    """Configuration for migration pipeline"""
    # Supabase connection
    supabase_host: str
    supabase_port: int = 5432
    supabase_db: str = "postgres"
    supabase_user: str = "postgres"
    supabase_password: str = ""
    
    # GCS bucket configuration
    gcs_bucket: str = "gs://landbrugsdata-raw-data"
    
    # Performance settings
    batch_size: int = 10000
    max_workers: int = 4
    connection_pool_size: int = 8
    
    # Migration settings
    dry_run: bool = False
    validate_data: bool = True
    skip_existing: bool = True
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None

@dataclass
class MigrationStats:
    """Track migration statistics"""
    total_records: int = 0
    processed_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    batches_completed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    errors: List[str] = field(default_factory=list)
    
    def add_error(self, error: str):
        self.errors.append(f"{datetime.now()}: {error}")
        
    def get_duration(self) -> float:
        return (datetime.now() - self.start_time).total_seconds()
        
    def get_rate(self) -> float:
        duration = self.get_duration()
        return self.processed_records / duration if duration > 0 else 0

class SupabaseMigrationPipeline:
    """High-performance ETL pipeline for Parquet to Supabase migration"""
    
    def __init__(self, config: MigrationConfig):
        self.config = config
        self.logger = self._setup_logging()
        self.connection_pool = None
        self.duckdb_conn = None
        
        # Migration mappings based on analysis document
        self.table_mappings = {
            'companies': {
                'source': 'cvr_enrichment_companies',
                'key_fields': ['cvr_number'],
                'transform_func': self._transform_companies
            },
            'field_boundaries': {
                'source': 'fvm_marker_2025',
                'key_fields': ['cvr_number', 'field_id'],
                'transform_func': self._transform_field_boundaries
            },
            'production_sites': {
                'source': ['properties', 'property_owners'],
                'key_fields': ['chr_number'],
                'transform_func': self._transform_production_sites
            },
            'yearly_financials': {
                'source': 'cvr_enrichment_financial',
                'key_fields': ['cvr_number', 'year'],
                'transform_func': self._transform_yearly_financials
            },
            'company_leadership': {
                'source': 'cvr_enrichment_leadership',
                'key_fields': ['cvr_number'],
                'transform_func': self._transform_company_leadership
            },
            'company_owners': {
                'source': 'cvr_enrichment_leadership',
                'key_fields': ['cvr_number'],
                'transform_func': self._transform_company_owners
            },
            'field_yearly_data': {
                'source': 'field_analysis_2025',
                'key_fields': ['field_uuid', 'year'],
                'transform_func': self._transform_field_yearly_data
            },
            'field_bnbo_areas': {
                'source': 'field_environmental_analysis_fields_2024',
                'key_fields': ['field_uuid', 'year'],
                'transform_func': self._transform_field_bnbo_areas
            },
            'field_wetland_areas': {
                'source': 'field_analysis_2024',
                'key_fields': ['field_uuid', 'year'],
                'transform_func': self._transform_field_wetland_areas
            },
            'pesticide_applications': {
                'source': 'pesticide_proximity_2023_2024',
                'key_fields': ['DisaggregatedID'],
                'transform_func': self._transform_pesticide_applications
            },
            'animal_capacity_log': {
                'source': 'herd_sizes',
                'key_fields': ['size_id'],
                'transform_func': self._transform_animal_capacity_log
            },
            'animal_transports': {
                'source': 'chr_transportation_analysis',
                'key_fields': ['sender_chr_number', 'movement_date'],
                'transform_func': self._transform_animal_transports
            },
            'vet_events': {
                'source': 'property_vet_events',
                'key_fields': ['event_id'],
                'transform_func': self._transform_vet_events
            },
            'employee_monthly_counts': {
                'source': 'cvr_enrichment_monthly',
                'key_fields': ['cvr_number', 'year', 'month'],
                'transform_func': self._transform_employee_monthly_counts
            },
            'visa_yearly_counts': {
                'source': 'Landbrugsvisum_statistik',
                'key_fields': ['company_id', 'year', 'nationality'],
                'transform_func': self._transform_visa_yearly_counts
            },
            'incidents': {
                'source': ['worker_safety', 'arbejdstilsynet_inspections'],
                'key_fields': ['id'],
                'transform_func': self._transform_incidents
            }
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('supabase_migration')
        logger.setLevel(getattr(logging, self.config.log_level))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler if specified
        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    async def initialize(self):
        """Initialize database connections and DuckDB"""
        self.logger.info("Initializing migration pipeline...")
        
        # Initialize PostgreSQL connection pool
        try:
            self.connection_pool = ThreadedConnectionPool(
                minconn=2,
                maxconn=self.config.connection_pool_size,
                host=self.config.supabase_host,
                port=self.config.supabase_port,
                database=self.config.supabase_db,
                user=self.config.supabase_user,
                password=self.config.supabase_password
            )
            self.logger.info("PostgreSQL connection pool initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize connection pool: {e}")
            raise
        
        # Initialize DuckDB with spatial extension
        try:
            self.duckdb_conn = duckdb.connect(':memory:')
            self.duckdb_conn.execute("INSTALL spatial")
            self.duckdb_conn.execute("LOAD spatial")
            self.logger.info("DuckDB initialized with spatial extension")
        except Exception as e:
            self.logger.error(f"Failed to initialize DuckDB: {e}")
            raise
    
    def _get_connection(self):
        """Get database connection from pool"""
        return self.connection_pool.getconn()
    
    def _return_connection(self, conn):
        """Return connection to pool"""
        self.connection_pool.putconn(conn)
    
    def _load_parquet_data(self, source_path: str) -> pd.DataFrame:
        """Load parquet data using DuckDB for optimal performance"""
        try:
            # Use DuckDB to read parquet files efficiently
            query = f"SELECT * FROM read_parquet('{source_path}')"
            df = self.duckdb_conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} records from {source_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load parquet data from {source_path}: {e}")
            raise
    
    def _validate_data(self, df: pd.DataFrame, table_name: str) -> Tuple[pd.DataFrame, List[str]]:
        """Validate data quality and format"""
        errors = []
        original_count = len(df)
        
        # Remove duplicates
        if table_name in self.table_mappings:
            key_fields = self.table_mappings[table_name]['key_fields']
            if all(field in df.columns for field in key_fields):
                df = df.drop_duplicates(subset=key_fields)
                duplicate_count = original_count - len(df)
                if duplicate_count > 0:
                    self.logger.warning(f"Removed {duplicate_count} duplicate records from {table_name}")
        
        # Basic validation
        if df.empty:
            errors.append(f"No data found for {table_name}")
        
        return df, errors
    
    def _bulk_insert_copy(self, conn, table_name: str, df: pd.DataFrame, columns: List[str]):
        """High-performance bulk insert using COPY command"""
        try:
            # Create CSV buffer
            buffer = StringIO()
            df[columns].to_csv(buffer, index=False, header=False, na_rep='\\N')
            buffer.seek(0)
            
            # Use COPY command for maximum performance
            cursor = conn.cursor()
            cursor.copy_from(
                buffer,
                table_name,
                columns=columns,
                sep=',',
                null='\\N'
            )
            conn.commit()
            cursor.close()
            
            self.logger.debug(f"COPY inserted {len(df)} records into {table_name}")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"COPY insert failed for {table_name}: {e}")
            raise
    
    def _bulk_insert_batch(self, conn, table_name: str, df: pd.DataFrame, columns: List[str]):
        """Fallback bulk insert using execute_values"""
        try:
            cursor = conn.cursor()
            
            # Prepare data tuples
            data_tuples = [tuple(row) for row in df[columns].values]
            
            # Create INSERT query
            placeholders = ','.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
            
            # Use execute_values for better performance than executemany
            execute_values(cursor, query, data_tuples, page_size=1000)
            conn.commit()
            cursor.close()
            
            self.logger.debug(f"Batch inserted {len(df)} records into {table_name}")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Batch insert failed for {table_name}: {e}")
            raise
    
    async def migrate_table(self, table_name: str) -> MigrationStats:
        """Migrate a single table"""
        stats = MigrationStats()
        self.logger.info(f"Starting migration for table: {table_name}")
        
        try:
            # Get table configuration
            table_config = self.table_mappings.get(table_name)
            if not table_config:
                raise ValueError(f"No configuration found for table {table_name}")
            
            # Load source data
            source_datasets = table_config['source']
            if isinstance(source_datasets, str):
                source_datasets = [source_datasets]
            
            all_data = []
            for source in source_datasets:
                # Construct source path (simplified - adjust for your GCS structure)
                source_path = f"{self.config.gcs_bucket}/silver/{source}/latest/*.parquet"
                df = self._load_parquet_data(source_path)
                all_data.append(df)
            
            # Combine data if multiple sources
            if len(all_data) == 1:
                source_df = all_data[0]
            else:
                # Join logic depends on table - implement based on analysis
                source_df = pd.concat(all_data, ignore_index=True)
            
            stats.total_records = len(source_df)
            
            # Validate data
            if self.config.validate_data:
                source_df, validation_errors = self._validate_data(source_df, table_name)
                stats.errors.extend(validation_errors)
            
            # Transform data
            transform_func = table_config['transform_func']
            transformed_df = await transform_func(source_df)
            
            if self.config.dry_run:
                self.logger.info(f"DRY RUN: Would migrate {len(transformed_df)} records to {table_name}")
                stats.processed_records = len(transformed_df)
                stats.successful_records = len(transformed_df)
                return stats
            
            # Get database connection
            conn = self._get_connection()
            
            try:
                # Process in batches for large datasets
                batch_size = self.config.batch_size
                total_batches = (len(transformed_df) + batch_size - 1) // batch_size
                
                for batch_idx in range(0, len(transformed_df), batch_size):
                    batch_df = transformed_df.iloc[batch_idx:batch_idx + batch_size]
                    
                    try:
                        # Get target columns (excluding auto-generated fields)
                        target_columns = list(batch_df.columns)
                        if 'id' in target_columns and table_name != 'companies':
                            target_columns.remove('id')  # Let database generate UUIDs
                        
                        # Try COPY first (fastest), fallback to batch insert
                        try:
                            self._bulk_insert_copy(conn, table_name, batch_df, target_columns)
                        except Exception as copy_error:
                            self.logger.warning(f"COPY failed, using batch insert: {copy_error}")
                            self._bulk_insert_batch(conn, table_name, batch_df, target_columns)
                        
                        stats.processed_records += len(batch_df)
                        stats.successful_records += len(batch_df)
                        stats.batches_completed += 1
                        
                        if stats.batches_completed % 10 == 0:
                            self.logger.info(
                                f"Progress {table_name}: {stats.batches_completed}/{total_batches} batches "
                                f"({stats.processed_records}/{stats.total_records} records, "
                                f"{stats.get_rate():.1f} records/sec)"
                            )
                    
                    except Exception as batch_error:
                        stats.failed_records += len(batch_df)
                        stats.add_error(f"Batch {batch_idx//batch_size} failed: {batch_error}")
                        self.logger.error(f"Batch insert failed: {batch_error}")
                        
                        if stats.failed_records > stats.successful_records * 0.1:  # >10% failure rate
                            raise Exception(f"Too many failures in {table_name} migration")
            
            finally:
                self._return_connection(conn)
            
            self.logger.info(
                f"Migration completed for {table_name}: "
                f"{stats.successful_records}/{stats.total_records} records "
                f"in {stats.get_duration():.1f}s "
                f"({stats.get_rate():.1f} records/sec)"
            )
            
        except Exception as e:
            stats.add_error(f"Table migration failed: {e}")
            self.logger.error(f"Migration failed for {table_name}: {e}")
            raise
        
        return stats
    
    # ============================================================================
    # TRANSFORMATION FUNCTIONS (based on parquet_to_supabase_schema_analysis.md)
    # ============================================================================
    
    async def _transform_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CVR companies data"""
        self.logger.info("Transforming companies data...")
        
        # Select and rename columns based on schema mapping
        result = pd.DataFrame({
            'cvr_number': df['cvr_number'].astype(str),
            'company_name': df['company_name'].fillna(''),
            'address': df.get('address', '').fillna(''),
            'postal_code': df.get('postal_code', '').fillna(''),
            'city': df.get('city', '').fillna(''),
            'municipality': df.get('municipality_name', '').fillna(''),
            'address_geom': df.get('address_geom_wkt'),  # PostGIS geometry
            'advertisement_protection': df.get('advertisement_protection', False).fillna(False)
        })
        
        # Validate CVR numbers
        result = result[result['cvr_number'].str.match(r'^\d{8}$', na=False)]
        
        return result
    
    async def _transform_field_boundaries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform FVM marker data to field boundaries"""
        self.logger.info("Transforming field boundaries data...")
        
        result = pd.DataFrame({
            'field_identifier': df['field_id'].astype(str),
            'field_name': None,  # Not available in FVM data
            'geom': df['geometry'],  # PostGIS geometry
            'area_ha': df['area_ha'],
            # company_id will be resolved via CVR lookup in database
            '_cvr_number': df['cvr_number'].astype(str)  # Temporary field for lookup
        })
        
        return result.dropna(subset=['field_identifier', 'geom'])
    
    async def _transform_yearly_financials(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CVR financial data"""
        self.logger.info("Transforming yearly financials data...")
        
        result = pd.DataFrame({
            'year': pd.to_datetime(df['reporting_period_end']).dt.year,
            'reporting_period_start': pd.to_datetime(df['reporting_period_start']).dt.date,
            'reporting_period_end': pd.to_datetime(df['reporting_period_end']).dt.date,
            'net_profit_loss': df.get('net_profit_loss'),
            'gross_profit_loss': df.get('gross_profit_loss'),
            'operating_profit_loss': df.get('operating_profit_loss'),
            'profit_loss_before_tax': df.get('profit_loss_before_tax'),
            'employee_benefits_expense': df.get('employee_benefits_expense'),
            'depreciation_expense': df.get('depreciation_expense'),
            'tax_expense': df.get('tax_expense'),
            'total_assets': df.get('total_assets'),
            'total_equity': df.get('total_equity'),
            'current_assets': df.get('current_assets'),
            'noncurrent_assets': df.get('noncurrent_assets'),
            'cash_and_cash_equivalents': df.get('cash_and_cash_equivalents'),
            'contributed_capital': df.get('contributed_capital'),
            'liabilities_other_than_provisions': df.get('liabilities_other_than_provisions'),
            'shortterm_liabilities_other_than_provisions': df.get('shortterm_liabilities_other_than_provisions'),
            'longterm_liabilities_other_than_provisions': df.get('longterm_liabilities_other_than_provisions'),
            'provisions': df.get('provisions'),
            'average_number_of_employees': df.get('average_number_of_employees'),
            'equity_ratio': df.get('equity_ratio'),
            'return_on_assets': df.get('return_on_assets'),
            'publication_type': df.get('publication_type', ''),
            'case_number': df.get('case_number', ''),
            '_cvr_number': df['cvr_number'].astype(str)
        })
        
        return result.dropna(subset=['year', '_cvr_number'])
    
    # Additional transformation functions would be implemented here
    # following the same pattern for each table...
    
    async def _transform_company_leadership(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CVR leadership data"""
        # Implementation based on complex nested JSON structure
        # from leadership_parsed field
        pass
    
    async def _transform_company_owners(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CVR ownership data"""
        # Implementation for ownership percentage extraction
        pass
    
    async def _transform_field_yearly_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform field environmental analysis data"""
        # Implementation for comprehensive field environmental data
        pass
    
    async def _transform_field_bnbo_areas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform BNBO area data"""
        # Implementation for BNBO status and area calculations
        pass
    
    async def _transform_field_wetland_areas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform wetland area data"""
        # Implementation for wetland restoration status
        pass
    
    async def _transform_pesticide_applications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform pesticide proximity analysis data"""
        # Implementation for pesticide application records with BMD risk data
        pass
    
    async def _transform_animal_capacity_log(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CHR herd sizes to animal capacity log"""
        # Implementation for animal capacity tracking
        pass
    
    async def _transform_animal_transports(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CHR transportation analysis"""
        # Implementation for animal transport records
        pass
    
    async def _transform_vet_events(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CHR veterinary events"""
        # Implementation for veterinary health events
        pass
    
    async def _transform_employee_monthly_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform CVR monthly employment data"""
        # Implementation for monthly employee counts
        pass
    
    async def _transform_visa_yearly_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform agricultural visa statistics"""
        # Implementation for work permit data
        pass
    
    async def _transform_incidents(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform worker safety and inspection data"""
        # Implementation for incident tracking
        pass
    
    async def run_full_migration(self, tables: Optional[List[str]] = None) -> Dict[str, MigrationStats]:
        """Run complete migration for specified tables or all tables"""
        if tables is None:
            tables = list(self.table_mappings.keys())
        
        self.logger.info(f"Starting full migration for {len(tables)} tables: {tables}")
        
        # Initialize connections
        await self.initialize()
        
        results = {}
        overall_start = time.time()
        
        try:
            # Migration order based on dependencies (companies first, etc.)
            ordered_tables = self._get_migration_order(tables)
            
            for table_name in ordered_tables:
                try:
                    stats = await self.migrate_table(table_name)
                    results[table_name] = stats
                    
                    # Update migration metadata in database
                    await self._update_migration_metadata(table_name, stats)
                    
                except Exception as e:
                    self.logger.error(f"Failed to migrate {table_name}: {e}")
                    results[table_name] = MigrationStats()
                    results[table_name].add_error(str(e))
        
        finally:
            # Cleanup connections
            if self.connection_pool:
                self.connection_pool.closeall()
            if self.duckdb_conn:
                self.duckdb_conn.close()
        
        # Summary report
        total_duration = time.time() - overall_start
        total_records = sum(stats.total_records for stats in results.values())
        total_successful = sum(stats.successful_records for stats in results.values())
        
        self.logger.info(
            f"Migration completed in {total_duration:.1f}s. "
            f"Successfully migrated {total_successful}/{total_records} records "
            f"({total_successful/total_records*100:.1f}% success rate)"
        )
        
        return results
    
    def _get_migration_order(self, tables: List[str]) -> List[str]:
        """Get migration order based on foreign key dependencies"""
        # Define dependency order (tables that must be migrated first)
        dependency_order = [
            'species',           # Referenced by many animal tables
            'companies',         # Referenced by most tables
            'field_boundaries',  # Referenced by field data tables
            'production_sites',  # Referenced by animal data
            # All other tables can follow
        ]
        
        ordered = []
        for table in dependency_order:
            if table in tables:
                ordered.append(table)
                tables.remove(table)
        
        # Add remaining tables
        ordered.extend(tables)
        return ordered
    
    async def _update_migration_metadata(self, table_name: str, stats: MigrationStats):
        """Update migration metadata in database"""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO migration_metadata 
                (migration_name, source_dataset, target_table, records_processed, 
                 records_success, records_failed, completed_at, status, performance_stats)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                f"parquet_migration_{datetime.now().strftime('%Y%m%d')}",
                self.table_mappings[table_name]['source'],
                table_name,
                stats.processed_records,
                stats.successful_records,
                stats.failed_records,
                datetime.now(),
                'completed' if stats.failed_records == 0 else 'completed_with_errors',
                json.dumps({
                    'duration_seconds': stats.get_duration(),
                    'rate_records_per_second': stats.get_rate(),
                    'batches_completed': stats.batches_completed
                })
            ))
            conn.commit()
            cursor.close()
        except Exception as e:
            self.logger.error(f"Failed to update migration metadata: {e}")
        finally:
            self._return_connection(conn)

def main():
    """Main entry point for migration pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Parquet to Supabase Migration Pipeline')
    parser.add_argument('--host', required=True, help='Supabase host')
    parser.add_argument('--password', required=True, help='Supabase password')
    parser.add_argument('--tables', nargs='*', help='Specific tables to migrate')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without actual migration')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--log-file', help='Log file path')
    
    args = parser.parse_args()
    
    config = MigrationConfig(
        supabase_host=args.host,
        supabase_password=args.password,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        log_level=args.log_level,
        log_file=args.log_file
    )
    
    pipeline = SupabaseMigrationPipeline(config)
    
    # Run migration
    results = asyncio.run(pipeline.run_full_migration(args.tables))
    
    # Print summary
    print("\n" + "="*80)
    print("MIGRATION SUMMARY")
    print("="*80)
    for table, stats in results.items():
        status = "✅ SUCCESS" if stats.failed_records == 0 else "⚠️  PARTIAL" if stats.successful_records > 0 else "❌ FAILED"
        print(f"{status} {table}: {stats.successful_records}/{stats.total_records} records")
        if stats.errors:
            print(f"   Errors: {len(stats.errors)}")

if __name__ == '__main__':
    main()


