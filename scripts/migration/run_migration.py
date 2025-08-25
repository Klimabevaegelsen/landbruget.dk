#!/usr/bin/env python3
"""
Migration Orchestration Script
Manages the complete Parquet to Supabase migration process

Usage:
    python run_migration.py --environment staging --phase schema
    python run_migration.py --environment production --phase data --tables companies field_boundaries
"""

import os
import sys
import asyncio
import argparse
import subprocess
from pathlib import Path
from typing import List, Optional
import logging
from dataclasses import dataclass
from datetime import datetime
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.migration.parquet_to_supabase_etl import SupabaseMigrationPipeline, MigrationConfig

@dataclass
class EnvironmentConfig:
    """Environment-specific configuration"""
    name: str
    supabase_host: str
    supabase_password: str
    gcs_bucket: str
    batch_size: int = 10000
    max_workers: int = 4
    
    @classmethod
    def from_env(cls, env_name: str):
        """Load configuration from environment variables"""
        if env_name == "local":
            return cls(
                name="local",
                supabase_host="127.0.0.1",
                supabase_password=os.getenv("SUPABASE_LOCAL_PASSWORD", "postgres"),
                gcs_bucket="gs://landbrugsdata-raw-data",
                batch_size=5000,  # Smaller batches for local
                max_workers=2
            )
        elif env_name == "staging":
            return cls(
                name="staging",
                supabase_host=os.getenv("SUPABASE_STAGING_HOST"),
                supabase_password=os.getenv("SUPABASE_STAGING_PASSWORD"),
                gcs_bucket="gs://landbrugsdata-raw-data",
                batch_size=10000,
                max_workers=4
            )
        elif env_name == "production":
            return cls(
                name="production",
                supabase_host=os.getenv("SUPABASE_PROD_HOST"),
                supabase_password=os.getenv("SUPABASE_PROD_PASSWORD"),
                gcs_bucket="gs://landbrugsdata-raw-data",
                batch_size=20000,  # Larger batches for production
                max_workers=8
            )
        else:
            raise ValueError(f"Unknown environment: {env_name}")

class MigrationOrchestrator:
    """Orchestrates the complete migration process"""
    
    def __init__(self, env_config: EnvironmentConfig):
        self.env_config = env_config
        self.logger = self._setup_logging()
        
        # Migration phases in order
        self.migration_phases = {
            'schema': {
                'description': 'Apply database schema migrations',
                'function': self._run_schema_migration,
                'required': True
            },
            'reference': {
                'description': 'Load reference data (species, etc.)',
                'function': self._run_reference_data_migration,
                'required': True,
                'tables': ['species']
            },
            'core': {
                'description': 'Load core entity data',
                'function': self._run_core_data_migration,
                'required': True,
                'tables': ['companies', 'field_boundaries', 'production_sites']
            },
            'financial': {
                'description': 'Load financial and company data',
                'function': self._run_financial_data_migration,
                'required': False,
                'tables': ['yearly_financials', 'company_leadership', 'company_owners']
            },
            'field': {
                'description': 'Load field and environmental data',
                'function': self._run_field_data_migration,
                'required': False,
                'tables': ['field_yearly_data', 'field_bnbo_areas', 'field_wetland_areas']
            },
            'pesticide': {
                'description': 'Load pesticide application data',
                'function': self._run_pesticide_data_migration,
                'required': False,
                'tables': ['pesticide_applications']
            },
            'animal': {
                'description': 'Load animal production data',
                'function': self._run_animal_data_migration,
                'required': False,
                'tables': ['animal_capacity_log', 'animal_transports', 'vet_events']
            },
            'worker': {
                'description': 'Load worker and safety data',
                'function': self._run_worker_data_migration,
                'required': False,
                'tables': ['employee_monthly_counts', 'visa_yearly_counts', 'incidents']
            },
            'computed': {
                'description': 'Generate computed tables and views',
                'function': self._run_computed_data_generation,
                'required': False
            },
            'validation': {
                'description': 'Validate migration results',
                'function': self._run_migration_validation,
                'required': True
            }
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for orchestrator"""
        logger = logging.getLogger('migration_orchestrator')
        logger.setLevel(logging.INFO)
        
        # Create logs directory
        log_dir = Path('logs/migration')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # File handler with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"migration_{self.env_config.name}_{timestamp}.log"
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Logging to: {log_file}")
        return logger
    
    async def run_migration(self, phases: List[str], tables: Optional[List[str]] = None, dry_run: bool = False):
        """Run migration for specified phases"""
        self.logger.info(f"Starting migration for environment: {self.env_config.name}")
        self.logger.info(f"Phases: {phases}")
        if tables:
            self.logger.info(f"Tables: {tables}")
        if dry_run:
            self.logger.info("DRY RUN MODE - No actual changes will be made")
        
        overall_start = datetime.now()
        results = {}
        
        try:
            # Validate phases
            invalid_phases = [p for p in phases if p not in self.migration_phases]
            if invalid_phases:
                raise ValueError(f"Invalid phases: {invalid_phases}")
            
            # Run each phase
            for phase in phases:
                phase_config = self.migration_phases[phase]
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"PHASE: {phase.upper()} - {phase_config['description']}")
                self.logger.info(f"{'='*60}")
                
                phase_start = datetime.now()
                
                try:
                    # Filter tables if specified
                    phase_tables = tables
                    if phase_tables and 'tables' in phase_config:
                        phase_tables = [t for t in tables if t in phase_config['tables']]
                        if not phase_tables and tables:
                            self.logger.info(f"No matching tables for phase {phase}, skipping...")
                            continue
                    
                    # Run phase
                    phase_result = await phase_config['function'](phase_tables, dry_run)
                    results[phase] = {
                        'status': 'success',
                        'result': phase_result,
                        'duration': (datetime.now() - phase_start).total_seconds()
                    }
                    
                    self.logger.info(f"✅ Phase {phase} completed successfully")
                    
                except Exception as e:
                    results[phase] = {
                        'status': 'failed',
                        'error': str(e),
                        'duration': (datetime.now() - phase_start).total_seconds()
                    }
                    
                    self.logger.error(f"❌ Phase {phase} failed: {e}")
                    
                    # Stop on required phase failure
                    if phase_config.get('required', False):
                        self.logger.error("Required phase failed, stopping migration")
                        break
        
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise
        
        # Summary report
        total_duration = (datetime.now() - overall_start).total_seconds()
        successful_phases = [p for p, r in results.items() if r['status'] == 'success']
        failed_phases = [p for p, r in results.items() if r['status'] == 'failed']
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info("MIGRATION SUMMARY")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Environment: {self.env_config.name}")
        self.logger.info(f"Total Duration: {total_duration:.1f}s")
        self.logger.info(f"Successful Phases: {len(successful_phases)}/{len(results)}")
        
        if successful_phases:
            self.logger.info(f"✅ Success: {', '.join(successful_phases)}")
        if failed_phases:
            self.logger.info(f"❌ Failed: {', '.join(failed_phases)}")
        
        return results
    
    async def _run_schema_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Apply database schema migrations"""
        self.logger.info("Applying database schema migrations...")
        
        if dry_run:
            self.logger.info("DRY RUN: Would apply schema migrations")
            return {"status": "dry_run"}
        
        # Use Supabase CLI to apply migrations
        try:
            if self.env_config.name == "local":
                # For local, use supabase db reset
                result = subprocess.run(
                    ["supabase", "db", "reset", "--local"],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent.parent.parent
                )
            else:
                # For remote, use supabase db push
                result = subprocess.run(
                    ["supabase", "db", "push", "--password", self.env_config.supabase_password],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent.parent.parent
                )
            
            if result.returncode != 0:
                raise Exception(f"Schema migration failed: {result.stderr}")
            
            self.logger.info("Schema migrations applied successfully")
            return {"status": "success", "output": result.stdout}
            
        except Exception as e:
            self.logger.error(f"Schema migration failed: {e}")
            raise
    
    async def _run_data_migration_phase(self, phase_tables: List[str], dry_run: bool):
        """Generic data migration for a set of tables"""
        config = MigrationConfig(
            supabase_host=self.env_config.supabase_host,
            supabase_password=self.env_config.supabase_password,
            gcs_bucket=self.env_config.gcs_bucket,
            batch_size=self.env_config.batch_size,
            max_workers=self.env_config.max_workers,
            dry_run=dry_run,
            validate_data=True,
            log_level="INFO"
        )
        
        pipeline = SupabaseMigrationPipeline(config)
        results = await pipeline.run_full_migration(phase_tables)
        
        return results
    
    async def _run_reference_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load reference data (species, etc.)"""
        phase_tables = ['species'] if not tables else [t for t in tables if t in ['species']]
        if not phase_tables:
            return {"status": "skipped", "reason": "No reference tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_core_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load core entity data"""
        core_tables = ['companies', 'field_boundaries', 'production_sites']
        phase_tables = core_tables if not tables else [t for t in tables if t in core_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No core tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_financial_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load financial and company data"""
        financial_tables = ['yearly_financials', 'company_leadership', 'company_owners']
        phase_tables = financial_tables if not tables else [t for t in tables if t in financial_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No financial tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_field_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load field and environmental data"""
        field_tables = ['field_yearly_data', 'field_bnbo_areas', 'field_wetland_areas']
        phase_tables = field_tables if not tables else [t for t in tables if t in field_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No field tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_pesticide_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load pesticide application data"""
        pesticide_tables = ['pesticide_applications']
        phase_tables = pesticide_tables if not tables else [t for t in tables if t in pesticide_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No pesticide tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_animal_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load animal production data"""
        animal_tables = ['animal_capacity_log', 'animal_transports', 'vet_events']
        phase_tables = animal_tables if not tables else [t for t in tables if t in animal_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No animal tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_worker_data_migration(self, tables: Optional[List[str]], dry_run: bool):
        """Load worker and safety data"""
        worker_tables = ['employee_monthly_counts', 'visa_yearly_counts', 'incidents']
        phase_tables = worker_tables if not tables else [t for t in tables if t in worker_tables]
        if not phase_tables:
            return {"status": "skipped", "reason": "No worker tables specified"}
        
        return await self._run_data_migration_phase(phase_tables, dry_run)
    
    async def _run_computed_data_generation(self, tables: Optional[List[str]], dry_run: bool):
        """Generate computed tables and refresh materialized views"""
        self.logger.info("Generating computed data and refreshing materialized views...")
        
        if dry_run:
            self.logger.info("DRY RUN: Would generate computed data")
            return {"status": "dry_run"}
        
        # Refresh materialized views
        materialized_views = [
            'animal_transport_weekly_summary',
            'animal_welfare_summary',
            'bnbo_summary',
            'carbon_emission_details_yearly',
            'carbon_summary',
            'land_use_summary',
            'environment_summary',
            'site_details_summary_ranked',
            'site_species_production_ranked',
            'wetlands_summary'
        ]
        
        # Connect and refresh views
        config = MigrationConfig(
            supabase_host=self.env_config.supabase_host,
            supabase_password=self.env_config.supabase_password
        )
        
        pipeline = SupabaseMigrationPipeline(config)
        await pipeline.initialize()
        
        conn = pipeline._get_connection()
        try:
            cursor = conn.cursor()
            
            for view in materialized_views:
                try:
                    self.logger.info(f"Refreshing materialized view: {view}")
                    cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
                    conn.commit()
                except Exception as e:
                    self.logger.warning(f"Failed to refresh view {view}: {e}")
            
            cursor.close()
            
        finally:
            pipeline._return_connection(conn)
            pipeline.connection_pool.closeall()
        
        return {"status": "success", "views_refreshed": materialized_views}
    
    async def _run_migration_validation(self, tables: Optional[List[str]], dry_run: bool):
        """Validate migration results"""
        self.logger.info("Validating migration results...")
        
        if dry_run:
            self.logger.info("DRY RUN: Would validate migration")
            return {"status": "dry_run"}
        
        # Run validation queries
        validation_queries = {
            'companies_count': "SELECT COUNT(*) FROM companies",
            'field_boundaries_count': "SELECT COUNT(*) FROM field_boundaries",
            'production_sites_count': "SELECT COUNT(*) FROM production_sites",
            'foreign_key_integrity': """
                SELECT 
                    'field_boundaries' as table_name,
                    COUNT(*) as records_with_invalid_fk
                FROM field_boundaries fb
                LEFT JOIN companies c ON fb.company_id = c.id
                WHERE c.id IS NULL
                UNION ALL
                SELECT 
                    'production_sites' as table_name,
                    COUNT(*) as records_with_invalid_fk
                FROM production_sites ps
                LEFT JOIN companies c ON ps.company_id = c.id
                WHERE c.id IS NULL
            """,
            'data_quality_check': """
                SELECT 
                    'companies' as table_name,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN cvr_number ~ '^[0-9]{8}$' THEN 1 END) as valid_cvr_format,
                    COUNT(CASE WHEN company_name IS NOT NULL AND company_name != '' THEN 1 END) as has_company_name
                FROM companies
            """
        }
        
        config = MigrationConfig(
            supabase_host=self.env_config.supabase_host,
            supabase_password=self.env_config.supabase_password
        )
        
        pipeline = SupabaseMigrationPipeline(config)
        await pipeline.initialize()
        
        conn = pipeline._get_connection()
        validation_results = {}
        
        try:
            cursor = conn.cursor()
            
            for check_name, query in validation_queries.items():
                try:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    validation_results[check_name] = results
                    self.logger.info(f"Validation {check_name}: {results}")
                except Exception as e:
                    validation_results[check_name] = f"Error: {e}"
                    self.logger.error(f"Validation {check_name} failed: {e}")
            
            cursor.close()
            
        finally:
            pipeline._return_connection(conn)
            pipeline.connection_pool.closeall()
        
        return {"status": "success", "validation_results": validation_results}

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Parquet to Supabase Migration Orchestrator')
    parser.add_argument('--environment', required=True, choices=['local', 'staging', 'production'],
                       help='Target environment')
    parser.add_argument('--phase', nargs='+', required=True,
                       choices=['schema', 'reference', 'core', 'financial', 'field', 'pesticide', 'animal', 'worker', 'computed', 'validation', 'all'],
                       help='Migration phases to run')
    parser.add_argument('--tables', nargs='*', help='Specific tables to migrate')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without actual changes')
    
    args = parser.parse_args()
    
    # Handle 'all' phase
    if 'all' in args.phase:
        args.phase = ['schema', 'reference', 'core', 'financial', 'field', 'pesticide', 'animal', 'worker', 'computed', 'validation']
    
    # Load environment configuration
    try:
        env_config = EnvironmentConfig.from_env(args.environment)
    except Exception as e:
        print(f"Failed to load environment configuration: {e}")
        sys.exit(1)
    
    # Run migration
    orchestrator = MigrationOrchestrator(env_config)
    
    try:
        results = asyncio.run(orchestrator.run_migration(args.phase, args.tables, args.dry_run))
        
        # Exit with error code if any required phase failed
        failed_phases = [p for p, r in results.items() if r['status'] == 'failed']
        if failed_phases:
            print(f"Migration failed with {len(failed_phases)} failed phases")
            sys.exit(1)
        else:
            print("Migration completed successfully")
            sys.exit(0)
            
    except Exception as e:
        print(f"Migration orchestration failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()


