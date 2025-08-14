#!/usr/bin/env python3
"""
Migration Validation Framework
Comprehensive validation and testing for Parquet to Supabase migration

Features:
- Data integrity validation
- Schema compliance checks
- Performance benchmarks
- Data quality assessment
- Referential integrity validation
- Business rule validation
"""

import asyncio
import logging
import pandas as pd
import duckdb
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
import json
import psycopg2
from pathlib import Path
import hashlib
import statistics

@dataclass
class ValidationResult:
    """Result of a validation check"""
    check_name: str
    status: str  # 'pass', 'fail', 'warning', 'error'
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict:
        return {
            'check_name': self.check_name,
            'status': self.status,
            'message': self.message,
            'details': self.details,
            'execution_time_ms': self.execution_time_ms,
            'timestamp': self.timestamp.isoformat()
        }

@dataclass
class ValidationSuite:
    """Collection of validation results"""
    name: str
    results: List[ValidationResult] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def add_result(self, result: ValidationResult):
        self.results.append(result)
    
    def complete(self):
        self.end_time = datetime.now()
    
    def get_summary(self) -> Dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == 'pass')
        failed = sum(1 for r in self.results if r.status == 'fail')
        warnings = sum(1 for r in self.results if r.status == 'warning')
        errors = sum(1 for r in self.results if r.status == 'error')
        
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        
        return {
            'suite_name': self.name,
            'total_checks': total,
            'passed': passed,
            'failed': failed,
            'warnings': warnings,
            'errors': errors,
            'success_rate': (passed / total * 100) if total > 0 else 0,
            'duration_seconds': duration,
            'status': 'pass' if failed == 0 and errors == 0 else 'fail'
        }

class MigrationValidator:
    """Comprehensive validation framework for migration"""
    
    def __init__(self, 
                 supabase_host: str,
                 supabase_password: str,
                 gcs_bucket: str = "gs://landbrugsdata-raw-data"):
        self.supabase_host = supabase_host
        self.supabase_password = supabase_password
        self.gcs_bucket = gcs_bucket
        
        self.logger = logging.getLogger('migration_validator')
        self.db_conn = None
        self.duckdb_conn = None
        
        # Validation configurations based on schema analysis
        self.table_validations = {
            'companies': {
                'required_fields': ['cvr_number', 'company_name'],
                'unique_fields': ['cvr_number'],
                'data_quality_checks': [
                    ('cvr_format', lambda df: df['cvr_number'].str.match(r'^\d{8}$').all()),
                    ('non_empty_name', lambda df: (df['company_name'].str.len() > 0).all())
                ],
                'expected_count_min': 10000,  # Based on analysis: expect significant company data
                'foreign_keys': []
            },
            'field_boundaries': {
                'required_fields': ['company_id', 'field_identifier', 'geom', 'area_ha'],
                'unique_fields': ['company_id', 'field_identifier'],
                'data_quality_checks': [
                    ('positive_area', lambda df: (df['area_ha'] > 0).all()),
                    ('valid_geometry', lambda df: df['geom'].notna().all())
                ],
                'expected_count_min': 500000,  # Based on analysis: 697k+ field records
                'foreign_keys': [('company_id', 'companies', 'id')]
            },
            'production_sites': {
                'required_fields': ['chr', 'company_id', 'site_name'],
                'unique_fields': ['chr'],
                'data_quality_checks': [
                    ('chr_format', lambda df: df['chr'].str.startswith('CHR').all()),
                    ('valid_coordinates', lambda df: df['location_geom'].notna().sum() > len(df) * 0.8)  # 80% should have coordinates
                ],
                'expected_count_min': 20000,  # CHR sites
                'foreign_keys': [('company_id', 'companies', 'id')]
            },
            'yearly_financials': {
                'required_fields': ['company_id', 'year'],
                'unique_fields': ['company_id', 'year'],
                'data_quality_checks': [
                    ('valid_year', lambda df: ((df['year'] >= 2016) & (df['year'] <= 2025)).all()),
                    ('reasonable_values', lambda df: df['total_assets'].abs().max() < 1e12 if 'total_assets' in df.columns else True)
                ],
                'expected_count_min': 10000,  # Based on analysis: 11,866 financial records
                'foreign_keys': [('company_id', 'companies', 'id')]
            },
            'pesticide_applications': {
                'required_fields': ['company_id', 'year', 'pesticide_name'],
                'unique_fields': [],  # Multiple applications per company/year allowed
                'data_quality_checks': [
                    ('valid_year', lambda df: ((df['year'] >= 2023) & (df['year'] <= 2024)).all()),
                    ('positive_area', lambda df: (df['ha_sprayed'] > 0).all() if 'ha_sprayed' in df.columns else True),
                    ('proximity_reasonable', lambda df: (df['proximity_water_m'] >= 0).all() if 'proximity_water_m' in df.columns else True)
                ],
                'expected_count_min': 1000000,  # Based on analysis: 24.9M records
                'foreign_keys': [('company_id', 'companies', 'id')]
            },
            'animal_capacity_log': {
                'required_fields': ['chr', 'species_name', 'capacity_count'],
                'unique_fields': [],  # Multiple capacity records per CHR
                'data_quality_checks': [
                    ('positive_capacity', lambda df: (df['capacity_count'] > 0).all()),
                    ('reasonable_capacity', lambda df: df['capacity_count'].max() < 100000000)  # Max 100M capacity
                ],
                'expected_count_min': 100000,  # Based on analysis: 133,339 capacity records
                'foreign_keys': [('chr', 'production_sites', 'chr')]
            }
        }
    
    async def initialize(self):
        """Initialize database connections"""
        try:
            # PostgreSQL connection
            self.db_conn = psycopg2.connect(
                host=self.supabase_host,
                port=5432,
                database="postgres",
                user="postgres",
                password=self.supabase_password
            )
            
            # DuckDB for parquet analysis
            self.duckdb_conn = duckdb.connect(':memory:')
            self.duckdb_conn.execute("INSTALL spatial")
            self.duckdb_conn.execute("LOAD spatial")
            
            self.logger.info("Validation connections initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize connections: {e}")
            raise
    
    async def run_full_validation(self) -> ValidationSuite:
        """Run complete validation suite"""
        suite = ValidationSuite("complete_migration_validation")
        
        self.logger.info("Starting complete migration validation...")
        
        try:
            await self.initialize()
            
            # 1. Schema validation
            schema_results = await self._validate_schema()
            suite.results.extend(schema_results)
            
            # 2. Data integrity validation
            integrity_results = await self._validate_data_integrity()
            suite.results.extend(integrity_results)
            
            # 3. Data quality validation
            quality_results = await self._validate_data_quality()
            suite.results.extend(quality_results)
            
            # 4. Referential integrity validation
            ref_integrity_results = await self._validate_referential_integrity()
            suite.results.extend(ref_integrity_results)
            
            # 5. Business rule validation
            business_results = await self._validate_business_rules()
            suite.results.extend(business_results)
            
            # 6. Performance validation
            performance_results = await self._validate_performance()
            suite.results.extend(performance_results)
            
            # 7. Data completeness validation
            completeness_results = await self._validate_data_completeness()
            suite.results.extend(completeness_results)
            
        except Exception as e:
            suite.add_result(ValidationResult(
                check_name="validation_framework_error",
                status="error",
                message=f"Validation framework error: {e}"
            ))
        
        finally:
            if self.db_conn:
                self.db_conn.close()
            if self.duckdb_conn:
                self.duckdb_conn.close()
        
        suite.complete()
        
        summary = suite.get_summary()
        self.logger.info(
            f"Validation completed: {summary['passed']}/{summary['total_checks']} passed "
            f"({summary['success_rate']:.1f}% success rate)"
        )
        
        return suite
    
    async def _validate_schema(self) -> List[ValidationResult]:
        """Validate database schema matches expected structure"""
        results = []
        
        self.logger.info("Validating database schema...")
        
        try:
            cursor = self.db_conn.cursor()
            
            # Check if all expected tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            
            existing_tables = [row[0] for row in cursor.fetchall()]
            expected_tables = list(self.table_validations.keys())
            
            missing_tables = set(expected_tables) - set(existing_tables)
            if missing_tables:
                results.append(ValidationResult(
                    check_name="missing_tables",
                    status="fail",
                    message=f"Missing tables: {missing_tables}",
                    details={"missing_tables": list(missing_tables)}
                ))
            else:
                results.append(ValidationResult(
                    check_name="all_tables_exist",
                    status="pass",
                    message="All expected tables exist"
                ))
            
            # Check table columns for each table
            for table_name in expected_tables:
                if table_name in existing_tables:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                        ORDER BY ordinal_position
                    """, (table_name,))
                    
                    columns = cursor.fetchall()
                    column_names = [col[0] for col in columns]
                    
                    # Check required fields
                    required_fields = self.table_validations[table_name]['required_fields']
                    missing_fields = set(required_fields) - set(column_names)
                    
                    if missing_fields:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_missing_columns",
                            status="fail",
                            message=f"Table {table_name} missing required columns: {missing_fields}",
                            details={"missing_columns": list(missing_fields)}
                        ))
                    else:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_schema_valid",
                            status="pass",
                            message=f"Table {table_name} has all required columns"
                        ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="schema_validation_error",
                status="error",
                message=f"Schema validation failed: {e}"
            ))
        
        return results
    
    async def _validate_data_integrity(self) -> List[ValidationResult]:
        """Validate data integrity (uniqueness, nulls, etc.)"""
        results = []
        
        self.logger.info("Validating data integrity...")
        
        try:
            cursor = self.db_conn.cursor()
            
            for table_name, config in self.table_validations.items():
                # Check record count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                
                min_expected = config.get('expected_count_min', 0)
                if record_count < min_expected:
                    results.append(ValidationResult(
                        check_name=f"{table_name}_record_count",
                        status="warning",
                        message=f"Table {table_name} has {record_count} records, expected at least {min_expected}",
                        details={"actual_count": record_count, "expected_min": min_expected}
                    ))
                else:
                    results.append(ValidationResult(
                        check_name=f"{table_name}_record_count",
                        status="pass",
                        message=f"Table {table_name} has sufficient records: {record_count}"
                    ))
                
                # Check uniqueness constraints
                unique_fields = config.get('unique_fields', [])
                if unique_fields:
                    fields_str = ', '.join(unique_fields)
                    cursor.execute(f"""
                        SELECT COUNT(*) as total_count,
                               COUNT(DISTINCT ({fields_str})) as unique_count
                        FROM {table_name}
                        WHERE {' AND '.join([f'{field} IS NOT NULL' for field in unique_fields])}
                    """)
                    
                    total, unique = cursor.fetchone()
                    if total != unique:
                        duplicates = total - unique
                        results.append(ValidationResult(
                            check_name=f"{table_name}_uniqueness",
                            status="fail",
                            message=f"Table {table_name} has {duplicates} duplicate records on {fields_str}",
                            details={"duplicate_count": duplicates, "unique_fields": unique_fields}
                        ))
                    else:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_uniqueness",
                            status="pass",
                            message=f"Table {table_name} uniqueness constraint satisfied for {fields_str}"
                        ))
                
                # Check required field nulls
                required_fields = config.get('required_fields', [])
                for field in required_fields:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {field} IS NULL")
                    null_count = cursor.fetchone()[0]
                    
                    if null_count > 0:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_{field}_nulls",
                            status="fail",
                            message=f"Table {table_name} has {null_count} NULL values in required field {field}",
                            details={"null_count": null_count, "field": field}
                        ))
                    else:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_{field}_nulls",
                            status="pass",
                            message=f"Required field {field} in {table_name} has no NULL values"
                        ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="data_integrity_error",
                status="error",
                message=f"Data integrity validation failed: {e}"
            ))
        
        return results
    
    async def _validate_data_quality(self) -> List[ValidationResult]:
        """Validate data quality using custom checks"""
        results = []
        
        self.logger.info("Validating data quality...")
        
        try:
            for table_name, config in self.table_validations.items():
                quality_checks = config.get('data_quality_checks', [])
                
                if quality_checks:
                    # Load table data for quality checks
                    query = f"SELECT * FROM {table_name} LIMIT 10000"  # Sample for performance
                    df = pd.read_sql(query, self.db_conn)
                    
                    if df.empty:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_quality_no_data",
                            status="warning",
                            message=f"No data in {table_name} for quality checks"
                        ))
                        continue
                    
                    for check_name, check_func in quality_checks:
                        try:
                            passed = check_func(df)
                            
                            if passed:
                                results.append(ValidationResult(
                                    check_name=f"{table_name}_{check_name}",
                                    status="pass",
                                    message=f"Data quality check {check_name} passed for {table_name}"
                                ))
                            else:
                                results.append(ValidationResult(
                                    check_name=f"{table_name}_{check_name}",
                                    status="fail",
                                    message=f"Data quality check {check_name} failed for {table_name}",
                                    details={"sample_size": len(df)}
                                ))
                        
                        except Exception as e:
                            results.append(ValidationResult(
                                check_name=f"{table_name}_{check_name}",
                                status="error",
                                message=f"Data quality check {check_name} error for {table_name}: {e}"
                            ))
        
        except Exception as e:
            results.append(ValidationResult(
                check_name="data_quality_error",
                status="error",
                message=f"Data quality validation failed: {e}"
            ))
        
        return results
    
    async def _validate_referential_integrity(self) -> List[ValidationResult]:
        """Validate foreign key relationships"""
        results = []
        
        self.logger.info("Validating referential integrity...")
        
        try:
            cursor = self.db_conn.cursor()
            
            for table_name, config in self.table_validations.items():
                foreign_keys = config.get('foreign_keys', [])
                
                for fk_field, ref_table, ref_field in foreign_keys:
                    # Check for orphaned records
                    cursor.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name} t
                        LEFT JOIN {ref_table} r ON t.{fk_field} = r.{ref_field}
                        WHERE t.{fk_field} IS NOT NULL AND r.{ref_field} IS NULL
                    """)
                    
                    orphaned_count = cursor.fetchone()[0]
                    
                    if orphaned_count > 0:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_{fk_field}_referential_integrity",
                            status="fail",
                            message=f"Table {table_name} has {orphaned_count} orphaned records for foreign key {fk_field} -> {ref_table}.{ref_field}",
                            details={
                                "orphaned_count": orphaned_count,
                                "foreign_key": fk_field,
                                "reference_table": ref_table,
                                "reference_field": ref_field
                            }
                        ))
                    else:
                        results.append(ValidationResult(
                            check_name=f"{table_name}_{fk_field}_referential_integrity",
                            status="pass",
                            message=f"Referential integrity maintained for {table_name}.{fk_field} -> {ref_table}.{ref_field}"
                        ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="referential_integrity_error",
                status="error",
                message=f"Referential integrity validation failed: {e}"
            ))
        
        return results
    
    async def _validate_business_rules(self) -> List[ValidationResult]:
        """Validate business-specific rules based on domain knowledge"""
        results = []
        
        self.logger.info("Validating business rules...")
        
        try:
            cursor = self.db_conn.cursor()
            
            # Business Rule 1: Companies should have valid CVR numbers
            cursor.execute("""
                SELECT COUNT(*) FROM companies 
                WHERE cvr_number !~ '^[0-9]{8}$' OR cvr_number IS NULL
            """)
            invalid_cvr_count = cursor.fetchone()[0]
            
            if invalid_cvr_count > 0:
                results.append(ValidationResult(
                    check_name="companies_valid_cvr_format",
                    status="fail",
                    message=f"{invalid_cvr_count} companies have invalid CVR number format",
                    details={"invalid_count": invalid_cvr_count}
                ))
            else:
                results.append(ValidationResult(
                    check_name="companies_valid_cvr_format",
                    status="pass",
                    message="All companies have valid CVR number format"
                ))
            
            # Business Rule 2: Field areas should be reasonable
            cursor.execute("""
                SELECT COUNT(*) FROM field_boundaries 
                WHERE area_ha <= 0 OR area_ha > 10000
            """)
            unreasonable_areas = cursor.fetchone()[0]
            
            if unreasonable_areas > 0:
                results.append(ValidationResult(
                    check_name="field_boundaries_reasonable_areas",
                    status="warning",
                    message=f"{unreasonable_areas} fields have unreasonable areas (<=0 or >10,000 ha)",
                    details={"unreasonable_count": unreasonable_areas}
                ))
            else:
                results.append(ValidationResult(
                    check_name="field_boundaries_reasonable_areas",
                    status="pass",
                    message="All field boundaries have reasonable areas"
                ))
            
            # Business Rule 3: Animal capacity should be reasonable
            cursor.execute("""
                SELECT COUNT(*) FROM animal_capacity_log 
                WHERE capacity_count <= 0 OR capacity_count > 50000000
            """)
            unreasonable_capacity = cursor.fetchone()[0]
            
            if unreasonable_capacity > 0:
                results.append(ValidationResult(
                    check_name="animal_capacity_reasonable",
                    status="warning",
                    message=f"{unreasonable_capacity} animal capacity records are unreasonable (<=0 or >50M)",
                    details={"unreasonable_count": unreasonable_capacity}
                ))
            else:
                results.append(ValidationResult(
                    check_name="animal_capacity_reasonable",
                    status="pass",
                    message="All animal capacity records are within reasonable ranges"
                ))
            
            # Business Rule 4: Pesticide applications should have reasonable dates
            cursor.execute("""
                SELECT COUNT(*) FROM pesticide_applications 
                WHERE application_date < '2020-01-01' OR application_date > CURRENT_DATE + INTERVAL '1 year'
            """)
            unreasonable_dates = cursor.fetchone()[0]
            
            if unreasonable_dates > 0:
                results.append(ValidationResult(
                    check_name="pesticide_applications_reasonable_dates",
                    status="fail",
                    message=f"{unreasonable_dates} pesticide applications have unreasonable dates",
                    details={"unreasonable_count": unreasonable_dates}
                ))
            else:
                results.append(ValidationResult(
                    check_name="pesticide_applications_reasonable_dates",
                    status="pass",
                    message="All pesticide applications have reasonable dates"
                ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="business_rules_error",
                status="error",
                message=f"Business rules validation failed: {e}"
            ))
        
        return results
    
    async def _validate_performance(self) -> List[ValidationResult]:
        """Validate database performance and query efficiency"""
        results = []
        
        self.logger.info("Validating performance...")
        
        try:
            cursor = self.db_conn.cursor()
            
            # Test query performance on key tables
            performance_tests = [
                ("companies_lookup", "SELECT * FROM companies WHERE cvr_number = '12345678'", 100),  # 100ms threshold
                ("field_boundaries_spatial", "SELECT COUNT(*) FROM field_boundaries WHERE ST_Area(geom) > 0", 1000),  # 1s threshold
                ("pesticide_applications_year", "SELECT COUNT(*) FROM pesticide_applications WHERE year = 2024", 500),  # 500ms threshold
                ("production_sites_company", "SELECT * FROM production_sites WHERE company_id = (SELECT id FROM companies LIMIT 1)", 200)  # 200ms threshold
            ]
            
            for test_name, query, threshold_ms in performance_tests:
                try:
                    start_time = datetime.now()
                    cursor.execute(query)
                    cursor.fetchall()  # Ensure all data is fetched
                    duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                    
                    if duration_ms > threshold_ms:
                        results.append(ValidationResult(
                            check_name=f"performance_{test_name}",
                            status="warning",
                            message=f"Query {test_name} took {duration_ms:.1f}ms (threshold: {threshold_ms}ms)",
                            details={"duration_ms": duration_ms, "threshold_ms": threshold_ms}
                        ))
                    else:
                        results.append(ValidationResult(
                            check_name=f"performance_{test_name}",
                            status="pass",
                            message=f"Query {test_name} performed well: {duration_ms:.1f}ms",
                            details={"duration_ms": duration_ms}
                        ))
                
                except Exception as e:
                    results.append(ValidationResult(
                        check_name=f"performance_{test_name}",
                        status="error",
                        message=f"Performance test {test_name} failed: {e}"
                    ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="performance_validation_error",
                status="error",
                message=f"Performance validation failed: {e}"
            ))
        
        return results
    
    async def _validate_data_completeness(self) -> List[ValidationResult]:
        """Validate data completeness compared to source parquet files"""
        results = []
        
        self.logger.info("Validating data completeness...")
        
        # This would compare record counts between source parquet and migrated data
        # For now, we'll do basic completeness checks
        
        try:
            cursor = self.db_conn.cursor()
            
            # Check for tables with no data
            empty_tables = []
            for table_name in self.table_validations.keys():
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                
                if count == 0:
                    empty_tables.append(table_name)
            
            if empty_tables:
                results.append(ValidationResult(
                    check_name="data_completeness_empty_tables",
                    status="fail",
                    message=f"Empty tables found: {empty_tables}",
                    details={"empty_tables": empty_tables}
                ))
            else:
                results.append(ValidationResult(
                    check_name="data_completeness_no_empty_tables",
                    status="pass",
                    message="No empty tables found"
                ))
            
            # Check data distribution
            cursor.execute("""
                SELECT 
                    'companies' as table_name, COUNT(*) as record_count
                FROM companies
                UNION ALL
                SELECT 
                    'field_boundaries' as table_name, COUNT(*) as record_count  
                FROM field_boundaries
                UNION ALL
                SELECT 
                    'pesticide_applications' as table_name, COUNT(*) as record_count
                FROM pesticide_applications
            """)
            
            distributions = cursor.fetchall()
            results.append(ValidationResult(
                check_name="data_completeness_distribution",
                status="pass",
                message="Data distribution recorded",
                details={"table_counts": {table: count for table, count in distributions}}
            ))
            
            cursor.close()
            
        except Exception as e:
            results.append(ValidationResult(
                check_name="data_completeness_error",
                status="error",
                message=f"Data completeness validation failed: {e}"
            ))
        
        return results
    
    def export_validation_report(self, suite: ValidationSuite, filepath: str):
        """Export validation results to JSON report"""
        report = {
            "validation_report": {
                "generated_at": datetime.now().isoformat(),
                "suite_summary": suite.get_summary(),
                "results": [result.to_dict() for result in suite.results]
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Validation report exported to {filepath}")
    
    def generate_validation_summary_html(self, suite: ValidationSuite, filepath: str):
        """Generate HTML validation report"""
        summary = suite.get_summary()
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Migration Validation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .summary {{ margin: 20px 0; }}
                .pass {{ color: green; }}
                .fail {{ color: red; }}
                .warning {{ color: orange; }}
                .error {{ color: darkred; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Migration Validation Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>Suite:</strong> {summary['suite_name']}</p>
                <p><strong>Total Checks:</strong> {summary['total_checks']}</p>
                <p><strong>Passed:</strong> <span class="pass">{summary['passed']}</span></p>
                <p><strong>Failed:</strong> <span class="fail">{summary['failed']}</span></p>
                <p><strong>Warnings:</strong> <span class="warning">{summary['warnings']}</span></p>
                <p><strong>Errors:</strong> <span class="error">{summary['errors']}</span></p>
                <p><strong>Success Rate:</strong> {summary['success_rate']:.1f}%</p>
                <p><strong>Duration:</strong> {summary['duration_seconds']:.1f} seconds</p>
            </div>
            
            <div class="results">
                <h2>Detailed Results</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Check Name</th>
                            <th>Status</th>
                            <th>Message</th>
                            <th>Execution Time (ms)</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for result in suite.results:
            status_class = result.status
            html_content += f"""
                        <tr>
                            <td>{result.check_name}</td>
                            <td class="{status_class}">{result.status.upper()}</td>
                            <td>{result.message}</td>
                            <td>{result.execution_time_ms:.1f}</td>
                        </tr>
            """
        
        html_content += """
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML validation report generated: {filepath}")

# CLI interface for running validation
async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Migration Validation Framework')
    parser.add_argument('--host', required=True, help='Supabase host')
    parser.add_argument('--password', required=True, help='Supabase password')
    parser.add_argument('--output', default='validation_report.json', help='Output file for validation report')
    parser.add_argument('--html', help='Generate HTML report at specified path')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Run validation
    validator = MigrationValidator(
        supabase_host=args.host,
        supabase_password=args.password
    )
    
    suite = await validator.run_full_validation()
    
    # Export reports
    validator.export_validation_report(suite, args.output)
    
    if args.html:
        validator.generate_validation_summary_html(suite, args.html)
    
    # Print summary
    summary = suite.get_summary()
    print(f"\nValidation Summary:")
    print(f"  Total Checks: {summary['total_checks']}")
    print(f"  Passed: {summary['passed']}")
    print(f"  Failed: {summary['failed']}")
    print(f"  Warnings: {summary['warnings']}")
    print(f"  Errors: {summary['errors']}")
    print(f"  Success Rate: {summary['success_rate']:.1f}%")
    
    # Exit with appropriate code
    exit_code = 0 if summary['status'] == 'pass' else 1
    exit(exit_code)

if __name__ == '__main__':
    asyncio.run(main())


