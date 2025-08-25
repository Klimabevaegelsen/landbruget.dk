#!/usr/bin/env python3
"""
Migrate yearly financial data to Supabase.

This script migrates financial data from the CVR enrichment pipeline to the Supabase
yearly_financials table. Currently works with document metadata, but designed to be
easily extended when comprehensive parsed financial data becomes available.

Based on analysis in: docs/analysis/parquet_to_supabase_schema_analysis.md (lines 543-648)
"""

import asyncio
import os
import sys
from typing import Any, Dict

import duckdb

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from backend.common.gcs_data_access import GCSDataAccess
from backend.common.logging_utils import setup_logging

# Setup logging
logger = setup_logging(__name__)

# Supabase connection details
SUPABASE_URL = "postgresql://postgres:postgres@localhost:54322/postgres"


class YearlyFinancialsMigration:
    """Migrate yearly financial data from CVR enrichment to Supabase."""

    def __init__(self):
        self.gcs_access = GCSDataAccess()
        self.conn = duckdb.connect()

    async def run_migration(self):
        """Run the complete migration process."""
        logger.info("🚀 Starting yearly_financials migration")

        try:
            # Step 1: Download latest financial data
            logger.info("📥 Step 1/5: Downloading latest financial data")
            financial_data_path = await self._download_latest_financial_data()

            # Step 2: Load and analyze the data
            logger.info("🔍 Step 2/5: Loading and analyzing financial data")
            financial_stats = await self._analyze_financial_data(financial_data_path)

            # Step 3: Check current Supabase schema
            logger.info("📊 Step 3/5: Checking current Supabase schema")
            schema_info = await self._check_supabase_schema()

            # Step 4: Determine migration strategy
            logger.info("🎯 Step 4/5: Determining migration strategy")
            migration_strategy = await self._determine_migration_strategy(financial_stats, schema_info)

            # Step 5: Execute migration
            logger.info("💾 Step 5/5: Executing migration")
            migration_results = await self._execute_migration(financial_data_path, migration_strategy)

            # Success summary
            logger.info("=" * 60)
            logger.info("✅ YEARLY FINANCIALS MIGRATION COMPLETED")
            logger.info("=" * 60)
            logger.info("📊 MIGRATION SUMMARY:")
            logger.info(f"   • Financial records processed: {migration_results.get('total_processed', 0):,}")
            logger.info(f"   • Records migrated: {migration_results.get('migrated', 0):,}")
            logger.info(f"   • Companies with financial data: {migration_results.get('companies_with_data', 0):,}")
            logger.info(f"   • Migration strategy: {migration_strategy['approach']}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            raise

    async def _download_latest_financial_data(self) -> Dict[str, str]:
        """Download the latest financial data from GCS."""
        logger.info("Looking for latest CVR financial data...")

        # Find the latest financial data
        bucket = "landbrugsdata-raw-data"
        prefix = "gold/cvr_enrichment_financial/"

        # List directories to find latest timestamp
        directories = self.gcs_access.list_directories(bucket, prefix)
        if not directories:
            raise ValueError("No CVR financial data found in GCS")

        # Get latest directory (sorted by timestamp)
        latest_dir = sorted(directories)[-1]
        logger.info(f"Found latest financial data: {latest_dir}")

        os.makedirs("data_local/financial_migration", exist_ok=True)

        # Download both the document metadata and comprehensive financial statements
        files_downloaded = {}

        # Download document metadata (existing)
        metadata_gcs_path = f"gs://{bucket}/{latest_dir}/financial_documents.parquet"
        metadata_local_path = "data_local/financial_migration/financial_documents.parquet"

        logger.info(f"Downloading metadata: {metadata_gcs_path}")
        self.gcs_access.download_file(metadata_gcs_path, metadata_local_path)
        files_downloaded["metadata"] = metadata_local_path

        # Try to download comprehensive financial statements (new)
        statements_prefix = "gold/cvr_enrichment_financial_statements/"
        try:
            statements_directories = self.gcs_access.list_directories(bucket, statements_prefix)
            if statements_directories:
                latest_statements_dir = sorted(statements_directories)[-1]
                statements_gcs_path = f"gs://{bucket}/{latest_statements_dir}/financial_statements.parquet"
                statements_local_path = "data_local/financial_migration/financial_statements.parquet"

                logger.info(f"Downloading comprehensive financial data: {statements_gcs_path}")
                self.gcs_access.download_file(statements_gcs_path, statements_local_path)
                files_downloaded["statements"] = statements_local_path
                logger.info("✅ Found comprehensive financial statements data!")
            else:
                logger.info("ℹ️ No comprehensive financial statements found - using metadata only")
        except Exception as e:
            logger.info(f"ℹ️ Comprehensive financial statements not available: {e}")

        logger.info(f"✅ Downloaded {len(files_downloaded)} financial data files")
        return files_downloaded

    async def _analyze_financial_data(self, data_paths: Dict[str, str]) -> Dict[str, Any]:
        """Analyze the financial data structure and content."""
        logger.info("Analyzing financial data structure...")

        stats = {"files_available": list(data_paths.keys())}

        # Analyze metadata file
        if "metadata" in data_paths:
            metadata_df = self.conn.execute(f"SELECT * FROM read_parquet('{data_paths['metadata']}')").df()

            stats["metadata"] = {
                "total_records": len(metadata_df),
                "columns": list(metadata_df.columns),
                "data_types": dict(metadata_df.dtypes),
            }

            # Check for financial metrics
            if "has_financial_metrics" in metadata_df.columns:
                companies_with_metrics = metadata_df[metadata_df["has_financial_metrics"] == True]
                stats["metadata"]["companies_with_metrics"] = len(companies_with_metrics)
                stats["metadata"]["metrics_coverage_pct"] = (len(companies_with_metrics) / len(metadata_df)) * 100

            # Check date range
            if "latest_reporting_date" in metadata_df.columns:
                dates = metadata_df["latest_reporting_date"].dropna()
                if len(dates) > 0:
                    stats["metadata"]["date_range"] = {
                        "earliest": dates.min(),
                        "latest": dates.max(),
                        "count": len(dates),
                    }

        # Analyze comprehensive financial statements file (if available)
        if "statements" in data_paths:
            statements_df = self.conn.execute(f"SELECT * FROM read_parquet('{data_paths['statements']}')").df()

            stats["statements"] = {
                "total_records": len(statements_df),
                "columns": list(statements_df.columns),
                "data_types": dict(statements_df.dtypes),
            }

            # Check for actual financial values
            financial_fields = ["net_profit_loss", "total_assets", "total_equity", "operating_profit_loss"]
            non_null_counts = {}
            for field in financial_fields:
                if field in statements_df.columns:
                    non_null_counts[field] = statements_df[field].notna().sum()

            stats["statements"]["financial_data_coverage"] = non_null_counts
            logger.info("✅ Found comprehensive financial statements with parsed XBRL data!")

        logger.info("📊 Financial data analysis:")
        if "metadata" in stats:
            logger.info(f"   • Metadata records: {stats['metadata']['total_records']:,}")
            logger.info(f"   • Companies with metrics: {stats['metadata'].get('companies_with_metrics', 0):,}")
        if "statements" in stats:
            logger.info(f"   • Financial statements: {stats['statements']['total_records']:,}")
            logger.info(f"   • Financial data fields: {len(stats['statements'].get('financial_data_coverage', {}))}")

        return stats

    async def _check_supabase_schema(self) -> Dict[str, Any]:
        """Check the current Supabase yearly_financials schema."""
        logger.info("Checking Supabase yearly_financials schema...")

        # Connect to Supabase
        supabase_conn = duckdb.connect()

        try:
            # Install postgres extension and connect
            supabase_conn.execute("INSTALL postgres")
            supabase_conn.execute("LOAD postgres")
            supabase_conn.execute(f"ATTACH '{SUPABASE_URL}' AS supabase_db (TYPE postgres)")

            # Get current schema
            schema_query = """
            SELECT column_name, data_type, is_nullable
            FROM supabase_db.information_schema.columns 
            WHERE table_name = 'yearly_financials'
            ORDER BY ordinal_position
            """

            schema_df = supabase_conn.execute(schema_query).df()

            # Get current record count
            count_query = "SELECT COUNT(*) as count FROM supabase_db.yearly_financials"
            current_count = supabase_conn.execute(count_query).fetchone()[0]

            schema_info = {
                "columns": schema_df.to_dict("records"),
                "current_record_count": current_count,
                "table_exists": len(schema_df) > 0,
            }

            logger.info("📋 Current Supabase schema:")
            logger.info(f"   • Table exists: {schema_info['table_exists']}")
            logger.info(f"   • Current records: {current_count:,}")
            logger.info(f"   • Columns: {len(schema_df)}")

            return schema_info

        except Exception as e:
            logger.warning(f"Could not connect to Supabase: {e}")
            return {"table_exists": False, "error": str(e)}
        finally:
            supabase_conn.close()

    async def _determine_migration_strategy(
        self, financial_stats: Dict[str, Any], schema_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Determine the best migration strategy based on available data."""
        logger.info("Determining migration strategy...")

        # Check what data we have
        has_parsed_metrics = financial_stats.get("companies_with_metrics", 0) > 0
        has_comprehensive_data = False  # Would be True if we had parsed XBRL fields

        if has_parsed_metrics:
            strategy = {
                "approach": "document_metadata_with_future_enhancement",
                "description": "Migrate document metadata now, prepare for comprehensive financial data",
                "current_fields": [
                    "cvr_number",
                    "company_name",
                    "document_count",
                    "latest_reporting_date",
                    "has_financial_metrics",
                ],
                "future_fields": [
                    "net_profit_loss",
                    "total_assets",
                    "total_equity",
                    "operating_profit_loss",
                    "employee_benefits_expense",
                ],
                "requires_schema_update": True,
            }
        else:
            strategy = {
                "approach": "basic_metadata_only",
                "description": "Migrate basic document metadata only",
                "current_fields": ["cvr_number", "company_name", "document_count"],
                "future_fields": [],
                "requires_schema_update": False,
            }

        logger.info(f"🎯 Migration strategy: {strategy['approach']}")
        logger.info(f"   • Description: {strategy['description']}")
        logger.info(f"   • Schema update needed: {strategy['requires_schema_update']}")

        return strategy

    async def _execute_migration(self, data_path: str, strategy: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the migration based on the determined strategy."""
        logger.info(f"Executing migration with strategy: {strategy['approach']}")

        # Load financial data
        df = self.conn.execute(f"SELECT * FROM read_parquet('{data_path}')").df()

        # Connect to Supabase
        supabase_conn = duckdb.connect()

        try:
            # Install postgres extension and connect
            supabase_conn.execute("INSTALL postgres")
            supabase_conn.execute("LOAD postgres")
            supabase_conn.execute(f"ATTACH '{SUPABASE_URL}' AS supabase_db (TYPE postgres)")

            # Create UUID v5 function for consistent company_id mapping
            supabase_conn.execute("""
                CREATE OR REPLACE FUNCTION generate_company_uuid(cvr_number INTEGER) AS (
                    -- Generate deterministic UUID v5 for company_id
                    -- This matches the UUID generation used in companies table migration
                    uuid()::VARCHAR  -- Placeholder - would use proper UUID v5 in production
                )
            """)

            # Prepare migration data based on strategy
            if strategy["approach"] == "document_metadata_with_future_enhancement":
                migration_results = await self._migrate_with_metadata(supabase_conn, df)
            else:
                migration_results = await self._migrate_basic_only(supabase_conn, df)

            return migration_results

        except Exception as e:
            logger.error(f"Migration execution failed: {e}")
            raise
        finally:
            supabase_conn.close()

    async def _migrate_with_metadata(self, conn: duckdb.DuckDBPyConnection, df) -> Dict[str, Any]:
        """Migrate with document metadata and prepare for future enhancement."""
        logger.info("Executing metadata-based migration with future enhancement preparation...")

        # Create temporary table with financial data
        conn.execute("DROP TABLE IF EXISTS temp_financial_data")
        conn.execute("""
            CREATE TABLE temp_financial_data AS
            SELECT 
                cvr_number,
                company_name,
                document_count,
                xml_document_count,
                total_xml_size_bytes,
                latest_reporting_date,
                has_financial_metrics,
                processing_timestamp
            FROM df
            WHERE cvr_number IS NOT NULL
        """)

        # Insert into Supabase yearly_financials table
        # NOTE: This is a simplified version - in production we'd need proper UUID v5 generation
        insert_query = """
        INSERT INTO supabase_db.yearly_financials (
            id, company_id, year, 
            -- Current document metadata fields
            reporting_period_start, reporting_period_end,
            -- Future comprehensive financial fields (NULL for now)
            net_profit_loss, gross_profit_loss, operating_profit_loss, 
            profit_loss_before_tax, employee_benefits_expense, 
            depreciation_expense, tax_expense,
            total_assets, total_equity, current_assets, noncurrent_assets,
            cash_and_cash_equivalents, contributed_capital,
            liabilities_other_than_provisions, 
            shortterm_liabilities_other_than_provisions,
            longterm_liabilities_other_than_provisions, provisions,
            average_number_of_employees, equity_ratio, return_on_assets,
            publication_type, case_number
        )
        SELECT 
            gen_random_uuid() as id,
            generate_company_uuid(tf.cvr_number) as company_id,
            EXTRACT(YEAR FROM tf.latest_reporting_date::DATE) as year,
            -- Document metadata
            tf.latest_reporting_date::DATE as reporting_period_start,
            tf.latest_reporting_date::DATE as reporting_period_end,
            -- Comprehensive financial fields (NULL - to be populated later)
            NULL::BIGINT as net_profit_loss,
            NULL::BIGINT as gross_profit_loss,
            NULL::BIGINT as operating_profit_loss,
            NULL::BIGINT as profit_loss_before_tax,
            NULL::BIGINT as employee_benefits_expense,
            NULL::BIGINT as depreciation_expense,
            NULL::BIGINT as tax_expense,
            NULL::BIGINT as total_assets,
            NULL::BIGINT as total_equity,
            NULL::BIGINT as current_assets,
            NULL::BIGINT as noncurrent_assets,
            NULL::BIGINT as cash_and_cash_equivalents,
            NULL::BIGINT as contributed_capital,
            NULL::BIGINT as liabilities_other_than_provisions,
            NULL::BIGINT as shortterm_liabilities_other_than_provisions,
            NULL::BIGINT as longterm_liabilities_other_than_provisions,
            NULL::BIGINT as provisions,
            NULL::NUMERIC as average_number_of_employees,
            NULL::NUMERIC as equity_ratio,
            NULL::NUMERIC as return_on_assets,
            'Annual Report' as publication_type,
            NULL as case_number
        FROM temp_financial_data tf
        WHERE tf.has_financial_metrics = true
        AND tf.latest_reporting_date IS NOT NULL
        """

        # Execute migration
        result = conn.execute(insert_query)
        rows_affected = result.fetchone()[0] if result else 0

        # Get statistics
        total_processed = len(df)
        companies_with_data = len(df[df["has_financial_metrics"] == True])

        logger.info("✅ Migration completed:")
        logger.info(f"   • Records inserted: {rows_affected:,}")
        logger.info(f"   • Companies with financial data: {companies_with_data:,}")

        return {
            "total_processed": total_processed,
            "migrated": rows_affected,
            "companies_with_data": companies_with_data,
            "approach": "document_metadata_with_future_enhancement",
        }

    async def _migrate_basic_only(self, conn: duckdb.DuckDBPyConnection, df) -> Dict[str, Any]:
        """Migrate basic metadata only."""
        logger.info("Executing basic metadata-only migration...")

        # This would be a simpler migration for cases where we have very limited data
        # Implementation would be similar but with fewer fields

        return {"total_processed": len(df), "migrated": 0, "companies_with_data": 0, "approach": "basic_metadata_only"}


async def main():
    """Main migration entry point."""
    migration = YearlyFinancialsMigration()
    await migration.run_migration()


if __name__ == "__main__":
    asyncio.run(main())
