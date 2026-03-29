"""
Gold layer processing for arbejdstilsynet inspections data.
Provides cleaned and business-ready workplace inspection data.

Refactored to use vanilla DuckDB instead of pandas for better performance
and consistency with the unified pipeline architecture.
"""

import logging
import os
from datetime import datetime
from typing import Any

from common.storage import StorageAccess

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface


class ArbjdstilsynetInspectionsGoldConfig(BaseJobConfig):
    """Configuration for Arbejdstilsynet Inspections gold layer."""

    name: str = "Arbejdstilsynet Inspections Gold"
    dataset: str = "arbejdstilsynet_inspections"
    type: str = "gold"
    description: str = "Clean and standardize workplace inspection data for business analytics"
    frequency: str = "weekly"
    bucket: str = (
        os.getenv("STORAGE_BUCKET")
        or os.getenv("R2_BUCKET")
        or os.getenv("GCS_BUCKET", "landbruget-data")
    )

    # Input silver dataset
    silver_dataset: str = "arbejdstilsynet_inspections"


class ArbjdstilsynetInspectionsGold(
    BaseSource[ArbjdstilsynetInspectionsGoldConfig], GoldJobInterface
):
    """Gold layer processor for Arbejdstilsynet Inspections data."""

    def __init__(self, config: ArbjdstilsynetInspectionsGoldConfig):
        super().__init__(config)
        self.storage = StorageAccess()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self, silver_data: dict[str, Any] | None = None) -> bool:
        """
        Process silver data into gold layer.

        Args:
            silver_data: Optional in-memory silver data from previous pipeline stages
                         (expects table names as values)

        Returns:
            bool: Success status
        """
        try:
            self.logger.info("Starting Arbejdstilsynet Inspections Gold processing")

            # Load silver data into DuckDB table
            table_name = await self._load_silver_data(silver_data)
            if table_name is None:
                raise ValueError("No silver data available for processing")

            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            self.logger.info(f"Loaded {row_count} inspection records")

            # Apply gold layer transformations using SQL
            gold_table = self._clean_and_standardize(table_name)
            gold_table = self._add_derived_fields(gold_table)
            gold_table = self._validate_business_rules(gold_table)

            # Save gold data
            await self._save_data(gold_table, stage="gold")

            self.logger.info("Arbejdstilsynet Inspections Gold processing completed")
            return True

        except Exception as e:
            self.logger.error(f"Error in Arbejdstilsynet Inspections Gold processing: {e}")
            return False

    async def _load_silver_data(self, silver_data: dict[str, Any] | None) -> str | None:
        """Load silver data from in-memory cache or cloud storage.

        Returns:
            Table name in DuckDB containing the silver data, or None if not found.
        """
        try:
            # Try in-memory first (table name passed from previous stage)
            if silver_data and self.config.silver_dataset in silver_data:
                self.logger.info("Using in-memory silver data")
                in_memory_data = silver_data[self.config.silver_dataset]
                if isinstance(in_memory_data, str):
                    # Already a table name
                    return in_memory_data
                # Register the data as a table
                self.conn.register("silver_inspections_raw", in_memory_data)
                return "silver_inspections_raw"

            # Fallback to cloud storage
            self.logger.info("Loading silver data from cloud storage")

            # Find latest silver data using pattern matching
            pattern = f"{self.config.bucket}/silver/{self.config.silver_dataset}/*/workplace_inspections.parquet"
            files = self.storage.list_files_with_timestamps(pattern)

            if not files:
                self.logger.error(f"No silver data found with pattern: {pattern}")
                return None

            # Sort by timestamp to get the most recent file
            files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
            latest_path, timestamp = files_sorted[0]

            self.logger.info(
                f"Reading latest silver data from: {latest_path} (timestamp: {timestamp})"
            )

            # Read parquet file directly into DuckDB table
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE silver_inspections AS
                SELECT * FROM '{latest_path}'
            """)

            return "silver_inspections"

        except Exception as e:
            self.logger.error(f"Error loading silver data: {e}")
            return None

    def _clean_and_standardize(self, table_name: str) -> str:
        """Apply additional cleaning and standardization to silver data using SQL."""
        try:
            self.logger.info("Cleaning and standardizing data")

            # First, restore Danish formatting
            formatted_table = self._restore_danish_formatting(table_name)

            # Create cleaned and standardized table with all transformations
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE gold_inspections_clean AS
                SELECT
                    *,
                    -- Standardize decision types with proper Danish formatting
                    CASE decision
                        WHEN 'strakspaabud' THEN 'Strakspabud'
                        WHEN 'paabud' THEN 'Pabud'
                        WHEN 'paatale' THEN 'Patale'
                        ELSE decision
                    END AS decision_type,

                    -- Clean and standardize industry names
                    TRIM(industry_formatted) AS industry_clean,

                    -- Clean company names - remove extra whitespace
                    INITCAP(TRIM(company_name)) AS company_name_clean,

                    -- Convert CVR number to integer (nullable)
                    TRY_CAST(cvr_number AS BIGINT) AS cvr_number_clean,

                    -- Extract postal code from address (4 digits)
                    regexp_extract(company_address, '(\\d{4})', 1) AS postal_code,

                    -- Extract city from address (text after postal code)
                    INITCAP(TRIM(regexp_extract(company_address, '\\d{4}\\s+(.+)', 1))) AS city,

                    -- Create severity score based on decision type
                    CASE decision
                        WHEN 'strakspaabud' THEN 3  -- Most severe
                        WHEN 'paabud' THEN 2
                        WHEN 'paatale' THEN 1
                        ELSE NULL
                    END AS severity_score,

                    -- Add year and month for temporal analysis
                    YEAR(date) AS year,
                    MONTH(date) AS month,
                    STRFTIME(date, '%Y-%m') AS year_month

                FROM {formatted_table}
            """)

            # Log CVR number preservation stats
            cvr_stats = self.conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(cvr_number_clean) AS with_cvr
                FROM gold_inspections_clean
            """).fetchone()

            if cvr_stats:
                self.logger.info(
                    f"Preserved CVR numbers for {cvr_stats[1]} of {cvr_stats[0]} records"
                )

            row_count = self.conn.execute("SELECT COUNT(*) FROM gold_inspections_clean").fetchone()[
                0
            ]
            self.logger.info(f"Cleaned and standardized {row_count} records")

            return "gold_inspections_clean"

        except Exception as e:
            self.logger.error(f"Error in cleaning and standardization: {e}")
            return table_name

    def _restore_danish_formatting(self, table_name: str) -> str:
        """Restore Danish characters and proper formatting from the normalized silver data."""
        try:
            self.logger.info("Restoring Danish characters and proper formatting")

            # Create table with Danish formatting restored
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE silver_formatted AS
                SELECT
                    *,
                    -- Restore Danish characters for work environment issues
                    CASE LOWER(work_env_issue)
                        WHEN 'arbejdspladsvurdering (apv)' THEN 'Arbejdspladsvurdering (APV)'
                        WHEN 'fald til lavere niveau' THEN 'Fald til lavere niveau'
                        WHEN 'eftersyn' THEN 'Eftersyn'
                        WHEN 'maskiner, anlaeg og trykbaerende udstyr' THEN 'Maskiner, anlaeg og trykbaerende udstyr'
                        WHEN 'kemisk risikovurdering' THEN 'Kemisk risikovurdering'
                        WHEN 'kraeftfremkaldende belastninger' THEN 'Kraeftfremkaldende belastninger'
                        WHEN 'luftvejsbelastninger' THEN 'Luftvejsbelastninger'
                        WHEN 'asbest' THEN 'Asbest'
                        WHEN 'nedfald af genstande, sammenstyrtning m.m.' THEN 'Nedfald af genstande, sammenstyrtning m.m.'
                        WHEN 'oevrige ulykkesrisici' THEN 'Ovrige ulykkesrisici'
                        WHEN 'psykisk arbejdsmiljoe' THEN 'Psykisk arbejdsmiljo'
                        WHEN 'stoej' THEN 'Stoj'
                        WHEN 'indeklima' THEN 'Indeklima'
                        WHEN 'ergonomi' THEN 'Ergonomi'
                        WHEN 'arbejdstid' THEN 'Arbejdstid'
                        WHEN 'unge under 18 aar' THEN 'Unge under 18 ar'
                        WHEN 'gravide og ammende' THEN 'Gravide og ammende'
                        WHEN 'velfaerdsforanstaltninger' THEN 'Velfaerdsforanstaltninger'
                        ELSE INITCAP(work_env_issue)
                    END AS work_env_issue_formatted,

                    -- Restore Danish characters for industry names
                    CASE LOWER(industry)
                        WHEN 'avl af malkekvaeg' THEN 'Avl af malkekvaeg'
                        WHEN 'avl af smaagrise' THEN 'Avl af smagrise'
                        WHEN 'dyrkning af groentsager og meloner, roedder og rodknolde' THEN 'Dyrkning af grontsager og meloner, rodder og rodknolde'
                        WHEN 'dyrkning af korn (undtagen ris), baelgfrugter og olieholdige froe' THEN 'Dyrkning af korn (undtagen ris), baelgfrugter og olieholdige fro'
                        WHEN 'stoetteaktiviteter i forbindelse med planteavl' THEN 'Stotteaktiviteter i forbindelse med planteavl'
                        WHEN 'anlaeg af ledningsnet til vaesker' THEN 'Anlaeg af ledningsnet til vaesker'
                        WHEN 'anlaeg af ledningsnet til elektricitet og tele' THEN 'Anlaeg af ledningsnet til elektricitet og tele'
                        WHEN 'anlaeg af jernbaner og undergrundsbaner' THEN 'Anlaeg af jernbaner og undergrundsbaner'
                        WHEN 'forberedende byggepladsarbejder' THEN 'Forberedende byggepladsarbejder'
                        WHEN 'anlaeg af veje og motorveje' THEN 'Anlaeg af veje og motorveje'
                        WHEN 'opfoersel af boligbyggeri' THEN 'Opforelse af boligbyggeri'
                        WHEN 'opfoersel af andre bygninger' THEN 'Opforelse af andre bygninger'
                        ELSE INITCAP(industry)
                    END AS industry_formatted

                FROM {table_name}
            """)

            # Apply additional character replacements for any remaining cases
            # Note: DuckDB's REPLACE function handles this efficiently
            self.conn.execute("""
                UPDATE silver_formatted
                SET
                    work_env_issue_formatted = REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(work_env_issue_formatted,
                                            'Aa', 'A'),
                                        'aa', 'a'),
                                    'Ae', 'AE'),
                                'ae', 'ae'),
                            'Oe', 'O'),
                        'oe', 'o'),
                    industry_formatted = REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(industry_formatted,
                                            'Aa', 'A'),
                                        'aa', 'a'),
                                    'Ae', 'AE'),
                                'ae', 'ae'),
                            'Oe', 'O'),
                        'oe', 'o')
            """)

            self.logger.info("Restored Danish formatting")
            return "silver_formatted"

        except Exception as e:
            self.logger.error(f"Error restoring Danish formatting: {e}")
            return table_name

    def _add_derived_fields(self, table_name: str) -> str:
        """Add derived analytical fields using SQL window functions and aggregations."""
        try:
            self.logger.info("Adding derived analytical fields")

            # Create table with all derived fields using window functions
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE gold_inspections_derived AS
                WITH
                -- Company inspection frequency (how many inspections this company has had)
                company_counts AS (
                    SELECT company_id, COUNT(*) AS company_inspection_count
                    FROM {table_name}
                    GROUP BY company_id
                ),

                -- Industry risk level based on average severity
                industry_severity AS (
                    SELECT industry_clean, AVG(severity_score) AS industry_avg_severity
                    FROM {table_name}
                    GROUP BY industry_clean
                ),

                -- Compliance rate by company (percentage of cases where complied = 1)
                company_compliance AS (
                    SELECT company_id, AVG(CAST(complied AS DOUBLE)) AS company_compliance_rate
                    FROM {table_name}
                    GROUP BY company_id
                ),

                -- Repeat offenders (companies with multiple severe inspections)
                severe_company_counts AS (
                    SELECT company_id, COUNT(*) AS severe_count
                    FROM {table_name}
                    WHERE severity_score >= 2
                    GROUP BY company_id
                )

                SELECT
                    t.*,
                    cc.company_inspection_count,
                    isev.industry_avg_severity,
                    ccomp.company_compliance_rate,

                    -- Time since last inspection for this company using window function
                    DATEDIFF('day',
                        LAG(t.date) OVER (PARTITION BY t.company_id ORDER BY t.date),
                        t.date
                    ) AS days_since_last_inspection,

                    -- Flag repeat offenders (companies with 2+ severe inspections)
                    COALESCE(scc.severe_count >= 2, FALSE) AS is_repeat_offender

                FROM {table_name} t
                LEFT JOIN company_counts cc ON t.company_id = cc.company_id
                LEFT JOIN industry_severity isev ON t.industry_clean = isev.industry_clean
                LEFT JOIN company_compliance ccomp ON t.company_id = ccomp.company_id
                LEFT JOIN severe_company_counts scc ON t.company_id = scc.company_id
            """)

            row_count = self.conn.execute(
                "SELECT COUNT(*) FROM gold_inspections_derived"
            ).fetchone()[0]
            self.logger.info(f"Added derived fields to {row_count} records")

            return "gold_inspections_derived"

        except Exception as e:
            self.logger.error(f"Error adding derived fields: {e}")
            return table_name

    def _validate_business_rules(self, table_name: str) -> str:
        """Apply business rules and data quality validation using SQL."""
        try:
            self.logger.info("Validating business rules and data quality")

            # Get current date for validation
            current_date = datetime.now().strftime("%Y-%m-%d")

            # Check for future dates
            future_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE date > '{current_date}'
            """).fetchone()[0]

            if future_count > 0:
                self.logger.warning(f"Removing {future_count} records with future dates")

            # Create validated table with quality flags and scores
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE gold_inspections_validated AS
                SELECT
                    *,
                    -- Flag potential data quality issues
                    CASE
                        WHEN postal_code IS NULL OR NOT regexp_matches(postal_code, '^\\d{4}$')
                            THEN TRUE
                        WHEN case_count > 10
                            THEN TRUE
                        ELSE FALSE
                    END AS has_quality_flag,

                    -- Add data quality score (0-1, where 1 is highest quality)
                    GREATEST(0.0, LEAST(1.0,
                        1.0
                        - CASE WHEN postal_code IS NULL OR NOT regexp_matches(postal_code, '^\\d{4}$')
                              THEN 0.3 ELSE 0.0 END
                        - CASE WHEN case_count > 10 THEN 0.2 ELSE 0.0 END
                    )) AS data_quality_score

                FROM {table_name}
                WHERE date <= '{current_date}'  -- Remove future dates
            """)

            # Log high case count records
            high_case_count = self.conn.execute("""
                SELECT COUNT(*) FROM gold_inspections_validated
                WHERE case_count > 10
            """).fetchone()[0]

            if high_case_count > 0:
                self.logger.info(f"Flagged {high_case_count} records with high case counts (>10)")

            # Get validation statistics
            stats = self.conn.execute("""
                SELECT
                    COUNT(*) AS total_records,
                    AVG(data_quality_score) AS avg_quality_score
                FROM gold_inspections_validated
            """).fetchone()

            self.logger.info(
                f"Validated {stats[0]} records with average quality score: {stats[1]:.3f}"
            )

            return "gold_inspections_validated"

        except Exception as e:
            self.logger.error(f"Error in business rule validation: {e}")
            return table_name

    async def _save_data(self, table_name: str, stage: str) -> str:
        """Save the gold data to cloud storage using DuckDB's COPY command."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Define output path
            dataset_path = f"{stage}/{self.config.dataset}/{timestamp}"
            filename = "workplace_inspections_gold.parquet"
            storage_path = f"{dataset_path}/{filename}"
            full_storage_path = f"{self.config.bucket}/{storage_path}"

            self.logger.info(f"Saving gold data to: {full_storage_path}")

            # Get record count before saving
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            # Save to cloud storage using the base class method
            self.storage.upload_from_duckdb_table(table_name, full_storage_path)

            self.logger.info(f"Successfully saved {row_count} records to gold layer")

            # Log summary statistics
            self._log_summary_stats(table_name)

            return storage_path

        except Exception as e:
            self.logger.error(f"Error saving gold data: {e}")
            raise

    def _log_summary_stats(self, table_name: str):
        """Log summary statistics about the gold dataset using SQL aggregations."""
        try:
            # Get basic stats
            basic_stats = self.conn.execute(f"""
                SELECT
                    COUNT(*) AS total_records,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date,
                    COUNT(DISTINCT COALESCE(cvr_number_clean, company_id)) AS unique_companies,
                    AVG(data_quality_score) AS avg_quality_score,
                    SUM(CASE WHEN is_repeat_offender THEN 1 ELSE 0 END) AS repeat_offenders
                FROM {table_name}
            """).fetchone()

            (
                total_records,
                min_date,
                max_date,
                unique_companies,
                avg_quality_score,
                repeat_offenders,
            ) = basic_stats

            # Get decision type counts
            decision_counts = self.conn.execute(f"""
                SELECT decision_type, COUNT(*) AS count
                FROM {table_name}
                GROUP BY decision_type
                ORDER BY count DESC
            """).fetchall()
            decision_dict = {row[0]: row[1] for row in decision_counts}

            # Get top 3 industries
            top_industries = self.conn.execute(f"""
                SELECT industry_clean, COUNT(*) AS count
                FROM {table_name}
                GROUP BY industry_clean
                ORDER BY count DESC
                LIMIT 3
            """).fetchall()
            industries_dict = {row[0]: row[1] for row in top_industries}

            # Get top 3 work environment issues
            top_work_env = self.conn.execute(f"""
                SELECT work_env_issue_formatted, COUNT(*) AS count
                FROM {table_name}
                WHERE work_env_issue_formatted IS NOT NULL
                GROUP BY work_env_issue_formatted
                ORDER BY count DESC
                LIMIT 3
            """).fetchall()
            work_env_dict = {row[0]: row[1] for row in top_work_env}

            self.logger.info("Gold Layer Summary Statistics:")
            self.logger.info(f"   Total records: {total_records:,}")
            self.logger.info(f"   Date range: {min_date} to {max_date}")
            self.logger.info(f"   Unique companies: {unique_companies:,}")
            self.logger.info(f"   Average data quality score: {avg_quality_score:.3f}")
            self.logger.info(f"   Repeat offenders: {repeat_offenders}")
            self.logger.info(f"   Decision types: {decision_dict}")
            self.logger.info(f"   Top 3 industries: {industries_dict}")
            if work_env_dict:
                self.logger.info(f"   Top 3 work environment issues: {work_env_dict}")

        except Exception as e:
            self.logger.warning(f"Could not generate summary statistics: {e}")
