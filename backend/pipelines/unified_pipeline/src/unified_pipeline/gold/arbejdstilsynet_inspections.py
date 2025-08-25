"""
Gold layer processing for arbejdstilsynet inspections data.
Provides cleaned and business-ready workplace inspection data.
"""

import logging
import os
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_access import GCSDataAccess


class ArbjdstilsynetInspectionsGoldConfig(BaseJobConfig):
    """Configuration for Arbejdstilsynet Inspections gold layer."""

    name: str = "Arbejdstilsynet Inspections Gold"
    dataset: str = "arbejdstilsynet_inspections"
    type: str = "gold"
    description: str = "Clean and standardize workplace inspection data for business analytics"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input silver dataset
    silver_dataset: str = "arbejdstilsynet_inspections"


class ArbjdstilsynetInspectionsGold(
    BaseSource[ArbjdstilsynetInspectionsGoldConfig], GoldJobInterface
):
    """Gold layer processor for Arbejdstilsynet Inspections data."""

    def __init__(self, config: ArbjdstilsynetInspectionsGoldConfig):
        super().__init__(config)
        self.gcs_access = GCSDataAccess()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def run(self, silver_data: Optional[Dict[str, pd.DataFrame]] = None) -> bool:
        """
        Process silver data into gold layer.

        Args:
            silver_data: Optional in-memory silver data from previous pipeline stages

        Returns:
            bool: Success status
        """
        try:
            self.logger.info("🏆 Starting Arbejdstilsynet Inspections Gold processing")

            # Load silver data
            df = await self._load_silver_data(silver_data)
            if df is None or df.empty:
                raise ValueError("No silver data available for processing")

            self.logger.info(f"📊 Loaded {len(df)} inspection records")

            # Apply gold layer transformations
            df_gold = self._clean_and_standardize(df)
            df_gold = self._add_derived_fields(df_gold)
            df_gold = self._validate_business_rules(df_gold)

            # Save gold data
            await self._save_data(df_gold, stage="gold")

            self.logger.info("✅ Arbejdstilsynet Inspections Gold processing completed")
            return True

        except Exception as e:
            self.logger.error(f"❌ Error in Arbejdstilsynet Inspections Gold processing: {e}")
            return False

    async def _load_silver_data(
        self, silver_data: Optional[Dict[str, pd.DataFrame]]
    ) -> Optional[pd.DataFrame]:
        """Load silver data from in-memory cache or GCS storage."""
        try:
            # Try in-memory first
            if silver_data and self.config.silver_dataset in silver_data:
                self.logger.info("📥 Using in-memory silver data")
                return silver_data[self.config.silver_dataset]

            # Fallback to GCS storage
            self.logger.info("💾 Loading silver data from GCS storage")

            # Find latest silver data using pattern matching
            pattern = f"gs://{self.config.bucket}/silver/{self.config.silver_dataset}/*/workplace_inspections.parquet"
            files = self.gcs_access.list_files_with_timestamps(pattern)

            if not files:
                self.logger.error(f"No silver data found with pattern: {pattern}")
                return None

            # Sort by timestamp to get the most recent file
            files_sorted = sorted(files, key=lambda x: x[1], reverse=True)
            latest_path, timestamp = files_sorted[0]

            self.logger.info(
                f"📥 Reading latest silver data from: {latest_path} (timestamp: {timestamp})"
            )

            # Read parquet file directly using gcsfs
            with self.gcs_access.fs.open(latest_path, "rb") as f:
                df = pd.read_parquet(f)

            return df

        except Exception as e:
            self.logger.error(f"Error loading silver data: {e}")
            return None

    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply additional cleaning and standardization to silver data."""
        try:
            self.logger.info("🧹 Cleaning and standardizing data")

            df_clean = df.copy()

            # Restore Danish characters and proper formatting
            df_clean = self._restore_danish_formatting(df_clean)

            # Standardize decision types with proper Danish formatting
            decision_mapping = {
                "strakspaabud": "Strakspåbud",
                "paabud": "Påbud",
                "paatale": "Påtale",
            }
            df_clean["decision_type"] = (
                df_clean["decision"].map(decision_mapping).fillna(df_clean["decision"])
            )

            # Clean and standardize industry names with proper capitalization
            df_clean["industry_clean"] = df_clean["industry_formatted"].str.strip()

            # Clean company names - remove extra whitespace and apply title case
            df_clean["company_name_clean"] = df_clean["company_name"].str.strip().str.title()

            # Extract postal code from address
            df_clean["postal_code"] = df_clean["company_address"].str.extract(
                r"(\d{4})", expand=False
            )

            # Extract city from address (text after postal code) with proper capitalization
            df_clean["city"] = df_clean["company_address"].str.extract(
                r"\d{4}\s+(.+)", expand=False
            )
            df_clean["city"] = df_clean["city"].str.strip().str.title()

            # Create severity score based on decision type
            severity_mapping = {
                "Strakspåbud": 3,  # Most severe
                "Påbud": 2,
                "Påtale": 1,
            }
            df_clean["severity_score"] = df_clean["decision_type"].map(severity_mapping)

            # Add year and month for temporal analysis
            df_clean["year"] = df_clean["date"].dt.year
            df_clean["month"] = df_clean["date"].dt.month
            df_clean["year_month"] = df_clean["date"].dt.to_period("M").astype(str)

            self.logger.info(f"✅ Cleaned and standardized {len(df_clean)} records")
            return df_clean

        except Exception as e:
            self.logger.error(f"Error in cleaning and standardization: {e}")
            return df

    def _restore_danish_formatting(self, df: pd.DataFrame) -> pd.DataFrame:
        """Restore Danish characters and proper formatting from the normalized silver data."""
        try:
            self.logger.info("🇩🇰 Restoring Danish characters and proper formatting")

            df_formatted = df.copy()

            # Restore Danish characters for work environment issues
            work_env_mapping = {
                "arbejdspladsvurdering (apv)": "Arbejdspladsvurdering (APV)",
                "fald til lavere niveau": "Fald til lavere niveau",
                "eftersyn": "Eftersyn",
                "maskiner, anlaeg og trykbaerende udstyr": "Maskiner, anlæg og trykbærende udstyr",
                "kemisk risikovurdering": "Kemisk risikovurdering",
                "kraeftfremkaldende belastninger": "Kræftfremkaldende belastninger",
                "luftvejsbelastninger": "Luftvejsbelastninger",
                "asbest": "Asbest",
                "nedfald af genstande, sammenstyrtning m.m.": (
                    "Nedfald af genstande, sammenstyrtning m.m."
                ),
                "oevrige ulykkesrisici": "Øvrige ulykkesrisici",
                "psykisk arbejdsmiljoe": "Psykisk arbejdsmiljø",
                "stoej": "Støj",
                "indeklima": "Indeklima",
                "ergonomi": "Ergonomi",
                "arbejdstid": "Arbejdstid",
                "unge under 18 aar": "Unge under 18 år",
                "gravide og ammende": "Gravide og ammende",
                "velfaerdsforanstaltninger": "Velfærdsforanstaltninger",
            }

            # Apply work environment issue formatting
            if "work_env_issue" in df_formatted.columns:
                df_formatted["work_env_issue_formatted"] = (
                    df_formatted["work_env_issue"]
                    .map(work_env_mapping)
                    .fillna(df_formatted["work_env_issue"].str.title())
                )

            # Restore Danish characters for industry names
            industry_mapping = {
                "avl af malkekvaeg": "Avl af malkekvæg",
                "avl af smaagrise": "Avl af smågrise",
                "dyrkning af groentsager og meloner, roedder og rodknolde": (
        "Dyrkning af grøntsager og meloner, rødder og rodknolde"
    ),
                "dyrkning af korn (undtagen ris), baelgfrugter og olieholdige froe": (
        "Dyrkning af korn (undtagen ris), bælgfrugter og olieholdige frø"
    ),
                "stoetteaktiviteter i forbindelse med planteavl": (
        "Støtteaktiviteter i forbindelse med planteavl"
    ),
                "anlaeg af ledningsnet til vaesker": "Anlæg af ledningsnet til væsker",
                "anlaeg af ledningsnet til elektricitet og tele": (
                    "Anlæg af ledningsnet til elektricitet og tele"
                ),
                "anlaeg af jernbaner og undergrundsbaner": "Anlæg af jernbaner og undergrundsbaner",
                "forberedende byggepladsarbejder": "Forberedende byggepladsarbejder",
                "anlaeg af veje og motorveje": "Anlæg af veje og motorveje",
                "opfoersel af boligbyggeri": "Opførelse af boligbyggeri",
                "opfoersel af andre bygninger": "Opførelse af andre bygninger",
            }

            # Apply industry formatting with fallback to title case
            if "industry" in df_formatted.columns:
                df_formatted["industry_formatted"] = (
                    df_formatted["industry"]
                    .map(industry_mapping)
                    .fillna(df_formatted["industry"].str.title())
                )

            # Fix common Danish character replacements
            for col in ["work_env_issue_formatted", "industry_formatted"]:
                if col in df_formatted.columns:
                    df_formatted[col] = (
                        df_formatted[col]
                        .str.replace("Aa", "Å", regex=False)
                        .str.replace("aa", "å", regex=False)
                        .str.replace("Ae", "Æ", regex=False)
                        .str.replace("ae", "æ", regex=False)
                        .str.replace("Oe", "Ø", regex=False)
                        .str.replace("oe", "ø", regex=False)
                    )

            self.logger.info("✅ Restored Danish formatting")
            return df_formatted

        except Exception as e:
            self.logger.error(f"Error restoring Danish formatting: {e}")
            return df

    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived analytical fields."""
        try:
            self.logger.info("📈 Adding derived analytical fields")

            df_derived = df.copy()

            # Company inspection frequency (how many inspections this company has had)
            company_counts = df_derived.groupby("company_id").size()
            df_derived["company_inspection_count"] = df_derived["company_id"].map(company_counts)

            # Industry risk level based on average severity
            industry_severity = df_derived.groupby("industry_clean")["severity_score"].mean()
            df_derived["industry_avg_severity"] = df_derived["industry_clean"].map(
                industry_severity
            )

            # Compliance rate by company (percentage of cases where complied = 1)
            company_compliance = df_derived.groupby("company_id")["complied"].mean()
            df_derived["company_compliance_rate"] = df_derived["company_id"].map(company_compliance)

            # Time since last inspection for this company
            df_sorted = df_derived.sort_values(["company_id", "date"])
            df_sorted["days_since_last_inspection"] = (
                df_sorted.groupby("company_id")["date"].diff().dt.days
            )
            df_derived["days_since_last_inspection"] = df_sorted["days_since_last_inspection"]

            # Flag repeat offenders (companies with multiple severe inspections)
            severe_cases = df_derived[df_derived["severity_score"] >= 2]
            repeat_offenders = severe_cases.groupby("company_id").size()
            repeat_offender_ids = repeat_offenders[repeat_offenders >= 2].index
            df_derived["is_repeat_offender"] = df_derived["company_id"].isin(repeat_offender_ids)

            self.logger.info(f"✅ Added derived fields to {len(df_derived)} records")
            return df_derived

        except Exception as e:
            self.logger.error(f"Error adding derived fields: {e}")
            return df

    def _validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules and data quality validation."""
        try:
            self.logger.info("✅ Validating business rules and data quality")

            df_validated = df.copy()

            # Remove any records with invalid dates (future dates)
            current_date = pd.Timestamp.now().normalize()
            future_date_mask = df_validated["date"] > current_date
            if future_date_mask.any():
                self.logger.warning(f"Removing {future_date_mask.sum()} records with future dates")
                df_validated = df_validated[~future_date_mask]

            # Flag potential data quality issues
            df_validated["has_quality_flag"] = False

            # Flag records with missing or invalid postal codes
            invalid_postal = df_validated["postal_code"].isna() | ~df_validated[
                "postal_code"
            ].str.match(r"^\d{4}$")
            df_validated.loc[invalid_postal, "has_quality_flag"] = True

            # Flag companies with unusually high case counts in single inspection
            high_case_count = df_validated["case_count"] > 10
            if high_case_count.any():
                self.logger.info(
                    f"Flagged {high_case_count.sum()} records with high case counts (>10)"
                )
                df_validated.loc[high_case_count, "has_quality_flag"] = True

            # Add data quality score (0-1, where 1 is highest quality)
            quality_score = pd.Series(1.0, index=df_validated.index)
            quality_score -= (
                df_validated["has_quality_flag"].astype(float) * 0.2
            )  # -0.2 for quality flags
            quality_score -= (
                df_validated["postal_code"].isna().astype(float) * 0.1
            )  # -0.1 for missing postal
            df_validated["data_quality_score"] = quality_score.clip(0, 1)

            self.logger.info(
                f"✅ Validated {len(df_validated)} records with average quality score: "
                f"{df_validated['data_quality_score'].mean():.3f}"
            )
            return df_validated

        except Exception as e:
            self.logger.error(f"Error in business rule validation: {e}")
            return df

    async def _save_data(self, df: pd.DataFrame, stage: str) -> str:
        """Save the gold data to GCS."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Define output path
            dataset_path = f"{stage}/{self.config.dataset}/{timestamp}"
            filename = "workplace_inspections_gold.parquet"
            gcs_path = f"{dataset_path}/{filename}"
            full_gcs_path = f"gs://{self.config.bucket}/{gcs_path}"

            self.logger.info(f"💾 Saving gold data to: {full_gcs_path}")

            # Save to GCS using streaming
            with self.gcs_access.fs.open(full_gcs_path, "wb") as f:
                df.to_parquet(f, index=False)

            self.logger.info(f"✅ Successfully saved {len(df)} records to gold layer")

            # Log summary statistics
            self._log_summary_stats(df)

            return gcs_path

        except Exception as e:
            self.logger.error(f"Error saving gold data: {e}")
            raise

    def _log_summary_stats(self, df: pd.DataFrame):
        """Log summary statistics about the gold dataset."""
        try:
            total_records = len(df)
            date_range = f"{df['date'].min()} to {df['date'].max()}"
            unique_companies = df["company_id"].nunique()

            decision_counts = df["decision_type"].value_counts()
            top_industries = df["industry_clean"].value_counts().head(3)
            top_work_env_issues = (
                df["work_env_issue_formatted"].value_counts().head(3)
                if "work_env_issue_formatted" in df.columns
                else {}
            )

            avg_quality_score = df["data_quality_score"].mean()
            repeat_offenders = df["is_repeat_offender"].sum()

            self.logger.info("📊 Gold Layer Summary Statistics:")
            self.logger.info(f"   • Total records: {total_records:,}")
            self.logger.info(f"   • Date range: {date_range}")
            self.logger.info(f"   • Unique companies: {unique_companies:,}")
            self.logger.info(f"   • Average data quality score: {avg_quality_score:.3f}")
            self.logger.info(f"   • Repeat offenders: {repeat_offenders}")
            self.logger.info(f"   • Decision types: {dict(decision_counts)}")
            self.logger.info(f"   • Top 3 industries: {dict(top_industries)}")
            if len(top_work_env_issues) > 0:
                self.logger.info(f"   • Top 3 work environment issues: {dict(top_work_env_issues)}")

        except Exception as e:
            self.logger.warning(f"Could not generate summary statistics: {e}")
