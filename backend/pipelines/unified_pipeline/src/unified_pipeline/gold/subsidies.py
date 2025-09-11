"""
Gold layer processing for subsidies data.
Provides company-level aggregations and analytics for agricultural subsidies.

This module creates the final analytical dataset by aggregating all subsidies
data at the company level. Key features include:

1. Company-level aggregation across all subsidy types
2. Time-based analysis (yearly, multi-year trends)
3. Subsidy category breakdowns
4. Geographic analysis (municipality, region)
5. Company size classification and analysis
6. Risk and compliance indicators
7. Export to various analytical formats

Output datasets:
- company_subsidies_summary: High-level company aggregations
- company_subsidies_yearly: Year-by-year breakdowns
- company_subsidies_categories: Category-wise analysis
- subsidy_trends_analysis: Trend and pattern analysis
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd
from pydantic import ConfigDict

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface
from unified_pipeline.util.gcs_access import GCSDataAccess
from unified_pipeline.util.timing import timed


class SubsidiesGoldConfig(BaseJobConfig):
    """Configuration for Subsidies gold layer."""

    name: str = "Subsidies Gold Analytics"
    dataset: str = "subsidies"
    type: str = "gold"
    description: str = (
        "Company-level subsidies aggregations and analytics for agricultural subsidies"
    )
    frequency: str = "monthly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Input datasets
    silver_dataset: str = "subsidies"

    # Output datasets
    dataset_company_summary: str = "company_subsidies_summary"
    dataset_yearly_breakdown: str = "company_subsidies_yearly"
    dataset_category_analysis: str = "company_subsidies_categories"
    dataset_trends_analysis: str = "subsidy_trends_analysis"

    # Analysis configuration
    analysis_start_year: int = 2018
    analysis_end_year: int = 2027

    # Company size thresholds (based on total subsidies)
    small_company_threshold: float = 100000.0  # Under 100k DKK
    medium_company_threshold: float = 1000000.0  # 100k-1M DKK
    large_company_threshold: float = 10000000.0  # 1M-10M DKK
    # Above 10M DKK = very large

    # Risk analysis thresholds
    high_risk_subsidy_threshold: float = 5000000.0  # Companies with >5M DKK
    subsidy_growth_alert_threshold: float = 2.0  # 200% year-over-year growth

    model_config = ConfigDict(frozen=True)


class SubsidiesGold(BaseSource[SubsidiesGoldConfig], GoldJobInterface):
    """Gold layer processor for Subsidies data."""

    def __init__(self, config: SubsidiesGoldConfig):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.gcs_access = GCSDataAccess()

    @timed(name="Subsidies gold layer processing")
    async def run(self, silver_data: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """
        Process subsidies data into gold layer analytics.

        Args:
            silver_data: Optional in-memory silver data from previous pipeline stages

        Returns:
            Dict[str, Any]: Processing results and analytics
        """
        try:
            self.log.info("🏆 Starting Subsidies Gold processing")

            # Load silver data
            df = await self._load_silver_data(silver_data)
            if df is None or df.empty:
                raise ValueError("No silver data available for processing")

            self.log.info(f"📊 Loaded {len(df)} subsidy records")

            # Apply gold layer transformations
            df_processed = self._process_subsidies_data(df)

            # Generate analytics
            results = await self._generate_company_analytics(df_processed)

            self.log.info("✅ Subsidies Gold processing completed successfully")
            self.log.info(f"📊 Processed {results.get('total_companies', 0):,} companies")
            self.log.info(
                f"💰 Total subsidies: {results.get('total_subsidies_amount', 0):,.2f} DKK"
            )
            self.log.info(f"📋 Categories: {len(results.get('subsidy_categories', []))}")

            return results

        except Exception as e:
            self.log.error(f"❌ Error in Subsidies Gold processing: {e}")
            raise

    async def _load_silver_data(
        self, silver_data: Optional[Dict[str, pd.DataFrame]]
    ) -> Optional[pd.DataFrame]:
        """Load silver data from in-memory cache or GCS storage."""
        try:
            # Try in-memory first
            if silver_data and self.config.silver_dataset in silver_data:
                self.log.info("📥 Using in-memory silver data")
                return silver_data[self.config.silver_dataset]

            # Load from GCS
            self.log.info("💾 Loading subsidies silver data from GCS")
            return await self._load_subsidies_from_gcs()

        except Exception as e:
            self.log.error(f"Failed to load silver data: {e}")
            return None

    async def _load_subsidies_from_gcs(self) -> pd.DataFrame:
        """Load subsidies data from GCS silver layer with reliable connection management."""
        self.log.info("🔍 Discovering silver subsidies datasets...")

        # FIXED: Use a fresh DuckDB connection to avoid conflicts between base and GCS access
        # Create a completely independent connection for data loading
        data_conn = duckdb.connect(database=":memory:")

        try:
            # Configure the fresh connection
            self._configure_fresh_connection(data_conn)

            # Simple, direct approach - load known subsidies directory
            subsidies_path = "gs://landbrugsdata-raw-data/silver/subsidies/20250803_200555"

            self.log.info(f"📂 Loading subsidies from: {subsidies_path}")

            # Get file list using gcsfs directly
            files = self.gcs_access.fs.glob(f"{subsidies_path}/*.parquet")

            if not files:
                raise ValueError(f"No parquet files found in {subsidies_path}")

            self.log.info(f"📄 Found {len(files)} files to load")

            # Load each file using the fresh connection
            dataframes = []
            for i, file_path in enumerate(files):
                try:
                    file_name = file_path.split("/")[-1]
                    self.log.info(f"    Loading {file_name}...")

                    # Use direct read_parquet with the fresh connection
                    table_name = f"temp_subsidies_{i}"
                    full_gs_path = (
                        f"gs://{file_path}" if not file_path.startswith("gs://") else file_path
                    )

                    data_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    data_conn.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{full_gs_path}')"
                    )

                    # Get the data
                    df = data_conn.execute(f"SELECT * FROM {table_name}").df()

                    if df is not None and not df.empty:
                        # Add metadata
                        df["silver_source_dataset"] = "subsidies"
                        df["silver_source_file"] = file_name
                        df["subsidy_file_type"] = file_name.replace(".parquet", "")
                        dataframes.append(df)
                        self.log.info(f"    ✅ {file_name}: {len(df):,} records")

                    # Cleanup
                    data_conn.execute(f"DROP TABLE IF EXISTS {table_name}")

                except Exception as e:
                    self.log.warning(f"    ⚠️ Could not load {file_name}: {e}")
                    continue

            if not dataframes:
                raise ValueError("No subsidies data could be loaded")

            # Combine all dataframes
            self.log.info("📊 Combining loaded datasets...")
            combined_df = pd.concat(dataframes, ignore_index=True, sort=False)

            # Apply data processing
            combined_df = self._extract_year_from_dates(combined_df)
            combined_df = self._calculate_subsidy_amounts(combined_df)

            # Log CVR analysis of raw data
            self._analyze_cvr_data(combined_df)

            self.log.info(f"✅ Loaded {len(combined_df):,} total subsidy records from silver layer")
            return combined_df

        finally:
            # Clean up the data connection
            try:
                data_conn.close()
            except Exception:
                pass

    def _configure_fresh_connection(self, conn: duckdb.DuckDBPyConnection):
        """Configure a fresh DuckDB connection for data loading."""
        try:
            # Basic performance settings
            conn.execute("SET memory_limit = '12GB'")
            conn.execute("SET max_memory = '12GB'")
            conn.execute("SET threads = 4")
            conn.execute("SET enable_progress_bar = true")

            # Install spatial extension if needed
            try:
                conn.execute("INSTALL spatial; LOAD spatial;")
            except Exception:
                pass

            # Register gcsfs filesystem
            import os

            from fsspec import filesystem

            gcs_access_key = os.getenv("GCS_ACCESS_KEY_ID")
            gcs_secret_key = os.getenv("GCS_SECRET_ACCESS_KEY")

            if gcs_access_key and gcs_secret_key:
                fs = filesystem(
                    "gs", access_key_id=gcs_access_key, secret_access_key=gcs_secret_key
                )
            else:
                fs = filesystem("gs")

            conn.register_filesystem(fs)

            self.log.debug("✅ Fresh connection configured successfully")

        except Exception as e:
            self.log.warning(f"Fresh connection configuration warning: {e}")

    def _analyze_cvr_data(self, df: pd.DataFrame) -> None:
        """Analyze CVR data to understand company distribution."""
        self.log.info("🔍 Analyzing CVR data distribution...")

        # Check for CVR-related columns
        cvr_columns = [col for col in df.columns if "cvr" in col.lower()]
        self.log.info(f"   CVR-related columns found: {cvr_columns}")

        # Check for company-related columns
        company_columns = [
            col
            for col in df.columns
            if any(
                term in col.lower()
                for term in ["company", "virksomhed", "bedrift", "organization", "beneficiary"]
            )
        ]
        self.log.info(f"   Company-related columns found: {company_columns}")

        # Analyze main CVR column if it exists
        if "cvr_number" in df.columns:
            cvr_series = df["cvr_number"]
            total_records = len(df)
            non_null_cvr = cvr_series.notna().sum()
            unique_cvr = cvr_series.nunique()

            self.log.info("   CVR Analysis:")
            self.log.info(f"     Total records: {total_records:,}")
            self.log.info(
                f"     Records with CVR: {non_null_cvr:,} ({non_null_cvr/total_records*100:.1f}%)"
            )
            self.log.info(f"     Unique CVR numbers: {unique_cvr:,}")

            # Check CVR format distribution
            cvr_str = cvr_series.astype(str)
            length_dist = cvr_str.str.len().value_counts().sort_index()
            self.log.info(f"   CVR length distribution: {dict(length_dist)}")

            # Show some example CVR values
            sample_cvrs = cvr_series.dropna().head(10).tolist()
            self.log.info(f"   Sample CVR values: {sample_cvrs}")
        else:
            self.log.warning("   No 'cvr_number' column found for analysis")

        # Analyze year distribution
        if "year" in df.columns:
            year_series = df["year"]
            year_counts = year_series.value_counts().sort_index()
            self.log.info("   Year distribution:")
            for year, count in year_counts.head(20).items():
                self.log.info(f"     {year}: {count:,} records")
            if len(year_counts) > 20:
                self.log.info(f"     ... and {len(year_counts) - 20} more years")
        else:
            self.log.warning("   No 'year' column found for analysis")

    def _combine_silver_datasets(self, silver_datasets: List[tuple]) -> pd.DataFrame:
        """Combine multiple silver datasets into one DataFrame."""
        all_dataframes = []

        for dataset_name, df in silver_datasets:
            # Standardize column names
            df = df.copy()

            # Add dataset source if not already present
            if "silver_source_dataset" not in df.columns:
                df["silver_source_dataset"] = dataset_name

            all_dataframes.append(df)

        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True, sort=False)

        # Extract year information
        combined_df = self._extract_year_from_dates(combined_df)

        return combined_df

    def _extract_year_from_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract subsidy year from various date columns and dataset sources."""
        self.log.info("📅 Extracting year information from subsidy records...")

        df = df.copy()

        # Initialize year column if it doesn't exist
        if "year" not in df.columns:
            df["year"] = None

        # Keep existing year values where they exist
        existing_years = df["year"].notna().sum()
        if existing_years > 0:
            self.log.info(f"   Found existing year data for {existing_years:,} records")

        # Extract from silver_source_dataset (folder names like '2023', '2024')
        def extract_year_from_dataset(row):
            if pd.notna(row["year"]):
                return row["year"]

            dataset = str(row.get("silver_source_dataset", ""))
            for potential_year in ["2023", "2024", "2025", "2026"]:
                if potential_year in dataset:
                    return int(potential_year)
            return None

        df["year"] = df.apply(extract_year_from_dataset, axis=1)

        # Extract from date columns as fallback
        date_columns = ["end_date", "start_date", "dato", "period_end", "period_start"]

        for col in date_columns:
            if col in df.columns:
                missing_years = df["year"].isna()
                if missing_years.sum() > 0:
                    try:
                        dates = pd.to_datetime(df.loc[missing_years, col], errors="coerce")
                        years = dates.dt.year
                        df.loc[missing_years, "year"] = years
                        filled = years.notna().sum()
                        if filled > 0:
                            self.log.info(f"   Extracted {filled:,} years from {col}")
                    except Exception as e:
                        self.log.debug(f"Could not extract years from {col}: {e}")

        years_extracted = df["year"].notna().sum()
        self.log.info(
            f"✅ Total records with year information: {years_extracted:,} out of {len(df):,}"
        )

        return df

    def _calculate_subsidy_amounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate standardized subsidy amounts from various data formats."""
        self.log.info("💰 Calculating subsidy amounts from raw data...")

        df = df.copy()
        df["amount_dkk"] = 0.0

        # Convert monetary amount columns
        amount_keywords = ["beløb", "beloeb", "amount", "kroner", "dkk"]
        for col in df.columns:
            if any(term in col.lower() for term in amount_keywords):
                try:
                    temp_col = df[col].astype(str)
                    temp_col = (
                        temp_col.str.replace(",", ".").str.replace(" ", "").str.replace("kr", "")
                    )
                    numeric_amounts = pd.to_numeric(temp_col, errors="coerce").fillna(0)
                    df["amount_dkk"] = df["amount_dkk"] + numeric_amounts
                    self.log.info(f"   Converted {col}: {numeric_amounts.sum():,.2f} DKK total")
                except Exception as e:
                    self.log.warning(f"   Failed to convert {col}: {e}")

        # Convert specific year payout columns
        for col in df.columns:
            if "udbetalt" in col.lower() and any(year in col for year in ["2023", "2024", "2025"]):
                try:
                    temp_col = df[col].astype(str)
                    temp_col = (
                        temp_col.str.replace(",", ".").str.replace(" ", "").str.replace("kr", "")
                    )
                    numeric_amounts = pd.to_numeric(temp_col, errors="coerce").fillna(0)
                    df["amount_dkk"] = df["amount_dkk"] + numeric_amounts
                    self.log.info(f"   Converted {col}: {numeric_amounts.sum():,.2f} DKK total")
                except Exception as e:
                    self.log.warning(f"   Failed to convert {col}: {e}")

        # Calculate from area if no monetary amounts
        if df["amount_dkk"].sum() == 0:
            area_cols = [
                col for col in df.columns if "area" in col.lower() or "areal" in col.lower()
            ]
            for area_col in area_cols:
                try:
                    area_values = pd.to_numeric(df[area_col], errors="coerce").fillna(0)
                    subsidy_rate = 1000.0  # DKK per hectare
                    area_amounts = area_values * subsidy_rate
                    df["amount_dkk"] = df["amount_dkk"] + area_amounts
                    self.log.info(
                        f"   Calculated from {area_col}: {area_amounts.sum():,.2f} DKK "
                        f"(rate: {subsidy_rate}/ha)"
                    )
                except Exception as e:
                    self.log.warning(f"   Failed to calculate from {area_col}: {e}")

        # Calculate from animal counts
        animal_cols = [col for col in df.columns if "antal" in col.lower() and "dyr" in col.lower()]
        for animal_col in animal_cols:
            try:
                animal_values = pd.to_numeric(df[animal_col], errors="coerce").fillna(0)
                animal_rate = 500.0  # DKK per animal
                animal_amounts = animal_values * animal_rate
                df["amount_dkk"] = df["amount_dkk"] + animal_amounts
                self.log.info(
                    f"   Calculated from {animal_col}: {animal_amounts.sum():,.2f} DKK "
                    f"(rate: {animal_rate}/animal)"
                )
            except Exception as e:
                self.log.warning(f"   Failed to calculate from {animal_col}: {e}")

        total_calculated = df["amount_dkk"].sum()
        records_with_amounts = (df["amount_dkk"] > 0).sum()

        self.log.info(f"💰 Total calculated subsidies: {total_calculated:,.2f} DKK")
        self.log.info(
            f"💰 Records with monetary amounts: {records_with_amounts:,} out of {len(df):,}"
        )

        return df

    def _process_subsidies_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process subsidies data - keeping ALL data without filtering."""
        self.log.info("🧹 Processing subsidies data (NO FILTERING - keeping all data)...")

        df = df.copy()

        # Log initial company count
        initial_records = len(df)
        initial_companies = 0
        if "cvr_number" in df.columns:
            # Count all non-null CVR entries first
            initial_cvr_count = df["cvr_number"].notna().sum()
            initial_companies = df["cvr_number"].nunique()
            self.log.info(
                f"   Initial: {initial_records:,} records, {initial_cvr_count:,} with CVR, "
                f"{initial_companies:,} unique CVR numbers"
            )
        else:
            self.log.warning("   No 'cvr_number' column found in data!")

        # Basic CVR formatting only (no length filtering)
        if "cvr_number" in df.columns:
            df["cvr_number"] = df["cvr_number"].astype(str).str.strip()
            self.log.info("   CVR numbers formatted (no filtering applied)")

        # NO YEAR FILTERING - keep all records regardless of year
        if "year" in df.columns:
            records_with_year = df["year"].notna().sum()
            records_without_year = df["year"].isna().sum()
            self.log.info(
                f"   Year data: {records_with_year:,} records with year, "
                f"{records_without_year:,} without year (all kept)"
            )

        # NO CVR NULL FILTERING - keep records even without CVR
        # NO AMOUNT FILTERING - keep records even with zero/null amounts

        final_count = len(df)
        final_companies = df["cvr_number"].nunique() if "cvr_number" in df.columns else 0

        self.log.info("   ✅ NO FILTERING APPLIED - All data preserved:")
        self.log.info(f"     Final records: {final_count:,}")
        self.log.info(f"     Final companies: {final_companies:,}")

        return df

    async def _generate_company_analytics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive company-level analytics."""
        self.log.info("📈 Generating company-level analytics...")

        # Company summary
        company_summary = self._calculate_company_summary(df)

        # Yearly breakdown
        yearly_breakdown = self._calculate_yearly_breakdown(df)

        # Category analysis
        category_analysis = self._calculate_category_analysis(df)

        # Generate summary statistics
        summary_stats = self._generate_summary_stats(
            df, company_summary, yearly_breakdown, category_analysis
        )

        # Save analytics datasets
        await self._save_analytics_data(company_summary, yearly_breakdown, category_analysis)

        return summary_stats

    def _calculate_company_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate company-level summary statistics."""
        self.log.info("📊 Calculating company summary statistics...")

        # Only calculate for records that have CVR numbers
        if "cvr_number" not in df.columns:
            self.log.warning("No CVR column found - returning empty summary")
            return pd.DataFrame()

        # Filter to records with CVR for company analysis
        df_with_cvr = df[
            df["cvr_number"].notna() & (df["cvr_number"] != "") & (df["cvr_number"] != "nan")
        ]

        if df_with_cvr.empty:
            self.log.warning("No records with valid CVR numbers found")
            return pd.DataFrame()

        self.log.info(
            f"   Analyzing {len(df_with_cvr):,} records with CVR numbers from "
            f"{df_with_cvr['cvr_number'].nunique():,} companies"
        )

        # Determine amount and category columns
        amount_col = "amount_dkk" if "amount_dkk" in df_with_cvr.columns else None
        if not amount_col:
            for col in ["beløb", "amount", "value", "kroner", "subsidies_amount", "area_ha"]:
                if col in df_with_cvr.columns:
                    amount_col = col
                    break

        category_col = None
        for col in ["subsidy_category", "subsidy_measure", "subsidy_type_code"]:
            if col in df_with_cvr.columns:
                category_col = col
                break

        # Group by company
        agg_dict = {
            "year": ["min", "max", "nunique"],
        }

        if amount_col:
            agg_dict[amount_col] = ["sum", "mean", "count"]

        if category_col:
            agg_dict[category_col] = "nunique"

        summary = df_with_cvr.groupby("cvr_number").agg(agg_dict).round(2)

        # Flatten column names
        summary.columns = [
            "_".join(col).strip() if isinstance(col, tuple) else col for col in summary.columns
        ]
        summary = summary.reset_index()

        # Add company size classification
        if amount_col:
            total_amount_col = f"{amount_col}_sum"
            if total_amount_col in summary.columns:
                summary["company_size"] = pd.cut(
                    summary[total_amount_col],
                    bins=[
                        0,
                        self.config.small_company_threshold,
                        self.config.medium_company_threshold,
                        self.config.large_company_threshold,
                        float("inf"),
                    ],
                    labels=["Small", "Medium", "Large", "Very Large"],
                    include_lowest=True,
                )

        self.log.info(f"   Generated summary for {len(summary):,} companies")
        return summary

    def _calculate_yearly_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate yearly breakdown by company."""
        self.log.info("📅 Calculating yearly breakdown...")

        # Only calculate for records that have CVR numbers
        if "cvr_number" not in df.columns:
            self.log.warning("No CVR column found - returning empty yearly breakdown")
            return pd.DataFrame()

        # Filter to records with CVR for company analysis
        df_with_cvr = df[
            df["cvr_number"].notna() & (df["cvr_number"] != "") & (df["cvr_number"] != "nan")
        ]

        if df_with_cvr.empty:
            self.log.warning("No records with valid CVR numbers found for yearly breakdown")
            return pd.DataFrame()

        if "year" not in df_with_cvr.columns:
            self.log.warning("No year column found for yearly breakdown")
            return pd.DataFrame()

        # Determine amount column
        amount_col = "amount_dkk" if "amount_dkk" in df_with_cvr.columns else None
        if not amount_col:
            for col in ["beløb", "amount", "value", "kroner"]:
                if col in df_with_cvr.columns:
                    amount_col = col
                    break

        if not amount_col:
            self.log.warning("No amount column found for yearly breakdown")
            return pd.DataFrame()

        # Group by company and year
        agg_dict = {amount_col: ["sum", "count"]}

        # Add category info if available
        for col in ["subsidy_category", "subsidy_measure", "subsidy_type_code"]:
            if col in df_with_cvr.columns:
                agg_dict[col] = "nunique"
                break

        yearly = df_with_cvr.groupby(["cvr_number", "year"]).agg(agg_dict).round(2)
        yearly.columns = [
            "_".join(col).strip() if isinstance(col, tuple) else col for col in yearly.columns
        ]
        yearly = yearly.reset_index()

        # Calculate year-over-year change
        if amount_col:
            total_col = f"{amount_col}_sum"
            if total_col in yearly.columns:
                yearly = yearly.sort_values(["cvr_number", "year"])
                yearly["yoy_change"] = yearly.groupby("cvr_number")[total_col].pct_change() * 100
                yearly["yoy_change"] = yearly["yoy_change"].replace([np.inf, -np.inf], np.nan)

        self.log.info(f"   Generated yearly breakdown: {len(yearly):,} company-year records")
        return yearly

    def _calculate_category_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate subsidy category analysis."""
        self.log.info("📋 Calculating category analysis...")

        # Find category and amount columns
        category_col = None
        for col in ["subsidy_category", "subsidy_measure", "subsidy_type_code"]:
            if col in df.columns:
                category_col = col
                break

        amount_col = "amount_dkk" if "amount_dkk" in df.columns else None
        if not amount_col:
            for col in ["beløb", "amount", "value"]:
                if col in df.columns:
                    amount_col = col
                    break

        if not category_col or not amount_col:
            self.log.warning("Missing category or amount columns for category analysis")
            return pd.DataFrame()

        # Group by company and category
        category_analysis = (
            df.groupby(["cvr_number", category_col]).agg({amount_col: ["sum", "count"]}).round(2)
        )

        category_analysis.columns = ["_".join(col).strip() for col in category_analysis.columns]
        category_analysis = category_analysis.reset_index()

        # Calculate category share for each company
        total_col = f"{amount_col}_sum"
        if total_col in category_analysis.columns:
            company_totals = category_analysis.groupby("cvr_number")[total_col].sum()
            category_analysis = category_analysis.merge(
                company_totals.rename("company_total"), left_on="cvr_number", right_index=True
            )

            # Avoid division by zero
            category_analysis["category_share"] = np.where(
                category_analysis["company_total"] > 0,
                (category_analysis[total_col] / category_analysis["company_total"] * 100).round(2),
                0,
            )

        self.log.info(
            f"   Generated category analysis: {len(category_analysis):,} company-category records"
        )
        return category_analysis

    def _generate_summary_stats(
        self,
        df: pd.DataFrame,
        company_summary: pd.DataFrame,
        yearly_breakdown: pd.DataFrame,
        category_analysis: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Generate summary statistics for the processing results."""

        # Calculate total subsidies amount
        total_subsidies_amount = df["amount_dkk"].sum() if "amount_dkk" in df.columns else 0.0

        # Get analysis period
        if "year" in df.columns:
            min_year = df["year"].min()
            max_year = df["year"].max()
            analysis_period = f"{min_year}-{max_year}" if min_year != max_year else str(min_year)
        else:
            analysis_period = f"{self.config.analysis_start_year}-{self.config.analysis_end_year}"

        # Get categories
        subsidy_categories = self._get_subsidy_categories(df)

        # Company size distribution
        company_size_dist = {}
        if "company_size" in company_summary.columns:
            company_size_dist = company_summary["company_size"].value_counts().to_dict()

        # Log category breakdown
        if subsidy_categories:
            self.log.info(
                f"📋 Category breakdown: {len(subsidy_categories)} unique categories found"
            )
            for i, category in enumerate(subsidy_categories[:10]):  # Show first 10
                self.log.info(f"   {i+1}. {category}")
            if len(subsidy_categories) > 10:
                self.log.info(f"   ... and {len(subsidy_categories) - 10} more")

        # Top subsidy types by amount
        top_subsidy_types = self._analyze_subsidy_types_by_amount(df)
        if top_subsidy_types:
            self.log.info("💰 Top subsidy types by amount:")
            for category_type, categories in top_subsidy_types.items():
                self.log.info(f"   {category_type}:")
                for cat_name, cat_data in list(categories.items())[:5]:  # Show top 5
                    self.log.info(
                        f"     • {cat_name}: {cat_data['total_amount']:,.2f} DKK "
                        f"({cat_data['record_count']:,} records)"
                    )

        return {
            "total_companies": len(company_summary),
            "total_subsidy_records": len(df),
            "total_subsidies_amount": total_subsidies_amount,
            "analysis_period": analysis_period,
            "subsidy_categories": subsidy_categories,
            "company_size_distribution": company_size_dist,
            "yearly_records": len(yearly_breakdown),
            "category_records": len(category_analysis),
            "top_subsidy_types": top_subsidy_types,
            "processing_timestamp": datetime.now().isoformat(),
        }

    def _get_subsidy_categories(self, df: pd.DataFrame) -> List[str]:
        """Extract subsidy categories from available category columns."""
        categories = []
        category_columns = [
            "subsidy_category",
            "subsidy_measure",
            "subsidy_type_code",
            "ordning",
            "silver_source_dataset",
        ]

        for col in category_columns:
            if col in df.columns:
                unique_values = df[col].dropna().unique()
                categories.extend([str(val) for val in unique_values if str(val) != "nan"])
                break

        # Also add dataset-based categories
        if "silver_source_dataset" in df.columns:
            datasets = df["silver_source_dataset"].dropna().unique()
            dataset_categories = [f"Dataset: {dataset}" for dataset in datasets]
            categories.extend(dataset_categories)

        categories = sorted(list(set(categories)))

        if categories:
            self.log.info(
                f"Found {len(categories)} subsidy categories. Examples: {categories[:10]}"
            )

        return categories

    def _analyze_subsidy_types_by_amount(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """Analyze subsidy types by total amount to show the biggest categories."""
        if "amount_dkk" not in df.columns:
            return {}

        analysis = {}
        category_columns = [
            "subsidy_measure",
            "subsidy_type_code",
            "ordning",
            "silver_source_dataset",
        ]

        for col in category_columns:
            if col in df.columns:
                category_totals = (
                    df.groupby(col)["amount_dkk"].agg(["sum", "count", "mean"]).round(2)
                )
                category_totals = category_totals.sort_values("sum", ascending=False)

                category_dict = {}
                for category, data in category_totals.head(10).iterrows():
                    category_dict[str(category)] = {
                        "total_amount": float(data["sum"]),
                        "record_count": int(data["count"]),
                        "average_amount": float(data["mean"]),
                    }

                analysis[col] = category_dict
                break

        return analysis

    async def _save_analytics_data(
        self,
        company_summary: pd.DataFrame,
        yearly_breakdown: pd.DataFrame,
        category_analysis: pd.DataFrame,
    ) -> None:
        """Save analytics datasets to GCS with reliable connection management."""
        self.log.info("💾 Saving analytics datasets...")

        datasets = [
            (company_summary, self.config.dataset_company_summary),
            (yearly_breakdown, self.config.dataset_yearly_breakdown),
            (category_analysis, self.config.dataset_category_analysis),
        ]

        # FIXED: Use a fresh connection for saving to avoid connection issues
        save_conn = duckdb.connect(database=":memory:")

        try:
            # Configure the save connection
            self._configure_fresh_connection(save_conn)

            for df, dataset_name in datasets:
                if not df.empty:
                    try:
                        # Create table in fresh DuckDB connection
                        table_name = f"temp_{dataset_name}"
                        save_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                        save_conn.register(table_name, df)

                        # Save to GCS using direct parquet export
                        timestamp = self.date_pattern
                        output_path = f"gold/{dataset_name}/{timestamp}/data.parquet"
                        full_gcs_path = f"gs://{self.config.bucket}/{output_path}"

                        # Export directly to GCS
                        save_conn.execute(
                            f"COPY {table_name} TO '{full_gcs_path}' (FORMAT PARQUET)"
                        )

                        self.log.info(
                            f"   ✅ Saved {dataset_name}: {len(df):,} records to {output_path}"
                        )

                    except Exception as e:
                        self.log.warning(f"Could not save {dataset_name}: {e}")
                else:
                    self.log.warning(f"Skipping empty dataset: {dataset_name}")

        finally:
            # Clean up the save connection
            try:
                save_conn.close()
            except Exception:
                pass

    def _validate_results(self, results: Dict[str, Any]) -> bool:
        """Validate the processing results."""
        required_fields = ["total_companies", "total_subsidy_records", "total_subsidies_amount"]

        for field in required_fields:
            if field not in results:
                self.log.warning(f"Missing required field: {field}")
                return False

        # Basic sanity checks
        if results.get("total_companies", 0) <= 0:
            self.log.warning("No companies found in results")
            return False

        if results.get("total_subsidy_records", 0) <= 0:
            self.log.warning("No subsidy records found in results")
            return False

        return True
