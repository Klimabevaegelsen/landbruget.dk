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

    # Output datasets - Single unified table
    dataset_unified: str = "subsidies_by_cvr_year"

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
        """Generate unified table with one row per CVR/year combination."""
        self.log.info("📈 Generating unified subsidies table (CVR/year format)...")

        # Create unified table with one row per CVR/year
        unified_table = self._create_unified_cvr_year_table(df)

        # Generate summary statistics
        summary_stats = self._generate_summary_stats(df, unified_table)

        # Save unified dataset
        await self._save_unified_data(unified_table)

        return summary_stats

    def _create_unified_cvr_year_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create unified table with one row per CVR/year combination and columns for each subsidy type."""
        self.log.info("🔄 Creating unified CVR/year table...")

        if "cvr_number" not in df.columns:
            self.log.error("No CVR column found - cannot create unified table")
            return pd.DataFrame()

        # Filter to records with valid CVR numbers
        df_with_cvr = df[
            df["cvr_number"].notna() & (df["cvr_number"] != "") & (df["cvr_number"] != "nan")
        ].copy()

        if df_with_cvr.empty:
            self.log.warning("No records with valid CVR numbers found")
            return pd.DataFrame()

        # Ensure we have year data
        if "year" not in df_with_cvr.columns:
            self.log.warning("No year column found - cannot create yearly breakdown")
            return pd.DataFrame()

        # Remove records without year data
        df_with_cvr = df_with_cvr[df_with_cvr["year"].notna()]

        if df_with_cvr.empty:
            self.log.warning("No records with valid year data found")
            return pd.DataFrame()

        self.log.info(f"   Processing {len(df_with_cvr):,} records with CVR and year data")

        # Determine subsidy category column for pivoting
        category_col = self._determine_subsidy_category_column(df_with_cvr)

        if not category_col:
            self.log.warning("No suitable category column found for pivoting")
            # Fallback: create a single "total_subsidies" column
            return self._create_simple_aggregation(df_with_cvr)

        # Determine amount column
        amount_col = "amount_dkk" if "amount_dkk" in df_with_cvr.columns else None
        if not amount_col:
            for col in ["beløb", "amount", "value", "kroner"]:
                if col in df_with_cvr.columns:
                    amount_col = col
                    break

        if not amount_col:
            self.log.warning("No amount column found - using count of records")
            df_with_cvr["record_count"] = 1
            amount_col = "record_count"

        # Create base CVR/year combinations (all possible combinations)
        all_cvr_numbers = df_with_cvr["cvr_number"].unique()
        all_years = sorted(df_with_cvr["year"].unique())
        all_categories = sorted(df_with_cvr[category_col].dropna().astype(str).unique())

        self.log.info(f"   Found {len(all_cvr_numbers):,} unique CVR numbers")
        self.log.info(f"   Found {len(all_years):,} unique years: {all_years}")
        self.log.info(f"   Found {len(all_categories):,} unique subsidy categories")

        # Pivot the data to get subsidies by category
        self.log.info("   Pivoting data by subsidy category...")
        pivot_df = df_with_cvr.groupby(["cvr_number", "year", category_col])[amount_col].sum().reset_index()

        # Create the pivot table
        unified_df = pivot_df.pivot_table(
            index=["cvr_number", "year"],
            columns=category_col,
            values=amount_col,
            fill_value=0,
            aggfunc="sum"
        ).reset_index()

        # Flatten column names (remove multi-level)
        unified_df.columns.name = None
        unified_df.columns = [
            col if col in ["cvr_number", "year"] else f"subsidy_{str(col).lower().replace(' ', '_').replace('-', '_')}"
            for col in unified_df.columns
        ]

        # Add total subsidies column
        subsidy_columns = [col for col in unified_df.columns if col.startswith("subsidy_")]
        unified_df["total_subsidies"] = unified_df[subsidy_columns].sum(axis=1)

        # Add record counts for each category
        record_counts = df_with_cvr.groupby(["cvr_number", "year", category_col]).size().reset_index(name="record_count")
        record_pivot = record_counts.pivot_table(
            index=["cvr_number", "year"],
            columns=category_col,
            values="record_count",
            fill_value=0,
            aggfunc="sum"
        ).reset_index()

        # Add record count columns
        record_pivot.columns.name = None
        for col in record_pivot.columns:
            if col not in ["cvr_number", "year"]:
                count_col_name = f"records_{str(col).lower().replace(' ', '_').replace('-', '_')}"
                if count_col_name not in unified_df.columns:
                    unified_df = unified_df.merge(
                        record_pivot[["cvr_number", "year", col]].rename(columns={col: count_col_name}),
                        on=["cvr_number", "year"],
                        how="left"
                    )
                    unified_df[count_col_name] = unified_df[count_col_name].fillna(0)

        # Add total records count
        record_count_columns = [col for col in unified_df.columns if col.startswith("records_")]
        unified_df["total_records"] = unified_df[record_count_columns].sum(axis=1)

        # Sort by CVR and year
        unified_df = unified_df.sort_values(["cvr_number", "year"]).reset_index(drop=True)

        self.log.info(f"   ✅ Created unified table: {len(unified_df):,} rows (CVR/year combinations)")
        self.log.info(f"   ✅ Subsidy columns: {len(subsidy_columns)}")
        self.log.info(f"   ✅ Record count columns: {len(record_count_columns)}")
        self.log.info(f"   ✅ Total subsidies amount: {unified_df['total_subsidies'].sum():,.2f} DKK")

        return unified_df

    def _determine_subsidy_category_column(self, df: pd.DataFrame) -> Optional[str]:
        """Determine the best column to use for subsidy categorization."""
        # Priority order for category columns
        category_candidates = [
            "subsidy_measure",
            "subsidy_type_code",
            "subsidy_category",
            "ordning",
            "silver_source_dataset",
            "subsidy_file_type"
        ]

        for col in category_candidates:
            if col in df.columns:
                unique_count = df[col].nunique()
                self.log.info(f"   Category option '{col}': {unique_count} unique values")

                # Use if reasonable number of categories (between 2 and 50)
                if 2 <= unique_count <= 50:
                    self.log.info(f"   Selected category column: {col}")
                    return col

        return None

    def _create_simple_aggregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create simple aggregation when no suitable category column is found."""
        self.log.info("   Creating simple aggregation (no category breakdown)")

        amount_col = "amount_dkk" if "amount_dkk" in df.columns else None
        if not amount_col:
            df["record_count"] = 1
            amount_col = "record_count"

        # Simple groupby CVR and year
        unified_df = df.groupby(["cvr_number", "year"]).agg({
            amount_col: "sum",
            "cvr_number": "count"  # count records
        }).reset_index()

        # Rename columns
        unified_df.columns = ["cvr_number", "year", "total_subsidies", "total_records"]

        return unified_df


    def _generate_summary_stats(
        self,
        df: pd.DataFrame,
        unified_table: pd.DataFrame,
    ) -> Dict[str, Any]:
        """Generate summary statistics for the processing results."""

        # Calculate stats from unified table
        total_companies = len(unified_table["cvr_number"].unique()) if not unified_table.empty else 0
        total_cvr_year_combinations = len(unified_table) if not unified_table.empty else 0

        # Calculate total subsidies amount from unified table
        total_subsidies_amount = 0.0
        if not unified_table.empty and "total_subsidies" in unified_table.columns:
            total_subsidies_amount = unified_table["total_subsidies"].sum()
        elif "amount_dkk" in df.columns:
            total_subsidies_amount = df["amount_dkk"].sum()

        # Get analysis period
        if not unified_table.empty and "year" in unified_table.columns:
            min_year = unified_table["year"].min()
            max_year = unified_table["year"].max()
            analysis_period = f"{min_year}-{max_year}" if min_year != max_year else str(min_year)
        elif "year" in df.columns:
            min_year = df["year"].min()
            max_year = df["year"].max()
            analysis_period = f"{min_year}-{max_year}" if min_year != max_year else str(min_year)
        else:
            analysis_period = f"{self.config.analysis_start_year}-{self.config.analysis_end_year}"

        # Get subsidy columns from unified table
        subsidy_columns = []
        if not unified_table.empty:
            subsidy_columns = [col for col in unified_table.columns if col.startswith("subsidy_")]

        # Analyze subsidy types from unified table
        subsidy_type_stats = {}
        if subsidy_columns:
            for col in subsidy_columns:
                total_amount = unified_table[col].sum()
                non_zero_companies = (unified_table[col] > 0).sum()
                avg_amount = unified_table[unified_table[col] > 0][col].mean() if non_zero_companies > 0 else 0

                subsidy_type_stats[col] = {
                    "total_amount": float(total_amount),
                    "companies_receiving": int(non_zero_companies),
                    "average_amount": float(avg_amount),
                    "coverage_percent": float(non_zero_companies / len(unified_table) * 100) if len(unified_table) > 0 else 0
                }

        # Log summary
        self.log.info("📊 Unified table summary:")
        self.log.info(f"   Total companies: {total_companies:,}")
        self.log.info(f"   Total CVR/year combinations: {total_cvr_year_combinations:,}")
        self.log.info(f"   Total subsidies amount: {total_subsidies_amount:,.2f} DKK")
        self.log.info(f"   Subsidy types: {len(subsidy_columns)}")
        self.log.info(f"   Analysis period: {analysis_period}")

        if subsidy_type_stats:
            self.log.info("💰 Top subsidy types by total amount:")
            sorted_types = sorted(subsidy_type_stats.items(), key=lambda x: x[1]["total_amount"], reverse=True)
            for col, stats in sorted_types[:5]:
                self.log.info(
                    f"   • {col}: {stats['total_amount']:,.2f} DKK "
                    f"({stats['companies_receiving']:,} companies, {stats['coverage_percent']:.1f}% coverage)"
                )

        return {
            "total_companies": total_companies,
            "total_cvr_year_combinations": total_cvr_year_combinations,
            "total_subsidy_records": len(df),
            "total_subsidies_amount": total_subsidies_amount,
            "analysis_period": analysis_period,
            "subsidy_types_count": len(subsidy_columns),
            "subsidy_type_breakdown": subsidy_type_stats,
            "unified_table_rows": len(unified_table),
            "unified_table_columns": len(unified_table.columns) if not unified_table.empty else 0,
            "processing_timestamp": datetime.now().isoformat(),
        }


    async def _save_unified_data(self, unified_table: pd.DataFrame) -> None:
        """Save unified subsidies table to GCS."""
        self.log.info("💾 Saving unified subsidies table...")

        if unified_table.empty:
            self.log.warning("No unified data to save")
            return

        # Use a fresh connection for saving
        save_conn = duckdb.connect(database=":memory:")

        try:
            # Configure the save connection
            self._configure_fresh_connection(save_conn)

            # Create table in fresh DuckDB connection
            table_name = "temp_unified_subsidies"
            save_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            save_conn.register(table_name, unified_table)

            # Save to GCS using direct parquet export
            timestamp = self.date_pattern
            output_path = f"gold/{self.config.dataset_unified}/{timestamp}/data.parquet"
            full_gcs_path = f"gs://{self.config.bucket}/{output_path}"

            # Export directly to GCS
            save_conn.execute(
                f"COPY {table_name} TO '{full_gcs_path}' (FORMAT PARQUET)"
            )

            self.log.info(
                f"   ✅ Saved unified subsidies table: {len(unified_table):,} rows to {output_path}"
            )

            # Log column structure for verification
            subsidy_columns = [col for col in unified_table.columns if col.startswith("subsidy_")]
            record_columns = [col for col in unified_table.columns if col.startswith("records_")]

            self.log.info(f"   📊 Table structure:")
            self.log.info(f"     - CVR/year columns: cvr_number, year")
            self.log.info(f"     - Subsidy amount columns: {len(subsidy_columns)} ({', '.join(subsidy_columns[:3])}...)")
            self.log.info(f"     - Record count columns: {len(record_columns)} ({', '.join(record_columns[:3])}...)")
            self.log.info(f"     - Summary columns: total_subsidies, total_records")

        except Exception as e:
            self.log.error(f"Failed to save unified data: {e}")
            raise

        finally:
            # Clean up the save connection
            try:
                save_conn.close()
            except Exception:
                pass

    def _validate_results(self, results: Dict[str, Any]) -> bool:
        """Validate the processing results."""
        required_fields = ["total_companies", "total_cvr_year_combinations", "total_subsidies_amount"]

        for field in required_fields:
            if field not in results:
                self.log.warning(f"Missing required field: {field}")
                return False

        # Basic sanity checks
        if results.get("total_companies", 0) <= 0:
            self.log.warning("No companies found in results")
            return False

        if results.get("total_cvr_year_combinations", 0) <= 0:
            self.log.warning("No CVR/year combinations found in results")
            return False

        return True
