import pandas as pd
from pathlib import Path
from ...base import BaseSource as Source
import os
import re
from typing import List, Dict, Any

class CatchCrops(Source):
    """Parser for Efterafgrøder (Catch Crops) data"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    @property
    def source_id(self) -> str:
        """Unique identifier for this source"""
        return "catch_crops"

    def _extract_year(self, filename: str) -> str:
        """Extract year from filename"""
        match = re.search(r'Efterafgrøder (\d{4})', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Year not found in filename: {filename}")

    def _read_excel_files(self) -> List[pd.DataFrame]:
        """Read all Efterafgrøder Excel files"""
        current_dir = Path(__file__).parent
        dfs = []

        for file in current_dir.glob('Efterafgrøder*.xlsx'):
            try:
                df = pd.read_excel(file, engine='openpyxl')
                df['source_file'] = file.name
                df['year'] = self._extract_year(file.name)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")

        if not dfs:
            raise FileNotFoundError("No Efterafgrøder files found")

        return dfs

    async def fetch(self) -> pd.DataFrame:
        """Fetch and process catch crops data"""
        dfs = self._read_excel_files()
        df = pd.concat(dfs, ignore_index=True)

        # Clean and standardize data
        df = df.fillna('')

        return df

    async def sync(self) -> int:
        """Sync catch crops data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/catch_crops_current.parquet"
        df.to_parquet(temp_file)

        # Upload to storage
        blob = self.bucket.blob('raw/catch_crops/current.parquet')
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)

class FertilizerAccounts(Source):
    """Parser for Gødningsregnskaber (Fertilizer Accounts) data"""

    EXPECTED_COLUMNS = [
        'CVR',
        'F_504_1',  # Base nitrogen quota
        'F_505_1',  # Additional nitrogen quota
        'F_512',    # Corrected quota
        'F_901',    # Total nitrogen consumption
        'F_610',    # Animal fertilizer usage
        'F_706_1',  # Mineral fertilizer usage
        'F_193'     # Other organic fertilizer usage
    ]

    # Define column types for better memory usage and performance
    COLUMN_TYPES = {
        'CVR': 'str',
        'F_504_1': 'float64',
        'F_505_1': 'float64',
        'F_512': 'float64',
        'F_901': 'float64',
        'F_610': 'float64',
        'F_706_1': 'float64',
        'F_193': 'float64',
        'year': 'str',
        'source_file': 'str'
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    @property
    def source_id(self) -> str:
        """Unique identifier for this source"""
        return "fertilizer_accounts"

    def _extract_year(self, filename: str) -> str:
        """Extract year from filename"""
        match = re.search(r'Gødningsregnskaber (\d{4})', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Year not found in filename: {filename}")

    def _read_excel_files(self) -> List[pd.DataFrame]:
        """Read all Gødningsregnskaber Excel files"""
        current_dir = Path(__file__).parent
        dfs = []

        for file in current_dir.glob('Gødningsregnskaber*.xlsx'):
            try:
                # Read with specified data types where possible
                df = pd.read_excel(
                    file,
                    engine='openpyxl',
                    dtype={
                        'CVR': str,
                        'year': str,
                        'source_file': str
                    }
                )
                df['source_file'] = file.name
                df['year'] = self._extract_year(file.name)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")

        if not dfs:
            raise FileNotFoundError("No Gødningsregnskaber files found")

        return dfs

    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        # Ensure all expected columns exist
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Convert numeric columns all at once
        numeric_cols = [
            'F_504_1', 'F_505_1', 'F_512', 'F_901',
            'F_610', 'F_706_1', 'F_193'
        ]

        # Convert to numeric in a single operation
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Calculate derived fields efficiently
        df = df.assign(
            original_quota=lambda x: x['F_504_1'] + x['F_505_1'],
            final_quota=lambda x: x['F_512'].fillna(x['F_504_1'] + x['F_505_1'])
        )

        # Calculate quota adjustment more efficiently
        df['quota_adjustment'] = df['final_quota'].div(df['original_quota']).fillna(1)

        # Ensure consistent data types
        for col, dtype in self.COLUMN_TYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Optimize memory usage
        df = df.copy()

        return df

    async def fetch(self) -> pd.DataFrame:
        """Fetch and process fertilizer accounts data"""
        dfs = self._read_excel_files()

        # Concatenate all dataframes at once
        df = pd.concat(dfs, ignore_index=True, copy=False)

        # Clean and standardize
        df = self._clean_and_standardize(df)

        return df

    async def sync(self) -> int:
        """Sync fertilizer accounts data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/fertilizer_accounts_current.parquet"
        df.to_parquet(
            temp_file,
            index=False,
            compression='snappy'  # Use snappy compression for better performance
        )

        # Upload to storage
        blob = self.bucket.blob('raw/fertilizer_accounts/current.parquet')
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)

class FieldPlanFertilizer(Source):
    """Parser for GKEA (Field Plan with Fertilizer Information) data"""

    EXPECTED_COLUMNS = [
        'CVR',
        'Marknummer',    # Field ID number
        'Areal',         # Total field area
        'Harmoni Areal', # Area eligible for fertilizer
        'N Kvote Mark'   # Field's base nitrogen quota
    ]

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

    @property
    def source_id(self) -> str:
        """Unique identifier for this source"""
        return "field_plan_fertilizer"

    def _extract_year(self, filename: str) -> str:
        """Extract year from filename"""
        match = re.search(r'GKEA(\d{4})', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Year not found in filename: {filename}")

    def _read_excel_file(self) -> pd.DataFrame:
        """Read GKEA Excel file"""
        current_dir = Path(__file__).parent
        files = list(current_dir.glob('GKEA*.xlsx'))

        if not files:
            raise FileNotFoundError("No GKEA files found")

        # Use the most recent file if multiple exist
        latest_file = max(files, key=lambda x: x.stat().st_mtime)

        try:
            df = pd.read_excel(latest_file, engine='openpyxl')
            df['source_file'] = latest_file.name
            df['year'] = self._extract_year(latest_file.name)
            return df
        except Exception as e:
            raise Exception(f"Error reading {latest_file}: {e}")

    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the dataframe"""
        # Ensure all expected columns exist
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Convert numeric columns
        numeric_cols = ['Areal', 'Harmoni Areal', 'N Kvote Mark']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Validate field areas
        df['is_valid'] = df['Harmoni Areal'] <= df['Areal']

        return df

    async def fetch(self) -> pd.DataFrame:
        """Fetch and process field plan fertilizer data"""
        df = self._read_excel_file()
        df = self._clean_and_standardize(df)
        return df

    async def sync(self) -> int:
        """Sync field plan fertilizer data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/field_plan_fertilizer_current.parquet"
        df.to_parquet(temp_file)

        # Upload to storage
        blob = self.bucket.blob('raw/field_plan_fertilizer/current.parquet')
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)
