import pandas as pd
from pathlib import Path
from ....base import BaseSource
import os


class WorkerSafety(BaseSource):
    """Danish Worker Safety Data parser"""

    async def fetch(self) -> pd.DataFrame:
        data_path = Path(__file__).parent / "worker_safety_2020-2024.xlsx"
        if not data_path.exists():
            raise FileNotFoundError(f"Worker safety data not found at {data_path}")

        # Read Excel file, skipping first two rows
        df = pd.read_excel(data_path, sheet_name="Landbrug mv", skiprows=2)

        # Rename columns to be more descriptive
        df = df.rename(columns={"Unnamed: 0": "cvr_number", "i alt": "total"})

        # Clean up the data
        # Remove any rows where cvr_number is NaN
        df = df.dropna(subset=["cvr_number"])

        # Convert cvr_number to string and remove any whitespace
        df["cvr_number"] = df["cvr_number"].astype(str).str.strip()

        # Convert all numeric columns to integers, replacing NaN with 0
        numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0).astype(int)

        # Sort by CVR number
        df = df.sort_values("cvr_number").reset_index(drop=True)

        # Save with proper encoding and quoting for testing
        if "test_parser" in str(data_path):
            df.to_csv(
                "worker_safety.csv",
                index=False,
                encoding="utf-8-sig",
                quoting=1,  # Quote strings
                quotechar='"',
                sep=",",
            )

            # Print some stats
            print(f"\nFound {len(df)} worker safety records")
            print("\nFirst few entries:")
            print(df.head())
            print("\nLast few entries:")
            print(df.tail())

        return df

    async def sync(self):
        """Sync the worker safety data"""
        df = await self.fetch()

        # Write to temporary local parquet file
        temp_file = "/tmp/worker_safety_current.parquet"
        df.to_parquet(temp_file)

        # Upload to storage
        blob = self.bucket.blob(f"raw/worker_safety/current.parquet")
        blob.upload_from_filename(temp_file)

        # Cleanup
        os.remove(temp_file)

        return len(df)
