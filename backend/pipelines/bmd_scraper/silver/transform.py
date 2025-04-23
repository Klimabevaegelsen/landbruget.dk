"""
Transform raw BMD Excel data into cleaned, structured Parquet format.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any, List, Tuple

import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from bronze.export import GCSStorage

# Configure module logger
logger = logging.getLogger("bmd_pipeline.silver.transform")


class BMDTransformer:
    """
    Transforms raw BMD Excel data into a clean, structured format.
    
    This class handles:
    - Reading raw Excel data
    - Cleaning and normalizing fields
    - Type casting
    - Data validation
    - Saving to Parquet format
    """
    
    def __init__(self, input_file: Path, output_dir: Path):
        """
        Initialize the transformer.
        
        Args:
            input_file: Path to raw Excel file from Bronze stage
            output_dir: Directory to save processed Parquet file
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.timestamp = input_file.parent.name
        self.metadata = {}
        
        # Try to load metadata from bronze stage
        metadata_path = input_file.parent / "metadata.json"
        if metadata_path.exists():
            with open(metadata_path, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
                logger.info(f"Loaded bronze metadata from {metadata_path}")
        
        # Create mapping for status fields normalization
        self.status_mapping = {
            # Danish status values to standardized values
            "Godkendt": "approved",
            "Udgået": "expired", 
            "Tilladt": "permitted",
            "Ikke godkendt": "not_approved",
            # Add more mappings as needed
        }
    
    def read_excel(self) -> pd.DataFrame:
        """
        Read the raw Excel file into a pandas DataFrame.
        
        Returns:
            DataFrame containing the raw data
        """
        logger.info(f"Reading Excel file from {self.input_file}")
        try:
            conn = duckdb.connect(database=':memory:')

            df = conn.execute(f"SELECT * FROM read_xlsx('{self.input_file}', range = 'A4:Z', stop_at_empty = true, header=true);").fetchdf()
            # Log the size of the dataframe
            logger.info(f"Read {len(df)} rows and {len(df.columns)} columns")
            conn.close()
            # Store column information in metadata
            self.silver_metadata = {
                "transform_timestamp": datetime.now().isoformat(),
                "source_file": str(self.input_file),
                "bronze_timestamp": self.timestamp,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
            }
            
            return df
        
        except Exception as e:
            conn.close()
            logger.exception(f"Error reading Excel file: {e}")
            raise
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to lowercase with underscores.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            DataFrame with standardized column names
        """
        # Map of original column names to standardized names
        column_mapping = {}
        
        for col in df.columns:
            # Convert to lowercase, replace spaces with underscores
            clean_col = col.lower().strip().replace(' ', '_')
            # Remove special characters
            clean_col = ''.join(c if c.isalnum() or c == '_' else '_' for c in clean_col)
            # Remove consecutive underscores
            while '__' in clean_col:
                clean_col = clean_col.replace('__', '_')
            # Remove leading/trailing underscores
            clean_col = clean_col.strip('_')
            
            column_mapping[col] = clean_col
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Log the column mapping
        logger.info(f"Column mapping: {column_mapping}")
        
        return df
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and normalize the data.
        
        Args:
            df: DataFrame with raw data
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Cleaning and normalizing data")
        
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Clean text columns (strip whitespace)
        for col in df_clean.select_dtypes(include=['object']).columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
        
        # Normalize status fields if they exist
        status_columns = [col for col in df_clean.columns if 'status' in col.lower() 
                         or 'godkendelse' in col.lower() or 'tilladelse' in col.lower()]
        
        for col in status_columns:
            df_clean[col] = df_clean[col].map(self.status_mapping).fillna(df_clean[col])
        
        # Handle duplicate rows
        if df_clean.duplicated().any():
            logger.warning(f"Found {df_clean.duplicated().sum()} duplicate rows, removing them")
            df_clean = df_clean.drop_duplicates()
        
        return df_clean
    
    def parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse and standardize date columns.
        
        Args:
            df: DataFrame with raw data
            
        Returns:
            DataFrame with standardized date columns
        """
        logger.info("Parsing date columns")
        
        # Identify potential date columns
        date_columns = [col for col in df.columns 
                       if any(date_term in col.lower() for date_term in 
                             ['date', 'dato', 'godkendelsesdato', 'udløbsdato', 'expiry'])]
        
        for col in date_columns:
            try:
                # Try converting to datetime
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Converted {col} to date format")
            except Exception as e:
                logger.warning(f"Could not convert {col} to date: {e}")
        
        return df
    
    def apply_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply correct data types to columns.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            DataFrame with correct types
        """
        logger.info("Applying data types")
        
        # Store the original dtypes for reference
        self.silver_metadata["original_dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
        
        # Create a DuckDB connection to use SQL for type conversion
        try:
            conn = duckdb.connect(database=':memory:')
            conn.register('df_raw', df)
            
            # Generate a SQL query to cast columns to appropriate types
            casts = []
            
            # Process numeric columns first
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    # Check if it's an integer or decimal
                    if df[col].dropna().apply(lambda x: x == int(x) if pd.notnull(x) else True).all():
                        casts.append(f'CAST("{col}" AS INTEGER) AS {col}')
                    else:
                        casts.append(f'CAST("{col}" AS DOUBLE) AS {col}')
                elif pd.api.types.is_datetime64_dtype(df[col]):
                    casts.append(f'CAST("{col}" AS DATE) AS {col}')
                else:
                    casts.append(f'CAST("{col}" AS VARCHAR) AS {col}')
            
            # Create the SQL query
            sql = f"""
            SELECT 
                {", ".join(casts)}
            FROM df_raw
            """
            
            # Execute the query
            df_typed = conn.execute(sql).df()
            
            # Close the connection
            conn.close()
            
            # Store the new dtypes
            self.silver_metadata["applied_dtypes"] = {col: str(dtype) for col, dtype in df_typed.dtypes.items()}
            
            return df_typed
            
        except Exception as e:
            logger.exception(f"Error applying types with DuckDB: {e}")
            logger.warning("Falling back to pandas for type conversion")
            
            # Fallback to pandas
            for col in df.columns:
                try:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        # Try to convert to int or float
                        if df[col].dropna().apply(lambda x: x == int(x) if pd.notnull(x) else True).all():
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # nullable int
                        else:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                except Exception as col_err:
                    logger.warning(f"Error converting column {col}: {col_err}")
            
            # Store the new dtypes
            self.silver_metadata["applied_dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
            
            return df
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
        """
        Validate the cleaned data and report issues.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Tuple of (validated DataFrame, validation issues)
        """
        logger.info("Validating data")
        
        validation_issues = {}
        
        # Check for missing values
        missing_counts = df.isna().sum()
        if missing_counts.any():
            missing_cols = missing_counts[missing_counts > 0]
            validation_issues["missing_values"] = [
                f"{col}: {count} missing values ({count/len(df):.1%})" 
                for col, count in missing_cols.items()
            ]
            logger.warning(f"Found columns with missing values: {missing_cols.to_dict()}")
        
        # Check for outliers in numeric columns
        for col in df.select_dtypes(include=['int64', 'float64']).columns:
            try:
                # Simple outlier detection using IQR
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
                if len(outliers) > 0:
                    pct = len(outliers) / len(df)
                    if pct > 0.01:  # Only report if more than 1% are outliers
                        if "outliers" not in validation_issues:
                            validation_issues["outliers"] = []
                        validation_issues["outliers"].append(
                            f"{col}: {len(outliers)} outliers ({pct:.1%})"
                        )
                        logger.warning(f"Found outliers in {col}: {len(outliers)} values ({pct:.1%})")
            except Exception as e:
                logger.warning(f"Could not check outliers for {col}: {e}")
        
        # Store validation results in metadata
        self.silver_metadata["validation"] = {
            "has_issues": bool(validation_issues),
            "issues": validation_issues
        }
        
        return df, validation_issues
    
    def save_parquet(self, df: pd.DataFrame) -> Path:
        """
        Save the processed DataFrame as a Parquet file.
        
        Args:
            df: Processed DataFrame
            
        Returns:
            Path to the saved Parquet file
        """
        # Create the output filename with timestamp
        output_filename = f"bmd_data_{self.timestamp}.parquet"
        output_path = self.output_dir / output_filename
        
        logger.info(f"Saving Parquet file to {output_path}")
        
        # Convert to PyArrow Table and write Parquet
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
        
        # Save metadata file
        metadata_path = self.output_dir / "metadata.json"
        self.silver_metadata["output_file"] = str(output_path)
        self.silver_metadata["output_size_bytes"] = os.path.getsize(output_path)
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.silver_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved metadata to {metadata_path}")
        
        return output_path
    
    def transform(self) -> Path:
        """
        Execute the full transformation process.
        
        Returns:
            Path to the processed Parquet file
        """
        logger.info(f"Starting transformation of {self.input_file}")
        
        # 1. Read the Excel file
        df = self.read_excel()
        
        # 2. Clean column names
        df = self.clean_column_names(df)
        
        # 3. Clean and normalize data
        df = self.clean_data(df)
        
        # 4. Parse dates
        df = self.parse_dates(df)
        
        # 5. Apply correct types
        df = self.apply_types(df)
        
        # 6. Validate data
        df, validation_issues = self.validate_data(df)
        
        # 7. Save to Parquet
        output_path = self.save_parquet(df)
        
        logger.info(f"Transformation completed successfully: {output_path}")
        return output_path


def upload_to_gcs(local_path: Path, bucket_name: str) -> bool:
    """
    Upload silver data to Google Cloud Storage.
    
    Args:
        local_path: Path to the local file
        bucket_name: GCS bucket name
        
    Returns:
        True if upload successful, False otherwise
    """
    try:
        storage = GCSStorage(bucket_name=bucket_name, prefix="silver/bmd")
        
        # Upload the Parquet file
        success = storage.upload_file(str(local_path))
        
        # Upload the metadata file
        metadata_path = local_path.parent / "metadata.json"
        if metadata_path.exists():
            storage.upload_file(str(metadata_path))
        
        return success
    except Exception as e:
        logger.exception(f"Error uploading to GCS: {e}")
        return False 