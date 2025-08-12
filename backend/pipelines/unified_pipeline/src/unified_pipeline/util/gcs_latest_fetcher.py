"""
GCS Latest File Fetcher Utility

This utility provides functions to fetch the latest available files from GCS
for pipeline steps that need to run independently without relying on previous
step outputs from the same pipeline run.
"""

import os
import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import logging

try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

from unified_pipeline.util.log_util import Logger

# Get logger instance
logger = Logger.get_logger()


class GCSLatestFetcher:
    """Utility class for fetching latest files from GCS."""
    
    def __init__(self, bucket_name: str = "landbrugsdata-raw-data"):
        """
        Initialize the GCS latest fetcher.
        
        Args:
            bucket_name: Name of the GCS bucket
        """
        self.bucket_name = bucket_name
        self.client = None
        self.bucket = None
        
        if GCS_AVAILABLE:
            try:
                self.client = storage.Client()
                self.bucket = self.client.bucket(bucket_name)
            except Exception as e:
                logger.warning(f"Failed to initialize GCS client: {e}")
                GCS_AVAILABLE = False
    
    def find_latest_file(self, 
                        base_path: str, 
                        file_pattern: str,
                        max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest file matching a pattern within a GCS path.
        
        Args:
            base_path: Base GCS path to search (e.g., "gold/cvr_enrichment/")
            file_pattern: Pattern to match files (e.g., "collection.parquet")
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest file, or None if not found
        """
        if not GCS_AVAILABLE or not self.bucket:
            logger.error("GCS not available - cannot fetch latest file")
            return None
            
        try:
            # Generate date patterns to search
            date_patterns = self._generate_date_patterns(max_days_back)
            
            # Search for files in reverse chronological order (newest first)
            for date_pattern in date_patterns:
                full_path = f"{base_path.rstrip('/')}/{date_pattern}/{file_pattern}"
                
                # Check if file exists
                blob = self.bucket.blob(full_path)
                if blob.exists():
                    gcs_path = f"gs://{self.bucket_name}/{full_path}"
                    logger.info(f"Found latest file: {gcs_path}")
                    return gcs_path
                    
            logger.warning(f"No file found matching pattern '{file_pattern}' in '{base_path}' within {max_days_back} days")
            return None
            
        except Exception as e:
            logger.error(f"Error finding latest file: {e}")
            return None
    
    def find_latest_files_multiple_patterns(self,
                                          base_path: str,
                                          file_patterns: List[str],
                                          max_days_back: int = 30) -> List[Optional[str]]:
        """
        Find the latest files for multiple patterns from the same date.
        
        Args:
            base_path: Base GCS path to search
            file_patterns: List of file patterns to match
            max_days_back: Maximum number of days to look back
            
        Returns:
            List of GCS paths (or None) corresponding to each pattern
        """
        if not GCS_AVAILABLE or not self.bucket:
            logger.error("GCS not available - cannot fetch latest files")
            return [None] * len(file_patterns)
        
        try:
            # Generate date patterns to search
            date_patterns = self._generate_date_patterns(max_days_back)
            
            # Search for files in reverse chronological order (newest first)
            for date_pattern in date_patterns:
                results = []
                all_found = True
                
                # Check if all patterns exist for this date
                for pattern in file_patterns:
                    full_path = f"{base_path.rstrip('/')}/{date_pattern}/{pattern}"
                    blob = self.bucket.blob(full_path)
                    
                    if blob.exists():
                        gcs_path = f"gs://{self.bucket_name}/{full_path}"
                        results.append(gcs_path)
                    else:
                        results.append(None)
                        all_found = False
                
                # If we found all files for this date, return them
                if all_found:
                    logger.info(f"Found all files for date {date_pattern}")
                    for i, (pattern, path) in enumerate(zip(file_patterns, results)):
                        logger.info(f"  {pattern}: {path}")
                    return results
                
                # If we found some but not all files, continue to next date
                # but log what we found
                found_count = len([r for r in results if r is not None])
                if found_count > 0:
                    logger.info(f"Found {found_count}/{len(file_patterns)} files for date {date_pattern}")
            
            # If we get here, we didn't find a complete set
            logger.warning(f"No complete set of files found for patterns {file_patterns} within {max_days_back} days")
            return [None] * len(file_patterns)
            
        except Exception as e:
            logger.error(f"Error finding latest files: {e}")
            return [None] * len(file_patterns)
    
    def find_latest_cvr_collection_data(self, max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest CVR collection data file.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest collection file
        """
        return self.find_latest_file(
            base_path="gold/cvr_enrichment",
            file_pattern="collection.parquet",
            max_days_back=max_days_back
        )
    
    def find_latest_company_data(self, max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest company fetching data file.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest company data file
        """
        return self.find_latest_file(
            base_path="gold/cvr_enrichment",
            file_pattern="company_fetching.parquet", 
            max_days_back=max_days_back
        )
    
    def find_latest_pnumber_data(self, max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest P-number data file.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest P-number data file
        """
        return self.find_latest_file(
            base_path="gold/cvr_enrichment",
            file_pattern="pnumber_fetching.parquet",
            max_days_back=max_days_back
        )
    
    def find_latest_financial_data(self, max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest financial documents data file.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest financial data file
        """
        return self.find_latest_file(
            base_path="gold/cvr_enrichment",
            file_pattern="financial_documents.parquet",
            max_days_back=max_days_back
        )
    
    def find_latest_address_data(self, max_days_back: int = 30) -> Optional[str]:
        """
        Find the latest address geocoding data file.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            GCS path to the latest address data file
        """
        return self.find_latest_file(
            base_path="gold/cvr_enrichment",
            file_pattern="address_geocoding.parquet",
            max_days_back=max_days_back
        )
    
    def find_latest_consolidation_inputs(self, max_days_back: int = 30) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Find the latest data files needed for consolidation step.
        
        Args:
            max_days_back: Maximum number of days to look back
            
        Returns:
            Tuple of (company_data, pnumber_data, financial_data, address_data) paths
        """
        patterns = [
            "company_fetching.parquet",
            "pnumber_fetching.parquet", 
            "financial_documents.parquet",
            "address_geocoding.parquet"
        ]
        
        results = self.find_latest_files_multiple_patterns(
            base_path="gold/cvr_enrichment",
            file_patterns=patterns,
            max_days_back=max_days_back
        )
        
        return tuple(results)
    
    def _generate_date_patterns(self, max_days_back: int) -> List[str]:
        """
        Generate date patterns to search, starting from today and going back.
        
        Args:
            max_days_back: Maximum number of days to generate patterns for
            
        Returns:
            List of date patterns in YYYY-MM-DD format, newest first
        """
        patterns = []
        current_date = datetime.now()
        
        for i in range(max_days_back):
            date = current_date - timedelta(days=i)
            pattern = date.strftime("%Y-%m-%d")
            patterns.append(pattern)
        
        return patterns


def create_gcs_fetcher(bucket_name: str = "landbrugsdata-raw-data") -> GCSLatestFetcher:
    """
    Create a GCS latest fetcher instance.
    
    Args:
        bucket_name: Name of the GCS bucket
        
    Returns:
        GCSLatestFetcher instance
    """
    return GCSLatestFetcher(bucket_name)


# Convenience functions for common use cases
def get_latest_cvr_collection_file(bucket_name: str = "landbrugsdata-raw-data", 
                                  max_days_back: int = 30) -> Optional[str]:
    """Get the latest CVR collection file path."""
    fetcher = create_gcs_fetcher(bucket_name)
    return fetcher.find_latest_cvr_collection_data(max_days_back)


def get_latest_company_file(bucket_name: str = "landbrugsdata-raw-data",
                           max_days_back: int = 30) -> Optional[str]:
    """Get the latest company data file path.""" 
    fetcher = create_gcs_fetcher(bucket_name)
    return fetcher.find_latest_company_data(max_days_back)


def get_latest_pnumber_file(bucket_name: str = "landbrugsdata-raw-data",
                           max_days_back: int = 30) -> Optional[str]:
    """Get the latest P-number data file path."""
    fetcher = create_gcs_fetcher(bucket_name)
    return fetcher.find_latest_pnumber_data(max_days_back)


def get_latest_financial_file(bucket_name: str = "landbrugsdata-raw-data",
                             max_days_back: int = 30) -> Optional[str]:
    """Get the latest financial data file path."""
    fetcher = create_gcs_fetcher(bucket_name)
    return fetcher.find_latest_financial_data(max_days_back)


def get_latest_address_file(bucket_name: str = "landbrugsdata-raw-data",
                           max_days_back: int = 30) -> Optional[str]:
    """Get the latest address data file path."""
    fetcher = create_gcs_fetcher(bucket_name)
    return fetcher.find_latest_address_data(max_days_back)