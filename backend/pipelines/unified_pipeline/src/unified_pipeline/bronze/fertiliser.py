"""
Fertiliser Bronze layer processing.
Handles raw fertiliser data ingestion from local parquet files or GCS.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

from ..base.bronze_base import BronzeJobInterface
from ..common.base import BronzeJobConfig, ConnectionManager


logger = logging.getLogger(__name__)


class FertiliserBronzeConfig(BronzeJobConfig):
    """Configuration for Fertiliser bronze processing."""
    
    dataset = "fertiliser"
    input_path: str = "data/fertiliser"
    gcs_bucket: Optional[str] = None
    gcs_path: Optional[str] = None
    
    def __init__(self, **data):
        """Initialize configuration."""
        super().__init__(**data)
        
        # Check for GCS configuration
        if os.getenv("FERTILISER_GCS_BUCKET"):
            self.gcs_bucket = os.getenv("FERTILISER_GCS_BUCKET")
            self.gcs_path = os.getenv("FERTILISER_GCS_PATH", "silver/fertiliser")


class FertiliserBronze(BronzeJobInterface):
    """Bronze layer processor for fertiliser data."""
    
    def __init__(self, config: FertiliserBronzeConfig):
        """Initialize the fertiliser bronze processor."""
        super().__init__(config)
        self.config = config
        self.connection_manager = ConnectionManager()
    
    async def run(self) -> Optional[Dict[str, Any]]:
        """
        Run the bronze processing for fertiliser data.
        
        For fertiliser data, the bronze step is minimal since we have
        structured parquet files already. This step mainly validates
        data availability and prepares it for silver processing.
        
        Returns:
            Dict containing metadata about available fertiliser files
        """
        logger.info("Starting fertiliser bronze processing")
        
        try:
            # Check data availability
            fertiliser_files = self._discover_fertiliser_files()
            
            if not fertiliser_files:
                logger.error("No fertiliser files found")
                return None
            
            logger.info(f"Discovered {len(fertiliser_files)} fertiliser files")
            
            # For fertiliser, we return metadata about available files
            # The actual data processing happens in silver layer
            return {
                "files_discovered": fertiliser_files,
                "input_path": str(self.config.input_path),
                "file_count": len(fertiliser_files),
                "categories": self._categorize_files(fertiliser_files)
            }
            
        except Exception as e:
            logger.error(f"Fertiliser bronze processing failed: {str(e)}")
            return None
    
    def _discover_fertiliser_files(self) -> List[Dict[str, Any]]:
        """Discover available fertiliser files."""
        files = []
        
        if self.config.gcs_bucket and self.config.gcs_path:
            # GCS path - would need gsutil or GCS client
            logger.info(f"Discovering files from GCS: {self.config.gcs_bucket}/{self.config.gcs_path}")
            # For now, assume local files are available
            # TODO: Implement GCS file discovery
        
        # Local file discovery
        input_path = Path(self.config.input_path)
        if not input_path.exists():
            logger.warning(f"Input path does not exist: {input_path}")
            return files
        
        # Find all parquet files
        for file_path in input_path.glob("*.parquet"):
            file_info = {
                "filename": file_path.name,
                "path": str(file_path),
                "category": self._categorize_file(file_path.name),
                "size": file_path.stat().st_size if file_path.exists() else 0
            }
            files.append(file_info)
        
        return files
    
    def _categorize_file(self, filename: str) -> str:
        """Categorize a fertiliser file by type."""
        if "Efterafgrøder" in filename:
            return "efterafgroeder"
        elif "GKEA" in filename:
            return "gkea"
        elif "Gødningsregnskaber" in filename:
            return "goedningsregnskaber"
        else:
            return "unknown"
    
    def _categorize_files(self, files: List[Dict[str, Any]]) -> Dict[str, int]:
        """Get count of files by category."""
        categories = {}
        for file_info in files:
            category = file_info["category"]
            categories[category] = categories.get(category, 0) + 1
        return categories