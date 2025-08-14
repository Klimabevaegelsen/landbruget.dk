#!/usr/bin/env python3
"""
Test script for fertiliser data harmonization.
"""

import sys
from pathlib import Path

# Add the pipeline to the path
sys.path.append("backend/pipelines/unified_pipeline/src")

import logging
from unified_pipeline.common.base import ConnectionManager
from unified_pipeline.silver.fertiliser import FertiliserSilverProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fertiliser_harmonization():
    """Test the fertiliser harmonization process."""
    
    logger.info("Starting fertiliser data harmonization test")
    
    try:
        # Initialize connection manager (using local DuckDB)
        connection_manager = ConnectionManager()
        
        # Create processor
        processor = FertiliserSilverProcessor(connection_manager)
        
        # Run harmonization on our downloaded data
        harmonized_table = processor.process(input_data_path="data/fertiliser")
        
        # Get summary statistics
        summary = processor.get_summary_statistics()
        
        logger.info("Harmonization completed successfully!")
        logger.info(f"Created table: {harmonized_table}")
        logger.info("Summary statistics:")
        logger.info(f"\n{summary.to_string()}")
        
        # Test some queries to validate the data
        with connection_manager.get_connection() as conn:
            # Check total records
            total_count = conn.execute(f"SELECT COUNT(*) as total FROM {harmonized_table}").fetchone()[0]
            logger.info(f"Total harmonized records: {total_count:,}")
            
            # Check data by source
            by_source = conn.execute(f"""
                SELECT data_source, data_type, COUNT(*) as count
                FROM {harmonized_table} 
                GROUP BY data_source, data_type
                ORDER BY count DESC
            """).fetchdf()
            logger.info("Records by source:")
            logger.info(f"\n{by_source.to_string()}")
            
            # Check year coverage
            year_coverage = conn.execute(f"""
                SELECT year, COUNT(*) as count, COUNT(DISTINCT cvr_number) as companies
                FROM {harmonized_table}
                WHERE year IS NOT NULL
                GROUP BY year
                ORDER BY year
            """).fetchdf()
            logger.info("Year coverage:")
            logger.info(f"\n{year_coverage.to_string()}")
            
            # Sample data check
            sample_data = conn.execute(f"""
                SELECT * FROM {harmonized_table}
                LIMIT 5
            """).fetchdf()
            logger.info("Sample harmonized data:")
            logger.info(f"\n{sample_data.to_string()}")
        
        logger.info("Test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_fertiliser_harmonization()
    sys.exit(0 if success else 1)