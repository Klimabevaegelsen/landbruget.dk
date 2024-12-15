import sys
import os
import logging
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parents[2])
sys.path.insert(0, project_root)

from backend.src.sources.parsers.herd_data import HerdDataParser

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_single_chr():
    # Test configuration with direct secrets
    config = {
        'enabled': True,
        'type': 'herd_data',
        'project_id': 'landbrugsdata-1',  # Updated to correct project ID
        'use_google_secrets': True,
        'secrets': {
            'fvm_username': '42731978',
            'fvm_password': 'GiXOEqqWjzWa3BXWCPGT1SINA'
        }
    }

    # Initialize parser
    parser = HerdDataParser(config)

    # Test with known CHR number
    test_chr = "10860"
    
    try:
        result = parser.query_herd(test_chr)
        logger.info(f"Result for CHR {test_chr}:")
        logger.info(result)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_single_chr() 