import subprocess
import sys
import os
import argparse
import datetime
import logging
import bronze.export
import silver.transform

PIPELINE_ROOT = os.path.dirname(os.path.abspath(__file__))
print("[DEBUG] DISPLAY =", os.environ.get("DISPLAY"))
print("[DEBUG] DOCKER_ENV =", os.environ.get("DOCKER_ENV"))

def parse_args():
    """Parse command line arguments for the pipeline.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Run the Arbejdstilsynet Inspections pipeline')
    
    # Calculate default start_date (6 months ago)
    
    parser.add_argument('--start-date',
                        type=str,
                        default=None,
                        help='Start date in YYYY-MM-DD format (default: 6 months ago)')
    
    parser.add_argument('--end-date',
                        type=str,
                        default=None,
                        help='End date in YYYY-MM-DD format (default: today)')
    
    parser.add_argument('--log-level', 
                        type=str,
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO',
                        help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    
    parser.add_argument('--gcs-bucket',
                        type=str,
                        help='Google Cloud Storage bucket for export')
    parser.add_argument('--stage', 
                        type=str,
                        choices=['all', 'bronze', 'silver'],
                        default='all'
                        )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Set logging level
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting pipeline with args: {args}")
      # Run Bronze Layer
    print("[main.py] Running Bronze Layer: export.py ...")
    if args.stage in ['all', 'bronze']:
        bronze.export.main(log_level=args.log_level,
                           gcs_bucket=args.gcs_bucket
                           )
    print("[main.py] Bronze Layer complete.")

    # Run Silver Layer
    print("[main.py] Running Silver Layer: transform.py ...")
    if args.stage in ['all', 'silver']:
        silver.transform.main(
            start_date=args.start_date,
            end_date=args.end_date,
            gcs_bucket=args.gcs_bucket,
            log_level=args.log_level
        )
    print("[main.py] Silver Layer complete.")

    print("[main.py] Pipeline finished successfully.")
