"""Svineflytning Pipeline for fetching and processing pig movement data."""

import argparse
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
from pathlib import Path
import os
import sys
import concurrent.futures
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm.auto import tqdm

from bronze.load_svineflytning import (
    get_fvm_credentials,
    create_client,
    fetch_movements,
    ENDPOINTS
)
from bronze.export import save_movements, finalize_export

logger = logging.getLogger(__name__)

def process_parallel(func, tasks: List, workers: int, desc: str = None) -> List:
    """Execute tasks in parallel using a thread pool with progress tracking."""
    results = []
    with logging_redirect_tqdm():
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(func, *task) for task in tasks]
            
            # Create progress bar that works in both CI and interactive environments
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc=desc or func.__name__,
                unit='tasks',
                mininterval=1.0,  # Update at most once per second
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
            ):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    results.append(None)
    
    return results

def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Remove all existing handlers to start fresh
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    
    # Set up root logger at WARNING by default with a format that works well with tqdm
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure our pipeline loggers
    pipeline_logger = logging.getLogger('svineflytning_pipeline')
    pipeline_logger.setLevel(numeric_level)
    
    # Configure bronze module loggers
    bronze_logger = logging.getLogger('svineflytning_pipeline.bronze')
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)
    
    # Set third-party loggers to WARNING or higher
    for logger_name in ['zeep', 'urllib3', 'google', 'requests']:
        third_party_logger = logging.getLogger(logger_name)
        third_party_logger.setLevel(logging.WARNING)
        if numeric_level > logging.DEBUG:
            third_party_logger.propagate = False

def get_default_dates() -> tuple[date, date]:
    """Get default start and end dates (last 5 years)."""
    today = date.today()
    end_date = today
    start_date = today.replace(year=today.year - 5)  # 5 years ago from today
    return start_date, end_date

def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    start_date_def, end_date_def = get_default_dates()
    
    parser = argparse.ArgumentParser(description="Run the Svineflytning Data Pipeline.")
    parser.add_argument('--start-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                      default=start_date_def, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                      default=end_date_def, help='End date (YYYY-MM-DD)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      default='WARNING', help='Logging level')
    parser.add_argument('--progress', action='store_true',
                      help='Show progress information')
    parser.add_argument('--environment', choices=['prod', 'test'],
                      default='prod', help='Environment to use')
    parser.add_argument('--test', action='store_true',
                      help='Run in test mode with limited data')
    parser.add_argument('--workers', type=int, default=5,
                      help='Number of parallel workers')
    
    args = parser.parse_args()
    return vars(args)

def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args['log_level'])
    
    logger.warning("Starting Svineflytning pipeline")
    if args['progress']:
        logger.warning(f"Processing date range: {args['start_date']} to {args['end_date']}")
    
    try:
        # Get credentials and create client
        username, password = get_fvm_credentials()
        client = create_client(
            ENDPOINTS[args['environment']],
            username,
            password
        )
        
        # Calculate chunks for parallel processing
        total_days = (args['end_date'] - args['start_date']).days + 1
        chunk_size = 3  # API limit: maximum 3 days per request
        
        # Create tasks for parallel processing
        tasks = []
        current_date = args['start_date']
        while current_date <= args['end_date']:
            chunk_end = min(current_date + timedelta(days=chunk_size - 1), args['end_date'])
            tasks.append((client, current_date, chunk_end))
            current_date = chunk_end + timedelta(days=1)
        
        # Track total movements for progress reporting
        total_movements = 0
        
        # Fetch movements in parallel and save them to buffer as they come in
        with logging_redirect_tqdm():
            results = process_parallel(
                fetch_movements,
                tasks,
                args['workers'],
                "Fetching movements"
            )
            
            # Save movements as they come in
            for result in results:
                if result:
                    save_movements(result)
                    total_movements += len(result)
            
            if args['progress']:
                logger.warning(f"Total movements fetched: {total_movements}")
        
        # Finalize the export
        export_result = finalize_export()
        
        # Print information about the fetch and export
        logger.warning(f"Pipeline completed successfully")
        if args['progress']:
            logger.warning(f"Total chunks processed: {len(tasks)}")
            if "record_counts" in export_result:
                logger.warning(f"Total movements exported: {export_result['record_counts'].get('movements', 0)}")
            if "destination" in export_result:
                logger.warning(f"Data exported to: {export_result['destination']}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 