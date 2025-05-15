from bronze.fetch_company_data import DMAScraper
import os
from google.cloud import storage
from  datetime import datetime
import argparse
# inside backend/pipelines/dma_scraper/fetch_company_data.py
import os, sys
import time
import nest_asyncio
import asyncio
import aiohttp
from silver.transformation import transform_dma_json

ROOT = os.path.abspath(os.path.join(__file__, "..", "..", ".."))
sys.path.insert(0, ROOT)

from common.storage_interface import LocalStorage, GCSStorage
from bronze.fetch_company_detail import DMACompanyDetailScraper
PREFIX_BRONZE_SAVE_PATH = os.environ.get("BRONZE_OUTPUT_DIR", "bronze/dma")
PREFIX_SILVER_SAVE_PATH = os.environ.get("SILVER_OUTPUT_DIR", "silver/dma")
nest_asyncio.apply()

# Initialize GCS client and bucket
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
if ENVIRONMENT.lower() in ("production", "container"):
    storage_backend = GCSStorage(os.environ.get("GCS_BUCKET", "landbrugsdata-raw-data"))
else:
    storage_backend = LocalStorage(os.environ.get("BRONZE_OUTPUT_DIR", "."))

scraper = DMAScraper()

def save_data(data, timestamp, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/data.json"
    storage_backend.save_json(data, blob_name)
    print(f"Saved {blob_name} to storage")

def save_parquet(data, timestamp, PATH):
    timestamp_dir = os.path.join(PATH, timestamp)
    blob_name = f"{timestamp_dir}/data.parquet"
    storage_backend.save_parquet(data, blob_name)
    print(f"Saved {blob_name} to storage")

def parse_args():
    parser = argparse.ArgumentParser(description="DMA Scraper Pipeline")
    parser.add_argument(
        "--total-pages",
        type=int,
        default=None,
        help="Total number of pages to scrape",
    )
    parser.add_argument(
        "--silver",
        action='store_true',
        help="Run silver transformation stage",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        help="Timestamp directory for silver stage",
    )
    return parser.parse_args()

def silver(data, timestamp: str):
    df = transform_dma_json(data)
    save_parquet(df, timestamp, PREFIX_SILVER_SAVE_PATH)

def bronze(timestamp: str):
    args = parse_args()
    page = 1
    total_pages = args.total_pages
    all_page_results = []
    while total_pages is None or page <= total_pages:
        print(f"Fetching page {page}...")
        data = scraper.fetch_data(page)

        if total_pages is None:
            total_pages = data['pagination']['antalSider']
            print(f"Total pages: {total_pages}")

        page_results = scraper.extract_info(data)
        time.sleep(1)  # Add a delay to avoid overwhelming the server
        all_page_results.extend(page_results)
        page += 1
    detail_scraper = DMACompanyDetailScraper(all_page_results)
    loop = asyncio.get_event_loop()
    detailed_data = loop.run_until_complete(detail_scraper.process_miljoeaktoer_for_company_file_path())
    # Merge base and detail dicts by 'miljoeaktoerUrl'
    detail_lookup = {item.get('miljoeaktoerUrl'): item for item in detailed_data if item}
    merged_results = []
    for base in all_page_results:
        url = base.get('miljoeaktoerUrl')
        merged_results.append({**base, **detail_lookup.get(url, {})})
    save_data(merged_results, timestamp, PREFIX_BRONZE_SAVE_PATH)
    return merged_results

if __name__ == "__main__":
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    args = parse_args()
    if args.silver:
        if not args.timestamp:
            print("Error: --timestamp is required for silver stage")
            sys.exit(1)
        data = storage_backend.read_json(os.path.join(PREFIX_BRONZE_SAVE_PATH, args.timestamp, 'data.json'))
        silver(data, args.timestamp)
    else:
        data = bronze(timestamp)
        silver(data, timestamp)