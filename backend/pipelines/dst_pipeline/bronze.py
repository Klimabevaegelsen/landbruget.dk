#!/usr/bin/env python3
"""
Danish Statistics API Pipeline - Bronze Layer

This pipeline fetches data from Danmarks Statistik's (DST) open API and stores it
in the bronze layer following the medallion architecture pattern.

Bronze layer principles:
- Fetch and store data exactly as received from the source
- NO transformations or cleaning should occur at this stage
- Preserve original format with comprehensive metadata
- Support both local (dev) and GCS (production) storage
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import storage interface
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))
from common.storage_interface import GCSStorage, LocalStorage, StorageInterface


def setup_logging(level: str) -> None:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Fetch data from Danmarks Statistik API and store in bronze layer"
    )
    parser.add_argument("--table-id", required=True, help="Table ID to fetch")
    parser.add_argument(
        "--output-dir", default="./bronze", help="Output directory for bronze data"
    )
    parser.add_argument(
        "--lang", default="da", choices=["da", "en"], help="Language for API responses"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--variables", nargs="*", help="Specific variables to fetch (optional)"
    )
    parser.add_argument("--start-time", help="Start time filter (optional)")
    parser.add_argument("--end-time", help="End time filter (optional)")

    return parser.parse_args()


class DSTApiClient:
    """Client for interacting with Danmarks Statistik API"""

    def __init__(self, base_url: str = "https://api.statbank.dk/v1", lang: str = "da"):
        self.base_url = base_url
        self.lang = lang
        self.session = requests.Session()
        # Set proper headers for DST API
        self.session.headers.update(
            {
                "User-Agent": "DanishStatsPipeline/1.0",
                "Accept": "application/json",
            }
        )

    def get_table_info(self, table_id: str) -> Optional[Dict[str, Any]]:
        """Fetch table metadata from the API"""
        try:
            url = f"{self.base_url}/tableinfo"
            params = {"table": table_id, "lang": self.lang, "format": "JSON"}

            logging.info(f"Fetching table info for {table_id}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Return raw JSON response exactly as received
            return response.json()

        except requests.RequestException as e:
            logging.error(f"Failed to fetch table info for {table_id}: {e}")
            return None

    def get_table_data(
        self,
        table_id: str,
        variables: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch table data from the API"""
        try:
            url = f"{self.base_url}/data"

            # Build request payload based on table configuration
            payload = self._build_request_payload(
                table_id, variables, start_time, end_time
            )

            logging.info(f"Fetching data for {table_id}")
            logging.debug(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make request with exponential backoff
            response = self._make_request_with_retry(url, payload)
            if response is None:
                return None

            # Return raw JSON response exactly as received
            return response.json()

        except requests.RequestException as e:
            logging.error(f"Failed to fetch data for {table_id}: {e}")
            return None

    def _build_request_payload(
        self,
        table_id: str,
        variables: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build request payload based on table ID and parameters"""

        # Skip table info fetch since we know the configurations
        # table_info = self.get_table_info(table_id)
        # if not table_info:
        #     logging.warning(
        #         f"Could not get table info for {table_id}, using minimal request"
        #     )
        #     return {"table": table_id, "lang": self.lang, "format": "JSONSTAT"}

        # Extract available variables from table info
        # available_vars = {}
        # if "variables" in table_info:
        #     for var in table_info["variables"]:
        #         if "id" in var and "values" in var:
        #             available_vars[var["id"]] = [v["id"] for v in var["values"]]

        # Build payload based on known table configurations
        payload = {"table": table_id, "lang": self.lang, "format": "JSONSTAT"}

        # Add variable selections based on table type
        if table_id == "HST77":
            payload["variables"] = [
                {
                    "code": "OMRÅDE",
                    "values": ["000", "15", "04", "085", "07", "08", "09", "10", "081"],
                },
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "MÆNGDE4", "values": ["020"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "GARTN1":
            payload["variables"] = [
                {
                    "code": "OMRÅDE",
                    "values": ["000", "15", "04", "085", "07", "08", "09", "10", "081"],
                },
                {"code": "TAL", "values": ["*"]},
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "FRO":
            payload["variables"] = [
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "MÆNGDE4", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        elif table_id == "HALM1":
            payload["variables"] = [
                {
                    "code": "OMRÅDE",
                    "values": ["000", "15", "04", "085", "07", "08", "09", "10", "081"],
                },
                {"code": "AFGRØDE", "values": ["*"]},
                {"code": "ENHED", "values": ["*"]},
                {"code": "ANVENDELSE", "values": ["*"]},
                {"code": "Tid", "values": ["*"]},
            ]
        else:
            # For unknown tables, use minimal request
            if variables:
                payload["variables"] = [
                    {"code": var, "values": ["*"]} for var in variables
                ]

        # Add time filters if specified
        if start_time or end_time:
            # Find time variable in payload
            if "variables" in payload:
                for var in payload["variables"]:
                    if var["code"].lower() in ["tid", "time"]:
                        if start_time and end_time:
                            var["values"] = [f"{start_time}-{end_time}"]
                        elif start_time:
                            var["values"] = [f"{start_time}"]

        return payload

    def _make_request_with_retry(
        self, url: str, payload: Dict[str, Any], max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Make request with exponential backoff retry logic"""

        for attempt in range(max_retries):
            try:
                response = self.session.post(url, json=payload, timeout=30)

                if response.status_code == 429:
                    # Rate limited
                    wait_time = 2**attempt
                    logging.warning(
                        f"Rate limited, waiting {wait_time}s before retry {attempt + 1}"
                    )
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response

            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    logging.error(f"Request failed after {max_retries} attempts: {e}")
                    raise

                wait_time = 2**attempt
                logging.warning(f"Request failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)

        return None


def get_storage_interface() -> StorageInterface:
    """Get the appropriate storage interface based on environment"""
    gcs_bucket = os.getenv("GCS_BUCKET")
    environment = os.getenv("ENVIRONMENT", "dev")

    if environment == "prod" and gcs_bucket:
        logging.info(f"Using GCS storage with bucket: {gcs_bucket}")
        return GCSStorage(gcs_bucket)
    else:
        base_dir = os.getenv("LOCAL_STORAGE_DIR", "bronze/dst")
        logging.info(f"Using local storage with base directory: {base_dir}")
        return LocalStorage(base_dir)


def save_raw_data(
    data: Dict[str, Any],
    storage: StorageInterface,
    table_id: str,
    data_type: str,
    timestamp: str,
) -> None:
    """Save raw data exactly as received from API with metadata"""

    # Create paths following bronze layer structure - simplified
    date_str = datetime.now().strftime("%Y%m%d")
    data_path = f"{date_str}/{table_id}_{data_type}.json"
    metadata_path = f"{date_str}/{table_id}_{data_type}_metadata.json"

    # Save raw data exactly as received (no transformations)
    storage.save_json(data, data_path)

    # Create metadata following established patterns
    metadata = {
        "source_url": "https://api.statbank.dk/v1",
        "fetch_timestamp_utc_iso": datetime.now().isoformat(),
        "fetch_timestamp_dirname": date_str,
        "description": f"Raw {data_type} data from Danmarks Statistik API for table {table_id}",
        "pipeline_name": "dst_pipeline",
        "layer": "bronze",
        "file_format": "json",
        "table_id": table_id,
        "data_type": data_type,
        "data_filename": f"{table_id}_{data_type}.json",
        "relative_data_file_path": data_path,
        "record_count": len(data.get("data", [])) if data_type == "data" else 1,
        "api_lang": "da",
        "source_system": "Danmarks Statistik API",
    }

    # Add table-specific metadata
    if data_type == "data" and "data" in data:
        metadata["data_record_count"] = len(data["data"])
        metadata["columns"] = data.get("columns", [])
    elif data_type == "tableinfo" and "variables" in data:
        metadata["variable_count"] = len(data["variables"])
        metadata["variables"] = [
            var.get("id") for var in data["variables"] if "id" in var
        ]

    storage.save_json(metadata, metadata_path)

    logging.info(f"Saved raw {data_type} data to {data_path}")
    logging.info(f"Saved {data_type} metadata to {metadata_path}")


def main_with_args(args: argparse.Namespace):
    """Main pipeline execution with provided arguments"""
    setup_logging(args.log_level)

    logging.info("Starting DST API Bronze Layer pipeline")
    logging.info(f"Fetching data for table: {args.table_id}")

    # Initialize storage interface
    storage = get_storage_interface()

    # Initialize API client
    client = DSTApiClient(lang=args.lang)

    # Create date-based directory structure following bronze layer guidelines
    date_str = datetime.now().strftime("%Y%m%d")

    try:
        # Fetch table data
        table_data = client.get_table_data(
            args.table_id,
            variables=args.variables,
            start_time=args.start_time,
            end_time=args.end_time,
        )

        if table_data:
            save_raw_data(table_data, storage, args.table_id, "data", date_str)

            # Log summary statistics
            if "data" in table_data:
                record_count = len(table_data["data"])
                logging.info(
                    f"Successfully fetched and saved {record_count} data records"
                )

            if "columns" in table_data:
                column_count = len(table_data["columns"])
                logging.info(f"Data includes {column_count} columns")

        else:
            logging.error("Failed to fetch table data")
            raise SystemExit(1)

        logging.info("Bronze layer pipeline completed successfully")
        logging.info(f"Data stored with date: {date_str}")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise SystemExit(1)


def main():
    """Main pipeline execution with command line parsing"""
    args = parse_args()
    main_with_args(args)


if __name__ == "__main__":
    main()
