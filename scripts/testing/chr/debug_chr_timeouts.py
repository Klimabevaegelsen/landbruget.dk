#!/usr/bin/env python3
"""
Debug script for CHR pipeline timeout issues.
This script helps identify problematic herds and analyze timeout patterns.
"""

import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

# Add the backend directory to sys.path for imports
project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from backend.pipelines.chr_pipeline.bronze.load_chr_dyr import (
    _load_problematic_herds,
    create_soap_client,
    get_fvm_credentials,
    is_problematic_herd,
    load_animal_movements,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("chr_timeout_debug.log", mode="w")],
)
logger = logging.getLogger(__name__)


def analyze_herd_performance(
    herd_numbers: List[int],
    max_test_time: int = 300,  # 5 minutes per herd
    sample_size: int = 10,
) -> Dict[int, Dict]:
    """
    Analyze performance characteristics of specific herds.

    Args:
        herd_numbers: List of herd numbers to test
        max_test_time: Maximum time to spend testing each herd (seconds)
        sample_size: Maximum number of herds to test

    Returns:
        Dictionary mapping herd numbers to their performance metrics
    """
    logger.info(f"Analyzing performance for {len(herd_numbers)} herds (max {sample_size} samples)")

    # Load problematic herds first
    _load_problematic_herds()

    # Get credentials and create client
    try:
        username, password = get_fvm_credentials()
        client = create_soap_client("https://ws.fvst.dk/service/CHR_dyrWS?wsdl", username, password)
    except Exception as e:
        logger.error(f"Failed to create CHR client: {e}")
        return {}

    # Test date range (last 6 months for reasonable data size)
    end_date = date.today()
    start_date = end_date - timedelta(days=180)

    results = {}
    tested_count = 0

    for herd_number in herd_numbers:
        if tested_count >= sample_size:
            break

        logger.info(f"Testing herd {herd_number} ({tested_count + 1}/{min(sample_size, len(herd_numbers))})")

        # Skip if already known to be problematic
        if is_problematic_herd(herd_number):
            logger.info(f"Herd {herd_number} is already marked as problematic - skipping")
            results[herd_number] = {
                "status": "skipped_problematic",
                "request_time": None,
                "animal_count": None,
                "error": None,
            }
            tested_count += 1
            continue

        # Test the herd with timeout
        start_time = time.time()
        try:
            logger.info(f"Starting test request for herd {herd_number}...")

            # Use a shorter timeout for testing
            result = load_animal_movements(client, username, herd_number, start_date, end_date, max_retries=1)

            request_time = time.time() - start_time

            if result is None:
                logger.warning(f"Herd {herd_number}: No result returned")
                results[herd_number] = {
                    "status": "no_result",
                    "request_time": request_time,
                    "animal_count": None,
                    "error": "No result returned",
                }
            else:
                animal_count = result.get("summary_stats", {}).get("total_animals_processed", 0)
                movement_count = len(result.get("movements", []))

                logger.info(
                    f"Herd {herd_number}: Success in {request_time:.1f}s - {animal_count} animals, {movement_count} movements"
                )

                results[herd_number] = {
                    "status": "success",
                    "request_time": request_time,
                    "animal_count": animal_count,
                    "movement_count": movement_count,
                    "error": None,
                }

                # Flag slow herds
                if request_time > 60:
                    logger.warning(f"Herd {herd_number} is slow: {request_time:.1f}s")
                    results[herd_number]["slow_flag"] = True

        except Exception as e:
            request_time = time.time() - start_time
            logger.error(f"Herd {herd_number}: Error after {request_time:.1f}s: {e}")

            results[herd_number] = {
                "status": "error",
                "request_time": request_time,
                "animal_count": None,
                "error": str(e),
            }

        tested_count += 1

        # Check if we've exceeded our time budget
        if request_time > max_test_time:
            logger.warning(f"Herd {herd_number} exceeded max test time ({max_test_time}s)")
            break

    return results


def generate_timeout_report(results: Dict[int, Dict]) -> str:
    """Generate a summary report of timeout analysis."""

    report = []
    report.append("=== CHR Pipeline Timeout Analysis Report ===")
    report.append(f"Analysis time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"Total herds tested: {len(results)}")
    report.append("")

    # Categorize results
    successful = {k: v for k, v in results.items() if v["status"] == "success"}
    errors = {k: v for k, v in results.items() if v["status"] == "error"}
    no_results = {k: v for k, v in results.items() if v["status"] == "no_result"}
    skipped = {k: v for k, v in results.items() if v["status"] == "skipped_problematic"}

    report.append(f"Successful: {len(successful)}")
    report.append(f"Errors: {len(errors)}")
    report.append(f"No results: {len(no_results)}")
    report.append(f"Skipped (problematic): {len(skipped)}")
    report.append("")

    # Performance analysis
    if successful:
        times = [v["request_time"] for v in successful.values() if v["request_time"] is not None]
        if times:
            avg_time = sum(times) / len(times)
            max_time = max(times)
            min_time = min(times)

            report.append("=== Performance Analysis ===")
            report.append(f"Average request time: {avg_time:.1f}s")
            report.append(f"Max request time: {max_time:.1f}s")
            report.append(f"Min request time: {min_time:.1f}s")
            report.append("")

            # Identify slow herds
            slow_herds = [(k, v) for k, v in successful.items() if v["request_time"] > 60]
            if slow_herds:
                report.append("=== Slow Herds (>60s) ===")
                for herd_id, data in sorted(slow_herds, key=lambda x: x[1]["request_time"], reverse=True):
                    report.append(f"Herd {herd_id}: {data['request_time']:.1f}s ({data['animal_count']} animals)")
                report.append("")

    # Error analysis
    if errors:
        report.append("=== Error Analysis ===")
        error_types = {}
        for herd_id, data in errors.items():
            error_type = type(data["error"]).__name__ if data["error"] else "Unknown"
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append((herd_id, data))

        for error_type, herd_list in error_types.items():
            report.append(f"{error_type}: {len(herd_list)} herds")
            for herd_id, data in herd_list[:5]:  # Show first 5 examples
                report.append(f"  Herd {herd_id}: {data['error']}")
            if len(herd_list) > 5:
                report.append(f"  ... and {len(herd_list) - 5} more")
        report.append("")

    # Recommendations
    report.append("=== Recommendations ===")
    if slow_herds:
        report.append(f"- Consider marking {len(slow_herds)} slow herds as problematic")
        report.append("- Reduce timeout thresholds for GitHub Actions")

    if errors:
        report.append(f"- Investigate {len(errors)} herds with errors")
        report.append("- Check network connectivity and API limits")

    report.append("- Consider processing in smaller chunks")
    report.append("- Implement progressive timeout (shorter for known slow herds)")

    return "\n".join(report)


def main():
    """Main debugging function."""
    logger.info("Starting CHR timeout debugging...")

    # Test with a sample of herd numbers
    # In a real scenario, you'd get these from your actual pipeline
    test_herds = [
        # Add some known herd numbers from your pipeline
        # These would typically come from your herd list
        123456,
        234567,
        345678,
        456789,
        567890,
        678901,
        789012,
        890123,
        901234,
        12345,
    ]

    # You can also load herds from a file if available
    herds_file = Path("test_herds.json")
    if herds_file.exists():
        try:
            with open(herds_file, "r") as f:
                file_herds = json.load(f)
                if isinstance(file_herds, list):
                    test_herds = file_herds[:20]  # Test first 20
                    logger.info(f"Loaded {len(test_herds)} herds from {herds_file}")
        except Exception as e:
            logger.warning(f"Could not load herds from file: {e}")

    # Run the analysis
    results = analyze_herd_performance(test_herds, max_test_time=300, sample_size=10)

    # Generate and save report
    report = generate_timeout_report(results)

    # Save report to file
    report_file = Path("chr_timeout_analysis_report.txt")
    with open(report_file, "w") as f:
        f.write(report)

    # Also print to console
    print("\n" + report)

    # Save raw results as JSON
    results_file = Path("chr_timeout_analysis_results.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Analysis complete. Reports saved to {report_file} and {results_file}")


if __name__ == "__main__":
    main()
