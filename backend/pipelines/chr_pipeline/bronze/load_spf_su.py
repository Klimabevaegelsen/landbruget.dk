"""
SPF-SU Data Collection Module for CHR Pipeline

This module handles the collection of SPF-SU (Specific Pathogen Free - Swine Unit) data
for individual pig herd numbers as part of the CHR pipeline. SPF-SU data is only relevant
for pig herds (species_code = 15).
"""

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import aiohttp
from bronze.export import save_raw_data

logger = logging.getLogger(__name__)


async def load_spf_su_data(pig_herd_numbers: List[int], max_workers: int = None) -> List[Dict[str, Any]]:
    """
    Load SPF-SU data for individual pig herd numbers.

    Args:
        pig_herd_numbers: List of individual herd numbers for pig herds
        max_workers: Maximum number of concurrent requests (default: 3 for rate limiting)

    Returns:
        List of SPF-SU data dictionaries
    """
    if max_workers is None:
        max_workers = int(os.getenv("SPF_SU_MAX_WORKERS", "3"))

    logger.info(f"Starting SPF-SU data collection for {len(pig_herd_numbers)} pig herd numbers")
    logger.info(f"Using {max_workers} concurrent workers for rate limiting")
    logger.info(f"⏱️ Estimated time: {len(pig_herd_numbers) * 2 / 60:.1f} minutes at 30 requests/minute")

    results = []
    successful_requests = 0
    failed_requests = 0

    # Use semaphore to limit concurrent requests (rate limiting)
    semaphore = asyncio.Semaphore(max_workers)

    async with aiohttp.ClientSession() as session:
        tasks = []

        for herd_number in pig_herd_numbers:
            task = asyncio.create_task(_fetch_spf_su_for_herd(session, semaphore, herd_number))
            tasks.append(task)

        # Process with progress tracking
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            try:
                result = await task
                if result:
                    results.append(result)
                    successful_requests += 1
                    logger.debug(f"✅ Successfully processed herd {result.get('herdNumber', 'unknown')}")
                # Progress logging - scale frequency based on total count
                progress_interval = max(100, len(pig_herd_numbers) // 20)  # Show ~20 progress updates total
                if i % progress_interval == 0 or i == len(pig_herd_numbers):
                    # Get current counts from function attributes
                    current_not_found = getattr(_fetch_spf_su_for_herd, "not_found_count", 0)
                    current_errors = sum(getattr(_fetch_spf_su_for_herd, "error_counts", {}).values())

                    success_rate = (successful_requests / i) * 100
                    logger.warning(
                        f"Progress: {i}/{len(pig_herd_numbers)} processed ({i / len(pig_herd_numbers) * 100:.1f}%) | "
                        f"Success: {successful_requests} | Not Found: {current_not_found} | Errors: {current_errors} | "
                        f"Success Rate: {success_rate:.1f}%"
                    )

            except Exception as e:
                failed_requests += 1
                logger.error(f"Error processing herd number task: {e}")

    # Final summary - get final counts from function attributes
    final_not_found = getattr(_fetch_spf_su_for_herd, "not_found_count", 0)
    final_error_counts = getattr(_fetch_spf_su_for_herd, "error_counts", {})
    final_errors = sum(final_error_counts.values())

    total_processed = successful_requests + final_not_found + final_errors + failed_requests
    success_rate = (successful_requests / total_processed) * 100 if total_processed > 0 else 0

    logger.warning("SPF-SU data collection complete!")
    logger.warning("📊 Final Statistics:")
    logger.warning(f"  Total processed: {total_processed}")
    logger.warning(f"  Successful: {successful_requests}")
    logger.warning(f"  Not found (404): {final_not_found}")
    logger.warning(f"  HTTP errors: {final_errors}")
    if final_error_counts:
        for status_code, count in final_error_counts.items():
            logger.warning(f"    HTTP {status_code}: {count} occurrences")
    logger.warning(f"  Task exceptions: {failed_requests}")
    logger.warning(f"  Success rate: {success_rate:.1f}%")
    logger.warning(f"  Retrieved {len(results)} herd records for further processing")
    return results


async def _fetch_spf_su_for_herd(
    session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, herd_number: int
) -> Optional[Dict[str, Any]]:
    """
    Fetch SPF-SU data for a specific herd number using the individual herd endpoint.

    Args:
        session: aiohttp session
        semaphore: Semaphore for rate limiting
        herd_number: Herd number to fetch data for

    Returns:
        SPF-SU data dictionary or None if error/no data
    """

    async with semaphore:
        # Rate limiting: 2 second delay between requests (30 requests/minute)
        await asyncio.sleep(2)

        try:
            url = f"https://spfsus.dk/api/farm/{herd_number}/da/false/0/0/false?format=json"
            logger.debug(f"Fetching SPF-SU data for herd: {herd_number}")

            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()

                    # Save raw data
                    save_raw_data(raw_response=data, data_type="spf_su_herds", identifier=str(herd_number))

                    logger.debug(f"Successfully fetched SPF-SU data for herd {herd_number}")
                    return data

                elif response.status == 404:
                    logger.debug(f"No SPF-SU data available for herd {herd_number}")
                    # Track 404s separately for statistics
                    if not hasattr(_fetch_spf_su_for_herd, "not_found_count"):
                        _fetch_spf_su_for_herd.not_found_count = 0
                    _fetch_spf_su_for_herd.not_found_count += 1
                    return None
                else:
                    # Enhanced error logging for debugging - but limit detailed logs to avoid spam
                    response_text = await response.text()

                    # Always log the basic error
                    logger.warning(
                        f"HTTP {response.status} for herd {herd_number}: {response_text[:200]}{'...' if len(response_text) > 200 else ''}"
                    )

                    # Only log full details for first 3 errors of each status code to avoid spam
                    if not hasattr(_fetch_spf_su_for_herd, "error_counts"):
                        _fetch_spf_su_for_herd.error_counts = {}

                    status_code = response.status
                    if status_code not in _fetch_spf_su_for_herd.error_counts:
                        _fetch_spf_su_for_herd.error_counts[status_code] = 0

                    _fetch_spf_su_for_herd.error_counts[status_code] += 1

                    # Log detailed info only for first 3 occurrences of each error type
                    if _fetch_spf_su_for_herd.error_counts[status_code] <= 3:
                        logger.warning(
                            f"  Detailed error info for HTTP {status_code} (occurrence #{_fetch_spf_su_for_herd.error_counts[status_code]}):"
                        )
                        logger.warning(f"  URL: {url}")
                        logger.warning(f"  Response headers: {dict(response.headers)}")
                        logger.warning(f"  Response content-type: {response.headers.get('content-type', 'unknown')}")
                        logger.warning(
                            f"  Response content-length: {response.headers.get('content-length', 'unknown')}"
                        )
                        logger.warning(f"  Full response text: {response_text}")

                        # Try to parse response text as JSON to get more structured error info
                        try:
                            import json

                            json_response = json.loads(response_text)
                            logger.warning(f"  Parsed JSON response: {json_response}")
                        except:
                            logger.warning("  Response is not valid JSON")
                    elif _fetch_spf_su_for_herd.error_counts[status_code] == 4:
                        logger.warning(
                            f"  (Suppressing further detailed logs for HTTP {status_code} errors to avoid spam)"
                        )

                    return None

        except Exception as e:
            logger.error(f"Error fetching SPF-SU data for herd {herd_number}: {e}")
            logger.error(f"  URL attempted: {url}")
            logger.error(f"  Exception type: {type(e).__name__}")
            logger.error(f"  Exception details: {str(e)}")
            return None


def get_pig_herd_numbers(herd_to_species: Dict[int, int]) -> List[int]:
    """
    Extract herd numbers that are pig herds (species_code = 15).

    Args:
        herd_to_species: Dictionary mapping herd numbers to species codes

    Returns:
        List of herd numbers that are pig herds
    """
    pig_herd_numbers = []

    for herd_number, species_code in herd_to_species.items():
        if species_code == 15:  # 15 is the species code for pigs
            pig_herd_numbers.append(herd_number)

    logger.info(f"Found {len(pig_herd_numbers)} pig herd numbers out of {len(herd_to_species)} total herds")
    return pig_herd_numbers


# Test function for development
async def test_spf_su_api():
    """Test the SPF-SU API with a few known herd numbers."""
    test_herd_numbers = [10001, 10002, 10003]  # Replace with actual pig herd numbers

    logger.info("Testing SPF-SU API...")
    results = await load_spf_su_data(test_herd_numbers, max_workers=2)

    logger.info(f"Test complete. Retrieved {len(results)} records")
    for result in results[:3]:  # Show first 3 results
        logger.info(f"Sample result: {result}")


if __name__ == "__main__":
    # Set up logging for testing
    logging.basicConfig(level=logging.INFO)

    # Run test
    asyncio.run(test_spf_su_api())
