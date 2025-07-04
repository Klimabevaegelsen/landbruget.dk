"""
SPF-SU Data Collection Module for CHR Pipeline

This module handles the collection of SPF-SU (Specific Pathogen Free - Swine Unit) data
for individual pig herd numbers as part of the CHR pipeline. SPF-SU data is only relevant
for pig herds (species_code = 15).
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp
from bronze.export import save_raw_data

logger = logging.getLogger(__name__)


async def load_spf_su_data(pig_herd_numbers: List[int], max_workers: int = 5) -> List[Dict[str, Any]]:
    """
    Load SPF-SU data for individual pig herd numbers.

    Args:
        pig_herd_numbers: List of individual herd numbers for pig herds
        max_workers: Maximum number of concurrent requests (default: 5 for rate limiting)

    Returns:
        List of SPF-SU data dictionaries
    """
    logger.info(f"Starting SPF-SU data collection for {len(pig_herd_numbers)} pig herd numbers")
    logger.info(f"⏱️ Estimated time: {len(pig_herd_numbers) * 2 / 60:.1f} minutes at 30 requests/minute")

    results = []

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

                # Progress logging every 100 requests
                if i % 100 == 0:
                    logger.info(
                        f"Progress: {i}/{len(pig_herd_numbers)} herd numbers processed ({i / len(pig_herd_numbers) * 100:.1f}%)"
                    )

            except Exception as e:
                logger.error(f"Error processing herd number task: {e}")

    logger.info(f"SPF-SU data collection complete. Retrieved {len(results)} herd records")
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
                    return None
                else:
                    logger.warning(f"HTTP {response.status} for herd {herd_number}: {await response.text()}")
                    return None

        except Exception as e:
            logger.error(f"Error fetching SPF-SU data for herd {herd_number}: {e}")
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
