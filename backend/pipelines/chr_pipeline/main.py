# backend/chr_pipeline/main.py
"""CHR Pipeline for fetching and processing data."""

import concurrent.futures
import logging
import os
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import click
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from bronze.auth import (
    create_besaetning_client,
    create_chr_dyr_client,
    create_diko_client,
    create_ejendom_client,
    create_stamdata_client,
    get_fvm_credentials,
    get_legacy_fvm_credentials,
)
from bronze.export import (
    EXPORT_TIMESTAMP,
    export_context_data,
    get_data_buffer,
    import_context_data,
    save_raw_data,
)
from bronze.load_besaetning import load_herd_details, load_herd_list
from bronze.load_diko import load_diko_flytninger
from bronze.load_ejendom import load_ejendom_oplysninger, load_ejendom_vet_events
from bronze.load_stamdata import load_species_usage_combinations
from bronze.load_vetstat import load_vetstat_antibiotics

# Import gold processing orchestrator
from gold.chr_gold_processing import process_gold_data as run_gold_processing
from silver import config

# Import silver processing orchestrator
from silver.chr_silver_processing import process_chr_data as run_silver_processing

# Import incremental processing utilities
try:
    from backend.pipelines.unified_pipeline.src.unified_pipeline.core.incremental_processing import (
        ProcessingRun,
        get_incremental_processor,
    )

    INCREMENTAL_PROCESSING_AVAILABLE = True
except ImportError:
    print("⚠️ Incremental processing utilities not available - using legacy full processing")
    INCREMENTAL_PROCESSING_AVAILABLE = False

from common.logging_utils import setup_pipeline_logger

logger = logging.getLogger(__name__)  # Replaced in setup_logging()


def calculate_date_range(processing_mode: str) -> tuple[str | None, str | None]:
    """
    Calculate start and end dates based on processing mode.

    Args:
        processing_mode: 'incremental', 'full', or 'backfill'

    Returns:
        Tuple of (start_date, end_date) as strings in YYYY-MM-DD format
    """
    from datetime import date, timedelta

    today = date.today()

    if processing_mode == "incremental":
        # Last 3 months (as per CHR plan)
        start_date = today - timedelta(days=90)
        end_date = today
        logger.info(f"Incremental mode: processing last 3 months ({start_date} to {end_date})")
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    if processing_mode == "full":
        # Full backfill (as per CHR plan: 2021-2025)
        start_date = "2021-01-01"
        end_date = today.strftime("%Y-%m-%d")
        logger.info(f"Full mode: processing complete range ({start_date} to {end_date})")
        return start_date, end_date

    if processing_mode == "backfill":
        # For backfill, don't override dates - let manual override handle it
        logger.info("Backfill mode: using manual date override or pipeline defaults")
        return None, None

    return None, None


def determine_processing_mode(bronze_timestamp: str) -> tuple[str, str | None]:
    """
    Simple processing mode determination - defaults to 3 months, manual override available.

    Args:
        bronze_timestamp: The current bronze timestamp

    Returns:
        Tuple of (processing_mode, backfill_timestamp)
        - processing_mode: 'full', 'incremental', or 'backfill'
        - backfill_timestamp: Bronze timestamp to use for backfill (if applicable)
    """
    # Check for manual override first
    env_processing_mode = os.getenv("CHR_PROCESSING_MODE", "incremental").lower()
    force_backfill_timestamp = os.getenv("CHR_FORCE_BACKFILL_TIMESTAMP")

    if force_backfill_timestamp:
        logger.info(f"Using forced backfill timestamp: {force_backfill_timestamp}")
        return "backfill", force_backfill_timestamp

    if env_processing_mode == "full":
        logger.info("Full processing mode selected")
        return "full", None

    # Default: incremental processing (3 months)
    logger.info("Using incremental processing mode (default 3 months)")
    return "incremental", None


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    global logger
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Set up pipeline logger using common utility
    logger = setup_pipeline_logger("backend.pipelines.chr_pipeline", level=log_level)

    # Configure bronze module loggers
    bronze_logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze")
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)

    # Set third-party loggers to WARNING or higher
    logging.getLogger("zeep").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)


def get_client(context: dict[str, Any], client_name: str):
    """Get a CHR client lazily, creating it only when needed."""
    if client_name not in context["clients"]:
        logging.info(f"Creating {client_name} client lazily")

        client_creators = {
            "stamdata": create_stamdata_client,
            "besaetning": create_besaetning_client,
            "diko": create_diko_client,
            "ejendom": create_ejendom_client,
            "chr_dyr": create_chr_dyr_client,
        }

        if client_name not in client_creators:
            raise ValueError(f"Unknown client type: {client_name}")

        try:
            context["clients"][client_name] = client_creators[client_name]()
        except Exception as e:
            logging.error(f"Failed to create {client_name} client: {e}")
            raise

    return context["clients"][client_name]


def get_default_dates() -> tuple[date, date]:
    """Get default start and end dates (previous month)."""
    today = date.today()
    end_date = today.replace(day=1) - timedelta(days=1)  # Last day of previous month
    start_date = end_date.replace(day=1)  # First day of previous month
    return start_date, end_date


def generate_monthly_date_ranges(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """
    Generate monthly date ranges for VetStat API calls.
    VetStat API can only handle one month at a time.

    Args:
        start_date: Start date for the overall period
        end_date: End date for the overall period

    Returns:
        List of (month_start, month_end) tuples
    """
    monthly_ranges = []

    # Start from the first day of the start month
    current_date = start_date.replace(day=1)

    while current_date <= end_date:
        # Get the last day of the current month
        last_day = monthrange(current_date.year, current_date.month)[1]
        month_end = current_date.replace(day=last_day)

        # Don't go beyond the requested end date
        if month_end > end_date:
            month_end = end_date

        monthly_ranges.append((current_date, month_end))

        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

        # Break if we've gone past the end date
        if current_date > end_date:
            break

    return monthly_ranges


def create_chr_groups(
    chr_to_species: dict[int, set], num_groups: int = 4
) -> dict[int, dict[int, set]]:
    """
    Divide CHR numbers into balanced groups for parallel processing.

    Args:
        chr_to_species: Dictionary mapping CHR numbers to their species sets
        num_groups: Number of groups to create (default: 4)

    Returns:
        Dictionary mapping group number (1-based) to CHR-to-species mappings for that group
    """
    if not chr_to_species:
        return {}

    # Sort CHR numbers for deterministic grouping
    sorted_chrs = sorted(chr_to_species.keys())

    # Calculate group assignments based on CHR number ranges for balanced distribution
    chr_groups = {}
    for i in range(num_groups):
        chr_groups[i + 1] = {}

    # Distribute CHRs across groups using modulo for even distribution
    for idx, chr_num in enumerate(sorted_chrs):
        group_id = (idx % num_groups) + 1
        chr_groups[group_id][chr_num] = chr_to_species[chr_num]

    # Log group statistics
    for group_id, group_chrs in chr_groups.items():
        total_combinations = sum(len(species_set) for species_set in group_chrs.values())
        logging.info(
            f"CHR Group {group_id}: {len(group_chrs)} CHRs, {total_combinations} CHR/species combinations"
        )

    return chr_groups


def filter_chr_by_group(
    chr_to_species: dict[int, set], chr_group: int, num_groups: int = 4
) -> dict[int, set]:
    """
    Filter CHR numbers to only include those assigned to the specified group.

    Args:
        chr_to_species: Dictionary mapping CHR numbers to their species sets
        chr_group: Group number to filter for (1-based)
        num_groups: Total number of groups (default: 4)

    Returns:
        Filtered dictionary containing only CHRs for the specified group
    """
    if chr_group < 1 or chr_group > num_groups:
        logging.warning(f"Invalid CHR group {chr_group}. Must be between 1 and {num_groups}")
        return {}

    chr_groups = create_chr_groups(chr_to_species, num_groups)
    filtered_chrs = chr_groups.get(chr_group, {})

    total_combinations = sum(len(species_set) for species_set in filtered_chrs.values())
    logging.info(
        f"Filtered to CHR group {chr_group}: {len(filtered_chrs)} CHRs, {total_combinations} CHR/species combinations"
    )

    return filtered_chrs


def _parse_species_codes(raw: str | None) -> list[int] | None:
    """Convert comma-separated species codes string to list of ints."""
    if not raw:
        return None
    try:
        return [int(code.strip()) for code in raw.split(",")]
    except ValueError as exc:
        raise click.BadParameter("test_species_codes must be comma-separated integers") from exc


def fetch_stamdata(
    client: Any, username: str, test_species_codes: list[int] | None = None
) -> list[dict]:
    """Fetch and parse species/usage combinations."""
    logger.info("Fetching species/usage combinations...")
    response = load_species_usage_combinations(client, username)

    if not response or not hasattr(response, "Response"):
        logger.error("Invalid or empty Stamdata response")
        return []

    # Save the raw response to the export buffer
    save_raw_data(raw_response=response, data_type="stamdata_species_usage", identifier="all")

    combinations = []
    for combo in response.Response if isinstance(response.Response, list) else [response.Response]:
        try:
            species_code = int(getattr(combo, "DyreArtKode", 0))
            if test_species_codes and species_code not in test_species_codes:
                continue

            combinations.append(
                {
                    "species_code": species_code,
                    "usage_code": int(getattr(combo, "BrugsArtKode", 0)),
                    "species_text": str(getattr(combo, "DyreArtTekst", "")),
                    "usage_text": str(getattr(combo, "BrugsArtTekst", "")),
                }
            )
        except (ValueError, AttributeError) as e:
            logger.warning(f"Skipping invalid combination: {e}")

    logger.info(f"Found {len(combinations)} valid combinations")
    return combinations


def fetch_herds(
    client: Any,
    username: str,
    combinations: list[dict],
    limit_total: int | None = None,
    limit_per_species: int | None = None,
) -> dict[int, int]:
    """Fetch herd numbers for each species/usage combination."""
    logger.info("Fetching herd numbers...")
    herd_to_species = {}
    herds_count_per_species = {}  # Track counts per species

    for combo in combinations:
        species_code = combo["species_code"]
        usage_code = combo["usage_code"]

        # Skip this combo if the per-species limit is already reached for this species
        if (
            limit_per_species is not None
            and herds_count_per_species.get(species_code, 0) >= limit_per_species
        ):
            logger.debug(
                f"Skipping combo for species {species_code}, usage {usage_code} as limit {limit_per_species} reached."
            )
            continue

        start_number = 0
        while True:  # Loop for pagination within a combo
            # Check per-species limit before fetching more pages for this combo
            if (
                limit_per_species is not None
                and herds_count_per_species.get(species_code, 0) >= limit_per_species
            ):
                logger.debug(
                    f"Stopping pagination for species {species_code} as limit {limit_per_species} reached."
                )
                break  # Stop fetching pages for this combo

            try:
                # load_herd_list returns (herd_list, has_more, last_herd_in_batch)
                herd_list, has_more, last_herd = load_herd_list(
                    client, username, species_code, usage_code, start_number
                )

                herds_added_this_batch = 0
                # Process the herd list
                for herd_number in herd_list:
                    if herd_number > 0:
                        # Check per-species limit before adding
                        if (
                            limit_per_species is not None
                            and herds_count_per_species.get(species_code, 0) >= limit_per_species
                        ):
                            # We might still fetch more herds than needed in the last batch for a species,
                            # but we won't add them if the limit is hit.
                            continue  # Skip this herd if species limit reached

                        if herd_number not in herd_to_species:
                            herd_to_species[herd_number] = species_code
                            # Increment count for this species
                            herds_count_per_species[species_code] = (
                                herds_count_per_species.get(species_code, 0) + 1
                            )
                            herds_added_this_batch += 1

                            # Check total limit ONLY if per-species limit is NOT active
                            if (
                                limit_per_species is None
                                and limit_total is not None
                                and len(herd_to_species) >= limit_total
                            ):
                                logger.info(f"Total herd limit ({limit_total}) reached.")
                                logger.info(
                                    f"Found {len(herd_to_species)} unique herds across "
                                    f"{len(herds_count_per_species)} species."
                                )
                                for species, count in herds_count_per_species.items():
                                    logger.info(f"  Species {species}: {count} herds")
                                return herd_to_species

                logger.debug(
                    f"Processed batch for species {species_code}, usage {usage_code}. "
                    f"Added {herds_added_this_batch} new herds."
                )

                # Check if we need to continue pagination
                if not has_more:
                    break  # No more pages for this combo

                if last_herd is not None:
                    start_number = last_herd + 1
                else:  # Should not happen if has_more is False, but safety break
                    logger.warning("load_herd_list indicated more pages but returned no last_herd.")
                    break

            except Exception as e:
                logger.error(
                    f"Error fetching herds for species {species_code}, usage {usage_code}: {e}"
                )
                break  # Stop processing this combo on error

        logger.info(
            f"Finished fetching herds. Found {len(herd_to_species)} unique herds "
            f"across {len(herds_count_per_species)} species."
        )
    for species, count in herds_count_per_species.items():
        logger.info(f"  Species {species}: {count} herds")

    return herd_to_species


def process_parallel(func, tasks: list, workers: int, desc: str | None = None) -> list:
    """Execute tasks in parallel using a thread pool with progress tracking."""
    results = []

    with (
        logging_redirect_tqdm(),
        concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor,
    ):
        futures = [executor.submit(func, *task) for task in tasks]

        # Map futures to their task info for better error reporting
        future_to_task = dict(zip(futures, tasks, strict=False))
        future_to_index = {future: i for i, future in enumerate(futures)}

        # Create progress bar that works in both CI and interactive environments
        completed_count = 0
        pbar = tqdm(
            total=len(futures),
            desc=desc or func.__name__,
            unit="tasks",
            mininterval=1.0,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

        # Track start time for performance monitoring
        start_time = time.time()
        running_tasks = {}  # future -> start_time
        last_log_time = start_time
        success_count = 0
        error_count = 0

        try:
            for future in concurrent.futures.as_completed(futures):
                task_idx = future_to_index[future]
                task_info = future_to_task[future]

                # Remove from running tasks
                if future in running_tasks:
                    task_duration = time.time() - running_tasks[future]
                    del running_tasks[future]
                else:
                    task_duration = 0

                try:
                    result = future.result()
                    results.append(result)
                    success_count += 1

                    # Log slow tasks for debugging
                    if task_duration > 60:  # Log tasks taking more than 1 minute
                        if len(task_info) >= 3:  # Has herd number
                            herd_number = task_info[2]
                            logger.info(
                                f"Task {task_idx} (herd {herd_number}) completed in {task_duration:.1f}s"
                            )
                        else:
                            logger.info(f"Task {task_idx} completed in {task_duration:.1f}s")

                except Exception as e:
                    error_count += 1
                    logger.error(f"Task {task_idx} failed: {e}")
                    if len(task_info) >= 3:  # If task has herd number
                        herd_number = task_info[2]
                        logger.error(f"Herd {herd_number} failed with error: {e}")
                        logger.error(f"Task details: {task_info}")
                    else:
                        logger.error(f"Task {task_idx} details: {task_info}")
                    results.append(None)

                completed_count += 1
                pbar.update(1)

                # Log performance stats every 1000 tasks or every 5 minutes
                current_time = time.time()
                if (completed_count % 1000 == 0) or (current_time - last_log_time > 300):
                    elapsed = current_time - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    remaining_tasks = len(futures) - completed_count
                    eta_seconds = remaining_tasks / rate if rate > 0 else 0
                    logger.info(
                        f"Progress: {completed_count}/{len(futures)} tasks completed "
                        f"({completed_count / len(futures) * 100:.1f}%) - "
                        f"Rate: {rate:.2f} tasks/sec - "
                        f"Success: {success_count}, Errors: {error_count} - "
                        f"ETA: {eta_seconds / 3600:.1f}h"
                    )
                    last_log_time = current_time

        finally:
            pbar.close()

    return results


def get_required_steps(target_step: str) -> list[str]:
    """Get the list of bronze steps required to run before the target step."""
    # Only return bronze dependencies
    step_dependencies = {
        # Bronze steps
        "stamdata": [],
        "herds": ["stamdata"],
        "herd_details": ["stamdata", "herds"],
        "diko": ["stamdata", "herds"],  # Assuming diko depends on knowing the herds
        "animal_movements": [
            "stamdata",
            "herds",
        ],  # CHR_dyr animal movements with integrated discovery
        "ejendom": [
            "stamdata",
            "herds",
            "herd_details",
        ],  # Assuming ejendom depends on CHR numbers from details
        "vetstat": [
            "stamdata",
            "herds",
            "herd_details",
        ],  # Assuming vetstat depends on CHR numbers from details
        "spf_su": [
            "stamdata",
            "herds",
            "herd_details",
        ],  # SPF-SU depends on CHR numbers from details
        # Silver steps - all depend on all bronze steps implicitly
        "silver_vet_practices": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_properties": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_herds": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_herd_sizes": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_animal_movements": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_property_vet_events": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_antibiotic_usage": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
        "silver_all": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ],
    }
    # Filter out any silver steps from dependencies, only return bronze ones
    return [s for s in step_dependencies.get(target_step, []) if not s.startswith("silver_")]


def run_bronze_step(step: str, context: dict[str, Any]) -> dict[str, Any]:
    """Run a specific pipeline step and update the context."""
    logging.info(f"Starting step: {step}")

    # Only handle bronze steps here
    if step == "stamdata":
        context["combinations"] = fetch_stamdata(
            get_client(context, "stamdata"),
            context["username"],
            context["args"]["test_species_codes"],
        )
        if not context["combinations"]:
            raise ValueError("No valid species/usage combinations found")
        if context["args"]["progress"]:
            logging.info(f"Found {len(context['combinations'])} species/usage combinations")

    elif step == "herds":
        if "combinations" not in context:
            raise ValueError("Cannot run 'herds' step without first running 'stamdata'")

        context["herd_to_species"] = fetch_herds(
            get_client(context, "besaetning"),
            context["username"],
            context["combinations"],
            limit_total=context["args"]["limit_total_herds"],
            limit_per_species=context["args"]["limit_herds_per_species"],
        )
        if not context["herd_to_species"]:
            raise ValueError("No valid herds found")
        if context["args"]["progress"]:
            logging.info(f"Found {len(context['herd_to_species'])} herds")

    elif step == "herd_details":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'herd_details' step without first running 'herds'")

        # Store herd details and build CHR number mapping
        context["herd_details"] = []
        context["chr_to_species"] = {}

        herd_tasks = [
            (get_client(context, "besaetning"), context["username"], herd_num, species_code)
            for herd_num, species_code in context["herd_to_species"].items()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(herd_tasks)} herd detail tasks")

        results = process_parallel(
            load_herd_details, herd_tasks, context["args"]["workers"], "Processing herd details"
        )

        # Process results to build chr_to_species mapping
        for result, task in zip(results, herd_tasks, strict=False):
            if result and hasattr(result, "Response") and result.Response:
                context["herd_details"].append(result)
                # Extract CHR number from the response
                # Handle potential variations in response structure
                response_items = (
                    result.Response if isinstance(result.Response, list) else [result.Response]
                )
                for response_item in response_items:
                    if hasattr(response_item, "Besaetning") and hasattr(
                        response_item.Besaetning, "ChrNummer"
                    ):
                        chr_number = response_item.Besaetning.ChrNummer
                        if chr_number:  # Ensure CHR number is not None or 0
                            species_code = task[3]  # Get species code directly from the task
                            if chr_number not in context["chr_to_species"]:
                                context["chr_to_species"][chr_number] = set()
                            context["chr_to_species"][chr_number].add(species_code)

        if context["args"]["progress"]:
            logging.info(
                f"Processed {len(context['herd_details'])} herd details, "
                f"found {len(context['chr_to_species'])} unique CHR numbers"
            )

    elif step == "diko":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'diko' step without first running 'herds'")

        diko_tasks = [
            (get_client(context, "diko"), context["username"], herd_num, species_code)
            for herd_num, species_code in context["herd_to_species"].items()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(diko_tasks)} DIKO tasks")

        results = process_parallel(
            load_diko_flytninger, diko_tasks, context["args"]["workers"], "Processing DIKO tasks"
        )
        context["diko_results"] = (
            results  # Keep results in context for potential future use or export
        )

        if context["args"]["progress"]:
            successful = sum(1 for r in results if r)
            logging.info(f"Completed DIKO tasks. Success: {successful}/{len(diko_tasks)}")

    elif step == "ejendom":
        if "chr_to_species" not in context:
            raise ValueError("Cannot run 'ejendom' step without first running 'herd_details'")

        # Filter CHRs by group if CHR group is specified
        chr_to_species_filtered = context["chr_to_species"]
        if context["args"]["chr_group"] is not None:
            chr_to_species_filtered = filter_chr_by_group(
                context["chr_to_species"], context["args"]["chr_group"]
            )
            if not chr_to_species_filtered:
                logging.warning(f"No CHRs found for group {context['args']['chr_group']}")
                return context

        ejendom_tasks = [
            (get_client(context, "ejendom"), context["username"], chr_num)
            for chr_num in chr_to_species_filtered
        ]

        if context["args"]["progress"]:
            group_info = (
                f" (CHR group {context['args']['chr_group']})"
                if context["args"]["chr_group"]
                else ""
            )
            logging.info(f"Processing {len(ejendom_tasks)} ejendom tasks{group_info}")

        # Run both ejendom operations
        oplysninger_results = process_parallel(
            load_ejendom_oplysninger,
            ejendom_tasks,
            context["args"]["workers"],
            "Processing Ejendom Oplysninger",
        )
        vet_events_results = process_parallel(
            load_ejendom_vet_events,
            ejendom_tasks,
            context["args"]["workers"],
            "Processing Ejendom Vet Events",
        )
        # Results are stored in the buffer by the load functions

        if context["args"]["progress"]:
            successful_opl = sum(1 for r in oplysninger_results if r)
            successful_vet = sum(1 for r in vet_events_results if r)
            logging.info(
                f"Completed Ejendom tasks. Oplysninger success: {successful_opl}/{len(ejendom_tasks)}, "
                f"Vet events success: {successful_vet}/{len(ejendom_tasks)}"
            )

    elif step == "animal_movements":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'animal_movements' step without first running 'herds'")

        # Get all cattle herds for processing
        cattle_herds = [
            herd_num
            for herd_num, species_code in context["herd_to_species"].items()
            if species_code == 12
        ]

        if not cattle_herds:
            logging.warning("No cattle herds found for animal movements")
            context["animal_movements_results"] = []
            return context

        # Import the smart aggregation function with integrated discovery
        from bronze.animal_movements import load_cattle_movement_summaries
        from bronze.load_chr_dyr import initialize_consolidated_processing

        # Initialize consolidated processing for CHR_dyr movement summaries
        logger.info("🚀 Initializing consolidated CHR_dyr processing...")
        if not initialize_consolidated_processing():
            logger.error("❌ Failed to initialize consolidated processing")
            context["animal_movements_results"] = []
            return context

        logger.info(
            f"🚀 Starting animal movements processing with integrated discovery for {len(cattle_herds)} cattle herds"
        )

        # Process all herds - discovery and volume management will happen inline as needed
        movement_tasks = [
            (
                get_client(context, "chr_dyr"),
                context["username"],
                herd_num,
                context["args"]["start_date"],
                context["args"]["end_date"],
            )
            for herd_num in cattle_herds
        ]

        if context["args"]["progress"]:
            logging.info(
                f"Processing {len(movement_tasks)} herd movement tasks with integrated discovery"
            )

        results = process_parallel(
            load_cattle_movement_summaries,
            movement_tasks,
            context["args"]["workers"],
            "Animal movements with discovery",
        )

        # Filter successful results
        successful_results = [r for r in results if r is not None]
        context["animal_movements_results"] = successful_results

        if context["args"]["progress"]:
            logger.info(
                f"Animal movements completed: {len(successful_results)}/{len(results)} herds successful"
            )

        # CRITICAL FIX: Finalize consolidated processing to save animal movements data to GCS
        try:
            from bronze.load_chr_dyr import finalize_consolidated_processing

            logger.info("🚀 Finalizing consolidated animal movements processing...")
            success = finalize_consolidated_processing()
            if success:
                logger.info("✅ Animal movements data successfully saved to GCS")
            else:
                logger.warning("⚠️ Consolidated processing finalization returned False")
        except Exception as e:
            logger.error(f"❌ Failed to finalize consolidated processing: {e}", exc_info=True)
            # Don't raise - let the pipeline continue, but log the error

    elif step == "vetstat":
        if "chr_to_species" not in context:
            raise ValueError("Cannot run 'vetstat' step without first running 'herd_details'")

        # Generate monthly date ranges for VetStat API limitation
        start_date = context["args"]["start_date"]
        end_date = context["args"]["end_date"]
        monthly_ranges = generate_monthly_date_ranges(start_date, end_date)

        logging.info(
            f"VetStat will process {len(monthly_ranges)} monthly chunks from {start_date} to {end_date}"
        )
        for i, (month_start, month_end) in enumerate(monthly_ranges):
            logging.debug(f"  Month {i + 1}: {month_start} to {month_end}")

        # Filter CHRs by group if CHR group is specified
        chr_to_species_filtered = context["chr_to_species"]
        if context["args"]["chr_group"] is not None:
            chr_to_species_filtered = filter_chr_by_group(
                context["chr_to_species"], context["args"]["chr_group"]
            )
            if not chr_to_species_filtered:
                logging.warning(f"No CHRs found for group {context['args']['chr_group']}")
                return context

        # Create tasks for each CHR/species/month combination
        vetstat_tasks = [
            (chr_num, species, month_start, month_end)
            for chr_num, species_set in chr_to_species_filtered.items()
            for species in species_set
            for month_start, month_end in monthly_ranges
        ]

        if not vetstat_tasks:
            logging.warning(
                "No valid CHR number and species code combinations found for VetStat data"
            )
        elif context["args"]["progress"]:
            combinations = sum(len(species_set) for species_set in chr_to_species_filtered.values())
            group_info = (
                f" (CHR group {context['args']['chr_group']})"
                if context["args"]["chr_group"]
                else ""
            )
            logging.info(
                f"Processing {len(vetstat_tasks)} VetStat tasks{group_info} "
                f"({combinations} CHR/species combinations × {len(monthly_ranges)} months)"
            )

        try:
            # Use VetStat-specific worker count if available, otherwise fall back to general workers
            vetstat_workers = int(os.getenv("VETSTAT_MAX_WORKERS", context["args"]["workers"]))
            logging.info(
                f"Using {vetstat_workers} workers for VetStat processing "
                f"(vs {context['args']['workers']} general workers)"
            )
            results = process_parallel(
                load_vetstat_antibiotics, vetstat_tasks, vetstat_workers, "Processing VetStat tasks"
            )
            # Results are stored in the buffer by the load function
            if context["args"]["progress"]:
                successful = sum(
                    1 for r in results if r
                )  # Check if results were returned (even if empty)
                logging.info(f"Completed VetStat tasks. Success: {successful}/{len(vetstat_tasks)}")
        except Exception as e:
            logging.error(f"Error processing VetStat tasks: {e}", exc_info=True)
            raise

    elif step == "spf_su":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'spf_su' step without first running 'herds'")

        # Import SPF-SU functions
        from bronze.load_spf_su import get_pig_herd_numbers, load_spf_su_data

        # Get pig herd numbers (species_code = 15)
        pig_herd_numbers = get_pig_herd_numbers(context["herd_to_species"])

        if not pig_herd_numbers:
            logging.warning("No pig herds found for SPF-SU data collection")
            context["spf_su_results"] = []
            return context

        if context["args"]["progress"]:
            logging.info(f"Processing {len(pig_herd_numbers)} SPF-SU tasks for pig herds")

        # Run SPF-SU data collection asynchronously
        import asyncio

        try:
            results = asyncio.run(load_spf_su_data(pig_herd_numbers))
            context["spf_su_results"] = results
            if context["args"]["progress"]:
                successful = len([r for r in results if r])
                logging.info(
                    f"Completed SPF-SU tasks. Success: {successful}/{len(pig_herd_numbers)}"
                )
        except Exception as e:
            logging.error(f"Error processing SPF-SU tasks: {e}", exc_info=True)
            raise

    else:
        logger.warning(f"Attempted to run unknown bronze step: {step}")

    return context


@click.command()
@click.option(
    "--steps",
    default="all",
    help="Pipeline steps to run (all, stamdata, herds, herd_details, diko, animal_movements, ejendom, vetstat, silver_*)",
)
@click.option(
    "--test-species-codes", default=None, help="Comma-separated list of species codes for testing"
)
@click.option(
    "--limit-total-herds", type=int, default=None, help="Limit total number of herds to process"
)
@click.option(
    "--limit-herds-per-species", type=int, default=None, help="Limit number of herds per species"
)
@click.option(
    "--workers",
    type=int,
    default=int(os.getenv("CHR_MAX_WORKERS", "5")),
    help="Number of parallel workers",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    default="WARNING",
    help="Logging level",
)
@click.option("--progress", is_flag=True, help="Show progress information")
@click.option("--start-date", default=None, help="Start date for data collection (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="End date for data collection (YYYY-MM-DD)")
@click.option(
    "--skip-dependencies",
    is_flag=True,
    help="Skip running dependency steps (for parallel job execution)",
)
def main(
    steps,
    test_species_codes,
    limit_total_herds,
    limit_herds_per_species,
    workers,
    log_level,
    progress,
    start_date,
    end_date,
    skip_dependencies,
):
    """CHR Data Pipeline."""
    # Record start time for execution tracking
    start_time_dt = datetime.now()

    # Parse species codes
    test_species_codes = _parse_species_codes(test_species_codes)

    # Parse dates
    default_start, default_end = get_default_dates()
    if start_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise click.BadParameter("start_date must be in YYYY-MM-DD format") from exc
    else:
        start_date = default_start
    if end_date:
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise click.BadParameter("end_date must be in YYYY-MM-DD format") from exc
    else:
        end_date = default_end

    # Build args dict for backward compatibility with downstream code
    args = {
        "steps": steps,
        "test_species_codes": test_species_codes,
        "limit_total_herds": limit_total_herds,
        "limit_herds_per_species": limit_herds_per_species,
        "workers": workers,
        "log_level": log_level,
        "progress": progress,
        "start_date": start_date,
        "end_date": end_date,
        "skip_dependencies": skip_dependencies,
    }

    setup_logging(args["log_level"])

    # Initialize pipeline metadata manager
    from common.pipeline_metadata import MetadataManager

    pipeline_metadata_manager = MetadataManager()
    logger.info("Pipeline metadata system initialized")

    try:
        # Determine steps to run first to decide if we need FVM credentials
        requested_step = args["steps"]

        # Check if this is a silver-only operation
        is_silver_only = requested_step.startswith("silver_")

        # Check if we should import context from a previous job
        context_import_path = os.getenv("CONTEXT_IMPORT_PATH")

        # For silver-only operations, we might not need FVM credentials if:
        # 1. We have existing bronze data files, OR
        # 2. We have data in the buffer, OR
        # 3. We're importing context from a previous job
        needs_fvm_credentials = True

        if is_silver_only:
            # Check if we have existing bronze data or context
            buffer_data = get_data_buffer()
            has_buffer_data = bool(buffer_data)
            has_context_import = context_import_path and os.path.exists(context_import_path)

            # Check if we have bronze data files available locally
            bronze_dir_override = os.getenv("BRONZE_DATE_FOLDER_OVERRIDE")

            # In GitHub Actions, read bronze timestamp from artifact file
            if not bronze_dir_override and os.path.exists("/tmp/bronze_timestamp.txt"):
                with open("/tmp/bronze_timestamp.txt") as f:
                    bronze_dir_override = f.read().strip()
                logging.warning(f"Using bronze timestamp from artifact: {bronze_dir_override}")

            has_bronze_files = False

            # PRIORITY 1: Check override directory first (most reliable)
            if bronze_dir_override:
                bronze_path = config.BRONZE_BASE_DIR / bronze_dir_override
                if bronze_path.exists() and any(bronze_path.glob("*.json")):
                    has_bronze_files = True
                    logging.warning(f"Found bronze files in override directory: {bronze_path}")
                else:
                    logging.warning(
                        f"Override directory specified but no files found: {bronze_path}"
                    )

            # PRIORITY 2: Check current export timestamp directory
            if not has_bronze_files:
                bronze_path = config.BRONZE_BASE_DIR / EXPORT_TIMESTAMP
                if bronze_path.exists() and any(bronze_path.glob("*.json")):
                    has_bronze_files = True
                    logging.warning(
                        f"Found bronze files in current timestamp directory: {bronze_path}"
                    )

            # Check if streaming mode is available for GCS data access
            bronze_timestamp = bronze_dir_override or EXPORT_TIMESTAMP
            can_use_streaming = (
                # Force streaming if explicitly requested
                os.getenv("CHR_FORCE_STREAMING", "false").lower() == "true"
                or
                # Use streaming in GitHub Actions for memory efficiency
                os.getenv("GITHUB_ACTIONS") == "true"
                or
                # Use streaming if we have bronze timestamp and no buffer data
                (bronze_timestamp and not buffer_data)
            )

            if (
                has_buffer_data
                or has_context_import
                or has_bronze_files
                or (can_use_streaming and bronze_timestamp)
            ):
                logging.warning(
                    f"Silver-only operation detected with existing data. "
                    f"Buffer: {has_buffer_data}, Context: {has_context_import}, "
                    f"Files: {has_bronze_files}, Streaming: {can_use_streaming and bronze_timestamp}, "
                    f"Override: {bronze_dir_override}"
                )
                needs_fvm_credentials = False
            else:
                logging.warning(
                    f"Silver-only operation but no existing data found. Will need to run bronze steps. "
                    f"Buffer: {has_buffer_data}, Context: {has_context_import}, "
                    f"Files: {has_bronze_files}, Streaming: {can_use_streaming and bronze_timestamp}, "
                    f"Override: {bronze_dir_override}"
                )

        # Initialize context based on whether we need FVM credentials
        if context_import_path and os.path.exists(context_import_path):
            logging.warning(f"Importing context data from {context_import_path}")
            imported_context = import_context_data(Path(context_import_path))

            if needs_fvm_credentials:
                # Initialize context with imported data and FVM credentials
                try:
                    username, _password, _certificate, _private_key = get_fvm_credentials()
                except Exception as e:
                    logging.warning(f"Robust authentication failed, falling back to legacy: {e}")
                    username, _password = get_legacy_fvm_credentials()

                context = {
                    "args": args,
                    "username": username,
                    "clients": {},  # Create clients lazily when needed
                    # Merge imported context
                    **imported_context,
                }
            else:
                # Initialize minimal context for silver-only processing
                context = {
                    "args": args,
                    "username": None,  # Not needed for silver processing
                    "clients": {},  # Not needed for silver processing
                    # Merge imported context
                    **imported_context,
                }

            if not args.get("start_date") and imported_context.get("args", {}).get("start_date"):
                context["args"]["start_date"] = imported_context["args"]["start_date"]
            if not args.get("end_date") and imported_context.get("args", {}).get("end_date"):
                context["args"]["end_date"] = imported_context["args"]["end_date"]

            # Ensure all current args are preserved (especially progress flag)
            # Current args should take precedence over imported args
            context["args"] = {**imported_context.get("args", {}), **args}

        else:
            if needs_fvm_credentials:
                # Initialize fresh context with FVM credentials
                try:
                    username, _password, _certificate, _private_key = get_fvm_credentials()
                except Exception as e:
                    logging.warning(f"Robust authentication failed, falling back to legacy: {e}")
                    username, __password = get_legacy_fvm_credentials()

                context = {
                    "args": args,
                    "username": username,
                    "clients": {},  # Create clients lazily when needed
                }
            else:
                # Initialize minimal context for silver-only processing
                context = {
                    "args": args,
                    "username": None,  # Not needed for silver processing
                    "clients": {},  # Not needed for silver processing
                }

        # Determine steps to run
        if requested_step == "all":
            bronze_steps_to_run = [
                "stamdata",
                "herds",
                "herd_details",
                "diko",
                "animal_movements",  # Now includes integrated discovery
                "ejendom",
                "vetstat",
                "spf_su",
            ]
            run_silver = True
        elif requested_step.startswith("silver_"):
            # If a silver step is requested, run all bronze prerequisites only if we need FVM credentials
            if needs_fvm_credentials:
                bronze_steps_to_run = get_required_steps(requested_step)
            else:
                bronze_steps_to_run = []  # Skip bronze steps for silver-only with existing data
            run_silver = True
        elif requested_step in [
            "gold_processing",
            "veterinary_timeline",
            "transportation_analysis",
        ]:
            # Gold layer processing - no bronze steps needed, but run silver if no existing silver data
            bronze_steps_to_run = []
            run_silver = True  # Always run silver before gold to ensure we have fresh silver data
        elif args.get("skip_dependencies", False):
            # Skip dependencies - only run the specific step (for parallel job execution)
            bronze_steps_to_run = [requested_step]
            run_silver = False
        else:
            # Only run requested bronze step and its prerequisites
            bronze_steps_to_run = [*get_required_steps(requested_step), requested_step]
            run_silver = False

        # Ensure unique bronze steps in order
        unique_bronze_steps = []
        for step in [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
            "spf_su",
        ]:
            if step in bronze_steps_to_run and step not in unique_bronze_steps:
                unique_bronze_steps.append(step)

        # Run bronze steps sequentially (only if we have any to run)
        if unique_bronze_steps:
            logging.warning(f"Running bronze steps: {', '.join(unique_bronze_steps)}")
            for step in unique_bronze_steps:
                context = run_bronze_step(step, context)
            logging.warning("Bronze steps completed.")
        else:
            logging.warning(
                "No bronze steps to run - proceeding with silver processing using existing data."
            )

        # Export context data if requested (for multi-job workflows)
        context_export_path = os.getenv("CONTEXT_EXPORT_PATH")
        if context_export_path:
            logging.warning(f"Exporting context data to {context_export_path}")
            export_context_data(context, Path(context_export_path))

        # Run silver processing if requested
        if run_silver:
            logging.warning("Starting silver processing...")
            try:
                # Determine silver output directory
                silver_dir = config.SILVER_BASE_DIR / EXPORT_TIMESTAMP
                silver_dir.mkdir(parents=True, exist_ok=True)  # Ensure it exists

                # For silver processing, check if we need to use files or buffer
                buffer_data = get_data_buffer()
                bronze_dir_override = os.getenv("BRONZE_DATE_FOLDER_OVERRIDE")

                # In GitHub Actions, read bronze timestamp from artifact file
                if not bronze_dir_override and os.path.exists("/tmp/bronze_timestamp.txt"):
                    with open("/tmp/bronze_timestamp.txt") as f:
                        bronze_dir_override = f.read().strip()
                    logging.warning(
                        f"Using bronze timestamp from artifact for streaming: {bronze_dir_override}"
                    )

                # Determine bronze timestamp for streaming mode
                bronze_timestamp = bronze_dir_override or EXPORT_TIMESTAMP

                # Use streaming mode for memory efficiency (recommended approach)
                use_streaming_mode = (
                    # Force streaming if explicitly requested
                    os.getenv("CHR_FORCE_STREAMING", "false").lower() == "true"
                    or
                    # Use streaming in GitHub Actions for memory efficiency
                    os.getenv("GITHUB_ACTIONS") == "true"
                    or
                    # Use streaming if we have bronze timestamp and no buffer data
                    (bronze_timestamp and not buffer_data)
                )

                # Determine processing mode (incremental, backfill, or full)
                processing_mode = "full"  # Default fallback
                backfill_timestamp = None

                if use_streaming_mode and bronze_timestamp and INCREMENTAL_PROCESSING_AVAILABLE:
                    try:
                        processing_mode, backfill_timestamp = determine_processing_mode(
                            bronze_timestamp
                        )
                        logging.warning(f"🎯 Processing mode determined: {processing_mode}")

                        # Calculate date ranges based on processing mode
                        start_date, end_date = calculate_date_range(processing_mode)
                        if start_date and end_date:
                            logging.warning(f"📅 Date range: {start_date} to {end_date}")
                            # Update the context args to use the calculated date ranges
                            context["args"]["start_date"] = datetime.strptime(
                                start_date, "%Y-%m-%d"
                            ).date()
                            context["args"]["end_date"] = datetime.strptime(
                                end_date, "%Y-%m-%d"
                            ).date()
                            logging.warning("✅ Updated pipeline args with incremental date range")

                        # Track processing run start (simple upsert)
                        processor = get_incremental_processor()
                        run_id = processor.track_processing_run(
                            ProcessingRun(
                                pipeline_name="chr",
                                bronze_timestamp=backfill_timestamp or bronze_timestamp,
                                processing_mode=processing_mode,
                            )
                        )
                        logging.info(f"📊 Tracking processing run with ID: {run_id}")

                    except Exception as e:
                        logging.error(f"Error in incremental processing setup: {e}")
                        logging.warning("Falling back to full processing mode")

                # Choose the appropriate bronze timestamp for processing
                effective_bronze_timestamp = backfill_timestamp or bronze_timestamp

                if use_streaming_mode and effective_bronze_timestamp:
                    # Use streaming mode for memory efficiency
                    mode_desc = (
                        f"{processing_mode.upper()} processing"
                        if processing_mode != "full"
                        else "STREAMING"
                    )
                    logging.warning(
                        f"🚀 Using {mode_desc} mode for memory-efficient processing "
                        f"(bronze_timestamp: {effective_bronze_timestamp})"
                    )

                    if processing_mode == "backfill":
                        logging.warning(
                            f"📅 Using historical data from: {effective_bronze_timestamp}"
                        )

                    success = run_silver_processing(
                        silver_dir=silver_dir,
                        export_timestamp=EXPORT_TIMESTAMP,
                        bronze_timestamp=effective_bronze_timestamp,
                        force_streaming=True,
                    )
                    if not success:
                        raise RuntimeError(f"Silver processing ({processing_mode} mode) failed")

                elif buffer_data:
                    # Use legacy in-memory mode
                    logging.warning("📝 Using LEGACY mode with in-memory buffer data")
                    logging.warning("⚠️ Legacy mode may cause memory issues with large datasets")
                    run_silver_processing(
                        in_memory_data=buffer_data,
                        silver_dir=silver_dir,
                        export_timestamp=EXPORT_TIMESTAMP,
                        bronze_timestamp=effective_bronze_timestamp,
                    )
                elif bronze_dir_override:
                    # Use legacy file-based mode
                    logging.warning(
                        f"📁 Using LEGACY mode with bronze files from override directory: {bronze_dir_override}"
                    )
                    logging.warning("⚠️ Legacy mode may cause memory issues with large datasets")
                    run_silver_processing(
                        in_memory_data=None,
                        silver_dir=silver_dir,
                        export_timestamp=EXPORT_TIMESTAMP,
                        bronze_timestamp=effective_bronze_timestamp,
                    )
                else:
                    # No valid bronze data source found - fail explicitly
                    raise RuntimeError("No bronze data source available for silver processing")

                logging.warning(f"✅ Silver processing completed. Output in: {silver_dir}")

                # Create and save metadata for CHR data sources
                if pipeline_metadata_manager:
                    try:
                        processing_duration = time.time() - start_time_dt.timestamp()

                        # Create metadata for CHR animal movements
                        if unique_bronze_steps and "animal_movements" in unique_bronze_steps:
                            chr_movements_metadata = pipeline_metadata_manager.create_metadata(
                                source_key="chr_animal_movements",
                                record_count=len(context.get("animal_movements_results", [])),
                                processing_duration=processing_duration,
                                file_size_bytes=None,  # Will be calculated automatically
                                source_datasets=None,
                            )
                            metadata_path = pipeline_metadata_manager.save_metadata(
                                chr_movements_metadata,
                                silver_dir / "chr_animal_movements_metadata.json",
                            )
                            logger.info(
                                f"✅ CHR animal movements metadata saved to {metadata_path}"
                            )

                        # Create metadata for CHR properties
                        if unique_bronze_steps and any(
                            step in unique_bronze_steps for step in ["ejendom", "herd_details"]
                        ):
                            chr_properties_metadata = pipeline_metadata_manager.create_metadata(
                                source_key="chr_properties",
                                record_count=len(context.get("chr_to_species", {})),
                                processing_duration=processing_duration,
                                file_size_bytes=None,
                                source_datasets=None,
                            )
                            metadata_path = pipeline_metadata_manager.save_metadata(
                                chr_properties_metadata, silver_dir / "chr_properties_metadata.json"
                            )
                            logger.info(f"✅ CHR properties metadata saved to {metadata_path}")

                        # Create metadata for CHR herds
                        if unique_bronze_steps and "herd_details" in unique_bronze_steps:
                            chr_herds_metadata = pipeline_metadata_manager.create_metadata(
                                source_key="chr_herds",
                                record_count=len(context.get("herd_details", [])),
                                processing_duration=processing_duration,
                                file_size_bytes=None,
                                source_datasets=None,
                            )
                            metadata_path = pipeline_metadata_manager.save_metadata(
                                chr_herds_metadata, silver_dir / "chr_herds_metadata.json"
                            )
                            logger.info(f"✅ CHR herds metadata saved to {metadata_path}")

                    except Exception as e:
                        logger.error(f"❌ Failed to create CHR pipeline metadata: {e}")

                # --- Gold Layer Processing ---
                # Run gold processing if this is an "all" run or a specific gold step
                if requested_step in [
                    "all",
                    "gold_processing",
                    "veterinary_timeline",
                    "transportation_analysis",
                ]:
                    try:
                        logging.warning("🥇 Starting Gold Layer Processing...")
                        gold_success = run_gold_processing(
                            export_timestamp=EXPORT_TIMESTAMP,
                            silver_dir=silver_dir,
                            step=requested_step,
                        )

                        if gold_success:
                            logging.warning("✅ Gold processing completed successfully")
                        else:
                            logging.error("❌ Gold processing failed")
                            # For gold-specific steps, fail the pipeline if gold processing fails
                            if requested_step in [
                                "gold_processing",
                                "veterinary_timeline",
                                "transportation_analysis",
                            ]:
                                raise RuntimeError("Gold processing failed")

                    except Exception as e:
                        logging.error(f"❌ Gold processing failed: {e}", exc_info=True)
                        # For gold-specific steps, fail the pipeline if gold processing fails
                        if requested_step in [
                            "gold_processing",
                            "veterinary_timeline",
                            "transportation_analysis",
                        ]:
                            raise
                        # For "all" runs, don't fail the entire pipeline for gold processing failure

            except Exception as e:
                logging.error(f"❌ Silver processing failed: {e}", exc_info=True)
                raise  # Re-raise to indicate pipeline failure

        # Finalize bronze export (if we have bronze data to export)
        buffer_data = get_data_buffer()
        if unique_bronze_steps or buffer_data:
            # But don't clear buffer if we're exporting context (dependent jobs might need the data)
            clear_buffer_after_export = not bool(context_export_path)
            logging.warning("Finalizing bronze export...")

            # Add debugging info about buffer size before export
            if buffer_data:
                total_records = sum(
                    len(data.get("json", [])) + len(data.get("xml", []))
                    for data in buffer_data.values()
                )
                logging.warning(
                    f"Buffer contains {total_records} total records across {len(buffer_data)} data types"
                )
                for key, data in buffer_data.items():
                    json_count = len(data.get("json", []))
                    xml_count = len(data.get("xml", []))
                    logging.warning(f"  {key}: {json_count} JSON records, {xml_count} XML records")
            else:
                logging.warning(
                    "No buffer data found, but finalizing export anyway due to bronze steps run"
                )

            # FIXED: Actually call finalize_export to save consolidated files instead of thousands of individual ones
            try:
                from bronze.export import finalize_export

                finalize_export(clear_buffer=clear_buffer_after_export)
                logging.warning(
                    "✅ Bronze export finalization completed - consolidated files saved to GCS"
                )
            except Exception as e:
                logging.error(f"Error finalizing bronze export: {e}", exc_info=True)
                raise
        else:
            logging.warning("No bronze data to export - skipping bronze export finalization.")

        # --- Final Cleanup and Summary ---
        logger.info("=== CHR Pipeline Execution Complete ===")
        logger.info(f"Total execution time: {datetime.now() - start_time_dt}")

        # Run comprehensive cleanup
        try:
            # Import cleanup module with absolute import
            from backend.pipelines.chr_pipeline.cleanup_temp_files import (
                cleanup_temp_files,
                monitor_disk_usage,
            )

            logger.info("Running final cleanup of temporary files...")
            monitor_disk_usage()
            cleaned_files, total_size = cleanup_temp_files()
            if cleaned_files > 0:
                logger.info(
                    f"Final cleanup: {cleaned_files} files removed, {total_size / (1024 * 1024):.2f} MB freed"
                )
            monitor_disk_usage()
        except ImportError as import_error:
            logger.warning(f"Cleanup module not available: {import_error}")
        except Exception as e:
            logger.warning(f"Error during final cleanup: {e}")

        # Force final garbage collection
        import gc

        gc.collect()

        logger.info("CHR Pipeline finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
