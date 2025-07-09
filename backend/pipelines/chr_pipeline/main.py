# backend/chr_pipeline/main.py
"""CHR Pipeline for fetching and processing data."""

import argparse
import concurrent.futures
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from bronze.export import EXPORT_TIMESTAMP, export_context_data, finalize_export, get_data_buffer, import_context_data
from bronze.load_besaetning import ENDPOINTS as BES_ENDPOINTS
from bronze.load_besaetning import create_soap_client as create_bes_client
from bronze.load_besaetning import get_fvm_credentials, load_herd_details, load_herd_list
from bronze.load_diko import ENDPOINTS as DIKO_ENDPOINTS
from bronze.load_diko import create_soap_client as create_diko_client
from bronze.load_diko import load_diko_flytninger
from bronze.load_ejendom import ENDPOINTS as EJD_ENDPOINTS
from bronze.load_ejendom import create_soap_client as create_ejd_client
from bronze.load_ejendom import load_ejendom_oplysninger, load_ejendom_vet_events
from bronze.load_stamdata import ENDPOINTS as STAMDATA_ENDPOINTS
from bronze.load_stamdata import create_soap_client as create_stamdata_client
from bronze.load_stamdata import load_species_usage_combinations
from bronze.load_vetstat import load_vetstat_antibiotics
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from silver import config

# Import silver processing orchestrator
from silver.chr_silver_processing import process_chr_data as run_silver_processing

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Set up root logger at WARNING by default with a format that works well with tqdm
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Configure our pipeline loggers
    pipeline_logger = logging.getLogger("backend.pipelines.chr_pipeline")
    pipeline_logger.setLevel(numeric_level)

    # Configure bronze module loggers
    bronze_logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze")
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)

    # Set third-party loggers to WARNING or higher
    logging.getLogger("zeep").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    # Prevent log propagation for specific modules when not in DEBUG
    if numeric_level > logging.DEBUG:
        for logger_name in ["zeep", "urllib3", "google", "requests"]:
            logging.getLogger(logger_name).propagate = False


def get_default_dates() -> tuple[date, date]:
    """Get default start and end dates (previous month)."""
    today = date.today()
    end_date = today.replace(day=1) - timedelta(days=1)  # Last day of previous month
    start_date = end_date.replace(day=1)  # First day of previous month
    return start_date, end_date


def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="CHR Data Pipeline")

    parser.add_argument(
        "--steps",
        type=str,
        default="all",
        help="Pipeline steps to run (all, stamdata, herds, herd_details, diko, animal_movements, ejendom, vetstat, silver_*)",
    )
    parser.add_argument("--test-species-codes", type=str, help="Comma-separated list of species codes for testing")
    parser.add_argument("--limit-total-herds", type=int, help="Limit total number of herds to process")
    parser.add_argument("--limit-herds-per-species", type=int, help="Limit number of herds per species")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")
    parser.add_argument("--log-level", type=str, default="WARNING", help="Logging level")
    parser.add_argument("--progress", action="store_true", help="Show progress information")
    parser.add_argument("--start-date", type=str, help="Start date for data collection (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for data collection (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-dependencies", action="store_true", help="Skip running dependency steps (for parallel job execution)"
    )

    args = parser.parse_args()

    # Convert test_species_codes to list of integers if provided
    test_species_codes = None
    if args.test_species_codes:
        try:
            test_species_codes = [int(code.strip()) for code in args.test_species_codes.split(",")]
        except ValueError:
            raise ValueError("test_species_codes must be comma-separated integers")

    # Parse dates if provided
    start_date, end_date = get_default_dates()
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("start_date must be in YYYY-MM-DD format")
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("end_date must be in YYYY-MM-DD format")

    return {
        "steps": args.steps,
        "test_species_codes": test_species_codes,
        "limit_total_herds": args.limit_total_herds,
        "limit_herds_per_species": args.limit_herds_per_species,
        "workers": args.workers,
        "log_level": args.log_level,
        "progress": args.progress,
        "start_date": start_date,
        "end_date": end_date,
        "skip_dependencies": args.skip_dependencies,
    }


def fetch_stamdata(client: Any, username: str, test_species_codes: Optional[List[int]] = None) -> List[Dict]:
    """Fetch and parse species/usage combinations."""
    logger.info("Fetching species/usage combinations...")
    response = load_species_usage_combinations(client, username)

    if not response or not hasattr(response, "Response"):
        logger.error("Invalid or empty Stamdata response")
        return []

    # Save the raw response to the export buffer
    from bronze.export import save_raw_data

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
    combinations: List[Dict],
    limit_total: Optional[int] = None,
    limit_per_species: Optional[int] = None,
) -> Dict[int, int]:
    """Fetch herd numbers for each species/usage combination."""
    logger.info("Fetching herd numbers...")
    herd_to_species = {}
    herds_count_per_species = {}  # Track counts per species

    for combo in combinations:
        species_code = combo["species_code"]
        usage_code = combo["usage_code"]

        # Skip this combo if the per-species limit is already reached for this species
        if limit_per_species is not None and herds_count_per_species.get(species_code, 0) >= limit_per_species:
            logger.debug(
                f"Skipping combo for species {species_code}, usage {usage_code} as limit {limit_per_species} reached."
            )
            continue

        start_number = 0
        while True:  # Loop for pagination within a combo
            # Check per-species limit before fetching more pages for this combo
            if limit_per_species is not None and herds_count_per_species.get(species_code, 0) >= limit_per_species:
                logger.debug(f"Stopping pagination for species {species_code} as limit {limit_per_species} reached.")
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
                            herds_count_per_species[species_code] = herds_count_per_species.get(species_code, 0) + 1
                            herds_added_this_batch += 1

                            # Check total limit ONLY if per-species limit is NOT active
                            if (
                                limit_per_species is None
                                and limit_total is not None
                                and len(herd_to_species) >= limit_total
                            ):
                                logger.info(f"Total herd limit ({limit_total}) reached.")
                                logger.info(
                                    f"Found {len(herd_to_species)} unique herds across {len(herds_count_per_species)} species."
                                )
                                for species, count in herds_count_per_species.items():
                                    logger.info(f"  Species {species}: {count} herds")
                                return herd_to_species

                logger.debug(
                    f"Processed batch for species {species_code}, usage {usage_code}. Added {herds_added_this_batch} new herds."
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
                logger.error(f"Error fetching herds for species {species_code}, usage {usage_code}: {e}")
                break  # Stop processing this combo on error

        logger.info(
            f"Finished fetching herds. Found {len(herd_to_species)} unique herds across {len(herds_count_per_species)} species."
        )
    for species, count in herds_count_per_species.items():
        logger.info(f"  Species {species}: {count} herds")

    return herd_to_species


def process_parallel(func, tasks: List, workers: int, desc: str = None) -> List:
    """Execute tasks in parallel using a thread pool with progress tracking."""
    results = []

    with logging_redirect_tqdm():
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(func, *task) for task in tasks]

            # Map futures to their task info for better error reporting
            future_to_task = {future: task for future, task in zip(futures, tasks)}
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

                        # Log slow tasks for debugging
                        if task_duration > 60:  # Log tasks taking more than 1 minute
                            if len(task_info) >= 3:  # Has herd number
                                herd_number = task_info[2]
                                logger.info(f"Task {task_idx} (herd {herd_number}) completed in {task_duration:.1f}s")
                            else:
                                logger.info(f"Task {task_idx} completed in {task_duration:.1f}s")

                    except Exception as e:
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

            finally:
                pbar.close()

    return results


def get_required_steps(target_step: str) -> List[str]:
    """Get the list of bronze steps required to run before the target step."""
    # Only return bronze dependencies
    step_dependencies = {
        # Bronze steps
        "stamdata": [],
        "herds": ["stamdata"],
        "herd_details": ["stamdata", "herds"],
        "diko": ["stamdata", "herds"],  # Assuming diko depends on knowing the herds
        "animal_movements": ["stamdata", "herds"],  # CHR_dyr animal movements depend on knowing the herds
        "ejendom": ["stamdata", "herds", "herd_details"],  # Assuming ejendom depends on CHR numbers from details
        "vetstat": ["stamdata", "herds", "herd_details"],  # Assuming vetstat depends on CHR numbers from details
        "spf_su": ["stamdata", "herds", "herd_details"],  # SPF-SU depends on CHR numbers from details
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
        "silver_all": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat", "spf_su"],
    }
    # Filter out any silver steps from dependencies, only return bronze ones
    required_bronze = [s for s in step_dependencies.get(target_step, []) if not s.startswith("silver_")]
    return required_bronze


def run_bronze_step(step: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Run a specific pipeline step and update the context."""
    logging.info(f"Starting step: {step}")

    # Only handle bronze steps here
    if step == "stamdata":
        context["combinations"] = fetch_stamdata(
            context["clients"]["stamdata"], context["username"], context["args"]["test_species_codes"]
        )
        if not context["combinations"]:
            raise ValueError("No valid species/usage combinations found")
        if context["args"]["progress"]:
            logging.info(f"Found {len(context['combinations'])} species/usage combinations")

    elif step == "herds":
        if "combinations" not in context:
            raise ValueError("Cannot run 'herds' step without first running 'stamdata'")

        context["herd_to_species"] = fetch_herds(
            context["clients"]["besaetning"],
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
            (context["clients"]["besaetning"], context["username"], herd_num, species_code)
            for herd_num, species_code in context["herd_to_species"].items()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(herd_tasks)} herd detail tasks")

        results = process_parallel(load_herd_details, herd_tasks, context["args"]["workers"], "Processing herd details")

        # Process results to build chr_to_species mapping
        for result, task in zip(results, herd_tasks):
            if result and hasattr(result, "Response") and result.Response:
                context["herd_details"].append(result)
                # Extract CHR number from the response
                # Handle potential variations in response structure
                response_items = result.Response if isinstance(result.Response, list) else [result.Response]
                for response_item in response_items:
                    if hasattr(response_item, "Besaetning") and hasattr(response_item.Besaetning, "ChrNummer"):
                        chr_number = response_item.Besaetning.ChrNummer
                        if chr_number:  # Ensure CHR number is not None or 0
                            species_code = task[3]  # Get species code directly from the task
                            if chr_number not in context["chr_to_species"]:
                                context["chr_to_species"][chr_number] = set()
                            context["chr_to_species"][chr_number].add(species_code)

        if context["args"]["progress"]:
            logging.info(
                f"Processed {len(context['herd_details'])} herd details, found {len(context['chr_to_species'])} unique CHR numbers"
            )

    elif step == "diko":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'diko' step without first running 'herds'")

        diko_tasks = [
            (context["clients"]["diko"], context["username"], herd_num, species_code)
            for herd_num, species_code in context["herd_to_species"].items()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(diko_tasks)} DIKO tasks")

        results = process_parallel(
            load_diko_flytninger, diko_tasks, context["args"]["workers"], "Processing DIKO tasks"
        )
        context["diko_results"] = results  # Keep results in context for potential future use or export

        if context["args"]["progress"]:
            successful = sum(1 for r in results if r)
            logging.info(f"Completed DIKO tasks. Success: {successful}/{len(diko_tasks)}")

    elif step == "ejendom":
        if "chr_to_species" not in context:
            raise ValueError("Cannot run 'ejendom' step without first running 'herd_details'")

        ejendom_tasks = [
            (context["clients"]["ejendom"], context["username"], chr_num)
            for chr_num in context["chr_to_species"].keys()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(ejendom_tasks)} ejendom tasks")

        # Run both ejendom operations
        oplysninger_results = process_parallel(
            load_ejendom_oplysninger, ejendom_tasks, context["args"]["workers"], "Processing Ejendom Oplysninger"
        )
        vet_events_results = process_parallel(
            load_ejendom_vet_events, ejendom_tasks, context["args"]["workers"], "Processing Ejendom Vet Events"
        )
        # Results are stored in the buffer by the load functions

        if context["args"]["progress"]:
            successful_opl = sum(1 for r in oplysninger_results if r)
            successful_vet = sum(1 for r in vet_events_results if r)
            logging.info(
                f"Completed Ejendom tasks. Oplysninger success: {successful_opl}/{len(ejendom_tasks)}, Vet events success: {successful_vet}/{len(ejendom_tasks)}"
            )

    elif step == "animal_movements":
        if "herd_to_species" not in context:
            raise ValueError("Cannot run 'animal_movements' step without first running 'herds'")

        # Filter for cattle herds only (species code 12) - USE SMART CHR_DYR AGGREGATION
        cattle_herds = {
            herd_num: species_code
            for herd_num, species_code in context["herd_to_species"].items()
            if species_code == 12
        }

        if not cattle_herds:
            logging.warning("No cattle herds (species code 12) found for animal movements")
            context["animal_movements_results"] = []
            return context

        # Import the smart aggregation function
        from bronze.load_chr_dyr import create_soap_client as create_chr_dyr_client
        from bronze.load_chr_dyr import load_cattle_movement_summaries

        # Create CHR_dyr client for smart aggregation
        if "chr_dyr" not in context["clients"]:
            # Get credentials
            username, password = get_fvm_credentials()
            context["clients"]["chr_dyr"] = create_chr_dyr_client(
                "https://ws.fvst.dk/service/CHR_dyrWS?wsdl", username, password
            )

        # Create smart aggregation tasks for cattle herds
        cattle_movement_tasks = [
            (
                context["clients"]["chr_dyr"],
                context["username"],
                herd_num,
                context["args"]["start_date"],
                context["args"]["end_date"],
            )
            for herd_num, species_code in cattle_herds.items()
        ]

        if context["args"]["progress"]:
            logging.info(f"Processing {len(cattle_movement_tasks)} cattle herds with smart aggregation")

            # Load and report problematic herds at the start
            try:
                from bronze.load_chr_dyr import _load_problematic_herds, is_problematic_herd

                _load_problematic_herds()  # Force load to get current count

                problematic_count = sum(1 for herd_num, _ in cattle_herds.items() if is_problematic_herd(herd_num))
                if problematic_count > 0:
                    logging.warning(f"Found {problematic_count} problematic herds that will be skipped")
                else:
                    logging.info("No problematic herds found - all herds will be processed")
            except Exception as e:
                logging.debug(f"Could not check problematic herds: {e}")

        # Use the smart aggregation function that processes individual records but stores summaries
        def smart_cattle_task(client, username, herd_num, start_date, end_date):
            return load_cattle_movement_summaries(client, username, herd_num, start_date, end_date)

        # Process in chunks to avoid overwhelming GitHub Actions runner
        chunk_size = 25  # Reduced from 50 to 25 for better progress tracking and memory management
        total_chunks = (len(cattle_movement_tasks) + chunk_size - 1) // chunk_size

        # Track overall statistics (no result accumulation to prevent memory issues)
        total_successful = 0
        total_movements = 0
        processed_herds = []  # Only track herd numbers for context, not full results
        failed_herds = []  # Track failed herds for retry or exclusion

        for chunk_idx in range(total_chunks):
            start_idx = chunk_idx * chunk_size
            end_idx = min(start_idx + chunk_size, len(cattle_movement_tasks))
            chunk_tasks = cattle_movement_tasks[start_idx:end_idx]

            if context["args"]["progress"]:
                logging.info(f"Processing chunk {chunk_idx + 1}/{total_chunks} ({len(chunk_tasks)} herds)")
                # Log the herd numbers in this chunk for debugging
                herd_numbers_in_chunk = [task[2] for task in chunk_tasks]
                logging.info(f"Chunk {chunk_idx + 1} herd numbers: {herd_numbers_in_chunk}")

            # Track timing for performance monitoring
            chunk_start_time = time.time()

            try:
                chunk_results = process_parallel(
                    smart_cattle_task,
                    chunk_tasks,
                    context["args"]["workers"],
                    f"Processing Smart Cattle Movements (Chunk {chunk_idx + 1}/{total_chunks})",
                )
            except Exception as e:
                logging.error(f"Chunk {chunk_idx + 1} failed with error: {e}")
                # Create empty results for failed chunk
                chunk_results = [None] * len(chunk_tasks)

            chunk_duration = time.time() - chunk_start_time

            # Track only herd numbers for context (no result accumulation to prevent memory issues)
            # Full data is already saved to storage by load_cattle_movement_summaries
            successful_herd_numbers = []
            failed_herd_numbers = []

            for i, result in enumerate(chunk_results):
                herd_number = chunk_tasks[i][2]  # Extract herd number from task

                if result and result.get("processed_successfully", False):
                    successful_herd_numbers.append(result.get("reporting_herd_number", herd_number))
                else:
                    failed_herd_numbers.append(herd_number)

            processed_herds.extend(successful_herd_numbers)
            failed_herds.extend(failed_herd_numbers)

            # Update totals
            total_successful += sum(1 for r in chunk_results if r and r.get("processed_successfully", False))
            total_movements += sum(
                r.get("movement_count", 0) for r in chunk_results if r and r.get("processed_successfully", False)
            )

            # Log progress after each chunk with timing information
            if context["args"]["progress"]:
                success_rate = len(successful_herd_numbers) / len(chunk_tasks) * 100 if chunk_tasks else 0
                avg_time_per_herd = chunk_duration / len(chunk_tasks) if chunk_tasks else 0

                logging.info(
                    f"Chunk {chunk_idx + 1} completed: {len(successful_herd_numbers)}/{len(chunk_tasks)} successful ({success_rate:.1f}%), "
                    f"duration: {chunk_duration:.1f}s (avg {avg_time_per_herd:.1f}s/herd)"
                )
                logging.info(
                    f"Overall progress: {total_successful}/{len(cattle_movement_tasks)} herds ({total_successful / len(cattle_movement_tasks) * 100:.1f}%), {total_movements} total movements"
                )

                if failed_herd_numbers:
                    logging.warning(f"Failed herds in chunk {chunk_idx + 1}: {failed_herd_numbers}")

            # Progressive cleanup: Clear chunk results from memory after processing
            # (Individual herd data is already saved directly to storage by load_cattle_movement_summaries)
            del chunk_results
            del successful_herd_numbers
            del failed_herd_numbers

            # Force garbage collection and memory monitoring for large datasets
            import gc

            if chunk_idx % 2 == 0:  # Every 2 chunks (reduced from 3)
                gc.collect()

                # Log memory usage for monitoring
                if context["args"]["progress"]:
                    try:
                        import psutil

                        process = psutil.Process(os.getpid())
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        logging.info(f"Memory usage after chunk {chunk_idx + 1}: {memory_mb:.1f} MB")

                        # Warning if memory usage is getting high (updated for 16GB GitHub Actions runners)
                        if memory_mb > 8000:  # 8GB warning (50% of 16GB limit)
                            logging.warning(f"High memory usage detected: {memory_mb:.1f} MB")
                        elif memory_mb > 12000:  # 12GB critical (75% of 16GB limit)
                            logging.error(
                                f"Critical memory usage: {memory_mb:.1f} MB - approaching GitHub Actions limit"
                            )

                    except Exception:
                        pass  # Ignore if psutil is not available

            # CRITICAL: More aggressive cleanup every chunk to prevent accumulation
            # Clear any lingering temporary files from /tmp
            if chunk_idx % 3 == 0:  # Every 3 chunks
                try:
                    import glob

                    temp_files = glob.glob("/tmp/chr_streaming_*")
                    for temp_file in temp_files:
                        try:
                            if os.path.exists(temp_file):
                                os.unlink(temp_file)
                                logging.debug(f"Cleaned up orphaned temp file: {temp_file}")
                        except Exception as e:
                            logging.debug(f"Could not remove temp file {temp_file}: {e}")
                except Exception as e:
                    logging.debug(f"Error during temp file cleanup: {e}")

            # Force aggressive garbage collection for memory-constrained environments
            if os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("MEMORY_CONSTRAINED") == "true":
                gc.collect()
                gc.collect()  # Double collection for aggressive cleanup

        # Store only essential summary data for context (no full results to prevent memory issues)
        context["animal_movements_results"] = {
            "total_successful": total_successful,
            "total_movements": total_movements,
            "processed_herd_count": len(processed_herds),
            "failed_herd_count": len(failed_herds),
            "processed_herds_sample": processed_herds[:10],  # Only keep first 10 for debugging
            "failed_herds_sample": failed_herds[:10] if failed_herds else [],  # Track failed herds
            "processing_completed": True,
        }

        # Finalize streaming files to flush any remaining data
        try:
            from bronze.load_chr_dyr import _finalize_streaming_files

            _finalize_streaming_files()
            if context["args"]["progress"]:
                logging.info("✅ Finalized streaming cattle movement files")
        except Exception as e:
            logging.warning(f"Error finalizing streaming files: {e}")

        if context["args"]["progress"]:
            logging.info(
                f"Completed Smart Cattle Movement tasks. Success: {total_successful}/{len(cattle_movement_tasks)}, Total movement summaries: {total_movements}"
            )

            # Save final problematic herds list
            try:
                from bronze.load_chr_dyr import _save_problematic_herds

                _save_problematic_herds()
                logging.info("Saved updated problematic herds list to persistent storage")
            except Exception as e:
                logging.warning(f"Could not save problematic herds: {e}")

    elif step == "vetstat":
        if "chr_to_species" not in context:
            raise ValueError("Cannot run 'vetstat' step without first running 'herd_details'")

        vetstat_tasks = [
            (chr_num, species, context["args"]["start_date"], context["args"]["end_date"])
            for chr_num, species_set in context["chr_to_species"].items()
            for species in species_set
        ]

        if not vetstat_tasks:
            logging.warning("No valid CHR number and species code combinations found for VetStat data")
        elif context["args"]["progress"]:
            logging.info(f"Processing {len(vetstat_tasks)} VetStat tasks")

        try:
            results = process_parallel(
                load_vetstat_antibiotics, vetstat_tasks, context["args"]["workers"], "Processing VetStat tasks"
            )
            # Results are stored in the buffer by the load function
            if context["args"]["progress"]:
                successful = sum(1 for r in results if r)  # Check if results were returned (even if empty)
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
            results = asyncio.run(load_spf_su_data(pig_herd_numbers, max_workers=5))
            context["spf_su_results"] = results
            if context["args"]["progress"]:
                successful = len([r for r in results if r])
                logging.info(f"Completed SPF-SU tasks. Success: {successful}/{len(pig_herd_numbers)}")
        except Exception as e:
            logging.error(f"Error processing SPF-SU tasks: {e}", exc_info=True)
            raise

    else:
        logger.warning(f"Attempted to run unknown bronze step: {step}")

    return context


def main():
    """Main pipeline execution."""
    # Record start time for execution tracking
    start_time = datetime.now()

    args = parse_args()
    setup_logging(args["log_level"])

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

            # Check if we have bronze data files available
            bronze_dir_override = os.getenv("BRONZE_DATE_FOLDER_OVERRIDE")
            has_bronze_files = False

            # PRIORITY 1: Check override directory first (most reliable)
            if bronze_dir_override:
                bronze_path = config.BRONZE_BASE_DIR / bronze_dir_override
                if bronze_path.exists() and any(bronze_path.glob("*.json")):
                    has_bronze_files = True
                    logging.warning(f"Found bronze files in override directory: {bronze_path}")
                else:
                    logging.warning(f"Override directory specified but no files found: {bronze_path}")

                    # GITHUB ACTIONS FIX: Try alternative path if in GitHub Actions
                    if os.getenv("GITHUB_ACTIONS") == "true":
                        # Try the standard GitHub Actions data path as fallback
                        fallback_bronze_path = Path("/tmp/data/bronze/chr") / bronze_dir_override
                        if fallback_bronze_path.exists() and any(fallback_bronze_path.glob("*.json")):
                            has_bronze_files = True
                            logging.warning(
                                f"Found bronze files in GitHub Actions fallback path: {fallback_bronze_path}"
                            )
                            # Update the config to use the correct path for later processing
                            config.BRONZE_BASE_DIR = Path("/tmp/data/bronze/chr")
                        else:
                            logging.warning(f"GitHub Actions fallback path also not found: {fallback_bronze_path}")

            # PRIORITY 2: Check current export timestamp directory
            if not has_bronze_files:
                bronze_path = config.BRONZE_BASE_DIR / EXPORT_TIMESTAMP
                if bronze_path.exists() and any(bronze_path.glob("*.json")):
                    has_bronze_files = True
                    logging.warning(f"Found bronze files in current timestamp directory: {bronze_path}")

            # PRIORITY 3: Check any timestamped subdirectories (fallback)
            if not has_bronze_files and config.BRONZE_BASE_DIR.exists():
                for subdir in config.BRONZE_BASE_DIR.iterdir():
                    if subdir.is_dir() and any(subdir.glob("*.json")):
                        has_bronze_files = True
                        logging.warning(f"Found bronze files in fallback directory: {subdir}")
                        break

            if has_buffer_data or has_context_import or has_bronze_files:
                logging.warning(
                    f"Silver-only operation detected with existing data. Buffer: {has_buffer_data}, Context: {has_context_import}, Files: {has_bronze_files}, Override: {bronze_dir_override}"
                )
                needs_fvm_credentials = False
            else:
                logging.warning(
                    f"Silver-only operation but no existing data found. Will need to run bronze steps. Buffer: {has_buffer_data}, Context: {has_context_import}, Files: {has_bronze_files}, Override: {bronze_dir_override}"
                )

        # Initialize context based on whether we need FVM credentials
        if context_import_path and os.path.exists(context_import_path):
            logging.warning(f"Importing context data from {context_import_path}")
            imported_context = import_context_data(Path(context_import_path))

            if needs_fvm_credentials:
                # Initialize context with imported data and FVM credentials
                username, password = get_fvm_credentials()
                context = {
                    "args": args,
                    "username": username,
                    "clients": {
                        "stamdata": create_stamdata_client(STAMDATA_ENDPOINTS["stamdata"], username, password),
                        "besaetning": create_bes_client(BES_ENDPOINTS["besaetning"], username, password),
                        "ejendom": create_ejd_client(EJD_ENDPOINTS["ejendom"], username, password),
                        "diko": create_diko_client(DIKO_ENDPOINTS["diko"], username, password),
                    },
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
                username, password = get_fvm_credentials()
                context = {
                    "args": args,
                    "username": username,
                    "clients": {
                        "stamdata": create_stamdata_client(STAMDATA_ENDPOINTS["stamdata"], username, password),
                        "besaetning": create_bes_client(BES_ENDPOINTS["besaetning"], username, password),
                        "ejendom": create_ejd_client(EJD_ENDPOINTS["ejendom"], username, password),
                        "diko": create_diko_client(DIKO_ENDPOINTS["diko"], username, password),
                    },
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
                "animal_movements",
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
        elif args.get("skip_dependencies", False):
            # Skip dependencies - only run the specific step (for parallel job execution)
            bronze_steps_to_run = [requested_step]
            run_silver = False
        else:
            # Only run requested bronze step and its prerequisites
            bronze_steps_to_run = get_required_steps(requested_step) + [requested_step]
            run_silver = False

        # Ensure unique bronze steps in order
        unique_bronze_steps = []
        for step in ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat", "spf_su"]:
            if step in bronze_steps_to_run and step not in unique_bronze_steps:
                unique_bronze_steps.append(step)

        # Run bronze steps sequentially (only if we have any to run)
        if unique_bronze_steps:
            logging.warning(f"Running bronze steps: {', '.join(unique_bronze_steps)}")
            for step in unique_bronze_steps:
                context = run_bronze_step(step, context)
            logging.warning("Bronze steps completed.")
        else:
            logging.warning("No bronze steps to run - proceeding with silver processing using existing data.")

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

                if buffer_data:
                    # Use in-memory data
                    logging.warning("Using in-memory buffer data for silver processing")
                    run_silver_processing(in_memory_data=buffer_data, silver_dir=silver_dir)
                elif bronze_dir_override:
                    # Use bronze files
                    bronze_path = config.BRONZE_BASE_DIR / bronze_dir_override
                    logging.warning(f"Using bronze files from {bronze_path} for silver processing")
                    run_silver_processing(
                        bronze_dir=bronze_path, silver_dir=silver_dir, export_timestamp=bronze_dir_override
                    )
                else:
                    # Fallback to buffer (might be empty, but let silver processing handle it)
                    logging.warning("No specific bronze data source found, using buffer")
                    run_silver_processing(in_memory_data=buffer_data, silver_dir=silver_dir)

                logging.warning(f"Silver processing completed. Output in: {silver_dir}")
            except Exception as e:
                logging.error(f"Silver processing failed: {e}", exc_info=True)
                raise  # Re-raise to indicate pipeline failure

        # Finalize bronze export (only if we have bronze data to export)
        if unique_bronze_steps or get_data_buffer():
            # But don't clear buffer if we're exporting context (dependent jobs might need the data)
            clear_buffer_after_export = not bool(context_export_path)
            logging.warning("Finalizing bronze export...")

            # Add debugging info about buffer size before export
            buffer_data = get_data_buffer()
            if buffer_data:
                total_records = sum(
                    len(data.get("json", [])) + len(data.get("xml", [])) for data in buffer_data.values()
                )
                logging.warning(f"Buffer contains {total_records} total records across {len(buffer_data)} data types")
                for key, data in buffer_data.items():
                    json_count = len(data.get("json", []))
                    xml_count = len(data.get("xml", []))
                    logging.warning(f"  {key}: {json_count} JSON records, {xml_count} XML records")

            finalize_export(clear_buffer=clear_buffer_after_export)
            logging.warning("Bronze export finalization completed.")
        else:
            logging.warning("No bronze data to export - skipping bronze export finalization.")

        # --- Final Cleanup and Summary ---
        logger.info("=== CHR Pipeline Execution Complete ===")
        logger.info(f"Total execution time: {datetime.now() - start_time}")

        # Run comprehensive cleanup
        try:
            from .cleanup_temp_files import cleanup_temp_files, monitor_disk_usage

            logger.info("Running final cleanup of temporary files...")
            monitor_disk_usage()
            cleaned_files, total_size = cleanup_temp_files()
            if cleaned_files > 0:
                logger.info(f"Final cleanup: {cleaned_files} files removed, {total_size / (1024 * 1024):.2f} MB freed")
            monitor_disk_usage()
        except Exception as e:
            logger.warning(f"Error during final cleanup: {e}")

        # Force final garbage collection
        import gc

        gc.collect()

        logger.info("CHR Pipeline finished successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
