# backend/chr_pipeline/main.py
"""CHR Pipeline for fetching and processing data."""

import argparse
import concurrent.futures
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from bronze.export import EXPORT_TIMESTAMP, finalize_export, get_data_buffer
from bronze.load_besaetning import ENDPOINTS as BES_ENDPOINTS
from bronze.load_besaetning import create_soap_client as create_bes_client
from bronze.load_besaetning import get_fvm_credentials, load_herd_details, load_herd_list
from bronze.load_chr_dyr import ENDPOINTS as CHR_DYR_ENDPOINTS
from bronze.load_chr_dyr import create_soap_client as create_chr_dyr_client
from bronze.load_chr_dyr import load_animal_movements_task
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
from silver import config

# Import silver processing orchestrator
from silver.chr_silver_processing import process_chr_data as run_silver_processing
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Remove all existing handlers to start fresh
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
    start_date_def, end_date_def = get_default_dates()

    parser = argparse.ArgumentParser(description="Run the CHR Data Pipeline.")
    parser.add_argument(
        "--steps",
        type=str,
        default="all",
        choices=[
            "all",
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "ejendom",
            "animal_movements",
            "vetstat",
            "silver_all",  # Only silver steps
            "silver_vet_practices",
            "silver_properties",
            "silver_herds",
            "silver_herd_sizes",
            "silver_animal_movements",
            "silver_chr_dyr_animal_movements",
            "silver_property_vet_events",
            "silver_antibiotic_usage",
        ],
        help="Pipeline steps to run (bronze steps run sequentially before silver)",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=start_date_def,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=end_date_def,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")
    parser.add_argument("--test-species-codes", type=str, help='Comma-separated species codes (e.g., "12,13,14,15")')
    parser.add_argument("--limit-total-herds", type=int, help="Limit total number of herds processed")
    parser.add_argument("--limit-herds-per-species", type=int, help="Limit number of herds processed per species")
    parser.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="WARNING", help="Logging level"
    )
    parser.add_argument("--progress", action="store_true", help="Show progress information")

    args = parser.parse_args()

    # Add validation for mutually exclusive arguments
    if args.limit_total_herds is not None and args.limit_herds_per_species is not None:
        parser.error("Cannot specify both --limit-total-herds and --limit-herds-per-species")

    # Convert test species codes to list if provided
    if args.test_species_codes:
        args.test_species_codes = [int(code.strip()) for code in args.test_species_codes.split(",")]

    return vars(args)


def fetch_stamdata(client: Any, username: str, test_species_codes: Optional[List[int]] = None) -> List[Dict]:
    """Fetch and parse species/usage combinations."""
    logger.info("Fetching species/usage combinations...")
    response = load_species_usage_combinations(client, username)

    if not response or not hasattr(response, "Response"):
        logger.error("Invalid or empty Stamdata response")
        return []

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

                        # Add herd if not already seen (herd_to_species ensures uniqueness across all species)
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

                # Update start number for next page
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

            # Create progress bar that works in both CI and interactive environments
            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc=desc or func.__name__,
                unit="tasks",
                mininterval=1.0,  # Update at most once per second
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            ):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    results.append(None)

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
        # Silver steps - all depend on all bronze steps implicitly
        "silver_vet_practices": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"],
        "silver_properties": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"],
        "silver_herds": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"],
        "silver_herd_sizes": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"],
        "silver_animal_movements": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
        ],
        "silver_property_vet_events": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
        ],
        "silver_antibiotic_usage": [
            "stamdata",
            "herds",
            "herd_details",
            "diko",
            "animal_movements",
            "ejendom",
            "vetstat",
        ],
        "silver_all": ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"],
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

        # Filter for cattle herds only (species code 12)
        cattle_herds = {
            herd_num: species_code
            for herd_num, species_code in context["herd_to_species"].items()
            if species_code == 12
        }

        if not cattle_herds:
            logging.warning("No cattle herds (species code 12) found for animal movements")
            context["animal_movements_results"] = []
            return context

        # Create CHR_dyr client
        if "chr_dyr" not in context["clients"]:
            # Get credentials
            username, password = get_fvm_credentials()
            context["clients"]["chr_dyr"] = create_chr_dyr_client(CHR_DYR_ENDPOINTS["chr_dyr"], username, password)

        # Create tasks for cattle herds only
        chr_dyr_tasks = [
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
            logging.info(f"Processing {len(chr_dyr_tasks)} animal movement tasks for cattle herds")

        results = process_parallel(
            load_animal_movements_task, chr_dyr_tasks, context["args"]["workers"], "Processing Animal Movements"
        )
        context["animal_movements_results"] = results

        if context["args"]["progress"]:
            successful = sum(1 for r in results if r)
            logging.info(f"Completed Animal Movement tasks. Success: {successful}/{len(chr_dyr_tasks)}")

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
    else:
        logger.warning(f"Attempted to run unknown bronze step: {step}")

    return context


def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args["log_level"])

    try:
        # Initialize context
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

        # Determine steps to run
        requested_step = args["steps"]
        if requested_step == "all":
            bronze_steps_to_run = [
                "stamdata",
                "herds",
                "herd_details",
                "diko",
                "animal_movements",
                "ejendom",
                "vetstat",
            ]
            run_silver = True
        elif requested_step.startswith("silver_"):
            # If a silver step is requested, run all bronze prerequisites
            bronze_steps_to_run = get_required_steps(requested_step)
            run_silver = True
        else:
            # Only run requested bronze step and its prerequisites
            bronze_steps_to_run = get_required_steps(requested_step) + [requested_step]
            run_silver = False

        # Ensure unique bronze steps in order
        unique_bronze_steps = []
        for step in ["stamdata", "herds", "herd_details", "diko", "animal_movements", "ejendom", "vetstat"]:
            if step in bronze_steps_to_run and step not in unique_bronze_steps:
                unique_bronze_steps.append(step)

        # Run bronze steps sequentially
        logging.warning(f"Running bronze steps: {', '.join(unique_bronze_steps)}")
        for step in unique_bronze_steps:
            context = run_bronze_step(step, context)
        logging.warning("Bronze steps completed.")

        # Run silver processing if requested
        if run_silver:
            logging.warning("Starting silver processing...")
            try:
                # Determine silver output directory
                silver_dir = config.SILVER_BASE_DIR / EXPORT_TIMESTAMP
                silver_dir.mkdir(parents=True, exist_ok=True)  # Ensure it exists

                # Call the consolidated silver processing function
                run_silver_processing(in_memory_data=get_data_buffer(), silver_dir=silver_dir)
                logging.warning(f"Silver processing completed. Output in: {silver_dir}")
            except Exception as e:
                logging.error(f"Silver processing failed: {e}", exc_info=True)
                raise  # Re-raise to indicate pipeline failure

        # Finalize bronze export (always run this to save fetched bronze data)
        logging.warning("Finalizing bronze export...")
        finalize_export(clear_buffer=True)  # Clear buffer after potentially being used by silver

        logging.warning(f"Pipeline run for steps '{requested_step}' completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
