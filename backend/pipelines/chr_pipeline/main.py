# backend/chr_pipeline/main.py
"""Main module for CHR data pipeline."""

import logging
import argparse
from datetime import date, timedelta, datetime
import concurrent.futures
import os
from dotenv import load_dotenv

# Load environment variables at the start
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import functions from Bronze layer modules
from bronze.load_stamdata import load_species_usage_combinations, create_soap_client as create_stamdata_client, ENDPOINTS as STAMDATA_ENDPOINTS
from bronze.load_besaetning import load_herd_list, load_herd_details, get_fvm_credentials as get_bes_creds, create_soap_client as create_bes_client, ENDPOINTS as BES_ENDPOINTS
from bronze.load_ejendom import load_ejendom_oplysninger, load_ejendom_vet_events, get_fvm_credentials as get_ejd_creds, create_soap_client as create_ejd_client, ENDPOINTS as EJD_ENDPOINTS
from bronze.load_vetstat import load_vetstat_antibiotics
from bronze.load_diko import create_soap_client as create_diko_client, load_diko_flytninger, ENDPOINTS as DIKO_ENDPOINTS, VALID_DIKO_SPECIES
from bronze.export import finalize_export

# --- Helper function for parallel Herd Detail fetching ---
def process_single_herd_details(task_data):
    """Worker function to fetch details for a single herd."""
    besaetning_client, username, herd_number, species_code = task_data
    chr_number = None # Default to None
    try:
        # Fetch raw Zeep response object
        raw_response_object = load_herd_details(besaetning_client, username, herd_number, species_code)

        # Attempt to parse CHR Nummer using attribute access on the Zeep object
        if raw_response_object:
            try:
                # Navigate the Zeep object structure
                # Path based on common patterns, might need adjustment if structure differs
                response_body = getattr(raw_response_object, 'Response', None)
                if response_body and isinstance(response_body, list) and len(response_body) > 0:
                     besaetning_info = getattr(response_body[0], 'Besaetning', None)
                     if besaetning_info:
                         # --- CORRECTED LOGIC: Get ChrNummer directly from Besaetning --- 
                         chr_number_str = getattr(besaetning_info, 'ChrNummer', None)
                         if chr_number_str:
                             try:
                                 chr_number = int(chr_number_str) # Convert to int
                                 if chr_number <= 0:
                                     logger.warning(f"  Extracted non-positive CHR number ({chr_number}) for herd {herd_number}")
                                     chr_number = None # Treat as invalid
                             except (ValueError, TypeError):
                                 logger.warning(f"  Could not convert extracted CHR number '{chr_number_str}' to int for herd {herd_number}")
                                 chr_number = None
                         # --- End CORRECTED LOGIC ---

                         # --- OLD LOGIC (Incorrect) ---
                         # ejendom_list = getattr(besaetning_info, 'Ejendom', [])
                         # if ejendom_list and isinstance(ejendom_list, list) and len(ejendom_list) > 0:
                         #     # Get CHRNummer from the first Ejendom in the list
                         #     chr_number_str = getattr(ejendom_list[0], 'CHRNummer', None)
                         #     if chr_number_str:
                         #         try:
                         #             chr_number = int(chr_number_str) # Convert to int
                         #             if chr_number <= 0:
                         #                 logger.warning(f"  Extracted non-positive CHR number ({chr_number}) for herd {herd_number}")
                         #                 chr_number = None # Treat as invalid
                         #         except (ValueError, TypeError):
                         #             logger.warning(f"  Could not convert extracted CHR number '{chr_number_str}' to int for herd {herd_number}")
                         #             chr_number = None
                         #     # else: CHRNummer attribute missing from Ejendom
                         # # else: Ejendom list is empty or missing
                         # --- End OLD LOGIC ---

                     # else: Besaetning attribute missing from Response
                # else: Response element missing or not a list

                if chr_number is None:
                     # Log only if we expected to find it but failed at some step above
                     # REMOVED VERBOSE LOGGING of full response
                     logger.warning(f"  Could not extract valid CHR number for herd {herd_number} (Species: {species_code}). Response structure might be unexpected.")

            except (AttributeError, IndexError, TypeError) as parse_exc:
                 # Catch errors during navigation/parsing
                 # REMOVED VERBOSE LOGGING of full response
                 logger.warning(f"  Error parsing CHR number from details for herd {herd_number} (Species: {species_code}): {parse_exc}.", exc_info=False)
                 chr_number = None # Ensure it's None on parsing error

        # Return consistent tuple format
        return herd_number, species_code, chr_number

    except Exception as exc:
        # Catch broader errors during the API call itself or unexpected issues
        logger.error(f"  Error fetching/processing details for herd {herd_number} (Species: {species_code}): {exc}", exc_info=True) # Log full traceback here
        # Return consistent tuple format even on broader failure
        return herd_number, species_code, None # Return None for CHR number on error

# --- Helper function for parallel Ejendom Detail fetching ---
def process_single_ejendom_details(task_args):
    ejendom_client, username, chr_number = task_args
    logger.debug(f"Fetching ejendom data for CHR: {chr_number}...")
    try:
        # These calls internally save the raw data via export.py
        load_ejendom_oplysninger(ejendom_client, username, chr_number)
        load_ejendom_vet_events(ejendom_client, username, chr_number)
        return True
    except Exception as e:
        logger.error(f"  Error loading ejendom details for CHR {chr_number}: {e}", exc_info=False)
        return False

# --- Helper function for parallel VetStat fetching ---
def process_single_vetstat(task_args):
    chr_number, species_code, start_date, end_date = task_args
    logger.debug(f"Fetching VetStat for CHR: {chr_number}, Species: {species_code}...")
    try:
        vetstat_response = load_vetstat_antibiotics(chr_number, species_code, start_date, end_date)
        return vetstat_response is not None # Return True if response received, False otherwise
    except Exception as e:
        logger.error(f"  Error loading VetStat data for CHR {chr_number}, Species {species_code}: {e}", exc_info=False)
        return False

# --- Helper function for parallel DIKO fetching ---
def process_single_diko(task_data):
    """Worker function to fetch DIKO movements for a single herd/species."""
    diko_client, username, herd_number, species_code = task_data
    try:
        # Define which species DIKO actually supports (e.g., not pigs)
        # For now, let's try calling it for all and let the API return errors if needed.
        # supported_species = {12, 13, 14} # Example: Cattle, Sheep, Goats
        # if species_code not in supported_species:
        #     logger.debug(f"Skipping DIKO for unsupported species {species_code} (Herd: {herd_number})")
        #     return False # Indicate skipped

        logger.debug(f"Fetching DIKO movements for Herd: {herd_number}, Species: {species_code}...")
        response = load_diko_flytninger(diko_client, username, herd_number, species_code)
        return response is not None # Return True if response received, False otherwise
    except Exception as e:
        logger.error(f"  Error loading DIKO movements for Herd: {herd_number}, Species: {species_code}: {e}", exc_info=False)
    return False # Indicate failure


def parse_stamdata(stamdata_response) -> list:
    """Parses the ListDyrearterMedBrugsarter response into a list of dicts."""
    combinations = []
    if not stamdata_response or not hasattr(stamdata_response, 'Response') or not stamdata_response.Response:
        logger.warning("Stamdata response is empty or invalid.")
        return combinations

    # Zeep usually returns a list directly if there are multiple items
    # Ensure it's a list even if only one item is returned
    response_list = stamdata_response.Response
    if not isinstance(response_list, list):
        response_list = [response_list]

    for combo in response_list:
        try:
            species_code = int(getattr(combo, 'DyreArtKode', None))
            usage_code = int(getattr(combo, 'BrugsArtKode', None))
            species_text = str(getattr(combo, 'DyreArtTekst', ''))
            usage_text = str(getattr(combo, 'BrugsArtTekst', ''))

            if species_code is not None and usage_code is not None:
                combinations.append({
                    'species_code': species_code,
                    'species_text': species_text,
                    'usage_code': usage_code,
                    'usage_text': usage_text
                })
        except (TypeError, ValueError, AttributeError) as e:
            logger.warning(f"Skipping invalid stamdata combination: {combo}. Error: {e}")
            continue
    return combinations


def main(args):
    """Main orchestration function for the CHR Bronze pipeline."""

    # --- Configure Logging Level ---
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info(f"Log level set to: {args.log_level.upper()}")

    logger.info("--- Starting CHR Bronze Pipeline ---")

    # --- Central Credential/Client Management ---
    try:
        logger.info("Fetching FVM credentials...")
        username, password = get_bes_creds()
        logger.info("Credentials fetched.")

        logger.info("Creating SOAP clients...")
        stamdata_client = create_stamdata_client(STAMDATA_ENDPOINTS['stamdata'], username, password)
        besaetning_client = create_bes_client(BES_ENDPOINTS['besaetning'], username, password)
        ejendom_client = create_ejd_client(EJD_ENDPOINTS['ejendom'], username, password)
        diko_client = create_diko_client(DIKO_ENDPOINTS['diko'], username, password)
        logger.info("SOAP clients created.")

    except Exception as e:
        logger.error(f"Failed to get credentials or create clients: {e}", exc_info=True)
        return

    # --- 1. Load Stamdata (Species/Usage Combinations) ---
    logger.info("Step 1: Loading Species/Usage Combinations...")
    combinations = []
    try:
        stamdata_response = load_species_usage_combinations(stamdata_client, username)
        if stamdata_response:
            combinations = parse_stamdata(stamdata_response)
            logger.info(f"Successfully parsed {len(combinations)} species/usage combinations from Stamdata.")
        else:
             logger.error("Failed to retrieve Stamdata. Exiting.")
             return
        if not combinations:
            logger.error("No valid combinations parsed from Stamdata. Exiting.")
            return
    except Exception as e:
        logger.error(f"Error loading or parsing Stamdata: {e}", exc_info=True)
        return

    # --- Filter combinations if test species codes are provided ---
    if args.test_species_codes:
        test_codes = [int(code.strip()) for code in args.test_species_codes.split(",")]
        combinations = [c for c in combinations if c['species_code'] in test_codes]
        logger.info(f"Filtered to {len(combinations)} combinations matching test species codes {test_codes}")

    # --- Apply limit on combinations if specified ---
    if args.limit_combinations is not None and args.limit_combinations > 0:
        logger.warning(f"Limiting processing to the first {args.limit_combinations} combinations.")
        combinations = combinations[:args.limit_combinations]
        if not combinations:
             logger.error("Combination limit resulted in an empty list. Exiting.")
             return

    # --- Data Structures for Tracking ---
    # Map herd number to its associated species code
    herd_to_species_map = {}
    # Map CHR number to a set of (herd_number, species_code) tuples it came from
    chr_to_source_map = {}

    # --- 2. Load ALL Herd Numbers (with Pagination) ---
    logger.info("Step 2: Loading ALL Herd Numbers for each combination (with pagination)...")
    total_herds_found = 0
    limit_total_herds_reached = False # Flag to break outer loop
    for i, combo in enumerate(combinations):
        species_code = combo['species_code']
        usage_code = combo['usage_code']
        logger.info(f" -> [{i+1}/{len(combinations)}] Fetching herds for Species: {species_code}, Usage: {usage_code}...")

        herds_for_combo = set()
        start_herd_number = 0 # API uses 0 or None for the first page
        page = 1
        max_pages = 1000 # Safety break for potential infinite loops

        while page <= max_pages:
            logger.debug(f"    Fetching page {page} starting from herd {start_herd_number}")
            try:
                # Call the updated load_herd_list which now returns parsed data
                herd_list, has_more, last_herd_in_batch = load_herd_list(
                    besaetning_client,
                    username,
                    species_code,
                    usage_code,
                    start_herd_number=start_herd_number if start_herd_number > 0 else None
                )

                if not herd_list and not has_more and last_herd_in_batch is None:
                    # This likely indicates an error from load_herd_list or no herds found
                    logger.warning(f"    No herds found or error occurred on page {page}. Stopping pagination.")
                    break

                new_herds_on_page = 0
                for herd_num in herd_list:
                    if herd_num not in herds_for_combo:
                        herds_for_combo.add(herd_num)
                        new_herds_on_page += 1

                logger.debug(f"    Page {page}: Found {new_herds_on_page} new herds from list of {len(herd_list)}. Last herd in batch: {last_herd_in_batch}. Has More: {has_more}")

                if not has_more:
                    logger.debug("    API indicates no more herds. Stopping pagination.")
                    break

                if last_herd_in_batch is None or last_herd_in_batch <= start_herd_number:
                    logger.warning(f"    Pagination logic error: last_herd_in_batch ({last_herd_in_batch}) is not greater than start_herd_number ({start_herd_number}). Stopping.")
                    break

                # Prepare for next page
                start_herd_number = last_herd_in_batch + 1
                page += 1
                # Optional: Add a small delay between pages if needed
                # import time
                # time.sleep(0.1)

                # --- Check total herd limit after processing a page ---
                if args.limit_total_herds is not None and len(herd_to_species_map) >= args.limit_total_herds:
                    logger.warning(f"Reached total herd limit ({args.limit_total_herds}). Stopping herd fetch for this combination.")
                    break # Stop pagination for this combo

            except Exception as e:
                logger.error(f"    Error processing page {page} for {species_code}/{usage_code}: {e}", exc_info=True)
                logger.warning(f"    Skipping further pagination for this combination due to error.")
                break # Stop pagination for this combo on error

        logger.info(f" -> Found {len(herds_for_combo)} unique herds for {species_code}/{usage_code}.")
        # Update the map: associate these herds with this species code
        for herd_num in herds_for_combo:
            # If a herd is associated with multiple species (unlikely but possible), just keep one mapping.
            # Also check limit *before* adding, to avoid slightly exceeding the limit
            if args.limit_total_herds is not None and len(herd_to_species_map) >= args.limit_total_herds:
                 limit_total_herds_reached = True
                 logger.warning(f"Reached total herd limit ({args.limit_total_herds}) during herd mapping. Breaking outer loop.")
                 break # Stop adding herds for this combo

            if herd_num not in herd_to_species_map:
                herd_to_species_map[herd_num] = species_code
        # Don't update total_herds_found here, use len(herd_to_species_map) for limit checks

        if limit_total_herds_reached:
             break # Break the outer combinations loop

    logger.info(f"Finished fetching herd lists. Total unique herds identified (or limited to): {len(herd_to_species_map)}")

    # --- Check if any herds were found before proceeding ---
    if not herd_to_species_map:
        logger.warning("No herds were found or processed (potentially due to limits). Skipping subsequent steps.")
        logger.info("--- CHR Bronze Pipeline Finished (Early Exit) ---")
        return

    # --- 3 & 3.5. Load Herd Details & DIKO Movements (Initiated Sequentially, Run Concurrently) ---
    logger.info("Step 3 & 3.5: Initiating parallel loading for Herd Details and DIKO Movements...")

    # --- 3. Load Herd Details for ALL Unique Herds (Parallel) ---
    logger.info(f"Step 3: Loading Herd Details for {len(herd_to_species_map)} unique herds (in parallel)...")
    chr_to_source_map = {} # Initialize here, before the loop
    # Prepare tasks for the executor
    tasks = [(besaetning_client, username, herd_num, spec_code) for herd_num, spec_code in herd_to_species_map.items()]

    processed_herd_count = 0
    # Use ThreadPoolExecutor for parallel execution
    # Use a sensible default like 10 workers, good for I/O bound tasks on runners
    num_workers = args.workers
    logger.info(f"Using {num_workers} workers for parallel fetching.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Use map to preserve order roughly, or submit for more flexibility
        futures = [executor.submit(process_single_herd_details, task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            processed_herd_count += 1
            if processed_herd_count % 500 == 0: # Log progress every 500 herds
                 logger.info(f"  Processed {processed_herd_count}/{len(tasks)} herd detail requests...")
            try:
                result = future.result() # Get the result first
                if result is not None and isinstance(result, (list, tuple)) and len(result) == 3:
                    herd_number, species_code, chr_number = result # Unpack only if valid
                    if chr_number is not None:
                        # Map CHR back to its source herd/species
                        if chr_number not in chr_to_source_map:
                            chr_to_source_map[chr_number] = set()
                        chr_to_source_map[chr_number].add((herd_number, species_code))
                else:
                    # Log if the result format is unexpected
                    # We might need to enhance logging later to include the original herd number for debugging
                    logger.warning(f"  Worker for herd details returned unexpected result format: {result}")
            except Exception as exc:
                 # Log unexpected errors from the future/worker itself
                 logger.error(f"  Error retrieving result from herd detail worker: {exc}", exc_info=False) # Keep this catch-all

    logger.info(f"Finished fetching herd details. Found {len(chr_to_source_map)} unique CHR numbers.")

    # --- 3.5. Load DIKO Movements (Parallel) ---
    logger.info(f"Step 3.5: Loading DIKO Movements for {len(herd_to_species_map)} unique herd/species pairs (in parallel)...")
    
    diko_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        diko_futures = [
            executor.submit(load_diko_flytninger, diko_client, username, herd_number, species_code)
            for herd_number, species_code in herd_to_species_map.items()
        ]
        
        for future in concurrent.futures.as_completed(diko_futures):
            result = future.result()
            if result is not None:  # Only add non-None results (skips unsupported species)
                diko_results.append(result)
    
    successful_diko = len([r for r in diko_results if r is not None])
    total_supported_species = len([pair for pair in herd_to_species_map.items() if pair[1] in VALID_DIKO_SPECIES])
    logger.info(f"Finished DIKO movement loading. Successful requests: {successful_diko}/{total_supported_species}")

    # --- 4 & 5. Load Ejendom Details & VetStat Data (Initiated Sequentially, Run Concurrently) ---
    # These still depend on chr_to_source_map from Step 3
    logger.info("Step 4 & 5: Initiating parallel loading for Ejendom Details and VetStat Data...")

    # --- 4. Load Ejendom Details for ALL Unique CHR Numbers (Parallel) ---
    logger.info(f"Step 4: Loading Ejendom Details for {len(chr_to_source_map)} unique CHR numbers (in parallel)...")
    tasks_ejendom = [(ejendom_client, username, chr_num) for chr_num in chr_to_source_map.keys()]
    processed_chr_count = 0
    ejendom_success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures_ejendom = [executor.submit(process_single_ejendom_details, task) for task in tasks_ejendom]
        for future in concurrent.futures.as_completed(futures_ejendom):
            processed_chr_count += 1
            if processed_chr_count % 500 == 0:
                 logger.info(f"  Processed {processed_chr_count}/{len(tasks_ejendom)} ejendom detail requests...")
            try:
                if future.result(): # Check if the worker function returned True
                    ejendom_success_count += 1
            except Exception as exc:
                 logger.error(f"  Error retrieving result from ejendom detail worker: {exc}", exc_info=False)

    logger.info(f"Finished fetching Ejendom details. Successful requests (pairs): {ejendom_success_count}/{len(tasks_ejendom)}")

    # --- 5. Load VetStat Data (Parallel) ---
    # Determine Date Range (same as before)
    today = date.today()
    end_date_def = today.replace(day=1) - timedelta(days=1)
    start_date_def = end_date_def.replace(day=1)
    start_date = args.start_date if args.start_date else start_date_def
    end_date = args.end_date if args.end_date else end_date_def
    logger.info(f"Step 5: Loading VetStat Data (parallel) for {len(chr_to_source_map)} CHR/Species pairs...")
    logger.info(f"Using date range: {start_date.isoformat()} to {end_date.isoformat()}...")

    # Identify unique CHR/Species pairs (already done in Step 3 for DIKO, reuse unique_chr_species_pairs)
    unique_chr_species_pairs = set()
    for chr_number, sources in chr_to_source_map.items():
        for herd_number, species_code in sources:
            pair = (chr_number, species_code)
            if pair not in unique_chr_species_pairs:
                unique_chr_species_pairs.add(pair)

    logger.info(f"Identified {len(unique_chr_species_pairs)} unique CHR/Species pairs for VetStat processing.")

    # Prepare VetStat tasks
    tasks_vetstat = [(chr_num, spec_code, start_date, end_date) for chr_num, spec_code in unique_chr_species_pairs]
    processed_vetstat_count = 0
    vetstat_success_count = 0
    if tasks_vetstat:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures_vetstat = [executor.submit(process_single_vetstat, task) for task in tasks_vetstat]
            for future in concurrent.futures.as_completed(futures_vetstat):
                processed_vetstat_count += 1
                if processed_vetstat_count % 500 == 0:
                    logger.info(f"  Processed {processed_vetstat_count}/{len(tasks_vetstat)} VetStat requests...")
                try:
                    if future.result(): # Check if worker returned True (success)
                        vetstat_success_count += 1
                except Exception as exc:
                    logger.error(f"  Error retrieving result from VetStat worker: {exc}", exc_info=False)

        logger.info(f"Finished VetStat loading. Successful requests: {vetstat_success_count}/{len(tasks_vetstat)}")
    else:
        logger.info("Skipping VetStat loading as no CHR/Species pairs were identified.")

    # --- Final Step: Write Buffered Data ---
    logger.info("Step 6: Writing all buffered data to consolidated files...")
    try:
        finalize_export()
        logger.info("Successfully finalized data export.")
    except Exception as e:
        logger.error(f"Error during final data export: {e}", exc_info=True)

    logger.info("--- CHR Bronze Pipeline Finished ---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the CHR Bronze Data Pipeline.")

    def valid_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            msg = f"Not a valid date: '{s}'. Use YYYY-MM-DD format."
            raise argparse.ArgumentTypeError(msg)

    parser.add_argument(
        '--start-date',
        type=valid_date,
        help='Start date for VetStat data fetch (YYYY-MM-DD). Defaults to first day of previous month.'
    )
    parser.add_argument(
        '--end-date',
        type=valid_date,
        help='End date for VetStat data fetch (YYYY-MM-DD). Defaults to last day of previous month.'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level (default: INFO).'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=10,
        help='Number of parallel workers for fetching data (default: 10).'
    )

    # --- Added Limiting Arguments ---
    parser.add_argument(
        '--limit-combinations',
        type=int,
        default=None, # Default: No limit
        help='Limit processing to the first N species/usage combinations.'
    )
    parser.add_argument(
        '--limit-total-herds',
        type=int,
        default=None, # Default: No limit
        help='Stop processing herd details and subsequent steps after finding N unique herds.'
    )
    parser.add_argument(
        '--test-species-codes',
        type=str,
        default=None,
        help="Comma-separated list of species codes to test (e.g., '12,13,14,15' for cattle,sheep,goats,pigs)."
    )
    # --- End Added Limiting Arguments ---

    args = parser.parse_args()

    # Adjust logging based on args before calling main
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main(args)
