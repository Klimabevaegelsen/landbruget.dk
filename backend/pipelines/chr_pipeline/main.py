# backend/chr_pipeline/main.py
"""CHR Pipeline for fetching and processing data."""

import argparse
import logging
import concurrent.futures
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any, Set
from tqdm.contrib.logging import logging_redirect_tqdm
from tqdm.auto import tqdm

from bronze.load_stamdata import (
    create_soap_client as create_stamdata_client,
    load_species_usage_combinations,
    ENDPOINTS as STAMDATA_ENDPOINTS
)
from bronze.load_besaetning import (
    create_soap_client as create_bes_client,
    load_herd_list,
    load_herd_details,
    get_fvm_credentials,
    ENDPOINTS as BES_ENDPOINTS
)
from bronze.load_ejendom import (
    create_soap_client as create_ejd_client,
    load_ejendom_oplysninger,
    load_ejendom_vet_events,
    ENDPOINTS as EJD_ENDPOINTS
)
from bronze.load_diko import (
    create_soap_client as create_diko_client,
    load_diko_flytninger,
    ENDPOINTS as DIKO_ENDPOINTS
)
from bronze.load_vetstat import load_vetstat_antibiotics
from bronze.export import finalize_export

logger = logging.getLogger(__name__)

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
    pipeline_logger = logging.getLogger('backend.pipelines.chr_pipeline')
    pipeline_logger.setLevel(numeric_level)
    
    # Configure bronze module loggers
    bronze_logger = logging.getLogger('backend.pipelines.chr_pipeline.bronze')
    bronze_logger.setLevel(numeric_level if numeric_level <= logging.INFO else logging.WARNING)
    
    # Set third-party loggers to WARNING or higher
    logging.getLogger('zeep').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Prevent log propagation for specific modules when not in DEBUG
    if numeric_level > logging.DEBUG:
        for logger_name in ['zeep', 'urllib3', 'google', 'requests']:
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
    parser.add_argument('--steps', type=str, default='all',
                      choices=['all', 'stamdata', 'herds', 'herd_details', 'diko', 'ejendom', 'vetstat'],
                      help='Pipeline steps to run')
    parser.add_argument('--start-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                      default=start_date_def, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
                      default=end_date_def, help='End date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=10,
                      help='Number of parallel workers')
    parser.add_argument('--test-species-codes', type=str,
                      help='Comma-separated species codes (e.g., "12,13,14,15")')
    parser.add_argument('--limit-total-herds', type=int,
                      help='Limit total number of herds processed')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      default='WARNING', help='Logging level')
    parser.add_argument('--progress', action='store_true',
                      help='Show progress information')
    
    args = parser.parse_args()
    
    # Convert test species codes to list if provided
    if args.test_species_codes:
        args.test_species_codes = [int(code.strip()) for code in args.test_species_codes.split(',')]
    
    return vars(args)

def fetch_stamdata(client: Any, username: str, test_species_codes: Optional[List[int]] = None) -> List[Dict]:
    """Fetch and parse species/usage combinations."""
    logger.info("Fetching species/usage combinations...")
    response = load_species_usage_combinations(client, username)
    
    if not response or not hasattr(response, 'Response'):
        logger.error("Invalid or empty Stamdata response")
        return []
    
    combinations = []
    for combo in response.Response if isinstance(response.Response, list) else [response.Response]:
        try:
            species_code = int(getattr(combo, 'DyreArtKode', 0))
            if test_species_codes and species_code not in test_species_codes:
                continue
                
            combinations.append({
                'species_code': species_code,
                'usage_code': int(getattr(combo, 'BrugsArtKode', 0)),
                'species_text': str(getattr(combo, 'DyreArtTekst', '')),
                'usage_text': str(getattr(combo, 'BrugsArtTekst', ''))
            })
        except (ValueError, AttributeError) as e:
            logger.warning(f"Skipping invalid combination: {e}")
            
    logger.info(f"Found {len(combinations)} valid combinations")
    return combinations

def fetch_herds(client: Any, username: str, combinations: List[Dict], limit: Optional[int] = None) -> Dict[int, int]:
    """Fetch herd numbers for each species/usage combination."""
    logger.info("Fetching herd numbers...")
    herd_to_species = {}
    
    for combo in combinations:
        species_code = combo['species_code']
        usage_code = combo['usage_code']
        
        start_number = 0
        while True:
            try:
                # load_herd_list returns (herd_list, has_more, last_herd_in_batch)
                herd_list, has_more, last_herd = load_herd_list(
                    client, username, species_code, usage_code, start_number
                )
                
                # Process the herd list
                for herd_number in herd_list:
                    if herd_number > 0:
                        herd_to_species[herd_number] = species_code
                        if limit and len(herd_to_species) >= limit:
                            return herd_to_species
                
                # Check if we need to continue pagination
                if not has_more:
                    break
                    
                # Update start number for next page
                if last_herd is not None:
                    start_number = last_herd + 1
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching herds for species {species_code}: {e}")
                break
                
    logger.info(f"Found {len(herd_to_species)} unique herds")
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

def run_step(step: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Run a specific pipeline step and update the context."""
    logger.warning(f"Starting step: {step}")
    
    if step == 'stamdata':
        context['combinations'] = fetch_stamdata(
            context['clients']['stamdata'],
            context['username'],
            context['args']['test_species_codes']
        )
        if not context['combinations']:
            raise ValueError("No valid species/usage combinations found")
        if context['args']['progress']:
            logger.warning(f"Found {len(context['combinations'])} species/usage combinations")
            
    elif step == 'herds':
        if 'combinations' not in context:
            raise ValueError("Cannot run 'herds' step without first running 'stamdata'")
            
        context['herd_to_species'] = fetch_herds(
            context['clients']['besaetning'],
            context['username'],
            context['combinations'],
            context['args']['limit_total_herds']
        )
        if not context['herd_to_species']:
            raise ValueError("No valid herds found")
        if context['args']['progress']:
            logger.warning(f"Found {len(context['herd_to_species'])} herds")
    
    elif step == 'herd_details':
        if 'herd_to_species' not in context:
            raise ValueError("Cannot run 'herd_details' step without first running 'herds'")
            
        # Store herd details and build CHR number mapping
        context['herd_details'] = []
        context['chr_to_species'] = {}
        
        herd_tasks = [(context['clients']['besaetning'], context['username'], herd_num, species_code) 
                      for herd_num, species_code in context['herd_to_species'].items()]
        
        if context['args']['progress']:
            logger.warning(f"Processing {len(herd_tasks)} herd detail tasks")
        
        results = process_parallel(load_herd_details, herd_tasks, context['args']['workers'], "Processing herd details")
        
        # Process results to build chr_to_species mapping
        for result in results:
            if result and hasattr(result, 'Response') and result.Response:
                context['herd_details'].append(result)
                # Extract CHR number from the response
                for response in result.Response:
                    if hasattr(response, 'Besaetning') and hasattr(response.Besaetning, 'ChrNummer'):
                        chr_number = response.Besaetning.ChrNummer
                        species_code = next((task[3] for task in herd_tasks if task[2] == chr_number), None)
                        if species_code:
                            if chr_number not in context['chr_to_species']:
                                context['chr_to_species'][chr_number] = set()
                            context['chr_to_species'][chr_number].add(species_code)
        
        if context['args']['progress']:
            logger.warning(f"Processed {len(context['herd_details'])} herd details, found {len(context['chr_to_species'])} unique CHR numbers")
        
    elif step == 'diko':
        if 'herd_to_species' not in context:
            raise ValueError("Cannot run 'diko' step without first running 'herds'")
            
        diko_tasks = [(context['clients']['diko'], context['username'], herd_num, species_code) 
                      for herd_num, species_code in context['herd_to_species'].items()]
        
        if context['args']['progress']:
            logger.warning(f"Processing {len(diko_tasks)} DIKO tasks")
        
        results = process_parallel(load_diko_flytninger, diko_tasks, context['args']['workers'], "Processing DIKO tasks")
        context['diko_results'] = results
        
        if context['args']['progress']:
            successful = sum(1 for r in results if r)
            logger.warning(f"Completed DIKO tasks. Success: {successful}/{len(diko_tasks)}")
        
    elif step == 'ejendom':
        if 'chr_to_species' not in context:
            raise ValueError("Cannot run 'ejendom' step without first running 'herd_details'")
            
        ejendom_tasks = [(context['clients']['ejendom'], context['username'], chr_num) 
                        for chr_num in context['chr_to_species'].keys()]
        
        if context['args']['progress']:
            logger.warning(f"Processing {len(ejendom_tasks)} ejendom tasks")
        
        # Run both ejendom operations
        oplysninger_results = process_parallel(load_ejendom_oplysninger, ejendom_tasks, context['args']['workers'], "Processing Ejendom Oplysninger")
        vet_events_results = process_parallel(load_ejendom_vet_events, ejendom_tasks, context['args']['workers'], "Processing Ejendom Vet Events")
        
        if context['args']['progress']:
            successful_opl = sum(1 for r in oplysninger_results if r)
            successful_vet = sum(1 for r in vet_events_results if r)
            logger.warning(f"Completed Ejendom tasks. Oplysninger success: {successful_opl}/{len(ejendom_tasks)}, Vet events success: {successful_vet}/{len(ejendom_tasks)}")
        
    elif step == 'vetstat':
        if 'chr_to_species' not in context:
            raise ValueError("Cannot run 'vetstat' step without first running 'herd_details'")
            
        vetstat_tasks = [
            (chr_num, species, context['args']['start_date'], context['args']['end_date'])
            for chr_num, species_set in context['chr_to_species'].items()
            for species in species_set
        ]
        
        if not vetstat_tasks:
            logger.warning("No valid CHR number and species code combinations found for VetStat data")
        elif context['args']['progress']:
            logger.warning(f"Processing {len(vetstat_tasks)} VetStat tasks")
            
        try:
            results = process_parallel(load_vetstat_antibiotics, vetstat_tasks, context['args']['workers'], "Processing VetStat tasks")
            if context['args']['progress']:
                successful = sum(1 for r in results if r)
                logger.warning(f"Completed VetStat tasks. Success: {successful}/{len(vetstat_tasks)}")
        except Exception as e:
            logger.error(f"Error processing VetStat tasks: {e}", exc_info=True)
            raise
    
    return context

def get_required_steps(target_step: str) -> List[str]:
    """Get the list of steps required to run before the target step."""
    step_dependencies = {
        'stamdata': [],
        'herds': ['stamdata'],
        'herd_details': ['stamdata', 'herds'],
        'diko': ['stamdata', 'herds'],
        'ejendom': ['stamdata', 'herds', 'herd_details'],
        'vetstat': ['stamdata', 'herds', 'herd_details', 'ejendom']
    }
    return step_dependencies.get(target_step, [])

def main():
    """Main pipeline execution."""
    args = parse_args()
    setup_logging(args['log_level'])
    
    try:
        # Initialize context
        username, password = get_fvm_credentials()
        context = {
            'args': args,
            'username': username,
            'clients': {
                'stamdata': create_stamdata_client(STAMDATA_ENDPOINTS['stamdata'], username, password),
                'besaetning': create_bes_client(BES_ENDPOINTS['besaetning'], username, password),
                'ejendom': create_ejd_client(EJD_ENDPOINTS['ejendom'], username, password),
                'diko': create_diko_client(DIKO_ENDPOINTS['diko'], username, password)
            }
        }
        
        # Determine steps to run
        if args['steps'] == 'all':
            steps_to_run = ['stamdata', 'herds', 'herd_details', 'diko', 'ejendom', 'vetstat']
        else:
            # Get required steps plus the target step
            steps_to_run = get_required_steps(args['steps']) + [args['steps']]
            
        # Run each step
        for step in steps_to_run:
            context = run_step(step, context)
            
        # Always finalize export
        logger.warning("Finalizing export...")
        finalize_export()
        
        logger.warning(f"Pipeline step(s) {', '.join(steps_to_run)} completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise SystemExit(1)

if __name__ == '__main__':
    main()
