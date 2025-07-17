"""Data processing and aggregation for CHR pipeline."""

import logging
from collections import defaultdict
from typing import Any, Dict

from .utils import parse_date

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.data_processing")


def aggregate_cattle_movements(response: Any, reporting_herd: int) -> Dict:
    """
    Aggregate individual cattle records into herd-level movement summaries.

    This function processes individual animal records and creates summaries
    similar to pig movement data structure:
    - Groups by movement date and counterparty herd
    - Counts animals moved
    - Determines movement direction (in/out)

    Args:
        response: Raw CHR_dyr response with individual animal records
        reporting_herd: The herd number doing the reporting

    Returns:
        Dict with aggregated movement summaries
    """
    import time

    # Track aggregation start time for performance monitoring
    aggregation_start_time = time.time()

    movement_summaries = {
        "reporting_herd_number": reporting_herd,
        "movements": [],
        "summary_stats": {
            "total_animals_processed": 0,
            "unique_movement_dates": set(),
            "counterparty_herds": set(),
            "date_range": {"start": None, "end": None},
        },
    }

    try:
        resp = response.Response[0] if isinstance(response.Response, list) else response.Response
        animals = getattr(resp, "Enkeltdyrsoplysninger", [])

        # Handle case where API returns count instead of detailed records
        if isinstance(animals, int):
            logger.info(f"Herd {reporting_herd}: API returned count only ({animals}), no detailed records available")
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "api_returned_count_only",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "animal_count_from_api": animals,
                },
                "processed_successfully": True,
            }

        # Handle non-iterable types
        if animals is None:
            logger.info(f"Herd {reporting_herd}: No animals found")
            animals = []
        elif not hasattr(animals, "__iter__") or isinstance(animals, (str, bytes)):
            logger.warning(f"Herd {reporting_herd}: Invalid animals data type ({type(animals)}) - using empty list")
            animals = []

        try:
            animals_count = len(animals) if animals else 0
        except TypeError:
            logger.error(f"Herd {reporting_herd}: Cannot get length of animals data - using empty list")
            animals = []
            animals_count = 0

        if not animals or animals_count == 0:
            logger.info(f"No animals found for herd {reporting_herd}")
            return movement_summaries

        logger.info(f"Herd {reporting_herd}: Processing {animals_count} individual animals...")

        # Skip extremely large datasets
        if animals_count > 100000:
            logger.warning(
                f"Herd {reporting_herd}: Dataset too large ({animals_count} animals) - skipping to prevent performance issues"
            )
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "dataset_too_large",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "dataset_size": animals_count,
                },
            }

        # Auto-detect high-volume herds
        if animals_count > 50000:
            from .volume_management import add_high_volume_herd, is_high_volume_herd

            if animals_count > 100000:
                suggested_days = 30
            elif animals_count > 75000:
                suggested_days = 60
            else:
                suggested_days = 90

            logger.warning(
                f"Herd {reporting_herd}: Large dataset ({animals_count} animals) detected - suggesting {suggested_days}-day chunks"
            )

            if not is_high_volume_herd(reporting_herd):
                add_high_volume_herd(reporting_herd, suggested_days, volume_estimate=animals_count)

            # Skip processing if extremely large
            if animals_count > 100000:
                return {
                    "reporting_herd_number": reporting_herd,
                    "movements": [],
                    "skipped_reason": "auto_chunking_required",
                    "suggested_chunk_days": suggested_days,
                    "summary_stats": {
                        "total_animals_processed": 0,
                        "unique_movement_dates": 0,
                        "counterparty_herds": 0,
                        "dataset_size": animals_count,
                    },
                }

        # Group movements by date and counterparty
        movement_groups = defaultdict(
            lambda: {
                "animal_count": 0,
                "movement_date": None,
                "counterparty_herd": None,
                "movement_type": None,
                "movement_reasons": [],
            }
        )

        # Process animals safely
        try:
            if not isinstance(animals, (list, tuple)):
                if hasattr(animals, "__iter__") and not isinstance(animals, (str, bytes)):
                    try:
                        animals = list(animals)
                    except (TypeError, ValueError) as e:
                        logger.error(f"Herd {reporting_herd}: Failed to convert animals to list: {e}")
                        animals = []
                else:
                    logger.warning(f"Herd {reporting_herd}: Animals is not iterable - using empty list")
                    animals = []

            for i, animal in enumerate(animals):
                try:
                    # Progress logging for large datasets
                    if i > 0 and i % 5000 == 0:
                        elapsed_time = time.time() - aggregation_start_time
                        logger.info(
                            f"Herd {reporting_herd}: Processed {i}/{len(animals)} animals ({i / len(animals) * 100:.1f}%) in {elapsed_time:.1f}s"
                        )

                    # Extract key movement information
                    ckr_nr = getattr(animal, "CkrNr", None)
                    entry_date = getattr(animal, "DatoIndgaaet", None)
                    exit_date = getattr(animal, "DatoAfgaaet", None)
                    source_herd = getattr(animal, "BesaetningsNummerFra", None)
                    dest_herd = getattr(animal, "BesaetningsNummerTil", None)
                    exit_reason = getattr(animal, "AarsagAfgaaet", None)

                    movement_summaries["summary_stats"]["total_animals_processed"] += 1

                    # Process incoming movements
                    if entry_date and source_herd and source_herd != reporting_herd:
                        movement_date = parse_date(entry_date)
                        if movement_date:
                            key = (movement_date, source_herd, "incoming")
                            movement_groups[key]["animal_count"] += 1
                            movement_groups[key]["movement_date"] = movement_date
                            movement_groups[key]["counterparty_herd"] = source_herd
                            movement_groups[key]["movement_type"] = "incoming"

                            movement_summaries["summary_stats"]["unique_movement_dates"].add(movement_date)
                            movement_summaries["summary_stats"]["counterparty_herds"].add(source_herd)

                    # Process outgoing movements
                    if exit_date and dest_herd and dest_herd != reporting_herd:
                        movement_date = parse_date(exit_date)
                        if movement_date:
                            key = (movement_date, dest_herd, "outgoing")
                            movement_groups[key]["animal_count"] += 1
                            movement_groups[key]["movement_date"] = movement_date
                            movement_groups[key]["counterparty_herd"] = dest_herd
                            movement_groups[key]["movement_type"] = "outgoing"
                            # Clean and validate the exit reason before adding
                            clean_reason = str(exit_reason).strip() if exit_reason is not None else None
                            if clean_reason:
                                movement_groups[key]["movement_reasons"].append(clean_reason)

                            movement_summaries["summary_stats"]["unique_movement_dates"].add(movement_date)
                            movement_summaries["summary_stats"]["counterparty_herds"].add(dest_herd)

                except Exception as e:
                    logger.debug(f"Error processing individual animal {getattr(animal, 'CkrNr', 'unknown')}: {e}")
                    continue

        except TypeError as e:
            logger.error(f"Herd {reporting_herd}: Failed to iterate over animals - {e}")
            return {
                "reporting_herd_number": reporting_herd,
                "movements": [],
                "skipped_reason": "iteration_failed",
                "summary_stats": {
                    "total_animals_processed": 0,
                    "unique_movement_dates": 0,
                    "counterparty_herds": 0,
                    "error_details": str(e),
                },
            }

        # Convert grouped movements to list format
        for (movement_date, counterparty_herd, movement_type), group_data in movement_groups.items():
            reasons = [r for r in group_data["movement_reasons"] if r]
            unique_reasons = list(set(reasons)) if reasons else []

            movement_summary = {
                "movement_id": f"{reporting_herd}_{counterparty_herd}_{movement_date}_{movement_type}",
                "movement_date": movement_date,
                "reporting_herd_number": reporting_herd,
                "counterparty_herd_number": counterparty_herd,
                "movement_type": movement_type,
                "animals_total": group_data["animal_count"],
                "contact_type": "Fra" if movement_type == "outgoing" else "Til",
                "movement_reasons": unique_reasons,
                "primary_reason": unique_reasons[0] if unique_reasons else None,
                "source_data": "chr_dyr_aggregated",
            }
            movement_summaries["movements"].append(movement_summary)

        # Update summary statistics
        dates = list(movement_summaries["summary_stats"]["unique_movement_dates"])
        if dates:
            movement_summaries["summary_stats"]["date_range"]["start"] = min(dates)
            movement_summaries["summary_stats"]["date_range"]["end"] = max(dates)

        # Keep both sets and counts for compatibility
        movement_summaries["summary_stats"]["unique_movement_dates_set"] = movement_summaries["summary_stats"][
            "unique_movement_dates"
        ]
        movement_summaries["summary_stats"]["counterparty_herds_set"] = movement_summaries["summary_stats"][
            "counterparty_herds"
        ]

        # Convert to counts for final output
        movement_summaries["summary_stats"]["unique_movement_dates"] = len(dates)
        movement_summaries["summary_stats"]["counterparty_herds"] = len(
            movement_summaries["summary_stats"]["counterparty_herds_set"]
        )

        logger.info(
            f"Herd {reporting_herd}: Processed {movement_summaries['summary_stats']['total_animals_processed']} "
            f"individual animals into {len(movement_summaries['movements'])} movement summaries"
        )

        return movement_summaries

    except Exception as e:
        logger.error(f"Error aggregating cattle movements for herd {reporting_herd}: {e}")
        return movement_summaries
