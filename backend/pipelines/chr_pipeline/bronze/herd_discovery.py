"""Herd volume discovery system for intelligent processing strategy."""

import json
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Import GCS access for persistent storage
try:
    from unified_pipeline.util.gcs_access import GCSDataAccess
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    GCSDataAccess = None

from .utils import create_base_request
from .volume_management import add_high_volume_herd, is_high_volume_herd

logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.herd_discovery")


def discover_herd_volumes_for_year(
    chr_dyr_client, username: str, herd_list: List[int], year: int,
    sample_weeks: int = 3, sample_strategy: str = "seasonal"
) -> Tuple[List[Dict], List[int]]:
    """
    Phase 1: Lightweight discovery to identify large herds before full processing.
    
    Args:
        chr_dyr_client: CHR_dyr SOAP client
        username: FVM username for authentication
        herd_list: List of herd numbers to analyze
        year: Year to analyze
        sample_weeks: Number of weeks to sample per herd (default: 3)
        sample_strategy: Sampling strategy ("seasonal", "random", "recent")
    
    Returns:
        Tuple of (large_herds_info, normal_herd_list)
    """
    logger.info(f"🔍 Starting herd volume discovery for {len(herd_list)} herds in year {year}")
    logger.info(f"📊 Using {sample_weeks}-week sampling with '{sample_strategy}' strategy")
    
    large_herds = []
    normal_herds = []
    discovery_stats = {
        "total_herds": len(herd_list),
        "herds_sampled": 0,
        "sampling_errors": 0,
        "large_herds_found": 0,
        "total_sample_time": 0,
    }
    
    # Get sample periods based on strategy
    sample_periods = get_sample_periods(year, sample_weeks, sample_strategy)
    logger.info(f"📅 Sample periods: {sample_periods}")
    
    for herd_idx, herd_number in enumerate(herd_list):
        try:
            logger.debug(f"🔍 Sampling herd {herd_number} ({herd_idx+1}/{len(herd_list)})")
            
            # Check if already known as high-volume
            if is_high_volume_herd(herd_number):
                logger.info(f"🐄 Herd {herd_number} already configured as high-volume, skipping discovery")
                large_herds.append({
                    'herd': herd_number,
                    'estimated_yearly': 0,  # Unknown, but known to be large
                    'suggested_chunk_days': 30,  # Default for known large herds
                    'discovery_method': 'pre_configured',
                    'confidence': 'high'
                })
                discovery_stats["large_herds_found"] += 1
                continue
            
            # Sample the herd across multiple periods
            total_sample_animals = 0
            total_sample_days = 0
            sampling_successful = False
            
            for period_start, period_end in sample_periods:
                try:
                    sample_count = sample_herd_period(chr_dyr_client, username, herd_number, period_start, period_end)
                    if sample_count > 0:
                        days_in_period = (period_end - period_start).days + 1
                        total_sample_animals += sample_count
                        total_sample_days += days_in_period
                        sampling_successful = True
                        logger.debug(f"  📅 Period {period_start}-{period_end}: {sample_count} animals in {days_in_period} days")
                    
                except Exception as e:
                    logger.warning(f"  ⚠️ Sampling failed for herd {herd_number} period {period_start}-{period_end}: {e}")
                    continue
            
            discovery_stats["herds_sampled"] += 1
            
            if not sampling_successful or total_sample_days == 0:
                logger.debug(f"  ➡️ Herd {herd_number}: No sample data, treating as normal")
                normal_herds.append(herd_number)
                continue
            
            # Calculate volume estimates
            animals_per_day = total_sample_animals / total_sample_days
            estimated_yearly = animals_per_day * 365
            
            # Determine processing strategy
            herd_info = classify_herd_volume(herd_number, estimated_yearly, sample_weeks, total_sample_animals)
            
            if herd_info['is_large']:
                large_herds.append(herd_info)
                discovery_stats["large_herds_found"] += 1
                
                # Pre-configure for future processing
                add_high_volume_herd(
                    herd_info['herd'], 
                    herd_info['suggested_chunk_days'], 
                    volume_estimate=int(estimated_yearly)
                )
                
                logger.info(
                    f"🐄 Large herd detected: {herd_number} "
                    f"(~{estimated_yearly:,.0f} animals/year, {herd_info['suggested_chunk_days']}-day chunks)"
                )
            else:
                normal_herds.append(herd_number)
                logger.debug(f"  ➡️ Herd {herd_number}: Normal volume (~{estimated_yearly:,.0f} animals/year)")
        
        except Exception as e:
            logger.error(f"❌ Discovery failed for herd {herd_number}: {e}")
            discovery_stats["sampling_errors"] += 1
            # Default to normal processing if discovery fails
            normal_herds.append(herd_number)
    
    # Log discovery summary
    logger.info(f"✅ Discovery completed for year {year}:")
    logger.info(f"  📊 Total herds: {discovery_stats['total_herds']}")
    logger.info(f"  📊 Successfully sampled: {discovery_stats['herds_sampled']}")
    logger.info(f"  📊 Sampling errors: {discovery_stats['sampling_errors']}")
    logger.info(f"  🐄 Large herds found: {discovery_stats['large_herds_found']}")
    logger.info(f"  🐄 Normal herds: {len(normal_herds)}")
    
    # Save discovery results
    save_discovery_results(year, large_herds, normal_herds, discovery_stats)
    
    return large_herds, normal_herds


def get_sample_periods(year: int, sample_weeks: int, strategy: str) -> List[Tuple[date, date]]:
    """Get optimal sampling periods based on strategy."""
    
    if strategy == "seasonal":
        # Sample across different seasons to capture seasonal variation
        base_periods = [
            (date(year, 2, 15), 7),   # Winter - calving season  
            (date(year, 6, 15), 7),   # Summer - grazing season
            (date(year, 10, 15), 7),  # Fall - movement season
        ]
    elif strategy == "recent":
        # Sample recent periods (last few months of the year)
        base_periods = [
            (date(year, 10, 1), 7),
            (date(year, 11, 1), 7), 
            (date(year, 12, 1), 7),
        ]
    else:  # random
        # Evenly distributed throughout year
        base_periods = [
            (date(year, 3, 1), 7),
            (date(year, 7, 1), 7),
            (date(year, 11, 1), 7),
        ]
    
    # Convert to actual date ranges, limiting by sample_weeks
    periods = []
    weeks_used = 0
    
    for start_date, days in base_periods:
        if weeks_used >= sample_weeks:
            break
            
        # Ensure we don't go past year end
        year_end = date(year, 12, 31)
        actual_end = min(start_date + timedelta(days=days-1), year_end)
        
        periods.append((start_date, actual_end))
        weeks_used += 1
    
    return periods[:sample_weeks]


def sample_herd_period(chr_dyr_client, username: str, herd_number: int, start_date: date, end_date: date) -> int:
    """Sample a single herd for a specific time period to estimate volume."""
    
    try:
        # Create request structure for count-only query
        GLRCHRWSInfoInboundFactory = chr_dyr_client.get_type("ns0:GLRCHRWSInfoInboundType")
        common_header = GLRCHRWSInfoInboundFactory(**create_base_request(username))

        CHR_dyrChrBesListeRequestTypeFactory = chr_dyr_client.get_type("ns0:CHR_dyrChrBesListeRequestType")
        request_params = CHR_dyrChrBesListeRequestTypeFactory(
            **{"BesaetningsNummer": herd_number, "PeriodeFra": start_date, "PeriodeTil": end_date}
        )

        payload_content = {"GLRCHRWSInfoInbound": common_header, "Request": request_params}

        # Make the request
        response = chr_dyr_client.service.besListAktOms(CHR_dyrChrBesListeRequest=payload_content)

        if response and hasattr(response, "Response") and response.Response:
            resp = response.Response[0] if isinstance(response.Response, list) else response.Response
            animals = getattr(resp, "Enkeltdyrsoplysninger", [])
            
            # Handle both count-only responses and actual data
            if isinstance(animals, int):
                return animals
            elif animals:
                return len(animals) if hasattr(animals, '__len__') else 0
            else:
                return 0
        
        return 0
        
    except Exception as e:
        logger.warning(f"Sampling failed for herd {herd_number} ({start_date} to {end_date}): {e}")
        return 0


def classify_herd_volume(herd_number: int, estimated_yearly: float, sample_weeks: int, total_sample: int) -> Dict[str, Any]:
    """Classify herd volume and determine processing strategy."""
    
    # Volume thresholds 
    if estimated_yearly > 500000:
        volume_category = "massive"
        chunk_days = 7      # Weekly chunks for massive herds
        risk_level = "extreme"
    elif estimated_yearly > 200000:
        volume_category = "very_large"
        chunk_days = 14     # Bi-weekly chunks for very large herds
        risk_level = "very_high"
    elif estimated_yearly > 100000:
        volume_category = "large"
        chunk_days = 30     # Monthly chunks for large herds
        risk_level = "high"
    elif estimated_yearly > 50000:
        volume_category = "moderate_large"  
        chunk_days = 90     # Quarterly chunks for moderate herds
        risk_level = "moderate"
    else:
        volume_category = "normal"
        chunk_days = 365    # Full year for normal herds
        risk_level = "low"
    
    # Confidence based on sample size and consistency
    if total_sample >= 100 and sample_weeks >= 2:
        confidence = "high"
    elif total_sample >= 50:
        confidence = "medium"
    else:
        confidence = "low"
    
    return {
        "herd": herd_number,
        "estimated_yearly": estimated_yearly,
        "volume_category": volume_category,
        "risk_level": risk_level,
        "suggested_chunk_days": chunk_days,
        "is_large": estimated_yearly > 50000,  # Threshold for special processing
        "confidence": confidence,
        "discovery_method": "sampling",
        "sample_data": {
            "sample_weeks": sample_weeks,
            "total_sample_animals": total_sample,
        }
    }


def save_discovery_results(year: int, large_herds: List[Dict], normal_herds: List[int], stats: Dict) -> None:
    """Save discovery results for future reference and optimization."""
    
    discovery_data = {
        "year": year,
        "discovery_timestamp": date.today().isoformat(),
        "large_herds": large_herds,
        "normal_herds": normal_herds,
        "statistics": stats,
        "version": "1.0"
    }
    
    if GCS_AVAILABLE:
        try:
            import os
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/discovery_results_{year}.json"
            
            gcs_data_access.upload_json(discovery_data, config_path)
            logger.info(f"💾 Saved discovery results to: {config_path}")
            
        except Exception as e:
            logger.warning(f"Failed to save discovery results to GCS: {e}")
    
    # Always save locally as backup
    try:
        import os
        local_path = f"/tmp/discovery_results_{year}.json"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        with open(local_path, 'w') as f:
            json.dump(discovery_data, f, indent=2, default=str)
        
        logger.debug(f"💾 Saved local backup: {local_path}")
        
    except Exception as e:
        logger.warning(f"Failed to save local discovery results: {e}")


def load_previous_discovery_results(year: int) -> Optional[Tuple[List[Dict], List[int]]]:
    """Load previous discovery results to avoid re-sampling."""
    
    if GCS_AVAILABLE:
        try:
            import os
            gcs_data_access = GCSDataAccess()
            bucket_name = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")
            config_path = f"gs://{bucket_name}/bronze/chr/config/discovery_results_{year}.json"
            
            if gcs_data_access.file_exists(config_path):
                discovery_data = gcs_data_access.download_json(config_path)
                
                # Check if results are recent (within last 30 days)
                from datetime import datetime
                discovery_date = datetime.fromisoformat(discovery_data.get("discovery_timestamp", "1970-01-01"))
                age_days = (datetime.now() - discovery_date).days
                
                if age_days <= 30:
                    logger.info(f"📋 Using cached discovery results from {discovery_date.date()} ({age_days} days old)")
                    return discovery_data["large_herds"], discovery_data["normal_herds"]
                else:
                    logger.info(f"📋 Discovery cache is {age_days} days old, will re-discover")
                    
        except Exception as e:
            logger.warning(f"Failed to load discovery results from GCS: {e}")
    
    return None