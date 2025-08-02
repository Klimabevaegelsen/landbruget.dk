"""CHR Gold Layer Processing orchestrator."""

import logging
from pathlib import Path
from typing import Optional

from .config import GOLD_BASE_DIR, SILVER_BASE_DIR
from .veterinary_timeline import process_veterinary_timeline
from .transportation_analysis import process_transportation_analysis


logger = logging.getLogger(__name__)


def process_gold_data(export_timestamp: str, silver_dir: Optional[Path] = None,
                     gold_dir: Optional[Path] = None, 
                     drive_silver_dir: Optional[Path] = None,
                     step: Optional[str] = None) -> bool:
    """
    Main orchestrator for CHR gold layer processing.
    
    Args:
        export_timestamp: Export timestamp for file naming
        silver_dir: Path to CHR silver data directory (optional)
        gold_dir: Output directory for gold data (optional)
        drive_silver_dir: Path to drive pipeline silver data (optional)
        step: Specific step to run ('veterinary_timeline', 'transportation_analysis', or None for all)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🥇 Starting CHR Gold Layer Processing...")
        
        # Note: No need to setup silver directories - we use GCS directly now
        logger.info("🔍 Using dynamic GCS data discovery - no local silver data required")
                
        if gold_dir is None:
            gold_dir = GOLD_BASE_DIR / export_timestamp
            
        gold_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📂 Gold output directory: {gold_dir}")
        
        # Determine which steps to run
        run_timeline = step is None or step in ["all", "gold_processing", "veterinary_timeline"]
        run_transport = step is None or step in ["all", "gold_processing", "transportation_analysis"]
        
        timeline_success = True  # Default to success if not running
        transport_success = True  # Default to success if not running
        
        # Process veterinary timeline (if requested)
        if run_timeline:
            logger.info("🏥 Processing veterinary timeline...")
            timeline_success = process_veterinary_timeline(
                export_timestamp=export_timestamp, 
                gold_dir=gold_dir
            )
        else:
            logger.info("⏭️ Skipping veterinary timeline (not requested)")
        
        # Process transportation analysis (if requested)
        if run_transport:
            logger.info("🚛 Processing transportation analysis...")
            transport_success = process_transportation_analysis(
                export_timestamp=export_timestamp,
                gold_dir=gold_dir
            )
        else:
            logger.info("⏭️ Skipping transportation analysis (not requested)")
        
        # All requested products must succeed for overall success
        overall_success = timeline_success and transport_success
        
        if overall_success:
            logger.info("✅ CHR Gold Layer Processing completed successfully")
            if run_timeline:
                logger.info(f"   ✅ Veterinary timeline: {'SUCCESS' if timeline_success else 'FAILED'}")
            if run_transport:
                logger.info(f"   ✅ Transportation analysis: {'SUCCESS' if transport_success else 'FAILED'}")
            return True
        else:
            logger.error("❌ CHR Gold Layer Processing failed")
            if run_timeline:
                logger.error(f"   ❌ Veterinary timeline: {'SUCCESS' if timeline_success else 'FAILED'}")
            if run_transport:
                logger.error(f"   ❌ Transportation analysis: {'SUCCESS' if transport_success else 'FAILED'}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error in CHR gold processing: {e}")
        return False


if __name__ == "__main__":
    # For testing
    import sys
    from datetime import datetime
    
    logging.basicConfig(level=logging.INFO)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    success = process_gold_data(timestamp)
    
    sys.exit(0 if success else 1)