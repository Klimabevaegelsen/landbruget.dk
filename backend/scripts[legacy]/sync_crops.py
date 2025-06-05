import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.config import SOURCES
from src.sources.static.crops.parser import CropCodes


async def main() -> Optional[int]:
    """Sync crop codes data to Cloud Storage"""
    load_dotenv()
    try:
        crops = CropCodes(SOURCES["crops"])
        total_synced = await crops.sync()
        logger.info(f"Total records synced: {total_synced:,}")
        return total_synced
    except Exception as e:
        logger.error(f"Error during sync: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        sys.exit(1)
