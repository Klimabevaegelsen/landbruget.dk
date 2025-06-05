import asyncio
import logging
import os
import signal
import sys

from src.config import SOURCES
from src.sources.parsers import get_source_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown = asyncio.Event()


def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully"""
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}. Starting graceful shutdown...")
    shutdown.set()


# Register signal handlers
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


async def run_sync() -> bool:
    """Run the sync process based on environment variable"""
    sync_type = os.getenv("SYNC_TYPE", "all")
    logger.info(f"Starting sync process for: {sync_type}")

    try:
        if sync_type == "all":
            success = True
            for source_id, config in SOURCES.items():
                if shutdown.is_set():
                    logger.info("Shutdown requested, stopping sync process")
                    return False

                if config["enabled"]:
                    source = get_source_handler(source_id, config)
                    if source:
                        try:
                            total_synced = await source.sync()
                            if total_synced is not None:
                                logger.info(f"{source_id} sync completed. Total records: {total_synced:,}")
                            else:
                                logger.error(f"{source_id} sync failed")
                                success = False
                        except Exception as e:
                            logger.error(f"Error syncing {source_id}: {str(e)}", exc_info=True)
                            success = False
            return success
        else:
            if sync_type not in SOURCES:
                logger.error(f"Unknown sync type: {sync_type}")
                return False

            config = SOURCES[sync_type]
            if not config["enabled"]:
                logger.error(f"Sync type {sync_type} is disabled")
                return False

            source = get_source_handler(sync_type, config)
            if not source:
                logger.error(f"No handler for sync type: {sync_type}")
                return False

            total_synced = await source.sync()
            if total_synced is not None:
                logger.info(f"{sync_type} sync completed. Total records: {total_synced:,}")
                return True
            logger.error(f"{sync_type} sync failed")
            return False

    except Exception as e:
        logger.error(f"Error during sync: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(run_sync())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(1)
