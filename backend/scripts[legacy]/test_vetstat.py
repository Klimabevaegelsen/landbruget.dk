import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Add backend directory to path
backend_dir = Path(__file__).parent.parent
sys.path.append(str(backend_dir))

from src.config import SOURCES
from src.sources.parsers.antibiotics import VetStatAntibioticsParser


async def main():
    try:
        # Calculate period
        today = date.today()
        last_day_prev_month = today.replace(day=1) - relativedelta(days=1)
        first_day = (last_day_prev_month - relativedelta(months=11)).replace(day=1)

        logger.info(f"Testing VetStat parser for period: {first_day} to {last_day_prev_month}")

        # Initialize parser
        parser = VetStatAntibioticsParser(SOURCES["vetstat"])

        # Test with a single CHR number first
        chr_number = 16218  # Test CHR number
        results = parser.process_soap_request(
            chr_number=chr_number, periode_fra=first_day.isoformat(), periode_til=last_day_prev_month.isoformat()
        )

        if results:
            logger.info("Response received and parsed:")
            df = pd.DataFrame(results)
            logger.info("\nData summary:")
            logger.info(f"Total records: {len(df)}")
            logger.info("\nColumns:")
            for col in df.columns:
                non_null = df[col].count()
                logger.info(f"{col}: {non_null} non-null values")

            logger.info("\nSample data:")
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", None)
            logger.info("\n" + str(df.head()))
        else:
            logger.error("No response received")

    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
