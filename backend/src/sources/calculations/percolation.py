import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from typing import Optional, Tuple
import logging
from ..parsers.dmi import DMIParser

logger = logging.getLogger(__name__)

class PercolationCalculator:
    """Calculator for estimating percolation using DMI climate data"""

    def __init__(self, dmi_parser: DMIParser):
        """
        Initialize the calculator with a DMI parser instance

        Args:
            dmi_parser: An instance of DMIParser to fetch climate data
        """
        self.dmi_parser = dmi_parser

    async def calculate_percolation(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None
    ) -> gpd.GeoDataFrame:
        """
        Calculate estimated percolation by subtracting evaporation from precipitation

        Args:
            start_time: Start time for the data (optional, defaults to 30 days ago)
            end_time: End time for the data (optional, defaults to now)
            bbox: Bounding box for the data [minx, miny, maxx, maxy] (optional, defaults to Denmark)

        Returns:
            GeoDataFrame with percolation estimates
        """
        try:
            # Fetch precipitation and evaporation data
            logger.info("Fetching precipitation and evaporation data")
            precip_gdf = await self.dmi_parser.fetch_precipitation_data(start_time, end_time, bbox)
            evap_gdf = await self.dmi_parser.fetch_evaporation_data(start_time, end_time, bbox)

            if precip_gdf.empty or evap_gdf.empty:
                logger.warning("No data available for calculation")
                return gpd.GeoDataFrame()

            # Merge the dataframes on geometry
            merged_gdf = precip_gdf.merge(
                evap_gdf[['geometry', 'value']],
                on='geometry',
                suffixes=('_precip', '_evap')
            )

            # Calculate percolation (precipitation - evaporation)
            merged_gdf['percolation'] = merged_gdf['value_precip'] - merged_gdf['value_evap']

            # Add metadata
            merged_gdf['calculation_time'] = datetime.now().isoformat()
            merged_gdf['start_time'] = start_time.isoformat() if start_time else None
            merged_gdf['end_time'] = end_time.isoformat() if end_time else None

            # Clean up columns
            result_gdf = merged_gdf[[
                'geometry',
                'percolation',
                'value_precip',
                'value_evap',
                'calculation_time',
                'start_time',
                'end_time'
            ]]

            # Rename columns for clarity
            result_gdf = result_gdf.rename(columns={
                'value_precip': 'precipitation',
                'value_evap': 'evaporation'
            })

            return result_gdf

        except Exception as e:
            logger.error(f"Error calculating percolation: {str(e)}")
            return gpd.GeoDataFrame()

    async def get_daily_percolation(
        self,
        days: int = 30,
        bbox: Optional[Tuple[float, float, float, float]] = None
    ) -> gpd.GeoDataFrame:
        """
        Get daily percolation estimates for the last N days

        Args:
            days: Number of days to look back (default: 30)
            bbox: Bounding box for the data [minx, miny, maxx, maxy] (optional)

        Returns:
            GeoDataFrame with daily percolation estimates
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        return await self.calculate_percolation(start_time, end_time, bbox)

    async def get_monthly_percolation(
        self,
        months: int = 1,
        bbox: Optional[Tuple[float, float, float, float]] = None
    ) -> gpd.GeoDataFrame:
        """
        Get monthly percolation estimates for the last N months

        Args:
            months: Number of months to look back (default: 1)
            bbox: Bounding box for the data [minx, miny, maxx, maxy] (optional)

        Returns:
            GeoDataFrame with monthly percolation estimates
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=months * 30)

        return await self.calculate_percolation(start_time, end_time, bbox)