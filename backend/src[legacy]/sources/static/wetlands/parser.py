from pathlib import Path

import geopandas as gpd
import pandas as pd

from ...base import Source


class Wetlands(Source):
    """Danish Wetlands shapefile parser"""

    async def fetch(self) -> pd.DataFrame:
        data_path = Path(__file__).parent / "data" / f"{self.config['filename']}.shp"
        if not data_path.exists():
            raise FileNotFoundError(f"Wetlands data not found at {data_path}")

        gdf = gpd.read_file(data_path)
        gdf = gdf.rename(columns={"OBJECTID": "wetland_id", "Kulstof": "carbon_content", "Areal_ha": "area_ha"})

        return gdf[["wetland_id", "carbon_content", "area_ha", "geometry"]]
