import logging
import os
from pathlib import Path
import matplotlib.pyplot as plt
from zipfile import ZipFile

from dotenv import load_dotenv
import geopandas as gpd
import requests

logger = logging.getLogger(__name__)

load_dotenv()

GIS_DATA_URL = os.getenv("GIS_DATA_URL")

DATA_DIR = Path(os.getenv("DATA_DIR"))
ZIP_FILE = DATA_DIR / "gis_data.zip"
SHP_FILE = DATA_DIR / "Mere_bedre_stoerre_239omr_CMEC2024_2.shp"
GPKG_FILE = DATA_DIR / 'mbs_natur.gpkg'


def fetch_gis_data():
    # Download the GIS data zip file
    if not ZIP_FILE.exists():
        logger.info(f"Downloading GIS data from {GIS_DATA_URL}")
        response = requests.get(GIS_DATA_URL)
        response.raise_for_status()
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)

def extract_gis_data():
    # Extract the GIS data zip file
    logger.info('Extracting GIS data')
    with ZipFile(ZIP_FILE, "r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

    gdf = gpd.read_file(SHP_FILE)
    gdf.to_file(GPKG_FILE, driver='GPKG')

    return gdf


def plot_gis_data(gdf):
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, column='Prioritet', legend=True)
    ax.set_title('Mere, bedre og større natur i Danmark')
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_aspect('equal')
    fig.savefig(DATA_DIR / 'mbs_natur.png')



def main():
    fetch_gis_data()
    gdf = extract_gis_data()


    

if __name__ == "__main__":
    main()