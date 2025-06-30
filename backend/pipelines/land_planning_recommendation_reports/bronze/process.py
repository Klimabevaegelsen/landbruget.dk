import logging
import os
from zipfile import ZipFile

import requests
import geopandas as gpd
from google.cloud import storage
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

load_dotenv('.env')


def make_request(url, params={}):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def fetch_file(url, file_path):
    if not file_path.exists():
        logger.info(f"Downloading data from {url}")
        response = make_request(url)
        with open(file_path, "wb") as f:
            f.write(response.content)


def extract_zip_file(zip_file, data_dir):
    # Extract the GIS data zip file
    logger.info(f'Extracting zip file {zip_file} to {data_dir}')
    with ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(data_dir)


def gdp_read_file(file_or_bytes):
    gdf = gpd.read_file(file_or_bytes)

    return gdf


def save_gpkg_file(gdf, gpkg_file):
    logger.info(f'Saving gpkg file {gpkg_file}')
    gdf.to_file(gpkg_file, driver='GPKG')

def upload_to_gcs(file_path):
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    blob_path = os.path.basename(file_path)

    print('GOOGLE_APPLICATION_CREDENTIALS', os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    print('bucket_name', bucket_name)


    logger.info(f'Uploading file {file_path} to {bucket_name}/{blob_path}')
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(file_path)
