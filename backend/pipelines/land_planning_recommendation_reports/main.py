import logging
import os
from pathlib import Path


from bronze.process import (
    make_request,
    fetch_file,
    extract_zip_file,
    gdp_read_file,
    save_gpkg_file,
    upload_to_gcs
)


logger = logging.getLogger(__name__)


DATA_DIR = Path("/data/raw/land_planning_recommendation_reports/")


def mere_bedre_stoerre_natur():
    """
        Petersen, A. H., B. Hasler, T. Laage-Thomsen, M. Termansen og C. Rahbek (2024):
        Mere, bedre og større natur i Danmark. Hvor, hvordan og hvor meget?
        Center for Makroøkologi, Evolution og Klima, Globe Institute, Københavns Universitet. 
    """
    url = (
        "https://macroecology.ku.dk/resources/mere-bedre-og-stoerre-natur-i-danmark-2024/"
        "GIS-data_Petersen-et-al-CMEC-2024.zip"
    )

    work_path = DATA_DIR / "mbs_natur"
    zip_file = work_path / "gis_data.zip"
    shp_file = work_path / "Mere_bedre_stoerre_239omr_CMEC2024_2.shp"
    gpkg_file = work_path / 'mbs_natur.gpkg'

    os.makedirs(work_path, exist_ok=True)

    fetch_file(url, zip_file)
    extract_zip_file(zip_file, work_path)
    gdf = gdp_read_file(shp_file)
    save_gpkg_file(gdf, gpkg_file)
    upload_to_gcs(gpkg_file)
   

def prioritering_af_biodiversitet_ved_udtagning_og_genopretning_af_kulstofrige_lavbundsjorde():
    """
        Brunbjerg, A.K., Bladt, J., Fløjgaard, C. & Ejrnæs, R. 2023. Prioritering af biodiversitet
        ved udtagning og genopretning af kulstofrige lavbundsjorder. Aarhus Universitet, DCE
        - Nationalt Center for Miljø og Energi,44 s. - Videnskabelig rapport nr. 544
        https://dce2.au.dk/pub/SR544.pdf
    """
    work_path = DATA_DIR / "prioritering_af_biodiv_kulstofrige_lavbundsjorde"
    gpkg_file = work_path / 'bioprio_lavbundsjorde.gpkg'

    os.makedirs(work_path, exist_ok=True)

    wfs_url = 'https://wfs2-miljoegis.mim.dk/vandprojekter/ows?version=1.0.0'
    params = {
        'SERVICE': 'WFS',
        'REQUEST': 'GetFeature',
        'VERSION': '1.0.0',
        'TYPENAME': 'kla_e23_bioprio',
        'OUTPUTFORMAT': 'application/json'
    }

    response = make_request(wfs_url, params)
    gdf = gdp_read_file(response.content)
    save_gpkg_file(gdf, gpkg_file)
    upload_to_gcs(gpkg_file)


def main():
    mere_bedre_stoerre_natur()
    prioritering_af_biodiversitet_ved_udtagning_og_genopretning_af_kulstofrige_lavbundsjorde()



if __name__ == "__main__":
    main()