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

from gold.process import upload_nature_report_data


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
    gdf = gdf.to_crs('EPSG:4326')
    save_gpkg_file(gdf, gpkg_file)
    upload_to_gcs(gpkg_file)

    upload_nature_report_data(
        report_name='Mere, bedre og større natur i Danmark',
        report_data=gdf,
        category_scores={
            '1. prioritet': {'biodiversity': 1.0, 'climate': 0.7, 'nitrogen': 0.5, 'recreation': 0.5},
            '2A. prioritet': {'biodiversity': 0.85, 'climate': 0.6, 'nitrogen': 0.4, 'recreation': 0.4},
            '2B. prioritet': {'biodiversity': 0.4, 'climate': 0.8, 'nitrogen': 0.6, 'recreation': 0.5},
            '3. prioritet': {'biodiversity': 0.3, 'climate': 0.6, 'nitrogen': 0.6, 'recreation': 0.5},
            '4. prioritet': {'biodiversity': 0.2, 'climate': 0.4, 'nitrogen': 0.4, 'recreation': 0.4}
        },
        category_column='Prioritet',
        geometry_column='geometry'
    )
   

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
    gdf = gdf.to_crs('EPSG:4326')
    save_gpkg_file(gdf, gpkg_file)
    #upload_to_gcs(gpkg_file)

    upload_nature_report_data(
        report_name='Prioritering af biodiversitet ved udtagning og genopretning af kulstofrige lavbundsjorder',
        report_data=gdf,
        category_scores={
            1:  {'biodiversity': 1.0, 'climate': 0.8, 'nitrogen': 0.6, 'recreation': 0.7},
            2:  {'biodiversity': 0.95, 'climate': 0.8, 'nitrogen': 0.6, 'recreation': 0.7},
            3:  {'biodiversity': 0.9, 'climate': 0.75, 'nitrogen': 0.6, 'recreation': 0.6},
            4:  {'biodiversity': 0.8, 'climate': 0.7, 'nitrogen': 0.6, 'recreation': 0.6},
            5:  {'biodiversity': 0.7, 'climate': 0.7, 'nitrogen': 0.6, 'recreation': 0.6},
            6:  {'biodiversity': 0.6, 'climate': 0.7, 'nitrogen': 0.6, 'recreation': 0.6},
            7:  {'biodiversity': 0.5, 'climate': 0.65, 'nitrogen': 0.6, 'recreation': 0.5},
            8:  {'biodiversity': 0.4, 'climate': 0.65, 'nitrogen': 0.6, 'recreation': 0.5},
            9:  {'biodiversity': 0.3, 'climate': 0.6, 'nitrogen': 0.7, 'recreation': 0.4},
            10: {'biodiversity': 0.2, 'climate': 0.5, 'nitrogen': 0.7, 'recreation': 0.4},
            11: {'biodiversity': 0.1, 'climate': 0.4, 'nitrogen': 0.6, 'recreation': 0.3},
            12: {'biodiversity': 0.0, 'climate': 0.1, 'nitrogen': 0.1, 'recreation': 0.2}
        },
        category_column='kategori',
        geometry_column='geometry'
    )



def main():
    #mere_bedre_stoerre_natur()
    prioritering_af_biodiversitet_ved_udtagning_og_genopretning_af_kulstofrige_lavbundsjorde()



if __name__ == "__main__":
    main()