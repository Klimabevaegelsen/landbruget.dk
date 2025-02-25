SOURCES = {
    "agricultural_fields": {
        "name": "Danish Agricultural Fields",
        "type": "arcgis",
        "description": "Weekly updated agricultural field data",
        "urls": {
            "fields": "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/12/query",
            "blocks": "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/2/query",
            "fields_2023": "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/13/query",
            "blocks_2023": "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/7/query",
            "blocks_2024": "https://kort.vd.dk/server/rest/services/Grunddata/Marker_og_Markblokke/MapServer/6/query"
        },
        "frequency": "weekly",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data"
    },
    "wetlands": {
        "name": "Danish Wetlands Map",
        "type": "wfs",
        "description": "Wetland areas from Danish EPA",
        "url": "https://wfs2-miljoegis.mim.dk/natur/wfs",
        "layer": "natur:kulstof2022",
        "frequency": "static",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data",
        "create_dissolved": True
    },
    "cadastral": {
        "name": "Danish Cadastral Properties",
        "type": "wfs",
        "description": "Current real estate property boundaries",
        "url": "https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS",
        "frequency": "weekly",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data"
    },
    "water_projects": {
        "name": "Danish Water Projects",
        "type": "wfs",
        "description": "Water projects from various Danish programs",
        "url": "https://geodata.fvm.dk/geoserver/wfs",
        "url_mim": "https://wfs2-miljoegis.mim.dk/vandprojekter/wfs",
        "url_nst": "https://gis.nst.dk/server/rest/services/autonom/Klima_lavbund_demarkation___offentlige_projekter/FeatureServer",
        "frequency": "weekly",
        "enabled": True,
        "create_combined": True,
        "combined_timeout": 3600,
        "bucket": "landbrugsdata-raw-data",
        "create_dissolved": True
    },
    "crops": {
        "name": "Danish Agricultural Crop Codes",
        "type": "static",
        "description": "Reference data for crop codes and compensation categories",
        "frequency": "static",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data"
    },
    "property_owners": {
        "name": "Danish Property Owners",
        "description": "Property owner data from Datafordeleren",
        "frequency": "weekly",
        "enabled": True,
        "type": "sftp",
        "bucket": "landbrugsdata-raw-data",
        "raw_folder": "raw",
        "processed_folder": "processed"
    },
    "herd_data": {
        "name": "Danish Herd Data",
        "type": "soap",
        "description": "Herd data from CHR (Central Husbandry Register)",
        "frequency": "weekly",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data",
        "wsdl_urls": {
            "stamdata": "https://ws.fvst.dk/service/CHR_stamdataWS?WSDL",
            "besaetning": "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl",
            "ejendom": "https://ws.fvst.dk/service/CHR_ejendomWS?wsdl",
            "ejer": "https://ws.fvst.dk/service/CHR_ejerWS?wsdl"
        }
    },
    "bnbo_status": {
        "name": "Danish BNBO Status",
        "type": "wfs",
        "description": "Municipal status for well-near protection areas (BNBO)",
        "url": "https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs",
        "layer": "dai:status_bnbo",
        "frequency": "weekly",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data",
        "create_dissolved": True
    },
    "slaugther_premiums": {
        "name": "Danish Slaughter Premiums",
        "type": "static",
        "description": "Slaughter premiums from Danish Agriculture & Food Council",
        "frequency": "static",
        "enabled": True,
        "bucket": "landbrugsdata-raw-data"
    }
}
