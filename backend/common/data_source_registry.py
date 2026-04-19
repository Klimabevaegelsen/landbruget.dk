from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class DataSourceType(StrEnum):
    """Types of data sources"""

    GOOGLE_DRIVE = "google_drive"
    API_REST = "api_rest"
    API_SOAP = "api_soap"
    WFS = "wfs"
    SFTP = "sftp"
    SCRAPING = "scraping"
    DATABASE = "database"
    FILE_UPLOAD = "file_upload"


class DataSourceInfo(BaseModel):
    """Manual configuration for data source business context"""

    # Business Context (YOU MUST FILL THESE IN)
    source_authority: str = Field(..., description="Who provided the data (e.g., 'Miljøstyrelsen', 'SEGES')")
    source_contact: str | None = Field(None, description="Contact person/email at the authority")
    data_acquisition_date: datetime | None = Field(None, description="When we obtained this data (manual entry)")
    data_acquisition_method: str = Field(
        ..., description="How we got it: 'API', 'Manual Upload', 'SFTP', 'Email', etc."
    )

    # Documentation (OPTIONAL - made documentation_url optional as requested)
    documentation_url: str | None = Field(None, description="Link to README/docs explaining this pipeline")
    data_description: str = Field(..., description="What this data represents")
    update_frequency: str | None = Field(None, description="How often this data is updated")

    # Technical Context
    pipeline_name: str = Field(..., description="Pipeline that processes this data")
    data_format: str = Field(..., description="Original format: 'Excel', 'JSON', 'XML', 'GeoJSON', etc.")
    data_source_type: DataSourceType = Field(..., description="Type of data source")

    # Frontend Display (YOU MUST PROVIDE THESE)
    display_name: str = Field(..., description="User-friendly name for frontend")
    display_description: str = Field(..., description="User-friendly description for frontend")

    # Custom metadata per source
    custom_fields: dict[str, Any] = Field(default_factory=dict)


# Central registry - YOU MUST CONFIGURE EACH DATA SOURCE HERE
# 🚨 MANUAL INPUT REQUIRED: Configure all your data sources below
DATA_SOURCE_REGISTRY: dict[str, DataSourceInfo] = {
    # ========================================
    # UNIFIED PIPELINE SOURCES
    # ========================================
    # WFS Sources from Danish authorities
    "cadastral": DataSourceInfo(
        source_authority="Geodatastyrelsen",
        source_contact="gst@gst.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="Official Danish cadastral parcel boundaries",
        update_frequency="Weekly",
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Matrikelgrænser",
        display_description="Officielle grænser for alle danske ejendomme og jordlodder",
        custom_fields={
            "wfs_endpoint": "https://wfs.dataforsyning.dk/",
            "layer_name": "cadastral_parcels",
        },
    ),
    "agricultural_fields": DataSourceInfo(
        source_authority="Vejdirektoratet",
        source_contact="vst@vst.dk",  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="ArcGIS REST API",
        data_description="Danish agricultural field and block boundaries",
        update_frequency="Weekly",
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.API_REST,
        display_name="Landbrugsarealer",
        display_description="Officielle landbrugsmarkgrænser og markblokdata",
        custom_fields={"api_type": "ArcGIS_REST"},
    ),
    "bnbo": DataSourceInfo(
        source_authority="Miljøstyrelsen",
        source_contact="mst@mst.dk",  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="SOAP API",
        data_description="Boringsnære beskyttelsesområder (Well Protection Areas) data",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="XML",
        data_source_type=DataSourceType.API_SOAP,
        display_name="Boringsnære Beskyttelsesområder",
        display_description="Beskyttelsesområder omkring vandboringer og brønde",
    ),
    "soil_types": DataSourceInfo(
        source_authority="Det Jordbrugsvidenskabelige Fakultet",
        source_contact="djf@agrsci.dk",  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="Danish soil types data from Environmental Portal",
        update_frequency="Annually",
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Jordtyper",
        display_description="Klassifikation af jordtyper i Danmark",
        custom_fields={
            "wfs_endpoint": "https://arld-extgeo.miljoeportal.dk/geoserver/wfs",
            "layer_name": "landbrugsdrift:DJF_FGJOR",
        },
    ),
    "dagi": DataSourceInfo(
        source_authority="Klimadatastyrelsen",
        source_contact="kds@kds.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="Danish Administrative Geographic Division boundaries",
        update_frequency="Annually",
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Danmarks Administrative Geografiske Inddelinger (DAGI)",
        display_description="Officielle administrative grænser (kommuner, regioner, mv.)",
    ),
    "jordbrugsanalyser": DataSourceInfo(
        source_authority="Landbrugsstyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="Danish agricultural analysis markers from WFS",
        update_frequency="Monthly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Landbrugsanalysemærkater",
        display_description="Markører til landbrugsanalyse og overvågning",
    ),
    "fvm_wfs": DataSourceInfo(
        source_authority="Landbrugsstyrelsen (FVM)",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="Danish FVM WFS Agricultural Data (Markblokke, Marker, Smaabiotoper)",
        update_frequency="Weekly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="FVM Landbrugsdata",
        display_description="Markblokke, markører og småbiotoper fra FVM-systemet",
    ),
    "wetlands": DataSourceInfo(
        source_authority="Miljøstyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="Danish wetlands data",
        update_frequency="Annually",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Vådområder",
        display_description="Beskyttede vådområder og habitater",
    ),
    "water_projects": DataSourceInfo(
        source_authority="Miljøstyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="Water management and restoration projects",
        update_frequency="Quarterly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Vandprojekter",
        display_description="Vandforvaltning og miljøgenopretningsprojekter",
    ),
    "water_typology": DataSourceInfo(
        source_authority="Miljøstyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="Water typology data (lakes, coastal waters, watercourses)",
        update_frequency="Annually",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="unified_pipeline",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="Vandtypologi",
        display_description="Klassifikation af vandområder efter type og karakteristika",
    ),
    "dst": DataSourceInfo(
        source_authority="Danmarks Statistik",
        source_contact="dst@dst.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="API",
        data_description="Danish Statistics API data",
        update_frequency="Varies by dataset",
        pipeline_name="unified_pipeline",
        data_format="JSON",
        data_source_type=DataSourceType.API_REST,
        display_name="Danmarks Statistik",
        display_description="Officiel statistik fra Danmarks Statistik",
        custom_fields={"api_base_url": "https://api.statbank.dk/"},
    ),
    "dmi": DataSourceInfo(
        source_authority="Danmarks Meteorologiske Institut (DMI)",
        source_contact="dmi@dmi.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="API",
        data_description="Danish Meteorological Institute climate data",
        update_frequency="Daily",
        pipeline_name="unified_pipeline",
        data_format="JSON/XML",
        data_source_type=DataSourceType.API_REST,
        display_name="Vejr- og Klimadata",
        display_description="Meteorologiske og klimadata fra DMI",
    ),
    # Gold layer combined datasets
    "property_cadastral_merge": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Combined property and cadastral dataset",
        update_frequency="Weekly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Ejendom-Matrikel Sammenfletning",
        display_description="Kombineret datasæt der sammenfletning ejendoms- og matrikeloplysninger",
    ),
    "field_production": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Field production estimates based on agricultural data",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Markproduktionsestimater",
        display_description="Estimeret landbrugsproduktion pr. mark",
    ),
    "field_area_analysis": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Comprehensive field area analysis",
        update_frequency="Weekly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Markarealanalyse",
        display_description="Detaljeret analyse af landbrugsmarkers arealer og karakteristika",
    ),
    "pesticide_disaggregation": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Disaggregated pesticide usage data by field",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Pesticidforbrugsanalyse",
        display_description="Detaljeret pesticidanvendelse opdelt efter mark og kemikalie",
    ),
    "pesticide_proximity": DataSourceInfo(
        source_authority="  Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Pesticide proximity analysis to sensitive areas",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Pesticid Nærhedsanalyse",
        display_description="Analyse af pesticidanvendelse i nærheden af skoler, vandkilder, osv.",
    ),
    "pesticide_compliance": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",
        data_acquisition_method="Data Pipeline Processing",
        data_description="Pesticide regulatory compliance analysis",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="Pesticid Overholdelse",
        display_description="Analyse af pesticidanvendelses overholdelse af regler",
    ),
    "pesticide_applications": DataSourceInfo(
        source_authority="Multiple (Pesticide Disaggregation + BMD)",
        data_acquisition_method="Data Processing Pipeline",
        data_description="Field-level pesticide applications with risk assessments and company linkage",
        update_frequency="Monthly",
        pipeline_name="unified_pipeline",
        data_format="Database Table",
        data_source_type=DataSourceType.DATABASE,
        display_name="Pesticidanvendelse (Mark-niveau)",
        display_description="Detaljeret pesticidanvendelse pr. mark med risikovurdering og virksomhedstilknytning",
    ),
    "kemidata_surface_water_pesticides": DataSourceInfo(
        source_authority="Danmarks Miljøportal",
        source_contact=None,
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="REST API",
        data_description="Pesticide detections in surface water (rivers, lakes) from Kemidata/VanDa discrete sampling",
        update_frequency="Quarterly",
        pipeline_name="unified_pipeline",
        data_format="CSV",
        data_source_type=DataSourceType.API_REST,
        display_name="Overfladevand Pesticider (Kemidata)",
        display_description="Laboratorieanalyser af pesticidkoncentrationer i danske vandløb og søer",
        custom_fields={
            "api_base_url": "https://kemidata.miljoeportal.dk/api/",
            "media_types": ["Vandløb", "Sø"],
        },
    ),
    # ========================================
    # CHR PIPELINE SOURCES
    # ========================================
    "chr_animal_movements": DataSourceInfo(
        source_authority="Fødevarestyrelsen",
        source_contact="chr@fvst.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="API",
        data_description="Live animal movement data from the Central Husbandry Register",
        update_frequency="Weekly",
        pipeline_name="chr_pipeline",
        data_format="JSON",
        data_source_type=DataSourceType.API_REST,
        display_name="Dyreflytninger",
        display_description="Realtidssporing af husdyrflytninger mellem bedrifter",
        custom_fields={
            "api_endpoint": "https://chr.fvst.dk/api/",
            "requires_authentication": True,
        },
    ),
    "chr_properties": DataSourceInfo(
        source_authority="Fødevarestyrelsen",
        source_contact="chr@fvst.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="API",
        data_description="Property information from CHR system",
        update_frequency="Weekly",
        pipeline_name="chr_pipeline",
        data_format="JSON",
        data_source_type=DataSourceType.API_REST,
        display_name="CHR Ejendomme",
        display_description="Bedrifts- og ejendomsoplysninger fra Centrale Husdyrregister",
    ),
    "chr_herds": DataSourceInfo(
        source_authority="Fødevarestyrelsen",
        source_contact="chr@fvst.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="API",
        data_description="Herd information from CHR system",
        update_frequency="Weekly",
        pipeline_name="chr_pipeline",
        data_format="JSON",
        data_source_type=DataSourceType.API_REST,
        display_name="CHR Herds",
        display_description="Livestock herd information and management data",
    ),
    "spf_su_herds": DataSourceInfo(
        source_authority="SPF-Sund",
        source_contact="sundhedsstyringen@lf.dk",
        data_acquisition_date=None,
        data_acquisition_method="API",
        data_description="SPF-SU herd health monitoring data",
        update_frequency="Weekly",
        pipeline_name="chr_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.FILE_UPLOAD,
        display_name="SPF-SU Besætningsdata",
        display_description="Besætningssundhedsovervågning fra SPF-SU systemet",
    ),
    "spf_su_salmonella": DataSourceInfo(
        source_authority="SPF-SU",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # 🚨 MANUAL INPUT NEEDED: Add acquisition date
        data_acquisition_method="Manual Upload",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="SPF-SU salmonella monitoring data",
        update_frequency="Monthly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="chr_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.FILE_UPLOAD,
        display_name="SPF-SU Salmonelladata",
        display_description="Salmonellaovervågning og testdata fra SPF-SU",
    ),
    # International animal movement data
    "intl_cattle_traces": DataSourceInfo(
        source_authority="Fødevarestyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # 🚨 MANUAL INPUT NEEDED: Add acquisition date
        data_acquisition_method="Manual Upload",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="International cattle movement traces",
        update_frequency="Monthly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="chr_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.FILE_UPLOAD,
        display_name="Internationale Kvægflytninger",
        display_description="Grænseoverskridende kvægflytningssporing",
    ),
    "intl_pig_movements": DataSourceInfo(
        source_authority="Fødevarestyrelsen",  # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # 🚨 MANUAL INPUT NEEDED: Add acquisition date
        data_acquisition_method="Manual Upload",  # 🚨 MANUAL INPUT NEEDED: Verify method
        data_description="International pig movement data",
        update_frequency="Monthly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="chr_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.FILE_UPLOAD,
        display_name="Internationale Svineflytninger",
        display_description="Grænseoverskridende svineflytningssporing",
    ),
    # ========================================
    # DRIVE DATA PIPELINE SOURCES
    # ========================================
    # 🚨 MANUAL INPUT NEEDED: Replace these with your actual Google Drive subfolders
    # Each subfolder in your Google Drive should have its own entry here
    "drive_animal_international_movements": DataSourceInfo(
        source_authority="International Livestock Authorities",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="movements@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="International animal movement tracking data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Internationale Dyreflytninger",
        display_description="Grænseoverskridende sporing af husdyrflytninger",
    ),
    "drive_animal_mortality": DataSourceInfo(
        source_authority="Veterinary Authorities",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="mortality@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Animal mortality reporting data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Dyredødelighed",
        display_description="Sporing og rapportering af husdyrdødelighed",
    ),
    "drive_animal_welfare": DataSourceInfo(
        source_authority="Animal Welfare Inspectors",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="welfare@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Animal welfare inspection reports and compliance data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Dyrevelfærd",
        display_description="Dyrevelfærdstilsyn og overholdelsesrapporter",
    ),
    "drive_fertiliser": DataSourceInfo(
        source_authority="Fertilizer Companies/Consultants",  # 🚨 YOU FILL: Who uploads fertilizer data?
        source_contact="fertilizer@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Fertilizer usage and composition data",
        update_frequency="Quarterly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Gødning",
        display_description="Gødningsanvendelse og sammensætningsdata fra producenter og konsulenter",
    ),
    "drive_pesticides": DataSourceInfo(
        source_authority="SEGES Innovation/Agricultural Consultants",  # 🚨 YOU FILL: Verify authority
        source_contact="pesticides@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Pesticide usage reports and application data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Pesticider",
        display_description="Pesticidanvendelsesrapporter og forbrugsdata",
    ),
    "drive_pig_tail_cutting": DataSourceInfo(
        source_authority="Pig Farmers/Veterinary Services",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="pigtail@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Pig tail cutting procedure reports and compliance data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Svinehaleafskæring",
        display_description="Rapporter om svinehaleafskæringsprocedurer og dyrevelfærdsoverholdelse",
    ),
    "drive_slurry_leaks": DataSourceInfo(
        source_authority="Environmental Authorities/Farmers",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="slurry@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Slurry leak incident reports and environmental impact data",
        update_frequency="As needed",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Gyllelækager",
        display_description="Slurry leak incident reports and environmental compliance data",
    ),
    "drive_stable_fires": DataSourceInfo(
        source_authority="Fire Departments/Insurance Companies",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="fires@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Stable fire incident reports and safety compliance data",
        update_frequency="As needed",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Staldebrande",
        display_description="Agricultural facility fire incident reports and safety data",
    ),
    "drive_subsidies": DataSourceInfo(
        source_authority="Danish Agricultural Agency",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="subsidies@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Agricultural subsidy applications and payment data",
        update_frequency="Quarterly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Tilskud",
        display_description="Agricultural subsidy and payment tracking data",
    ),
    "drive_transportation_accidents": DataSourceInfo(
        source_authority="Transportation Authorities/Police",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="transport@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Livestock transportation accident reports and safety data",
        update_frequency="As needed",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Transportulykker",
        display_description="Husdyrtransportulykker og sikkerhedsoverholdelse",
    ),
    "drive_work_permits": DataSourceInfo(
        source_authority="Danish Immigration Service/Employers",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="permits@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Agricultural worker permit applications and approvals",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Arbejdstilladelser",
        display_description="Agricultural sector work permit and employment data",
    ),
    "drive_worker_safety": DataSourceInfo(
        source_authority="Arbejdstilsynet/Safety Inspectors",  # 🚨 YOU FILL: Who uploads this data?
        source_contact="safety@example.dk",  # 🚨 YOU FILL: Contact email
        data_acquisition_date=datetime(2023, 1, 1),  # 🚨 YOU FILL: When did you start getting this?
        data_acquisition_method="Manual Upload to Google Drive",
        data_description="Agricultural worker safety inspection reports and compliance data",
        update_frequency="Monthly",  # 🚨 YOU FILL: How often do they upload?
        pipeline_name="drive_data_pipeline",
        data_format="Excel/PDF",
        data_source_type=DataSourceType.GOOGLE_DRIVE,
        display_name="Arbejdssikkerhed",
        display_description="Agricultural workplace safety inspection and compliance reports",
    ),
    # ========================================
    # BBR BUILDINGS PIPELINE SOURCES
    # ========================================
    "bbr_inspire_buildings": DataSourceInfo(
        source_authority="Styrelsen for Dataforsyning og Infrastruktur (SDFI)",
        source_contact="gst@sdfi.dk",
        data_acquisition_date=None,  # 🚨 MANUAL INPUT NEEDED: Add when file was obtained
        data_acquisition_method="File Download",
        data_description="Danish building data from Bygnings- og Boligregistret (BBR) with INSPIRE standardization",
        update_frequency="Static file, manual updates available",
        pipeline_name="bbr_buildings",
        data_format="GeoPackage",
        data_source_type=DataSourceType.FILE_UPLOAD,
        display_name="BBR Bygningsdata",
        display_description="Komplet BBR bygningsdata inkl. landbrugsstrukturer",
        custom_fields={
            "file_url": "https://ftp.dataforsyningen.dk/Bygninger_og_Adresser/Bygninger_og_Adresser_INSPIRE/DK_INSPIRE_BBR.zip",
            "file_size_gb": "~0.76",
        },
    ),
    "geodanmark_buildings": DataSourceInfo(
        source_authority="Styrelsen for Dataforsyning og Infrastruktur (SDFI)",
        source_contact="gst@sdfi.dk",
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="WFS API",
        data_description="GeoDanmark building footprints with BBRUUID links",
        update_frequency="Weekly",
        pipeline_name="bbr_buildings",
        data_format="GeoJSON",
        data_source_type=DataSourceType.WFS,
        display_name="GeoDanmark Bygninger",
        display_description="Bygningsomrids til krydsreference og forbedret klassifikation",
        custom_fields={
            "wfs_endpoint": "https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS",
            "requires_authentication": True,
        },
    ),
    # ========================================
    # ARBEJDSTILSYNET INSPECTIONS PIPELINE
    # ========================================
    "arbejdstilsynet_inspections": DataSourceInfo(
        source_authority="Arbejdstilsynet (Danish Working Environment Authority)",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # Scraping - always current
        data_acquisition_method="Web Scraping",
        data_description="Danish Work Environment Authority inspections data",
        update_frequency="Daily",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="arbejdstilsynet_inspections",
        data_format="CSV",
        data_source_type=DataSourceType.SCRAPING,
        display_name="Arbejdsmiljøtilsyn",
        display_description="Tilsynsdata fra Arbejdstilsynet",
        custom_fields={
            "source_url": "https://publicdata.at.dk/reports/powerbi/Data%20på%20nettet/Tilsynsindblikket?rs:embed=true"
        },
    ),
    # ========================================
    # DMA SCRAPER PIPELINE
    # ========================================
    "dma_companies": DataSourceInfo(
        source_authority="Erhvervsstyrelsen (Danish Business Authority)",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # Scraping - always current
        data_acquisition_method="Web Scraping",
        data_description="Company data from the Danish Business Authority (DMA)",
        update_frequency="Monthly",
        pipeline_name="dma_scraper",
        data_format="JSON",
        data_source_type=DataSourceType.SCRAPING,
        display_name="DMA Virksomhedsdata",
        display_description="Virksomhedsoplysninger og miljøtilladelser fra Erhvervsstyrelsen",
        custom_fields={"api_endpoint": "https://dma.mst.dk/soeg/page"},
    ),
    # ========================================
    # BMD SCRAPER PIPELINE
    # ========================================
    "bmd_pesticide_database": DataSourceInfo(
        source_authority="Miljøstyrelsen (Danish EPA)",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # Scraping - always current
        data_acquisition_method="Web Scraping",
        data_description="Pesticide data from the Danish Bekæmpelsesmiddel Database (BMD)",
        update_frequency="Weekly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="bmd_scraper",
        data_format="Excel",
        data_source_type=DataSourceType.SCRAPING,
        display_name="BMD Pesticide Database",
        display_description="Official Danish pesticide registration and approval database",
    ),
    # ========================================
    # SVINEFLYTNING PIPELINE
    # ========================================
    "svineflytning_movements": DataSourceInfo(
        source_authority="Landbrugsstyrelsen (FVM)",
        source_contact=None,  # 🚨 MANUAL INPUT NEEDED: Add contact if available
        data_acquisition_date=None,  # API - always current
        data_acquisition_method="SOAP API",
        data_description="Pig movement data from SvineflytningWS SOAP service",
        update_frequency="Real-time",
        pipeline_name="svineflytning_pipeline",
        data_format="JSON",
        data_source_type=DataSourceType.API_SOAP,
        display_name="Svineflytninger",
        display_description="Realtidssporing af svineflytninger mellem bedrifter",
        custom_fields={
            "soap_service": "SvineflytningWS",
            "requires_fvm_credentials": True,
        },
    ),
    # ========================================
    # PROPERTY OWNERS SFTP PIPELINE
    # ========================================
    "property_owners_sftp": DataSourceInfo(
        source_authority="Styrelsen for Dataforsyning og Infrastruktur (SDFI)",
        # 🚨 MANUAL INPUT NEEDED: Verify authority
        source_contact="gst@sdfi.dk",  # 🚨 MANUAL INPUT NEEDED: Verify contact
        data_acquisition_date=None,  # 🚨 MANUAL INPUT NEEDED: Add acquisition date
        data_acquisition_method="SFTP",
        data_description="Property ownership data via SFTP",
        update_frequency="Weekly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="property_owners_sftp",
        data_format="CSV/XML",  # 🚨 MANUAL INPUT NEEDED: Verify format
        data_source_type=DataSourceType.SFTP,
        display_name="Ejendomsejerskabsdata",
        display_description="Ejendomsejerskabsoplysninger via SFTP-overførsel",
    ),
    # ========================================
    # H3 PFAS EXPOSURE PIPELINE
    # ========================================
    "h3_pfas_exposure": DataSourceInfo(
        source_authority="Multiple (Combined Dataset)",  # 🚨 MANUAL INPUT NEEDED: Verify sources
        data_acquisition_method="Data Pipeline Processing",
        data_description="H3-gridded PFAS exposure analysis combining multiple data sources",
        update_frequency="Monthly",  # 🚨 MANUAL INPUT NEEDED: Verify frequency
        pipeline_name="h3_pfas_exposure_pipeline",
        data_format="Parquet",
        data_source_type=DataSourceType.DATABASE,
        display_name="PFAS Eksponeringsanalyse",
        display_description="Rumlig analyse af PFAS-forureningseksponering ved hjælp af H3-gridsystem",
    ),
    # ========================================
    # GEUS DATAVERSE PESTICIDES PIPELINE
    # ========================================
    "geus_dataverse_pesticides": DataSourceInfo(
        source_authority="GEUS (De Nationale Geologiske Undersøgelser for Danmark og Grønland)",
        documentation_url="https://doi.org/10.22008/FK2/IHVDXL",
        data_acquisition_method="HTTP API (Dataverse)",
        data_description="Groundwater pesticide analyses from GEUS Dataverse (633 substances, 4.2M+ analyses, 1981-2025)",
        update_frequency="Monthly",
        pipeline_name="geus_dataverse_pesticides",
        data_format="RDS/Parquet",
        data_source_type=DataSourceType.API_REST,
        display_name="GEUS Grundvandspesticider",
        display_description="Pesticidfund i grundvand fra GEUS Dataverse (633 stoffer, 4,2M+ analyser, 1981-2025)",
    ),
    "geus_dataverse_pfas": DataSourceInfo(
        source_authority="GEUS (De Nationale Geologiske Undersøgelser for Danmark og Grønland)",
        documentation_url="https://doi.org/10.22008/FK2/IHVDXL",
        data_acquisition_method="HTTP API (Dataverse)",
        data_description="Groundwater PFAS analyses from GEUS Dataverse (26 substances, 2012-2025)",
        update_frequency="Monthly",
        pipeline_name="geus_dataverse_pesticides",
        data_format="RDS/Parquet",
        data_source_type=DataSourceType.API_REST,
        display_name="GEUS Grundvands-PFAS",
        display_description="PFAS-fund i grundvand fra GEUS Dataverse (26 stoffer, 2012-2025)",
    ),
    "geus_clean_pesticides": DataSourceInfo(
        source_authority="GEUS (De Nationale Geologiske Undersøgelser for Danmark og Grønland)",
        documentation_url="https://doi.org/10.22008/FK2/IHVDXL",
        data_acquisition_method="HTTP API (Dataverse)",
        data_description="Sample-level clean groundwater pesticide dataset from GEUS Dataverse (Deliverable 2, ~9.4M analyses)",
        update_frequency="Monthly",
        pipeline_name="geus_dataverse_pesticides",
        data_format="RDS/Parquet",
        data_source_type=DataSourceType.API_REST,
        display_name="GEUS Rene Grundvandspesticider",
        display_description="Prøveniveau grundvandspesticiddata fra GEUS Dataverse (Leverance 2, ~9,4M analyser)",
    ),
    "geus_clean_all": DataSourceInfo(
        source_authority="GEUS (De Nationale Geologiske Undersøgelser for Danmark og Grønland)",
        documentation_url="https://doi.org/10.22008/FK2/IHVDXL",
        data_acquisition_method="HTTP API (Dataverse)",
        data_description="Sample-level clean groundwater dataset from GEUS Dataverse (all parameter groups: pesticides, PFAS, nitrate, etc.)",
        update_frequency="Monthly",
        pipeline_name="geus_dataverse_pesticides",
        data_format="RDS/Parquet",
        data_source_type=DataSourceType.API_REST,
        display_name="GEUS Rent Grundvand (alle parametre)",
        display_description="Prøveniveau grundvandsdata fra GEUS Dataverse (alle parametergrupper: pesticider, PFAS, nitrat m.fl.)",
    ),
    # ADD MORE SOURCES HERE AS NEEDED
    # 🚨 MANUAL INPUT NEEDED: Review all entries above and fill in missing information
}


def get_source_info(source_key: str) -> DataSourceInfo:
    """Get source info with validation"""
    if source_key not in DATA_SOURCE_REGISTRY:
        raise ValueError(
            f"Source key '{source_key}' not found in registry. Available keys: {list(DATA_SOURCE_REGISTRY.keys())}"
        )
    return DATA_SOURCE_REGISTRY[source_key]


def list_available_sources() -> list[str]:
    """List all available source keys"""
    return list(DATA_SOURCE_REGISTRY.keys())
