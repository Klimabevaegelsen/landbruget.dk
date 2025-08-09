"""
CLI configuration module for the unified pipeline.

This module defines the configuration options available when running the
unified pipeline from the command line. It contains enums for environment,
data sources, and processing stages, as well as a Pydantic model for
validating and managing CLI configuration.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel


class Env(Enum):
    """
    Environment configuration options for the pipeline.

    Attributes:
        local: Local development environment
        dev: Development environment
        prod: Production environment
    """

    local = "local"
    dev = "dev"
    prod = "prod"


class Source(Enum):
    """
    Data source options for the pipeline.

    Attributes:
        bnbo: BNBO (Boringsnære beskyttelsesområder) data source
        agricultural_fields: Danish Agricultural Fields from ArcGIS API
        cadastral: Danish Cadastral Properties from WFS
        soil_types: Danish Soil Types from Environmental Portal WFS
        dagi: Danish Administrative Geographic Division from WFS
        jordbrugsanalyser: Danish Jordbrugsanalyser Markers from WFS
        fvm_wfs: Danish FVM WFS Agricultural Data (Markblokke, Marker, Smaabiotoper)
        wetlands: Wetlands data source
        water_projects: Water projects data source
        water_typology: Water typology data source (lakes, coastal waters, watercourses)
        property_cadastral_merge: Property-Cadastral merge gold layer
        field_production: Field production estimates gold layer
        field_area_analysis: Field area analysis gold layer
        pesticide_disaggregation: Pesticide disaggregation gold layer
        pesticide_proximity: Pesticide proximity analysis gold layer
        pesticide_compliance: Pesticide regulatory compliance analysis gold layer
        dst: Danish Statistics (Danmarks Statistik) API data source
        dmi: Danish Meteorological Institute (DMI) climate data source
        arbejdstilsynet_inspections: Danish Work Environment Authority inspections data
    """

    bnbo = "bnbo"
    agricultural_fields = "agricultural_fields"
    cadastral = "cadastral"

    soil_types = "soil_types"
    dagi = "dagi"
    jordbrugsanalyser = "jordbrugsanalyser"
    fvm_wfs = "fvm_wfs"
    wetlands = "wetlands"
    water_projects = "water_projects"
    water_typology = "water_typology"
    property_cadastral_merge = "property_cadastral_merge"
    field_production = "field_production"
    field_area_analysis = "field_area_analysis" 
    pesticide_disaggregation = "pesticide_disaggregation"
    pesticide_proximity = "pesticide_proximity"
    pesticide_compliance = "pesticide_compliance"
    cvr_enrichment = "cvr_enrichment"
    dst = "dst"
    dmi = "dmi"
    arbejdstilsynet_inspections = "arbejdstilsynet_inspections"
    worker_safety = "worker_safety"
    work_permits = "work_permits"


class Stage(Enum):
    """
    Processing stage options for the pipeline.

    The unified pipeline follows a medallion architecture with multiple
    processing stages.

    Attributes:
        bronze: Initial data ingestion stage with minimal transformations
        silver: Cleaned and transformed data stage
        gold: Business-ready combined datasets stage
        enrichment: Enrichment-only stage for post-processing existing silver data
        all: Process bronze, silver, and gold stages sequentially
    """

    bronze = "bronze"
    silver = "silver"
    gold = "gold"
    enrichment = "enrichment"
    all = "all"


class FVMLayerType(Enum):
    """
    FVM WFS layer type options for matrix job processing.

    Attributes:
        markblokke: Field blocks data (2005-2026)
        marker: Field markers data (2008-2025)
        smaabiotoper: Small biotopes data (2023-2025)
        organic_areas: Organic areas data (2012-2024)
        organic_subsidies: Organic subsidies data (2019-2024)
        grassland_subsidies: Grassland subsidies data (2019-2024)
        environmental_subsidies: Environmental subsidies data (2019-2023)
    """

    markblokke = "markblokke"
    marker = "marker"
    smaabiotoper = "smaabiotoper"
    organic_areas = "organic_areas"
    organic_subsidies = "organic_subsidies"
    grassland_subsidies = "grassland_subsidies"
    environmental_subsidies = "environmental_subsidies"


class CliConfig(BaseModel):
    """
    Configuration model for command-line interface options.

    This model validates and stores the configuration options provided via
    the command line when running the pipeline.

    Attributes:
        env: The environment to use (local, dev, prod)
        source: The data source to process
        stage: The processing stage (bronze, silver, gold, all)
        fvm_layer_type: Optional FVM layer type filter for matrix jobs
        fvm_year: Optional year filter for matrix jobs

    Example:
        >>> config = CliConfig(source=Source.bnbo, stage=Stage.bronze)
        >>> print(f"Processing {config.source.value} in {config.stage.value} stage")

        >>> # Matrix job example
        >>> config = CliConfig(source=Source.fvm_wfs, stage=Stage.bronze,
        ...                   fvm_layer_type=FVMLayerType.markblokke, fvm_year=2024)
    """

    env: Env = Env.prod
    source: Source
    stage: Stage
    fvm_layer_type: Optional[FVMLayerType] = None
    fvm_year: Optional[int] = None
    pesticide_year: Optional[int] = None
    # CVR enrichment specific parameters
    test_limit: Optional[int] = None
    parse_financial_xml: bool = True
    max_financial_documents: int = 10
