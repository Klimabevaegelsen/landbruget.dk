"""
CLI configuration module for the unified pipeline.

This module defines the configuration options available when running the
unified pipeline from the command line. It contains enums for environment,
data sources, and processing stages, as well as a Pydantic model for
validating and managing CLI configuration.
"""

from enum import Enum

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
        agricultural_fields: Agricultural fields data source
        wetlands: Wetlands data source
    """

    bnbo = "bnbo"
    agricultural_fields = "agricultural_fields"
    cadastral = "cadastral"
    wetlands = "wetlands"
    water_projects = "water_projects"


class Stage(Enum):
    """
    Processing stage options for the pipeline.

    The unified pipeline follows a medallion architecture with multiple
    processing stages.

    Attributes:
        bronze: Initial data ingestion stage with minimal transformations
        silver: Cleaned and transformed data stage
        all: Process both bronze and silver stages sequentially
    """

    bronze = "bronze"
    silver = "silver"
    all = "all"


class CliConfig(BaseModel):
    """
    Configuration model for command-line interface options.

    This model validates and stores the configuration options provided via
    the command line when running the pipeline.

    Attributes:
        env: The environment to use (local, dev, prod)
        source: The data source to process
        stage: The processing stage (bronze, silver, all)

    Example:
        >>> config = CliConfig(source=Source.bnbo, stage=Stage.bronze)
        >>> print(f"Processing {config.source.value} in {config.stage.value} stage")
    """

    env: Env = Env.prod
    source: Source
    stage: Stage
