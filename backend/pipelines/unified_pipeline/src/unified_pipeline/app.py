"""
Main application module for the unified pipeline.

This module contains the main entry point and CLI interface for the unified
data pipeline application. It orchestrates different data processing stages
(bronze, silver) for various data sources.
"""

import asyncio

import click
from dotenv import load_dotenv

from unified_pipeline.bronze.agricultural_fields import (
    AgriculturalFieldsBronze,
    AgriculturalFieldsBronzeConfig,
)
from unified_pipeline.bronze.bnbo_status import BNBOStatusBronze, BNBOStatusBronzeConfig
from unified_pipeline.bronze.cadastral import CadastralBronze, CadastralBronzeConfig
from unified_pipeline.bronze.water_projects import WaterProjectsBronze, WaterProjectsBronzeConfig
from unified_pipeline.bronze.wetlands import WetlandsBronze, WetlandsBronzeConfig
from unified_pipeline.model import cli
from unified_pipeline.model.app_config import GCSConfig
from unified_pipeline.silver.agricultural_fields import (
    AgriculturalFieldsSilver,
    AgriculturalFieldsSilverConfig,
)
from unified_pipeline.silver.bnbo_status import BNBOStatusSilver, BNBOStatusSilverConfig
from unified_pipeline.silver.cadastral import CadastralSilver, CadastralSilverConfig

# from unified_pipeline.silver.water_projects import WaterProjectsSilver, WaterProjectsSilverConfig
from unified_pipeline.silver.wetlands import WetlandsSilver, WetlandsSilverConfig
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger

load_dotenv()
def execute(cli_config: cli.CliConfig) -> None:
    """
    Main execution function for processing pipeline data.

    This function initializes the appropriate data processing pipeline based on
    the provided CLI configuration. It handles source selection and processing
    stage (bronze, silver, or all stages).

    Args:
        cli_config (cli.CliConfig): Configuration containing source and stage settings

    Raises:
        ValueError: If the requested source/stage combination is not supported
    """
    log = Logger.get_logger()
    log.info("Starting Unified Pipeline.")

    gcs_util = GCSUtil(GCSConfig())

    # Define pipeline mapping for sources and stages
    pipeline_map = {
        cli.Source.bnbo: {
            cli.Stage.bronze: [(BNBOStatusBronze, BNBOStatusBronzeConfig)],
            cli.Stage.silver: [(BNBOStatusSilver, BNBOStatusSilverConfig)],
            cli.Stage.all: [
                (BNBOStatusBronze, BNBOStatusBronzeConfig),
                (BNBOStatusSilver, BNBOStatusSilverConfig),
            ],
        },
        cli.Source.agricultural_fields: {
            cli.Stage.bronze: [(AgriculturalFieldsBronze, AgriculturalFieldsBronzeConfig)],
            cli.Stage.silver: [(AgriculturalFieldsSilver, AgriculturalFieldsSilverConfig)],
            cli.Stage.all: [
                (AgriculturalFieldsBronze, AgriculturalFieldsBronzeConfig),
                (AgriculturalFieldsSilver, AgriculturalFieldsSilverConfig),
            ],
        },
        cli.Source.cadastral: {
            cli.Stage.bronze: [(CadastralBronze, CadastralBronzeConfig)],
            cli.Stage.silver: [(CadastralSilver, CadastralSilverConfig)],
            cli.Stage.all: [
                (CadastralBronze, CadastralBronzeConfig),
                (CadastralSilver, CadastralSilverConfig),
            ],
        },
        cli.Source.wetlands: {
            cli.Stage.bronze: [(WetlandsBronze, WetlandsBronzeConfig)],
            cli.Stage.silver: [(WetlandsSilver, WetlandsSilverConfig)],
            cli.Stage.all: [
                (WetlandsBronze, WetlandsBronzeConfig),
                (WetlandsSilver, WetlandsSilverConfig),
            ],
        },
        cli.Source.water_projects: {
            cli.Stage.bronze: [(WaterProjectsBronze, WaterProjectsBronzeConfig)],
            # cli.Stage.silver: [(WaterProjectsSilver, WaterProjectsSilverConfig)],
            cli.Stage.all: [
                (WaterProjectsBronze, WaterProjectsBronzeConfig),
                # (WaterProjectsSilver, WaterProjectsSilverConfig),
            ],
        },
    }
    # Retrieve jobs for given source and stage
    try:
        jobs = pipeline_map[cli_config.source][cli_config.stage]
    except KeyError:
        raise ValueError(f"Source {cli_config.source} and stage {cli_config.stage} not supported.")
    # Execute each job sequentially
    for job_cls, config_cls in jobs:
        log.info(f"Running {job_cls.__name__} for stage {cli_config.stage}")
        instance = job_cls(config=config_cls(), gcs_util=gcs_util)
        asyncio.run(instance.run())
        log.info(f"Finished {job_cls.__name__} for stage {cli_config.stage}")
    log.info(f"Finished running source {cli_config.source} in stage {cli_config.stage}.")


@click.command()
@click.option(
    "-e",
    "--env",
    "env",
    help="The environment to use. Default is prod.",
    type=click.Choice([env.value for env in cli.Env]),
    default="prod",
)
@click.option(
    "-s",
    "--source",
    "source",
    help="The source to use.",
    type=click.Choice([source.value for source in cli.Source]),
    required=True,
)
@click.option(
    "-j",
    "--stage",
    "stage",
    type=click.Choice([mode.value for mode in cli.Stage]),
    help="The stage to use. The options are bronze, silver, and all.",
    required=True,
)
def run_cli(
    env: str,
    source: str,
    stage: str,
) -> None:
    """
    CLI entry point for the unified pipeline application.

    This function parses command-line arguments and initializes the pipeline
    with the appropriate configuration. It serves as the main entry point
    when running the application from the command line.

    Args:
        env: The environment to use (prod, dev, etc.)
        source: The data source to process
        stage: The processing stage (bronze, silver, all)

    Example:
        $ python -m unified_pipeline -s bnbo -j bronze
    """
    app_config = cli.CliConfig(
        env=cli.Env(env),
        source=cli.Source(source),
        stage=cli.Stage(stage),
    )
    print(app_config)
    execute(app_config)
