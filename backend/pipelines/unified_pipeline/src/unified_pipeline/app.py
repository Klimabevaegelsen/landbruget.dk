"""
Main application module for the unified pipeline.

This module contains the main entry point and CLI interface for the unified
data pipeline application. It orchestrates different data processing stages
(bronze, silver) for various data sources.
"""

import asyncio
from typing import Optional

import click

from unified_pipeline.common.base import BaseSource
from unified_pipeline.model import cli
from unified_pipeline.model.app_config import GCSConfig
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger


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

    source: Optional[BaseSource] = None
    if cli_config.source == cli.Source.bnbo:
        if cli_config.stage == cli.Stage.bronze or cli_config.stage == cli.Stage.all:
            from unified_pipeline.bronze.bnbo_status import (
                BNBOStatusBronze,
                BNBOStatusBronzeConfig,
            )

            source = BNBOStatusBronze(
                config=BNBOStatusBronzeConfig(),
                gcs_util=gcs_util,
            )
        if cli_config.stage == cli.Stage.silver or cli_config.stage == cli.Stage.all:
            from unified_pipeline.silver.bnbo_status import (
                BNBOStatusSilver,
                BNBOStatusSilverConfig,
            )

            source = BNBOStatusSilver(
                config=BNBOStatusSilverConfig(),
                gcs_util=gcs_util,
            )
    elif cli_config.source == cli.Source.agricultural_fields:
        if cli_config.stage == cli.Stage.bronze or cli_config.stage == cli.Stage.all:
            from unified_pipeline.bronze.agricultural_fields import (
                AgriculturalFieldsBronze,
                AgriculturalFieldsBronzeConfig,
            )

            source = AgriculturalFieldsBronze(
                config=AgriculturalFieldsBronzeConfig(),
                gcs_util=gcs_util,
            )
        if cli_config.stage == cli.Stage.silver or cli_config.stage == cli.Stage.all:
            from unified_pipeline.silver.agricultural_fields import (
                AgriculturalFieldsSilver,
                AgriculturalFieldsSilverConfig,
            )

            source = AgriculturalFieldsSilver(
                config=AgriculturalFieldsSilverConfig(),
                gcs_util=gcs_util,
            )
    else:
        raise ValueError(f"Source {cli_config.source} and stage {cli_config.stage} not supported.")

    log.info(f"Running source {cli_config.source} in stage {cli_config.stage}.")
    if source is not None:
        asyncio.run(source.run())
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
    execute(app_config)
