import asyncio

import click

from unified_pipeline.bronze.bnbo_status import BNBOStatusBronze
from unified_pipeline.model import cli
from unified_pipeline.model.app_config import GCSConfig
from unified_pipeline.model.bnbo_status import BNBOStatusConfig
from unified_pipeline.util.gcs_util import GCSUtil
from unified_pipeline.util.log_util import Logger


def execute(cli_config: cli.CliConfig) -> None:
    """Main function."""
    log = Logger.get_logger()
    log.info("Starting Unified Pipeline.")

    gcs_util = GCSUtil(GCSConfig())

    if (cli_config.source == cli.Source.bnbo) and (
        cli_config.stage == cli.Stage.bronze or cli_config.stage == cli.Stage.all
    ):
        source = BNBOStatusBronze(
            config=BNBOStatusConfig(),
            gcs_util=gcs_util,
        )
    else:
        raise ValueError(f"Source {cli_config.source} and stage {cli_config.stage} not supported.")

    log.info(f"Running source {cli_config.source} in stage {cli_config.stage}.")
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
    This function returns the application configuration.

    :param env: The environment to use.
    :param source: The source to use.
    :param stage: The stage to use.
    :return: The application configuration.
    """
    app_config = cli.CliConfig(
        env=cli.Env(env),
        source=cli.Source(source),
        stage=cli.Stage(stage),
    )
    execute(app_config)
