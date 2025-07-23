"""
Main application module for the unified pipeline.

This module contains the main entry point and CLI interface for the unified
data pipeline application. It orchestrates different data processing stages
(bronze, silver, gold) for various data sources.
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
from unified_pipeline.bronze.dagi import DAGIBronze, DAGIBronzeConfig
from unified_pipeline.bronze.dmi import DMIBronze, DMIBronzeConfig
from unified_pipeline.bronze.dst import DSTBronze, DSTBronzeConfig
from unified_pipeline.bronze.fvm_wfs import FVMWFSBronze, FVMWFSBronzeConfig
from unified_pipeline.bronze.jordbrugsanalyser import (
    JordbrugsanalyserBronze,
    JordbrugsanalyserBronzeConfig,
)
from unified_pipeline.bronze.soil_types import SoilTypesBronze, SoilTypesBronzeConfig
from unified_pipeline.bronze.water_projects import WaterProjectsBronze, WaterProjectsBronzeConfig
from unified_pipeline.bronze.wetlands import WetlandsBronze, WetlandsBronzeConfig
from unified_pipeline.common.base import BronzeJobInterface, GoldJobInterface, SilverJobInterface
from unified_pipeline.gold.cvr_enrichment import (
    CVREnrichmentGold,
    CVREnrichmentGoldConfig,
)
from unified_pipeline.gold.field_area_analysis import (
    FieldAreaAnalysisGold,
    FieldAreaAnalysisGoldConfig,
)
from unified_pipeline.gold.field_production import (
    FieldProductionGold,
    FieldProductionGoldConfig,
)
from unified_pipeline.gold.pesticide_disaggregation import (
    PesticideDisaggregationGold,
    PesticideDisaggregationGoldConfig,
)
from unified_pipeline.gold.property_cadastral_merge import (
    PropertyCadastralMergeGold,
    PropertyCadastralMergeGoldConfig,
)
from unified_pipeline.gold.nles5_nitrogen_estimation import (
    NLES5NitrogenEstimationGold,
    NLES5NitrogenEstimationGoldConfig,
)
from unified_pipeline.model import cli
from unified_pipeline.silver.agricultural_fields import (
    AgriculturalFieldsSilver,
    AgriculturalFieldsSilverConfig,
)
from unified_pipeline.silver.bnbo_status import BNBOStatusSilver, BNBOStatusSilverConfig
from unified_pipeline.silver.cadastral import CadastralSilver, CadastralSilverConfig
from unified_pipeline.silver.dagi import DAGISilver, DAGISilverConfig
from unified_pipeline.silver.dmi import DMISilver, DMISilverConfig
from unified_pipeline.silver.dst import DSTSilver, DSTSilverConfig
from unified_pipeline.silver.dst_zone_mapping import DSTZoneMapping, DSTZoneMappingConfig
from unified_pipeline.silver.fvm_wfs import FVMWFSSilver, FVMWFSSilverConfig
from unified_pipeline.silver.jordbrugsanalyser import (
    JordbrugsanalyserSilver,
    JordbrugsanalyserSilverConfig,
)
from unified_pipeline.silver.soil_types import SoilTypesSilver, SoilTypesSilverConfig
from unified_pipeline.silver.water_projects import WaterProjectsSilver, WaterProjectsSilverConfig
from unified_pipeline.silver.wetlands import WetlandsSilver, WetlandsSilverConfig
from unified_pipeline.util.log_util import Logger

load_dotenv()


async def execute_pipeline_jobs(
    jobs: list, stage: cli.Stage, cli_config: cli.CliConfig
) -> tuple[int, int]:
    """
    Execute pipeline jobs with support for gold layer and in-memory data passing.

    This function handles the execution of bronze, silver, and gold jobs, implementing
    in-memory data passing when Stage.all is used. When bronze and silver jobs
    are run together, bronze data is passed directly to silver jobs without
    disk I/O for improved performance.

    Args:
        jobs: List of (job_class, config_class) tuples to execute
        stage: The stage being executed (bronze, silver, or all)
        cli_config: CLI configuration containing filtering parameters

    Returns:
        tuple[int, int]: (successful_jobs, total_jobs) counts
    """
    log = Logger.get_logger()
    bronze_data = None
    silver_data = {}
    successful_jobs = 0
    total_jobs = len(jobs)

    for job_cls, config_cls in jobs:
        log.info(f"Running {job_cls.__name__} for stage {stage}")

        # Create config instance and pass CLI config for FVM WFS filtering
        config_instance = config_cls()

        if hasattr(config_instance, "apply_cli_filters"):
            config_instance.apply_cli_filters(cli_config)

        instance = job_cls(config=config_instance)
        job_successful = False

        try:
            if issubclass(job_cls, BronzeJobInterface):
                # Bronze stage - get data for memory passing
                bronze_data = await instance.run()
                if bronze_data is not None:
                    job_successful = True
                    log.info(
                        f"Bronze job {job_cls.__name__} completed successfully with data for in-memory passing"
                    )
                else:
                    log.error(f"Bronze job {job_cls.__name__} failed - no data returned")

                # 🧹 CLEANUP: Bronze data will be cleared AFTER silver processing to allow in-memory passing
                # Log the size for monitoring purposes
                if bronze_data and isinstance(bronze_data, dict):
                    total_size = (
                        sum(len(str(v)) for v in bronze_data.values()) if bronze_data else 0
                    )
                    if total_size > 10_000_000:  # More than 10MB of string data
                        log.info(
                            f"📊 Bronze data size: {total_size:,} chars (will be cleared after silver processing)"
                        )

            elif issubclass(job_cls, SilverJobInterface):
                # Silver stage - pass in-memory data if available and collect results
                result = await instance.run(bronze_data=bronze_data)
                if result is not None:
                    job_successful = True
                    # Collect silver data for gold stage
                    dataset_name = instance.config.dataset
                    silver_data[dataset_name] = result
                    log.info(
                        f"Silver job {job_cls.__name__} completed successfully using {'in-memory' if bronze_data is not None else 'storage'} data"
                    )
                else:
                    log.error(f"Silver job {job_cls.__name__} failed - no data returned")

                # 🧹 CLEANUP: Clear bronze data after silver processing to free memory
                if bronze_data:
                    # Check if bronze_data is large (indicating it might cause memory issues)
                    if isinstance(bronze_data, dict):
                        total_size = (
                            sum(len(str(v)) for v in bronze_data.values()) if bronze_data else 0
                        )
                        if total_size > 10_000_000:  # More than 10MB of string data
                            log.info(
                                f"🧹 Clearing large bronze data ({total_size:,} chars) to prevent GitHub runner memory issues"
                            )

                    bronze_data = None
                    import gc

                    gc.collect()

            elif issubclass(job_cls, GoldJobInterface):
                # Gold stage - pass collected silver data
                await instance.run(silver_data=silver_data)
                # Gold jobs don't return data, so we consider them successful if they don't raise an exception
                job_successful = True
                log.info(f"Gold job {job_cls.__name__} completed successfully using silver data")

                # 🧹 CLEANUP: Clear silver data after gold processing to free memory
                if silver_data:
                    log.info("🧹 Clearing silver data after gold processing")
                    silver_data.clear()
                    import gc

                    gc.collect()

            else:
                log.error(f"Unknown job interface for {job_cls.__name__}")

            if job_successful:
                successful_jobs += 1

        except Exception as e:
            log.error(f"Error executing {job_cls.__name__}: {e}")
            import traceback

            log.error(f"Traceback: {traceback.format_exc()}")

    return successful_jobs, total_jobs


def execute(cli_config: cli.CliConfig) -> int:
    """
    Main execution function for processing pipeline data.

    This function initializes the appropriate data processing pipeline based on
    the provided CLI configuration. It handles source selection and processing
    stage (bronze, silver, or all stages) with support for in-memory data passing.

    Args:
        cli_config (cli.CliConfig): Configuration containing source and stage settings

    Returns:
        int: Exit code (0 for success, 1 for failure)

    Raises:
        ValueError: If the requested source/stage combination is not supported
    """
    log = Logger.get_logger()
    log.info("Starting Unified Pipeline.")

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
        cli.Source.soil_types: {
            cli.Stage.bronze: [(SoilTypesBronze, SoilTypesBronzeConfig)],
            cli.Stage.silver: [(SoilTypesSilver, SoilTypesSilverConfig)],
            cli.Stage.all: [
                (SoilTypesBronze, SoilTypesBronzeConfig),
                (SoilTypesSilver, SoilTypesSilverConfig),
            ],
        },
        cli.Source.dagi: {
            cli.Stage.bronze: [(DAGIBronze, DAGIBronzeConfig)],
            cli.Stage.silver: [
                (DAGISilver, DAGISilverConfig),
                (DSTZoneMapping, DSTZoneMappingConfig),
            ],
            cli.Stage.all: [
                (DAGIBronze, DAGIBronzeConfig),
                (DAGISilver, DAGISilverConfig),
                (DSTZoneMapping, DSTZoneMappingConfig),
            ],
        },
        cli.Source.jordbrugsanalyser: {
            cli.Stage.bronze: [(JordbrugsanalyserBronze, JordbrugsanalyserBronzeConfig)],
            cli.Stage.silver: [(JordbrugsanalyserSilver, JordbrugsanalyserSilverConfig)],
            cli.Stage.all: [
                (JordbrugsanalyserBronze, JordbrugsanalyserBronzeConfig),
                (JordbrugsanalyserSilver, JordbrugsanalyserSilverConfig),
            ],
        },
        cli.Source.fvm_wfs: {
            cli.Stage.bronze: [(FVMWFSBronze, FVMWFSBronzeConfig)],
            cli.Stage.silver: [(FVMWFSSilver, FVMWFSSilverConfig)],
            cli.Stage.all: [
                (FVMWFSBronze, FVMWFSBronzeConfig),
                (FVMWFSSilver, FVMWFSSilverConfig),
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
            cli.Stage.silver: [(WaterProjectsSilver, WaterProjectsSilverConfig)],
            cli.Stage.all: [
                (WaterProjectsBronze, WaterProjectsBronzeConfig),
                (WaterProjectsSilver, WaterProjectsSilverConfig),
            ],
        },
        cli.Source.property_cadastral_merge: {
            cli.Stage.gold: [(PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig)],
            cli.Stage.all: [
                # Note: This requires property_owners and cadastral silver data to be available
                # These should be run separately first or through dependent pipelines
                (PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig),
            ],
        },
        cli.Source.field_production: {
            cli.Stage.gold: [(FieldProductionGold, FieldProductionGoldConfig)],
            cli.Stage.all: [
                # Note: This requires agricultural_fields and dst_zone_mapping silver data to be available
                # These should be run separately first or through dependent pipelines
                (FieldProductionGold, FieldProductionGoldConfig),
            ],
        },
        cli.Source.field_area_analysis: {
            cli.Stage.gold: [(FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig)],
            cli.Stage.all: [
                # Note: This requires multiple silver datasets to be available:
                # agricultural_fields, property_cadastral_merged, soil_types, bnbo_status_dissolved,
                # wetlands_dissolved, water_projects_dissolved
                (FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig),
            ],
        },
        cli.Source.pesticide_disaggregation: {
            cli.Stage.gold: [(PesticideDisaggregationGold, PesticideDisaggregationGoldConfig)],
            cli.Stage.all: [
                # Note: This requires silver datasets to be available:
                # agricultural_fields, pesticides
                (PesticideDisaggregationGold, PesticideDisaggregationGoldConfig),
            ],
        },
        cli.Source.nles5_nitrogen_estimation: {
            cli.Stage.gold: [(NLES5NitrogenEstimationGold, NLES5NitrogenEstimationGoldConfig)],
            cli.Stage.all: [
                # Note: This requires silver datasets to be available:
                # agricultural_fields, soil_types, dmi (climate data)
                (NLES5NitrogenEstimationGold, NLES5NitrogenEstimationGoldConfig),
            ],
        },
        cli.Source.cvr_enrichment: {
            cli.Stage.gold: [(CVREnrichmentGold, CVREnrichmentGoldConfig)],
            cli.Stage.all: [
                # Note: This collects CVR numbers from all pipeline CVR collections
                # and fetches CVR register data for enrichment
                (CVREnrichmentGold, CVREnrichmentGoldConfig),
            ],
        },
        cli.Source.dst: {
            cli.Stage.bronze: [(DSTBronze, DSTBronzeConfig)],
            cli.Stage.silver: [(DSTSilver, DSTSilverConfig)],
            cli.Stage.all: [
                (DSTBronze, DSTBronzeConfig),
                (DSTSilver, DSTSilverConfig),
            ],
        },
        cli.Source.dmi: {
            cli.Stage.bronze: [(DMIBronze, DMIBronzeConfig)],
            cli.Stage.silver: [(DMISilver, DMISilverConfig)],
            cli.Stage.all: [
                (DMIBronze, DMIBronzeConfig),
                (DMISilver, DMISilverConfig),
            ],
        },
    }

    # Retrieve jobs for given source and stage
    try:
        jobs = pipeline_map[cli_config.source][cli_config.stage]
    except KeyError:
        raise ValueError(f"Source {cli_config.source} and stage {cli_config.stage} not supported.")

    # Execute jobs with support for in-memory data passing
    successful_jobs, total_jobs = asyncio.run(
        execute_pipeline_jobs(jobs, cli_config.stage, cli_config)
    )

    # Determine exit code based on job success
    if successful_jobs == 0:
        log.error(f"❌ Pipeline failed: No jobs completed successfully (0/{total_jobs})")
        return 1
    elif successful_jobs < total_jobs:
        log.warning(
            f"⚠️  Pipeline completed with partial success: {successful_jobs}/{total_jobs} jobs completed successfully"
        )
        return 0  # Still consider it a success if at least one job completed
    else:
        log.info(
            f"✅ Pipeline completed successfully: {successful_jobs}/{total_jobs} jobs completed successfully"
        )
        return 0


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
@click.option(
    "--fvm-layer-type",
    "fvm_layer_type",
    type=click.Choice([layer.value for layer in cli.FVMLayerType]),
    help="FVM layer type filter for matrix jobs (markblokke, marker, smaabiotoper).",
    required=False,
)
@click.option(
    "--fvm-year",
    "fvm_year",
    type=int,
    help="Year filter for FVM matrix jobs (e.g., 2024).",
    required=False,
)

def run_cli(
    env: str,
    source: str,
    stage: str,
    fvm_layer_type: str = None,
    fvm_year: int = None,
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
        fvm_layer_type: Optional FVM layer type filter for matrix jobs
        fvm_year: Optional year filter for FVM matrix jobs

    Example:
        $ python -m unified_pipeline -s bnbo -j bronze
        $ python -m unified_pipeline -s fvm_wfs -j bronze --fvm-layer-type markblokke --fvm-year 2024
    """
    app_config = cli.CliConfig(
        env=cli.Env(env),
        source=cli.Source(source),
        stage=cli.Stage(stage),
        fvm_layer_type=cli.FVMLayerType(fvm_layer_type) if fvm_layer_type else None,
        fvm_year=fvm_year,
    )
    print(app_config)
    exit_code = execute(app_config)
    exit(exit_code)
