"""
Main application module for the unified pipeline.

This module contains the main entry point and CLI interface for the unified
data pipeline application. It orchestrates different data processing stages
(bronze, silver, gold) for various data sources.
"""

import asyncio
import importlib.util
import os
import sys

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
from unified_pipeline.bronze.water_typology import WaterTypologyBronze, WaterTypologyBronzeConfig
from unified_pipeline.bronze.wetlands import WetlandsBronze, WetlandsBronzeConfig
from unified_pipeline.cli_scheduling import scheduling
from unified_pipeline.common.base import BronzeJobInterface, GoldJobInterface, SilverJobInterface
from unified_pipeline.gold.arbejdstilsynet_inspections import (
    ArbjdstilsynetInspectionsGold,
    ArbjdstilsynetInspectionsGoldConfig,
)

# Import new modular CVR enrichment steps from the package directory
from unified_pipeline.gold.cvr_enrichment.address_geocoding import (
    AddressGeocoding,
    AddressGeocodingConfig,
)
from unified_pipeline.gold.cvr_enrichment.company_fetching import (
    CompanyFetching,
    CompanyFetchingConfig,
)
from unified_pipeline.gold.cvr_enrichment.cvr_collection import CVRCollection, CVRCollectionConfig

# from unified_pipeline.gold.cvr_enrichment.data_consolidation import (
#     DataConsolidation,
#     DataConsolidationConfig,
# )  # REMOVED: Data consolidation step eliminated in architecture redesign
from unified_pipeline.gold.cvr_enrichment.financial_documents import (
    FinancialDocuments,
    FinancialDocumentsConfig,
)
from unified_pipeline.gold.cvr_enrichment.pnumber_fetching import (
    PNumberFetching,
    PNumberFetchingConfig,
)
from unified_pipeline.gold.cvr_geometry_datasets import (
    CVRGeometryDatasets,
    CVRGeometryDatasetsConfig,
)
from unified_pipeline.gold.field_area_analysis import (
    FieldAreaAnalysisGold,
    FieldAreaAnalysisGoldConfig,
)
from unified_pipeline.gold.field_production import (
    FieldProductionGold,
    FieldProductionGoldConfig,
)
from unified_pipeline.gold.pesticide_compliance import (
    PesticideComplianceGold,
    PesticideComplianceGoldConfig,
)
from unified_pipeline.gold.pesticide_disaggregation import (
    PesticideDisaggregationGold,
    PesticideDisaggregationGoldConfig,
)
from unified_pipeline.gold.pesticide_proximity import (
    PesticideProximityGold,
    PesticideProximityGoldConfig,
)
from unified_pipeline.gold.property_cadastral_merge import (
    PropertyCadastralMergeGold,
    PropertyCadastralMergeGoldConfig,
)
from unified_pipeline.gold.work_permits import (
    WorkPermitsGold,
    WorkPermitsGoldConfig,
)
from unified_pipeline.gold.worker_safety import (
    WorkerSafetyGold,
    WorkerSafetyGoldConfig,
)
from unified_pipeline.model import cli as cli_models
from unified_pipeline.model.scheduling import (
    get_pipeline_schedule,
    validate_dependencies,
)
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
from unified_pipeline.silver.water_typology import WaterTypologySilver, WaterTypologySilverConfig
from unified_pipeline.silver.wetlands import WetlandsSilver, WetlandsSilverConfig
from unified_pipeline.util.log_util import Logger

load_dotenv()

# Import legacy monolithic CVR enrichment from the specific .py file
# Note: We need to be specific because there's both cvr_enrichment.py and cvr_enrichment/ directory
# Get the path to the specific cvr_enrichment.py file
cvr_enrichment_file = os.path.join(os.path.dirname(__file__), "gold", "cvr_enrichment.py")
spec = importlib.util.spec_from_file_location("cvr_enrichment_legacy", cvr_enrichment_file)
cvr_enrichment_legacy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cvr_enrichment_legacy)

CVREnrichmentGold = cvr_enrichment_legacy.CVREnrichmentGold
CVREnrichmentGoldConfig = cvr_enrichment_legacy.CVREnrichmentGoldConfig


def validate_pipeline_dependencies(source: cli_models.Source) -> bool:
    """
    Validate that all dependencies for a pipeline source are satisfied.

    Args:
        source: The pipeline source to validate dependencies for

    Returns:
        True if all dependencies are satisfied, False otherwise
    """
    schedule_config = get_pipeline_schedule(source)
    if not schedule_config or not schedule_config.depends_on:
        return True  # No dependencies to check

    log = Logger.get_logger()

    # For now, we assume dependencies are satisfied if this function is called
    # In a more sophisticated implementation, we could check GCS for recent data
    log.info(
        f"Pipeline {source.value} depends on: {[dep.value for dep in schedule_config.depends_on]}"
    )
    log.info("Dependency validation: Assuming dependencies are satisfied (basic implementation)")

    return True


async def execute_pipeline_jobs(
    jobs: list, stage: cli_models.Stage, cli_config: cli_models.CliConfig
) -> tuple[int, int]:
    print(f"🚨 EXECUTE_JOBS: Starting with {len(jobs)} jobs for stage {stage}")
    print(f"🚨 EXECUTE_JOBS: CLI config pesticide_year = {cli_config.pesticide_year}")
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
        print(f"🚨 APP: About to run {job_cls.__name__} with config {config_cls.__name__}")

        # Create config instance and pass CLI config for FVM WFS filtering
        config_instance = config_cls()
        print(f"🚨 APP: Created config instance: {config_instance}")

        if hasattr(config_instance, "apply_cli_filters"):
            print(f"🚨 APP: Applying CLI filters to {config_cls.__name__}")
            config_instance.apply_cli_filters(cli_config)
            print(
                f"🚨 APP: After CLI filters - pesticide_year = "
                f"{getattr(config_instance, 'pesticide_year', 'NOT_SET')}"
            )
        else:
            print(f"🚨 APP: No apply_cli_filters method found on {config_cls.__name__}")

        print(f"🚨 APP: Creating instance of {job_cls.__name__}")
        instance = job_cls(config=config_instance)
        print("🚨 APP: Instance created successfully")
        job_successful = False

        try:
            if issubclass(job_cls, BronzeJobInterface):
                # Bronze stage - get data for memory passing
                bronze_data = await instance.run()
                if bronze_data is not None:
                    job_successful = True
                    log.info(
                        f"Bronze job {job_cls.__name__} completed successfully with data "
                        f"for in-memory passing"
                    )
                else:
                    log.error(f"Bronze job {job_cls.__name__} failed - no data returned")

                # 🧹 CLEANUP: Bronze data will be cleared AFTER silver processing
                # to allow in-memory passing
                # Log the size for monitoring purposes
                if bronze_data and isinstance(bronze_data, dict):
                    total_size = (
                        sum(len(str(v)) for v in bronze_data.values()) if bronze_data else 0
                    )
                    if total_size > 10_000_000:  # More than 10MB of string data
                        log.info(
                            f"📊 Bronze data size: {total_size:,} chars "
                            f"(will be cleared after silver processing)"
                        )

            elif issubclass(job_cls, SilverJobInterface):
                # Check if this is an enrichment-only job
                if stage == cli_models.Stage.enrichment and hasattr(
                    instance, "run_enrichment_only"
                ):
                    # Enrichment stage - run only enrichment functions
                    result = await instance.run_enrichment_only()
                    stage_description = "enrichment-only"
                else:
                    # Silver stage - pass in-memory data if available and collect results
                    result = await instance.run(bronze_data=bronze_data)
                    stage_description = (
                        f"{'in-memory' if bronze_data is not None else 'storage'} data"
                    )

                if result is not None:
                    job_successful = True
                    # Collect silver data for gold stage
                    dataset_name = instance.config.dataset
                    silver_data[dataset_name] = result
                    log.info(
                        f"Silver job {job_cls.__name__} completed successfully using "
                        f"{stage_description}"
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
                                f"🧹 Clearing large bronze data ({total_size:,} chars) to prevent "
                                f"GitHub runner memory issues"
                            )

                    bronze_data = None
                    import gc

                    gc.collect()

            elif issubclass(job_cls, GoldJobInterface):
                # Gold stage - pass collected silver data
                print(f"🚨 APP: About to call {job_cls.__name__}.run() with silver_data")
                print(
                    f"🚨 APP: Silver data keys: "
                    f"{list(silver_data.keys()) if silver_data else 'None'}"
                )
                await instance.run(silver_data=silver_data)
                print(f"🚨 APP: {job_cls.__name__}.run() completed successfully")
                # Gold jobs don't return data, so we consider them successful
                # if they don't raise an exception
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
            print(f"🚨 APP: EXCEPTION in {job_cls.__name__}: {e}")
            import traceback

            log.error(f"Traceback: {traceback.format_exc()}")
            print(f"🚨 APP: TRACEBACK: {traceback.format_exc()}")

    return successful_jobs, total_jobs


def execute(cli_config: cli_models.CliConfig) -> int:
    print(f"🚨 APP EXECUTE: Starting with config: {cli_config}")
    print(f"🚨 APP EXECUTE: Source = {cli_config.source}, Stage = {cli_config.stage}")
    print(f"🚨 APP EXECUTE: pesticide_year = {cli_config.pesticide_year}")
    """
    Main execution function for processing pipeline data.

    This function initializes the appropriate data processing pipeline based on
    the provided CLI configuration. It handles source selection and processing
    stage (bronze, silver, or all stages) with support for in-memory data passing.

    Enhanced with scheduling awareness and dependency validation.

    Args:
        cli_config (cli.CliConfig): Configuration containing source and stage settings

    Returns:
        int: Exit code (0 for success, 1 for failure)

    Raises:
        ValueError: If the requested source/stage combination is not supported
    """

    # Validate scheduling configuration on startup
    validation_errors = validate_dependencies()
    if validation_errors:
        print("🚨 SCHEDULING VALIDATION ERRORS:")
        for error in validation_errors:
            print(f"  ❌ {error}")
        print("⚠️ Proceeding with execution despite validation errors...")

    # Check pipeline dependencies
    if not validate_pipeline_dependencies(cli_config.source):
        print(f"❌ Dependencies not satisfied for pipeline: {cli_config.source.value}")
        return 1

    # Log scheduling information
    schedule_config = get_pipeline_schedule(cli_config.source)
    if schedule_config:
        print("📋 Pipeline Schedule Info:")
        print(f"   Frequency: {schedule_config.frequency.value}")
        print(f"   Priority: {schedule_config.priority}")
        print(f"   Dependencies: {[dep.value for dep in schedule_config.depends_on]}")
        print(f"   Description: {schedule_config.description}")
        if schedule_config.estimated_duration_minutes:
            print(f"   Estimated Duration: {schedule_config.estimated_duration_minutes} minutes")
    else:
        print(f"⚠️ No scheduling configuration found for {cli_config.source.value}")
    # Initialize logger with LOG_LEVEL environment variable BEFORE any other logging
    import os

    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    print(f"🚨 APP EXECUTE: Initializing logger with level: {log_level}")
    sys.stdout.flush()  # Force flush for GitHub Actions

    # Force reset the singleton logger to ensure LOG_LEVEL is respected
    Logger.LOG = None  # Reset singleton to force recreation with correct level
    log = Logger.get_logger(log_level)
    log.info("Starting Unified Pipeline.")
    sys.stdout.flush()  # Force flush after logger creation

    # Define pipeline mapping for sources and stages
    pipeline_map = {
        cli_models.Source.bnbo: {
            cli_models.Stage.bronze: [(BNBOStatusBronze, BNBOStatusBronzeConfig)],
            cli_models.Stage.silver: [(BNBOStatusSilver, BNBOStatusSilverConfig)],
            cli_models.Stage.all: [
                (BNBOStatusBronze, BNBOStatusBronzeConfig),
                (BNBOStatusSilver, BNBOStatusSilverConfig),
            ],
        },
        cli_models.Source.agricultural_fields: {
            cli_models.Stage.bronze: [(AgriculturalFieldsBronze, AgriculturalFieldsBronzeConfig)],
            cli_models.Stage.silver: [(AgriculturalFieldsSilver, AgriculturalFieldsSilverConfig)],
            cli_models.Stage.all: [
                (AgriculturalFieldsBronze, AgriculturalFieldsBronzeConfig),
                (AgriculturalFieldsSilver, AgriculturalFieldsSilverConfig),
            ],
        },
        cli_models.Source.cadastral: {
            cli_models.Stage.bronze: [(CadastralBronze, CadastralBronzeConfig)],
            cli_models.Stage.silver: [(CadastralSilver, CadastralSilverConfig)],
            cli_models.Stage.all: [
                (CadastralBronze, CadastralBronzeConfig),
                (CadastralSilver, CadastralSilverConfig),
            ],
        },
        cli_models.Source.soil_types: {
            cli_models.Stage.bronze: [(SoilTypesBronze, SoilTypesBronzeConfig)],
            cli_models.Stage.silver: [(SoilTypesSilver, SoilTypesSilverConfig)],
            cli_models.Stage.all: [
                (SoilTypesBronze, SoilTypesBronzeConfig),
                (SoilTypesSilver, SoilTypesSilverConfig),
            ],
        },
        cli_models.Source.dagi: {
            cli_models.Stage.bronze: [(DAGIBronze, DAGIBronzeConfig)],
            cli_models.Stage.silver: [
                (DAGISilver, DAGISilverConfig),
                (DSTZoneMapping, DSTZoneMappingConfig),
            ],
            cli_models.Stage.all: [
                (DAGIBronze, DAGIBronzeConfig),
                (DAGISilver, DAGISilverConfig),
                (DSTZoneMapping, DSTZoneMappingConfig),
            ],
        },
        cli_models.Source.jordbrugsanalyser: {
            cli_models.Stage.bronze: [(JordbrugsanalyserBronze, JordbrugsanalyserBronzeConfig)],
            cli_models.Stage.silver: [(JordbrugsanalyserSilver, JordbrugsanalyserSilverConfig)],
            cli_models.Stage.all: [
                (JordbrugsanalyserBronze, JordbrugsanalyserBronzeConfig),
                (JordbrugsanalyserSilver, JordbrugsanalyserSilverConfig),
            ],
        },
        cli_models.Source.fvm_wfs: {
            cli_models.Stage.bronze: [(FVMWFSBronze, FVMWFSBronzeConfig)],
            cli_models.Stage.silver: [(FVMWFSSilver, FVMWFSSilverConfig)],
            cli_models.Stage.enrichment: [(FVMWFSSilver, FVMWFSSilverConfig)],
            cli_models.Stage.all: [
                (FVMWFSBronze, FVMWFSBronzeConfig),
                (FVMWFSSilver, FVMWFSSilverConfig),
            ],
        },
        cli_models.Source.wetlands: {
            cli_models.Stage.bronze: [(WetlandsBronze, WetlandsBronzeConfig)],
            cli_models.Stage.silver: [(WetlandsSilver, WetlandsSilverConfig)],
            cli_models.Stage.all: [
                (WetlandsBronze, WetlandsBronzeConfig),
                (WetlandsSilver, WetlandsSilverConfig),
            ],
        },
        cli_models.Source.water_projects: {
            cli_models.Stage.bronze: [(WaterProjectsBronze, WaterProjectsBronzeConfig)],
            cli_models.Stage.silver: [(WaterProjectsSilver, WaterProjectsSilverConfig)],
            cli_models.Stage.all: [
                (WaterProjectsBronze, WaterProjectsBronzeConfig),
                (WaterProjectsSilver, WaterProjectsSilverConfig),
            ],
        },
        cli_models.Source.water_typology: {
            cli_models.Stage.bronze: [(WaterTypologyBronze, WaterTypologyBronzeConfig)],
            cli_models.Stage.silver: [(WaterTypologySilver, WaterTypologySilverConfig)],
            cli_models.Stage.all: [
                (WaterTypologyBronze, WaterTypologyBronzeConfig),
                (WaterTypologySilver, WaterTypologySilverConfig),
            ],
        },
        cli_models.Source.property_cadastral_merge: {
            cli_models.Stage.gold: [(PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires property_owners and cadastral silver data to be available
                # These should be run separately first or through dependent pipelines
                (PropertyCadastralMergeGold, PropertyCadastralMergeGoldConfig),
            ],
        },
        cli_models.Source.field_production: {
            cli_models.Stage.gold: [(FieldProductionGold, FieldProductionGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires fvm_marker and dst_zone_mapping silver data to be available
                # These should be run separately first or through dependent pipelines
                (FieldProductionGold, FieldProductionGoldConfig),
            ],
        },
        cli_models.Source.field_area_analysis: {
            cli_models.Stage.gold: [(FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires multiple silver datasets to be available:
                # fvm_marker, property_cadastral_merged, soil_types, bnbo_status_dissolved,
                # wetlands_dissolved, water_projects_dissolved
                (FieldAreaAnalysisGold, FieldAreaAnalysisGoldConfig),
            ],
        },
        cli_models.Source.pesticide_disaggregation: {
            cli_models.Stage.gold: [
                (PesticideDisaggregationGold, PesticideDisaggregationGoldConfig)
            ],
            cli_models.Stage.all: [
                # Note: This requires silver datasets to be available:
                # fvm_marker, pesticides
                (PesticideDisaggregationGold, PesticideDisaggregationGoldConfig),
            ],
        },
        cli_models.Source.pesticide_proximity: {
            cli_models.Stage.gold: [(PesticideProximityGold, PesticideProximityGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires gold pesticide_disaggregation and silver datasets:
                # bbr_buildings, water_typology, fvm_marker
                (PesticideProximityGold, PesticideProximityGoldConfig),
            ],
        },
        cli_models.Source.pesticide_compliance: {
            cli_models.Stage.gold: [(PesticideComplianceGold, PesticideComplianceGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires silver datasets to be available:
                # bmd, pesticides
                (PesticideComplianceGold, PesticideComplianceGoldConfig),
            ],
        },
        cli_models.Source.cvr_enrichment: {
            # Legacy monolithic approach
            cli_models.Stage.gold: [(CVREnrichmentGold, CVREnrichmentGoldConfig)],
            cli_models.Stage.all: [
                # Note: This collects CVR numbers from all pipeline CVR collections
                # and fetches CVR register data for enrichment
                (CVREnrichmentGold, CVREnrichmentGoldConfig),
            ],
            # New modular pipeline steps
            cli_models.Stage.collection: [(CVRCollection, CVRCollectionConfig)],
            cli_models.Stage.company_fetching: [(CompanyFetching, CompanyFetchingConfig)],
            cli_models.Stage.pnumber_fetching: [(PNumberFetching, PNumberFetchingConfig)],
            cli_models.Stage.financial_documents: [(FinancialDocuments, FinancialDocumentsConfig)],
            cli_models.Stage.address_geocoding: [(AddressGeocoding, AddressGeocodingConfig)],
            # cli.Stage.data_consolidation: [(DataConsolidation, DataConsolidationConfig)],
            # REMOVED: Eliminated in redesign
        },
        cli_models.Source.cvr_geometry_datasets: {
            cli_models.Stage.gold: [(CVRGeometryDatasets, CVRGeometryDatasetsConfig)],
            cli_models.Stage.all: [
                # Phase 1: CVR address points and CHR property points
                (CVRGeometryDatasets, CVRGeometryDatasetsConfig),
            ],
        },
        cli_models.Source.dst: {
            cli_models.Stage.bronze: [(DSTBronze, DSTBronzeConfig)],
            cli_models.Stage.silver: [(DSTSilver, DSTSilverConfig)],
            cli_models.Stage.all: [
                (DSTBronze, DSTBronzeConfig),
                (DSTSilver, DSTSilverConfig),
            ],
        },
        cli_models.Source.dmi: {
            cli_models.Stage.bronze: [(DMIBronze, DMIBronzeConfig)],
            cli_models.Stage.silver: [(DMISilver, DMISilverConfig)],
            cli_models.Stage.all: [
                (DMIBronze, DMIBronzeConfig),
                (DMISilver, DMISilverConfig),
            ],
        },
        cli_models.Source.arbejdstilsynet_inspections: {
            cli_models.Stage.gold: [
                (ArbjdstilsynetInspectionsGold, ArbjdstilsynetInspectionsGoldConfig)
            ],
            cli_models.Stage.all: [
                # Note: This requires arbejdstilsynet_inspections silver data to be available
                (ArbjdstilsynetInspectionsGold, ArbjdstilsynetInspectionsGoldConfig),
            ],
        },
        cli_models.Source.worker_safety: {
            cli_models.Stage.gold: [(WorkerSafetyGold, WorkerSafetyGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires worker safety silver data to be available
                (WorkerSafetyGold, WorkerSafetyGoldConfig),
            ],
        },
        cli_models.Source.work_permits: {
            cli_models.Stage.gold: [(WorkPermitsGold, WorkPermitsGoldConfig)],
            cli_models.Stage.all: [
                # Note: This requires work permits silver data from drive pipeline to be available
                (WorkPermitsGold, WorkPermitsGoldConfig),
            ],
        },
    }

    # Retrieve jobs for given source and stage
    print(f"🚨 APP: Looking up pipeline for source={cli_config.source}, stage={cli_config.stage}")
    try:
        jobs = pipeline_map[cli_config.source][cli_config.stage]
        print(f"🚨 APP: Found {len(jobs)} jobs: {[job[0].__name__ for job in jobs]}")
    except KeyError as e:
        print("🚨 APP: KeyError - source/stage combination not found in pipeline_map")
        raise ValueError(
            f"Source {cli_config.source} and stage {cli_config.stage} not supported."
        ) from e

    # Execute jobs with support for in-memory data passing
    print(f"🚨 APP: About to execute {len(jobs)} jobs with asyncio.run")
    successful_jobs, total_jobs = asyncio.run(
        execute_pipeline_jobs(jobs, cli_config.stage, cli_config)
    )
    print(f"🚨 APP: Execution completed - {successful_jobs}/{total_jobs} successful")

    # Determine exit code based on job success
    if successful_jobs == 0:
        log.error(f"❌ Pipeline failed: No jobs completed successfully (0/{total_jobs})")
        return 1
    elif successful_jobs < total_jobs:
        log.warning(
            f"⚠️  Pipeline completed with partial success: "
            f"{successful_jobs}/{total_jobs} jobs completed successfully"
        )
        return 0  # Still consider it a success if at least one job completed
    else:
        log.info(
            f"✅ Pipeline completed successfully: "
            f"{successful_jobs}/{total_jobs} jobs completed successfully"
        )
        return 0


@click.group()
def cli():
    """Unified Pipeline - Data processing and scheduling management."""
    pass


@cli.command("run")
@click.pass_context
@click.option(
    "-e",
    "--env",
    "env",
    help="The environment to use. Default is prod.",
    type=click.Choice([env.value for env in cli_models.Env]),
    default="prod",
)
@click.option(
    "-s",
    "--source",
    "source",
    help="The source to use.",
    type=click.Choice([source.value for source in cli_models.Source]),
    required=True,
)
@click.option(
    "-j",
    "--stage",
    "stage",
    type=click.Choice([mode.value for mode in cli_models.Stage]),
    help="The stage to use. The options are bronze, silver, and all.",
    required=True,
)
@click.option(
    "--fvm-layer-type",
    "fvm_layer_type",
    type=click.Choice([layer.value for layer in cli_models.FVMLayerType]),
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
@click.option(
    "--test-limit",
    "test_limit",
    type=int,
    help="Limit number of CVR numbers to process for testing (CVR enrichment only).",
    required=False,
)
@click.option(
    "--parse-financial-xml/--no-parse-financial-xml",
    "parse_financial_xml",
    default=True,
    help="Whether to download and parse XML financial documents (CVR enrichment only).",
)
@click.option(
    "--max-financial-documents",
    "max_financial_documents",
    type=int,
    default=10,
    help="Maximum number of financial documents to fetch per company (CVR enrichment only).",
)
@click.option(
    "--pesticide-year",
    "pesticide_year",
    type=int,
    help="Year filter for pesticide matrix jobs (e.g., 2018). "
    "If not specified, processes all available years.",
    required=False,
)
@click.option(
    "--batch-number",
    "batch_number",
    type=int,
    help="Batch number for parallel processing (1-based). Used with CVR enrichment pipeline steps.",
    required=False,
)
@click.option(
    "--total-batches",
    "total_batches",
    type=int,
    help="Total number of batches for parallel processing. "
    "Used with CVR enrichment pipeline steps.",
    required=False,
)
def run_cli(
    ctx,
    env: str,
    source: str,
    stage: str,
    fvm_layer_type: str = None,
    fvm_year: int = None,
    test_limit: int = None,
    parse_financial_xml: bool = True,
    max_financial_documents: int = 10,
    pesticide_year: int = None,
    batch_number: int = None,
    total_batches: int = None,
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
        $ python -m unified_pipeline -s fvm_wfs -j bronze \
            --fvm-layer-type markblokke --fvm-year 2024
    """
    app_config = cli_models.CliConfig(
        env=cli_models.Env(env),
        source=cli_models.Source(source),
        stage=cli_models.Stage(stage),
        fvm_layer_type=cli_models.FVMLayerType(fvm_layer_type) if fvm_layer_type else None,
        fvm_year=fvm_year,
        test_limit=test_limit,
        parse_financial_xml=parse_financial_xml,
        max_financial_documents=max_financial_documents,
        pesticide_year=pesticide_year,
        batch_number=batch_number,
        total_batches=total_batches,
    )
    print(app_config)
    exit_code = execute(app_config)
    exit(exit_code)


# Add scheduling subcommand
cli.add_command(scheduling)


# TODO: Incomplete implementation - get_pipeline_jobs function not defined
# def run_pipeline(pipeline_name: str, stage: cli_models.Stage) -> None:
#     """Run a specific pipeline by name and stage."""
#     pipeline_jobs = get_pipeline_jobs(pipeline_name)
#     asyncio.run(execute_pipeline_jobs(pipeline_jobs, stage))


if __name__ == "__main__":
    cli()
