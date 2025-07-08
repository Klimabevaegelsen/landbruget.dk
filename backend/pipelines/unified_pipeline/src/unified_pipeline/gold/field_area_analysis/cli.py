#!/usr/bin/env python3
"""
Command-line interface for Field Area Analysis Multi-Stage Pipeline

Usage:
    python -m unified_pipeline.gold.field_area_analysis.cli --stage=1 --job=water_projects_bnbo
    python -m unified_pipeline.gold.field_area_analysis.cli --stage=2 --job=fields_soil_types
    python -m unified_pipeline.gold.field_area_analysis.cli --stage=all
"""

import argparse
import asyncio
import sys
import time
from typing import Any, Dict

from unified_pipeline.util.log_util import Logger

from .base import FieldAnalysisStageConfig
from .stage1.fields_properties import FieldsPropertiesIntersection
from .stage1.fields_soil_types import FieldsSoilTypesIntersection

# Import all stage classes
from .stage1.water_projects_bnbo import WaterProjectsBNBOIntersection
from .stage1.water_projects_wetlands import WaterProjectsWetlandsIntersection
from .stage3.fields_bnbo_water import FieldsBNBOWaterCoverage
from .stage3.fields_wetland_water import FieldsWetlandWaterCoverage
from .stage4.final_bnbo import FinalBNBOAnalysis
from .stage4.final_wetland import FinalWetlandAnalysis
from .stage5.consolidate import ConsolidateResults

# Configure logging
logger = Logger.get_logger()

# Stage and job mapping
STAGE_JOBS = {
    1: {
        "water_projects_bnbo": WaterProjectsBNBOIntersection,
        "water_projects_wetlands": WaterProjectsWetlandsIntersection,
        "fields_properties": FieldsPropertiesIntersection,
        "fields_soil_types": FieldsSoilTypesIntersection,  # Moved from Stage 2 to Stage 1
    },
    # Stage 2 is now empty - environmental coverage moved to Stage 3
    3: {
        "fields_bnbo_water": FieldsBNBOWaterCoverage,
        "fields_wetland_water": FieldsWetlandWaterCoverage,
    },
    4: {
        "final_bnbo": FinalBNBOAnalysis,
        "final_wetland": FinalWetlandAnalysis,
    },
    5: {
        "consolidate": ConsolidateResults,
    },
}


async def run_stage_job(stage: int, job: str, config: FieldAnalysisStageConfig) -> Dict[str, Any]:
    """Run a specific stage job."""
    if stage not in STAGE_JOBS:
        raise ValueError(f"Invalid stage: {stage}. Valid stages: {list(STAGE_JOBS.keys())}")

    if job not in STAGE_JOBS[stage]:
        raise ValueError(
            f"Invalid job '{job}' for stage {stage}. Valid jobs: {list(STAGE_JOBS[stage].keys())}"
        )

    job_class = STAGE_JOBS[stage][job]
    job_instance = job_class(config)

    logger.info(f"🚀 Starting Stage {stage} Job: {job}")
    start_time = time.time()

    try:
        result = await job_instance.run()
        total_time = time.time() - start_time

        logger.info(
            f"✅ Stage {stage} Job '{job}' completed successfully in {total_time:.1f} seconds"
        )
        return result

    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"❌ Stage {stage} Job '{job}' failed after {total_time:.1f} seconds: {e}")
        raise


async def run_stage_all_jobs(stage: int, config: FieldAnalysisStageConfig) -> Dict[str, Any]:
    """Run all jobs in a stage sequentially."""
    stage_results = {}
    stage_start_time = time.time()

    # Handle empty stages
    if stage not in STAGE_JOBS or not STAGE_JOBS[stage]:
        logger.info(f"⏭️  Stage {stage} is empty, skipping")
        return {"stage": stage, "total_time": 0, "job_results": {}}

    logger.info(f"🚀 Starting all jobs for Stage {stage}")

    for job_name in STAGE_JOBS[stage]:
        try:
            result = await run_stage_job(stage, job_name, config)
            stage_results[job_name] = result
        except Exception as e:
            logger.error(f"Stage {stage} failed at job '{job_name}': {e}")
            raise

    total_time = time.time() - stage_start_time
    logger.info(f"✅ Stage {stage} completed all jobs in {total_time:.1f} seconds")

    return {"stage": stage, "total_time": total_time, "job_results": stage_results}


async def run_all_stages(config: FieldAnalysisStageConfig) -> Dict[str, Any]:
    """Run all stages sequentially."""
    pipeline_start_time = time.time()
    all_results = {}

    logger.info("🚀 Starting complete Field Area Analysis pipeline")

    # Run stages sequentially (dependencies handled by stage ordering)
    # Note: Stage 2 is empty in the current architecture
    for stage in [1, 2, 3, 4, 5]:
        try:
            stage_result = await run_stage_all_jobs(stage, config)
            all_results[f"stage_{stage}"] = stage_result

            # Log progress
            elapsed = time.time() - pipeline_start_time
            logger.info(
                f"📊 Pipeline progress: Stage {stage}/5 completed in {elapsed:.1f} seconds total"
            )

        except Exception as e:
            elapsed = time.time() - pipeline_start_time
            logger.error(f"❌ Pipeline failed at Stage {stage} after {elapsed:.1f} seconds: {e}")
            raise

    total_time = time.time() - pipeline_start_time
    logger.info(f"🎉 Complete Field Area Analysis pipeline finished in {total_time:.1f} seconds")

    return {"pipeline_total_time": total_time, "stage_results": all_results}


def create_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Field Area Analysis Multi-Stage Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single job
  python -m unified_pipeline.gold.field_area_analysis.cli --stage=1 --job=water_projects_bnbo
  
  # Run all jobs in a stage
  python -m unified_pipeline.gold.field_area_analysis.cli --stage=2
  
  # Run complete pipeline
  python -m unified_pipeline.gold.field_area_analysis.cli --stage=all
  
  # Use custom bucket
  python -m unified_pipeline.gold.field_area_analysis.cli --stage=1 --job=fields_properties --bucket=my-bucket

Stage/Job combinations:
  Stage 1: water_projects_bnbo, water_projects_wetlands, fields_properties, fields_soil_types
  Stage 3: fields_bnbo_water, fields_wetland_water (uses Stage 1 foundation data)
  Stage 4: final_bnbo, final_wetland
  Stage 5: consolidate
        """,
    )

    parser.add_argument(
        "--stage", type=str, required=True, help="Stage to run (1, 2, 3, 4, 5, or 'all')"
    )

    parser.add_argument(
        "--job",
        type=str,
        help="Specific job to run within the stage (optional, runs all jobs if not specified)",
    )

    parser.add_argument("--bucket", type=str, help="GCS bucket name (overrides default)")

    parser.add_argument(
        "--max-memory-gb", type=int, default=14, help="Maximum memory limit in GB (default: 14)"
    )

    parser.add_argument(
        "--max-threads", type=int, default=4, help="Maximum number of threads (default: 4)"
    )

    parser.add_argument(
        "--batch-size", type=int, default=250000, help="Batch size for processing (default: 250000)"
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    return parser


async def main():
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    # Configure logging level (logger already configured at module level)
    if args.verbose:
        # Get a new logger instance with DEBUG level for verbose output
        verbose_logger = Logger.get_logger("DEBUG")

    # Create configuration
    config_kwargs = {
        "max_memory_gb": args.max_memory_gb,
        "max_threads": args.max_threads,
        "batch_size": args.batch_size,
    }

    if args.bucket:
        config_kwargs["bucket"] = args.bucket

    config = FieldAnalysisStageConfig(**config_kwargs)

    logger.info("🔧 Configuration:")
    logger.info(f"   Bucket: {config.bucket}")
    logger.info(f"   Max Memory: {config.max_memory_gb}GB")
    logger.info(f"   Max Threads: {config.max_threads}")
    logger.info(f"   Batch Size: {config.batch_size:,}")

    try:
        if args.stage == "all":
            # Run complete pipeline
            result = await run_all_stages(config)

        elif args.stage.isdigit():
            stage_num = int(args.stage)

            if args.job:
                # Run specific job
                result = await run_stage_job(stage_num, args.job, config)
            else:
                # Run all jobs in stage
                result = await run_stage_all_jobs(stage_num, config)
        else:
            logger.error(f"Invalid stage: {args.stage}. Use 1-5 or 'all'")
            sys.exit(1)

        logger.info("🎉 Pipeline execution completed successfully!")

        # Print summary
        if "pipeline_total_time" in result:
            logger.info(f"📊 Total pipeline time: {result['pipeline_total_time']:.1f} seconds")
        elif "total_time" in result:
            logger.info(f"📊 Stage execution time: {result['total_time']:.1f} seconds")
        elif "execution_time" in result:
            logger.info(f"📊 Job execution time: {result['execution_time']:.1f} seconds")

    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
