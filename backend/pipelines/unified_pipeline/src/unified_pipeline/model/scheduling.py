"""
Pipeline scheduling configuration module.

This module defines scheduling metadata for all unified pipeline sources,
including execution frequency, dependencies, and orchestration rules.
"""

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel

from .cli import Source


class ScheduleFrequency(Enum):
    """
    Pipeline execution frequency options.

    Attributes:
        MANUAL: Manual execution only (workflow_dispatch)
        WEEKLY: Execute weekly on schedule
        MONTHLY: Execute monthly on schedule
        WEEKLY_AND_MONTHLY: Execute both weekly and monthly
    """

    MANUAL = "manual"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    WEEKLY_AND_MONTHLY = "weekly_and_monthly"


class PipelineScheduleConfig(BaseModel):
    """
    Configuration for individual pipeline scheduling.

    Attributes:
        frequency: How often the pipeline should run
        depends_on: List of sources that must complete successfully before this pipeline
        priority: Execution priority within the same frequency group (lower = higher priority)
        description: Human-readable description of the pipeline purpose
        estimated_duration_minutes: Estimated runtime for resource planning
    """

    frequency: ScheduleFrequency
    depends_on: List[Source] = []
    priority: int = 0
    description: str = ""
    estimated_duration_minutes: Optional[int] = None


# Pipeline scheduling configuration based on user requirements
PIPELINE_SCHEDULES: Dict[Source, PipelineScheduleConfig] = {
    # MANUAL PIPELINES
    Source.soil_types: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=1,
        description="Danish soil types data from Environmental Portal",
        estimated_duration_minutes=30,
    ),
    Source.wetlands: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=2,
        description="Wetlands data processing",
        estimated_duration_minutes=45,
    ),
    Source.water_typology: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=3,
        description="Water typology data (lakes, coastal waters, watercourses)",
        estimated_duration_minutes=60,
    ),
    Source.pesticide_disaggregation: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=4,
        description="Pesticide disaggregation gold layer processing",
        estimated_duration_minutes=120,
    ),
    Source.pesticide_compliance: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=5,
        description="Pesticide regulatory compliance analysis",
        estimated_duration_minutes=90,
    ),
    Source.worker_safety: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=6,
        description="Worker safety data processing",
        estimated_duration_minutes=30,
    ),
    Source.work_permits: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=7,
        description="Work permits data processing",
        estimated_duration_minutes=45,
    ),
    Source.dmi: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MANUAL,
        priority=8,
        description="Danish Meteorological Institute climate data",
        estimated_duration_minutes=90,
    ),
    # WEEKLY AND MONTHLY PIPELINES
    # Note: CHR pipeline runs in separate dedicated workflow due to complexity
    Source.cvr_enrichment: PipelineScheduleConfig(
        frequency=ScheduleFrequency.WEEKLY_AND_MONTHLY,
        priority=1,  # For weekly: priority 1, For monthly: runs in batch 2 after foundation
        description="CVR company data enrichment pipeline (runs weekly + monthly after foundation)",
        estimated_duration_minutes=180,
    ),
    # MONTHLY PIPELINES - Foundation Data (no dependencies)
    Source.dst: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=1,
        description="Danish Statistics (Danmarks Statistik) data",
        estimated_duration_minutes=60,
    ),
    Source.fvm_wfs: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=2,
        description="FVM WFS Agricultural Data (Markblokke, Marker, Smaabiotoper)",
        estimated_duration_minutes=240,
    ),
    Source.cadastral: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=3,
        description="Danish cadastral parcels via WFS",
        estimated_duration_minutes=120,
    ),
    Source.bnbo: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=4,
        description="BNBO (Boringsnære beskyttelsesområder) status data",
        estimated_duration_minutes=90,
    ),
    Source.dagi: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=5,
        description="Danish Administrative Geographic Division",
        estimated_duration_minutes=60,
    ),
    Source.water_projects: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=6,
        description="Water projects data processing",
        estimated_duration_minutes=75,
    ),
    Source.arbejdstilsynet_inspections: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        priority=7,
        description="Danish Work Environment Authority inspections",
        estimated_duration_minutes=45,
    ),
    # MONTHLY PIPELINES - With Dependencies
    Source.property_cadastral_merge: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        depends_on=[Source.cadastral],
        priority=10,  # Run after foundation data
        description="Property-Cadastral merge gold layer (depends on cadastral)",
        estimated_duration_minutes=90,
    ),
    Source.field_production: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        depends_on=[Source.fvm_wfs, Source.dst],
        priority=11,  # Run after foundation data
        description="Field production estimates (depends on FVM and DST)",
        estimated_duration_minutes=150,
    ),
    Source.field_area_analysis: PipelineScheduleConfig(
        frequency=ScheduleFrequency.MONTHLY,
        depends_on=[Source.fvm_wfs, Source.bnbo, Source.water_projects],
        priority=12,  # Run after foundation data
        description="Field area analysis (depends on FVM, BNBO, Water projects)",
        estimated_duration_minutes=180,
    ),
}


def get_pipeline_schedule(source: Source) -> Optional[PipelineScheduleConfig]:
    """
    Get the scheduling configuration for a pipeline source.

    Args:
        source: The pipeline source to get schedule for

    Returns:
        PipelineScheduleConfig if configured, None otherwise
    """
    return PIPELINE_SCHEDULES.get(source)


def get_sources_by_frequency(frequency: ScheduleFrequency) -> List[Source]:
    """
    Get all pipeline sources that should run at the specified frequency.

    Args:
        frequency: The schedule frequency to filter by

    Returns:
        List of sources sorted by priority
    """
    sources = []

    for source, config in PIPELINE_SCHEDULES.items():
        if config.frequency == frequency:
            sources.append(source)
        elif config.frequency == ScheduleFrequency.WEEKLY_AND_MONTHLY:
            # Include WEEKLY_AND_MONTHLY sources for both weekly and monthly queries
            if frequency in [ScheduleFrequency.WEEKLY, ScheduleFrequency.MONTHLY]:
                sources.append(source)

    # Sort by priority (lower number = higher priority)
    sources.sort(key=lambda s: PIPELINE_SCHEDULES[s].priority)
    return sources


def get_sources_by_frequency_for_batch(
    frequency: ScheduleFrequency, batch_type: str = "foundation"
) -> List[Source]:
    """
    Get pipeline sources for a specific frequency and batch type.

    Args:
        frequency: The schedule frequency to filter by
        batch_type: "foundation" for no dependencies, "dependent" for sources with dependencies

    Returns:
        List of sources sorted by priority
    """
    sources = get_sources_by_frequency(frequency)

    if batch_type == "foundation":
        # Return sources with no dependencies AND exclude WEEKLY_AND_MONTHLY sources
        filtered_sources = []
        for s in sources:
            config = PIPELINE_SCHEDULES[s]
            if not config.depends_on and config.frequency != ScheduleFrequency.WEEKLY_AND_MONTHLY:
                filtered_sources.append(s)
    elif batch_type == "dependent":
        # Return sources with dependencies OR WEEKLY_AND_MONTHLY sources (like CVR enrichment)
        filtered_sources = []
        for s in sources:
            config = PIPELINE_SCHEDULES[s]
            if config.depends_on or config.frequency == ScheduleFrequency.WEEKLY_AND_MONTHLY:
                filtered_sources.append(s)
    else:
        filtered_sources = sources

    # Sort by priority
    filtered_sources.sort(key=lambda s: PIPELINE_SCHEDULES[s].priority)
    return filtered_sources


def get_dependency_order(sources: List[Source]) -> List[List[Source]]:
    """
    Organize sources into execution batches based on dependencies.

    Sources with no dependencies run first, then sources that depend on them, etc.
    Sources within the same batch can run in parallel.

    Args:
        sources: List of sources to organize

    Returns:
        List of batches, where each batch is a list of sources that can run in parallel
    """
    if not sources:
        return []

    # Create a mapping of source to its dependencies
    source_deps = {}
    for source in sources:
        config = PIPELINE_SCHEDULES.get(source)
        if config:
            # Only include dependencies that are also in our sources list
            deps = [dep for dep in config.depends_on if dep in sources]
            source_deps[source] = deps
        else:
            source_deps[source] = []

    batches = []
    remaining_sources = set(sources)

    while remaining_sources:
        # Find sources with no remaining dependencies
        ready_sources = []
        for source in remaining_sources:
            deps = source_deps[source]
            if not any(dep in remaining_sources for dep in deps):
                ready_sources.append(source)

        if not ready_sources:
            # Circular dependency or error - add remaining sources to avoid infinite loop
            ready_sources = list(remaining_sources)

        # Sort by priority within the batch
        ready_sources.sort(
            key=lambda s: PIPELINE_SCHEDULES.get(
                s, PipelineScheduleConfig(frequency=ScheduleFrequency.MANUAL)
            ).priority
        )
        batches.append(ready_sources)

        # Remove processed sources
        remaining_sources -= set(ready_sources)

    return batches


def validate_dependencies() -> List[str]:
    """
    Validate that all pipeline dependencies are properly configured.

    Returns:
        List of validation error messages (empty if all valid)
    """
    errors = []

    for source, config in PIPELINE_SCHEDULES.items():
        for dep in config.depends_on:
            if dep not in PIPELINE_SCHEDULES:
                errors.append(
                    f"{source.value} depends on {dep.value} which is not configured in "
                    f"PIPELINE_SCHEDULES"
                )
            else:
                dep_config = PIPELINE_SCHEDULES[dep]
                # Allow dependencies between same frequency or if source is WEEKLY_AND_MONTHLY
                if (
                    config.frequency != dep_config.frequency
                    and config.frequency != ScheduleFrequency.WEEKLY_AND_MONTHLY
                    and dep_config.frequency != ScheduleFrequency.WEEKLY_AND_MONTHLY
                ):
                    errors.append(
                        f"{source.value} ({config.frequency.value}) depends on {dep.value} "
                        f"({dep_config.frequency.value}) - dependencies must have compatible "
                        f"schedules"
                    )

    return errors


def get_estimated_total_duration(sources: List[Source]) -> int:
    """
    Get estimated total duration for a list of sources, accounting for parallel execution.

    Args:
        sources: List of sources to calculate duration for

    Returns:
        Estimated duration in minutes
    """
    batches = get_dependency_order(sources)
    total_duration = 0

    for batch in batches:
        # Within a batch, sources run in parallel, so take the maximum duration
        batch_duration = max(
            PIPELINE_SCHEDULES.get(
                source, PipelineScheduleConfig(frequency=ScheduleFrequency.MANUAL)
            ).estimated_duration_minutes
            or 60
            for source in batch
        )
        total_duration += batch_duration

    return total_duration
