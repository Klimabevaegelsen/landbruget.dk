"""
CLI commands for pipeline scheduling management.

This module provides command-line utilities for viewing and managing
pipeline scheduling configurations.
"""

import click

try:
    from tabulate import tabulate
except ImportError:

    def tabulate(data, headers=None, tablefmt="grid"):
        """Fallback tabulate implementation when package not available."""
        if not data:
            return "No data"

        # Simple table formatting fallback
        result = []
        if headers:
            result.append(" | ".join(str(h) for h in headers))
            result.append("-" * (sum(len(str(h)) for h in headers) + len(headers) * 3 - 3))

        for row in data:
            result.append(" | ".join(str(cell) for cell in row))

        return "\n".join(result)


from unified_pipeline.model.cli import Source
from unified_pipeline.model.scheduling import (
    PIPELINE_SCHEDULES,
    ScheduleFrequency,
    get_dependency_order,
    get_sources_by_frequency,
    validate_dependencies,
)


@click.group()
def scheduling():
    """Pipeline scheduling management commands."""
    pass


@scheduling.command()
@click.option(
    "--frequency",
    type=click.Choice(["manual", "weekly", "monthly", "weekly_and_monthly", "all"]),
    default="all",
    help="Filter by schedule frequency",
)
@click.option("--show-dependencies", is_flag=True, help="Show dependency information")
def list_schedules(frequency: str, show_dependencies: bool):
    """List all pipeline schedules with their configurations."""

    if frequency == "all":
        sources = list(PIPELINE_SCHEDULES.keys())
    else:
        freq_enum = ScheduleFrequency(frequency)
        sources = get_sources_by_frequency(freq_enum)

    if not sources:
        click.echo(f"No pipelines found for frequency: {frequency}")
        return

    # Prepare table data
    headers = ["Source", "Frequency", "Priority", "Duration (min)", "Description"]
    if show_dependencies:
        headers.append("Dependencies")

    table_data = []
    for source in sources:
        config = PIPELINE_SCHEDULES[source]
        row = [
            source.value,
            config.frequency.value,
            config.priority,
            config.estimated_duration_minutes or "N/A",
            config.description[:60] + "..." if len(config.description) > 60 else config.description,
        ]

        if show_dependencies:
            deps = [dep.value for dep in config.depends_on] if config.depends_on else []
            row.append(", ".join(deps) if deps else "None")

        table_data.append(row)

    # Sort by frequency priority, then by pipeline priority
    freq_order = {"manual": 0, "weekly": 1, "monthly": 2, "weekly_and_monthly": 3}
    table_data.sort(key=lambda x: (freq_order.get(x[1], 4), int(x[2])))

    click.echo(f"\n📋 Pipeline Schedules ({len(table_data)} pipelines)")
    click.echo("=" * 80)
    click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))

    # Show summary by frequency
    freq_counts = {}
    total_duration = 0
    for source in sources:
        config = PIPELINE_SCHEDULES[source]
        freq_counts[config.frequency.value] = freq_counts.get(config.frequency.value, 0) + 1
        if config.estimated_duration_minutes:
            total_duration += config.estimated_duration_minutes

    click.echo("\n📊 Summary:")
    for freq, count in sorted(freq_counts.items()):
        click.echo(f"  {freq.title()}: {count} pipelines")
    click.echo(
        f"  Total estimated duration: {total_duration} minutes ({total_duration/60:.1f} hours)"
    )


@scheduling.command()
@click.option(
    "--frequency",
    type=click.Choice(["manual", "weekly", "monthly", "weekly_and_monthly"]),
    required=True,
    help="Schedule frequency to show execution order for",
)
def execution_order(frequency: str):
    """Show the execution order for pipelines of a specific frequency."""

    freq_enum = ScheduleFrequency(frequency)
    sources = get_sources_by_frequency(freq_enum)

    if not sources:
        click.echo(f"No pipelines found for frequency: {frequency}")
        return

    batches = get_dependency_order(sources)

    click.echo(f"\n🚀 Execution Order for {frequency.title()} Pipelines")
    click.echo("=" * 60)

    total_duration = 0
    for i, batch in enumerate(batches, 1):
        batch_duration = max(
            PIPELINE_SCHEDULES[source].estimated_duration_minutes or 60 for source in batch
        )
        total_duration += batch_duration

        click.echo(f"\n📦 Batch {i} (Parallel execution, ~{batch_duration} min):")

        for source in batch:
            config = PIPELINE_SCHEDULES[source]
            deps_str = ""
            if config.depends_on:
                deps_str = f" (depends on: {', '.join(dep.value for dep in config.depends_on)})"

            click.echo(f"  • {source.value:<30} {config.description[:40]}...{deps_str}")

    click.echo(
        f"\n⏱️ Total estimated duration: {total_duration} minutes ({total_duration/60:.1f} hours)"
    )
    click.echo(f"📊 {len(sources)} pipelines in {len(batches)} execution batches")


@scheduling.command()
def validate():
    """Validate pipeline scheduling configuration."""

    click.echo("🔍 Validating pipeline scheduling configuration...")

    errors = validate_dependencies()

    if not errors:
        click.echo("✅ All pipeline scheduling configurations are valid!")
        return

    click.echo(f"❌ Found {len(errors)} validation errors:")
    for i, error in enumerate(errors, 1):
        click.echo(f"  {i}. {error}")

    click.echo("\n💡 Please fix these errors before running scheduled pipelines.")


@scheduling.command()
@click.argument("source", type=click.Choice([s.value for s in Source]))
def info(source: str):
    """Show detailed information about a specific pipeline's schedule."""

    source_enum = Source(source)
    config = PIPELINE_SCHEDULES.get(source_enum)

    if not config:
        click.echo(f"❌ No scheduling configuration found for {source}")
        return

    click.echo(f"\n📋 Pipeline Schedule: {source}")
    click.echo("=" * 50)
    click.echo(f"Frequency:     {config.frequency.value}")
    click.echo(f"Priority:      {config.priority}")
    click.echo(f"Duration:      {config.estimated_duration_minutes or 'N/A'} minutes")
    click.echo(f"Description:   {config.description}")

    if config.depends_on:
        click.echo(f"Dependencies:  {', '.join(dep.value for dep in config.depends_on)}")
    else:
        click.echo("Dependencies:  None")

    # Show which pipelines depend on this one
    dependents = []
    for other_source, other_config in PIPELINE_SCHEDULES.items():
        if source_enum in other_config.depends_on:
            dependents.append(other_source.value)

    if dependents:
        click.echo(f"Used by:       {', '.join(dependents)}")
    else:
        click.echo("Used by:       None")


if __name__ == "__main__":
    scheduling()
