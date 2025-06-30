#!/usr/bin/env python3
"""
Generate FVM WFS Matrix Commands

This script generates all the individual commands needed to download FVM WFS data
in parallel using the matrix job approach. It can be used for:

1. Local testing of individual layers
2. Integration with other job schedulers (Slurm, PBS, etc.)
3. Manual parallel execution using GNU parallel or similar tools

Usage:
    python generate_fvm_matrix_commands.py --help
    python generate_fvm_matrix_commands.py --layer-types markblokke --years 2024,2023
    python generate_fvm_matrix_commands.py --output-format bash
    python generate_fvm_matrix_commands.py --output-format json > fvm_commands.json
"""

import argparse
import json
from typing import Any, Dict, List


def get_layer_years(layer_type: str) -> List[int]:
    """Get valid years for a layer type."""
    layer_years = {
        "markblokke": list(range(2005, 2027)),  # 2005-2026
        "marker": list(range(2008, 2026)),  # 2008-2025
        "smaabiotoper": [2023, 2024, 2025],  # 2023-2025
    }
    return layer_years.get(layer_type, [])


def generate_matrix_items(layer_types: List[str], years: List[int] = None) -> List[Dict[str, Any]]:
    """Generate matrix items for all layer type/year combinations."""
    matrix_items = []

    for layer_type in layer_types:
        valid_years = get_layer_years(layer_type)

        if years:
            # Filter to requested years that are valid for this layer type
            target_years = [y for y in years if y in valid_years]
        else:
            # Use all valid years for this layer type
            target_years = valid_years

        for year in target_years:
            matrix_items.append({"layer_type": layer_type, "year": year, "dataset": f"fvm_{layer_type}_{year}"})

    return matrix_items


def format_as_bash_commands(matrix_items: List[Dict[str, Any]], base_command: str = None) -> List[str]:
    """Format matrix items as bash commands."""
    if base_command is None:
        base_command = "python -m unified_pipeline -s fvm_wfs -j bronze"

    commands = []
    for item in matrix_items:
        cmd = f"{base_command} --fvm-layer-type {item['layer_type']} --fvm-year {item['year']}"
        commands.append(cmd)

    return commands


def format_as_parallel_script(commands: List[str]) -> str:
    """Format commands as a GNU parallel script."""
    script = """#!/bin/bash
# FVM WFS Parallel Download Script
# Usage: ./download_fvm_parallel.sh
# Requires: GNU parallel (install with: apt-get install parallel or brew install parallel)

set -e

echo "Starting parallel download of FVM WFS data..."
echo "Total jobs: {total_jobs}"
echo "Estimated time: 15-20 minutes with 10 parallel jobs"
echo ""

# Run commands in parallel with max 10 concurrent jobs
parallel -j 10 --progress --joblog fvm_download.log \\
""".format(total_jobs=len(commands))

    for i, cmd in enumerate(commands):
        script += f'    "{cmd}"'
        if i < len(commands) - 1:
            script += " \\\n"
        else:
            script += "\n"

    script += """
echo ""
echo "All downloads completed!"
echo "Check fvm_download.log for detailed results"
"""

    return script


def main():
    parser = argparse.ArgumentParser(
        description="Generate FVM WFS matrix download commands",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all commands
  python generate_fvm_matrix_commands.py
  
  # Generate commands for specific layer types
  python generate_fvm_matrix_commands.py --layer-types markblokke marker
  
  # Generate commands for specific years
  python generate_fvm_matrix_commands.py --years 2024 2023
  
  # Generate a parallel script
  python generate_fvm_matrix_commands.py --output-format parallel > download_fvm.sh
  chmod +x download_fvm.sh
  
  # Generate JSON for other tools
  python generate_fvm_matrix_commands.py --output-format json > fvm_matrix.json
        """,
    )

    parser.add_argument(
        "--layer-types",
        nargs="+",
        choices=["markblokke", "marker", "smaabiotoper"],
        default=["markblokke", "marker", "smaabiotoper"],
        help="Layer types to include (default: all)",
    )

    parser.add_argument(
        "--years", nargs="+", type=int, help="Specific years to include (default: all valid years for each layer type)"
    )

    parser.add_argument(
        "--output-format", choices=["bash", "json", "parallel"], default="bash", help="Output format (default: bash)"
    )

    parser.add_argument(
        "--base-command",
        default="python -m unified_pipeline -s fvm_wfs -j bronze",
        help="Base command to use (default: python -m unified_pipeline -s fvm_wfs -j bronze)",
    )

    args = parser.parse_args()

    # Generate matrix items
    matrix_items = generate_matrix_items(args.layer_types, args.years)

    # Output in requested format
    if args.output_format == "json":
        print(json.dumps(matrix_items, indent=2))

    elif args.output_format == "bash":
        commands = format_as_bash_commands(matrix_items, args.base_command)
        for cmd in commands:
            print(cmd)

    elif args.output_format == "parallel":
        commands = format_as_bash_commands(matrix_items, args.base_command)
        parallel_script = format_as_parallel_script(commands)
        print(parallel_script)

    # Print summary to stderr
    import sys

    print(f"# Generated {len(matrix_items)} commands", file=sys.stderr)

    layer_counts = {}
    for item in matrix_items:
        layer_type = item["layer_type"]
        layer_counts[layer_type] = layer_counts.get(layer_type, 0) + 1

    for layer_type, count in layer_counts.items():
        print(f"# {layer_type}: {count} layers", file=sys.stderr)


if __name__ == "__main__":
    main()
