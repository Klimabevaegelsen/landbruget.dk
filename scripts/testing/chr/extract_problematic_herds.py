#!/usr/bin/env python3
"""
Extract problematic herd numbers from CHR pipeline logs.
This script helps identify which herds were causing timeouts from log output.
"""

import json
import re
import sys
from pathlib import Path
from typing import List, Set


def extract_herd_numbers_from_log(log_content: str) -> Set[int]:
    """
    Extract herd numbers from log content.

    Args:
        log_content: Raw log content as string

    Returns:
        Set of herd numbers found in the logs
    """
    herd_numbers = set()

    # Pattern to match herd numbers in various log formats
    patterns = [
        r"Herd (\d+)",
        r"herd (\d+)",
        r"Task \d+ \(herd (\d+)\)",
        r"Processing.*herd.*?(\d+)",
        r"Chunk.*herd numbers: \[([0-9, ]+)\]",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, log_content, re.IGNORECASE)
        for match in matches:
            if isinstance(match, str):
                if "," in match:  # Handle comma-separated lists
                    numbers = [int(n.strip()) for n in match.split(",") if n.strip().isdigit()]
                    herd_numbers.update(numbers)
                elif match.isdigit():
                    herd_numbers.add(int(match))

    return herd_numbers


def extract_timeout_context(log_content: str) -> List[str]:
    """
    Extract lines around timeout messages for context.

    Args:
        log_content: Raw log content as string

    Returns:
        List of relevant log lines
    """
    lines = log_content.split("\n")
    timeout_context = []

    timeout_keywords = [
        "timeout",
        "timed out",
        "TimeoutError",
        "still running when timeout occurred",
        "Overall parallel processing timed out",
    ]

    for i, line in enumerate(lines):
        if any(keyword.lower() in line.lower() for keyword in timeout_keywords):
            # Include context lines (5 before and 5 after)
            start_idx = max(0, i - 5)
            end_idx = min(len(lines), i + 6)

            timeout_context.append("--- Timeout Context ---")
            for j in range(start_idx, end_idx):
                prefix = ">>> " if j == i else "    "
                timeout_context.append(f"{prefix}{lines[j]}")
            timeout_context.append("")

    return timeout_context


def analyze_chunk_progress(log_content: str) -> dict:
    """
    Analyze chunk processing progress to identify where timeouts occurred.

    Args:
        log_content: Raw log content as string

    Returns:
        Dictionary with chunk analysis
    """
    analysis = {
        "total_chunks": 0,
        "completed_chunks": 0,
        "last_chunk_started": None,
        "chunk_herd_mapping": {},
        "timeout_chunk": None,
    }

    # Find chunk processing patterns
    chunk_start_pattern = r"Processing chunk (\d+)/(\d+)"
    chunk_complete_pattern = r"Chunk (\d+) completed"
    chunk_herds_pattern = r"Chunk (\d+) herd numbers: \[([0-9, ]+)\]"

    # Track chunk starts
    for match in re.finditer(chunk_start_pattern, log_content):
        chunk_num = int(match.group(1))
        total_chunks = int(match.group(2))
        analysis["total_chunks"] = total_chunks
        analysis["last_chunk_started"] = chunk_num

    # Track chunk completions
    completed_chunks = set()
    for match in re.finditer(chunk_complete_pattern, log_content):
        chunk_num = int(match.group(1))
        completed_chunks.add(chunk_num)

    analysis["completed_chunks"] = len(completed_chunks)

    # Map herds to chunks
    for match in re.finditer(chunk_herds_pattern, log_content):
        chunk_num = int(match.group(1))
        herd_list_str = match.group(2)
        herds = [int(h.strip()) for h in herd_list_str.split(",") if h.strip().isdigit()]
        analysis["chunk_herd_mapping"][chunk_num] = herds

    # Identify timeout chunk
    if analysis["last_chunk_started"] and analysis["last_chunk_started"] not in completed_chunks:
        analysis["timeout_chunk"] = analysis["last_chunk_started"]

    return analysis


def main():
    """Main function to process log file."""
    if len(sys.argv) != 2:
        print("Usage: python extract_problematic_herds.py <log_file>")
        print("Example: python extract_problematic_herds.py github_actions.log")
        sys.exit(1)

    log_file = Path(sys.argv[1])

    if not log_file.exists():
        print(f"Error: Log file {log_file} not found")
        sys.exit(1)

    print(f"Analyzing log file: {log_file}")

    # Read log content
    try:
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            log_content = f.read()
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(1)

    # Extract herd numbers
    herd_numbers = extract_herd_numbers_from_log(log_content)
    print(f"Found {len(herd_numbers)} unique herd numbers in logs")

    # Extract timeout context
    timeout_context = extract_timeout_context(log_content)

    # Analyze chunk progress
    chunk_analysis = analyze_chunk_progress(log_content)

    # Generate report
    report = []
    report.append("=== CHR Pipeline Log Analysis ===")
    report.append(f"Log file: {log_file}")
    report.append(f"Total herd numbers found: {len(herd_numbers)}")
    report.append("")

    # Chunk analysis
    if chunk_analysis["total_chunks"] > 0:
        report.append("=== Chunk Processing Analysis ===")
        report.append(f"Total chunks: {chunk_analysis['total_chunks']}")
        report.append(f"Completed chunks: {chunk_analysis['completed_chunks']}")
        report.append(f"Last chunk started: {chunk_analysis['last_chunk_started']}")

        if chunk_analysis["timeout_chunk"]:
            report.append(f"Timeout occurred in chunk: {chunk_analysis['timeout_chunk']}")
            timeout_herds = chunk_analysis["chunk_herd_mapping"].get(chunk_analysis["timeout_chunk"], [])
            if timeout_herds:
                report.append(f"Herds in timeout chunk: {timeout_herds}")
        report.append("")

    # Herd numbers
    if herd_numbers:
        report.append("=== Extracted Herd Numbers ===")
        sorted_herds = sorted(list(herd_numbers))

        # Show first 20 herds
        report.append(f"First 20 herds: {sorted_herds[:20]}")
        if len(sorted_herds) > 20:
            report.append(f"... and {len(sorted_herds) - 20} more")
        report.append("")

    # Timeout context
    if timeout_context:
        report.append("=== Timeout Context ===")
        report.extend(timeout_context)

    # Save results
    output_dir = Path("chr_log_analysis")
    output_dir.mkdir(exist_ok=True)

    # Save report
    report_file = output_dir / "log_analysis_report.txt"
    with open(report_file, "w") as f:
        f.write("\n".join(report))

    # Save herd numbers for use with debugging script
    herds_file = output_dir / "extracted_herds.json"
    with open(herds_file, "w") as f:
        json.dump(sorted(list(herd_numbers)), f, indent=2)

    # Save chunk analysis
    chunk_file = output_dir / "chunk_analysis.json"
    with open(chunk_file, "w") as f:
        # Convert sets to lists for JSON serialization
        serializable_analysis = chunk_analysis.copy()
        json.dump(serializable_analysis, f, indent=2)

    # Print summary
    print("\n" + "\n".join(report))
    print(f"\nResults saved to {output_dir}/")
    print(f"- {report_file}")
    print(f"- {herds_file}")
    print(f"- {chunk_file}")

    # Provide next steps
    print("\nNext steps:")
    print(f"1. Use {herds_file} with debug_chr_timeouts.py to test specific herds")
    print("2. Review timeout context above to understand failure patterns")
    if chunk_analysis["timeout_chunk"]:
        print(f"3. Focus investigation on chunk {chunk_analysis['timeout_chunk']} herds")


if __name__ == "__main__":
    main()
