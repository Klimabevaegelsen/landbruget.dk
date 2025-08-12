"""
Batch management utilities for CVR enrichment pipeline.

This module provides utilities for managing batch processing across
the CVR enrichment pipeline steps, including batch splitting,
merging, and coordination.
"""

import math
from typing import Any, Dict, List, Optional, Set

from unified_pipeline.util.log_util import Logger


class CVRBatchManager:
    """
    Manager for handling batch processing in CVR enrichment pipeline.

    This class provides utilities for splitting CVR numbers into batches,
    managing batch dependencies, and coordinating parallel execution.
    """

    def __init__(self, batch_size: int = 50, max_batches: Optional[int] = None):
        """
        Initialize batch manager.

        Args:
            batch_size: Number of CVR numbers per batch
            max_batches: Maximum number of batches to create (None = no limit)
        """
        self.batch_size = batch_size
        self.max_batches = max_batches
        self.log = Logger.get_logger()

    def create_cvr_batches(self, cvr_numbers: Set[str]) -> List[List[str]]:
        """
        Split CVR numbers into batches for parallel processing.

        Args:
            cvr_numbers: Set of unique CVR numbers to batch

        Returns:
            List of batches, each containing a list of CVR numbers
        """
        cvr_list = sorted(list(cvr_numbers))
        total_cvrs = len(cvr_list)

        if total_cvrs == 0:
            self.log.warning("No CVR numbers to batch")
            return []

        # Calculate number of batches
        calculated_batches = math.ceil(total_cvrs / self.batch_size)

        if self.max_batches and calculated_batches > self.max_batches:
            # Adjust batch size to fit within max_batches limit
            adjusted_batch_size = math.ceil(total_cvrs / self.max_batches)
            actual_batches = self.max_batches
            self.log.info(
                f"Adjusting batch size from {self.batch_size} to {adjusted_batch_size} "
                f"to fit {total_cvrs} CVRs into {self.max_batches} batches"
            )
        else:
            adjusted_batch_size = self.batch_size
            actual_batches = calculated_batches

        # Create batches
        batches = []
        for i in range(actual_batches):
            start_idx = i * adjusted_batch_size
            end_idx = min(start_idx + adjusted_batch_size, total_cvrs)
            batch = cvr_list[start_idx:end_idx]
            batches.append(batch)

        self.log.info(
            f"Created {len(batches)} batches from {total_cvrs} CVR numbers "
            f"(batch size: {adjusted_batch_size}, last batch: {len(batches[-1]) if batches else 0})"
        )

        return batches

    def get_batch_summary(self, batches: List[List[str]]) -> Dict[str, Any]:
        """
        Get summary statistics for a set of batches.

        Args:
            batches: List of batches

        Returns:
            Dictionary with batch summary statistics
        """
        if not batches:
            return {
                "total_batches": 0,
                "total_items": 0,
                "avg_batch_size": 0,
                "min_batch_size": 0,
                "max_batch_size": 0,
                "batch_sizes": [],
            }

        batch_sizes = [len(batch) for batch in batches]
        total_items = sum(batch_sizes)

        return {
            "total_batches": len(batches),
            "total_items": total_items,
            "avg_batch_size": total_items / len(batches),
            "min_batch_size": min(batch_sizes),
            "max_batch_size": max(batch_sizes),
            "batch_sizes": batch_sizes,
        }

    def validate_batch_coverage(
        self, original_items: Set[str], batches: List[List[str]]
    ) -> Dict[str, Any]:
        """
        Validate that batches cover all original items without duplicates.

        Args:
            original_items: Original set of items to be batched
            batches: List of batches to validate

        Returns:
            Dictionary with validation results
        """
        # Flatten batches
        batch_items = []
        for batch in batches:
            batch_items.extend(batch)

        batch_set = set(batch_items)

        # Check for missing items
        missing_items = original_items - batch_set

        # Check for duplicate items
        duplicates = []
        seen = set()
        for item in batch_items:
            if item in seen:
                duplicates.append(item)
            else:
                seen.add(item)

        # Check for extra items
        extra_items = batch_set - original_items

        validation_result = {
            "is_valid": len(missing_items) == 0 and len(duplicates) == 0 and len(extra_items) == 0,
            "original_count": len(original_items),
            "batch_count": len(batch_items),
            "unique_batch_count": len(batch_set),
            "missing_items": list(missing_items),
            "duplicate_items": list(set(duplicates)),
            "extra_items": list(extra_items),
        }

        if not validation_result["is_valid"]:
            self.log.error(f"Batch validation failed: {validation_result}")
        else:
            self.log.info("Batch validation passed - all items covered exactly once")

        return validation_result

    def calculate_optimal_batch_count(
        self,
        total_items: int,
        max_batch_size: int = 100,
        min_batch_size: int = 10,
        target_batches: Optional[int] = None,
    ) -> int:
        """
        Calculate optimal number of batches based on constraints.

        Args:
            total_items: Total number of items to batch
            max_batch_size: Maximum items per batch
            min_batch_size: Minimum items per batch
            target_batches: Target number of batches (optional)

        Returns:
            Optimal number of batches
        """
        if total_items == 0:
            return 0

        # If target batches specified, check if it's feasible
        if target_batches:
            items_per_batch = total_items / target_batches
            if min_batch_size <= items_per_batch <= max_batch_size:
                return target_batches
            else:
                self.log.warning(
                    f"Target batches {target_batches} not feasible "
                    f"(would result in {items_per_batch:.1f} items per batch, "
                    f"outside range {min_batch_size}-{max_batch_size})"
                )

        # Calculate based on max batch size constraint
        min_batches_needed = math.ceil(total_items / max_batch_size)

        # Calculate based on min batch size constraint
        max_batches_allowed = math.floor(total_items / min_batch_size)

        # Choose a reasonable number between constraints
        if min_batches_needed <= max_batches_allowed:
            # Try to get close to ideal batch size (around 50-75% of max)
            ideal_batch_size = int(max_batch_size * 0.6)
            optimal_batches = math.ceil(total_items / ideal_batch_size)

            # Ensure within constraints
            optimal_batches = max(min_batches_needed, min(optimal_batches, max_batches_allowed))
        else:
            # Constraints conflict - use minimum required
            optimal_batches = min_batches_needed
            self.log.warning(
                f"Batch size constraints conflict for {total_items} items. "
                f"Using {optimal_batches} batches (may exceed min batch size requirement)"
            )

        self.log.info(
            f"Calculated optimal batch count: {optimal_batches} batches for {total_items} items "
            f"(~{total_items / optimal_batches:.1f} items per batch)"
        )

        return optimal_batches

    def merge_batch_results(self, batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge results from multiple batch processing operations.

        Args:
            batch_results: List of results from individual batches

        Returns:
            Merged results dictionary
        """
        if not batch_results:
            return {"total_batches": 0, "total_items": 0, "successful": 0, "failed": 0}

        merged = {
            "total_batches": len(batch_results),
            "total_items": 0,
            "successful": 0,
            "failed": 0,
            "batch_summaries": [],
        }

        for i, batch_result in enumerate(batch_results):
            batch_summary = {
                "batch_number": i + 1,
                "items": batch_result.get("total", 0),
                "successful": batch_result.get("successful", 0),
                "failed": batch_result.get("failed", 0),
                "success_rate": 0.0,
            }

            if batch_summary["items"] > 0:
                batch_summary["success_rate"] = batch_summary["successful"] / batch_summary["items"]

            merged["total_items"] += batch_summary["items"]
            merged["successful"] += batch_summary["successful"]
            merged["failed"] += batch_summary["failed"]
            merged["batch_summaries"].append(batch_summary)

        # Calculate overall success rate
        if merged["total_items"] > 0:
            merged["overall_success_rate"] = merged["successful"] / merged["total_items"]
        else:
            merged["overall_success_rate"] = 0.0

        self.log.info(
            f"Merged {merged['total_batches']} batch results: "
            f"{merged['successful']}/{merged['total_items']} successful "
            f"({merged['overall_success_rate']:.1%} success rate)"
        )

        return merged
