"""
Validation report generation for PR comments.

Generates markdown-formatted reports from validation results,
suitable for posting as GitHub PR comments.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .baseline_manager import ComparisonResult
from .identifier_validators import IdentifierValidationResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Complete validation report for a PR."""

    title: str
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    overall_passed: bool = True
    baseline_comparisons: list[ComparisonResult] = field(default_factory=list)
    identifier_validations: list[IdentifierValidationResult] = field(
        default_factory=list
    )
    custom_checks: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    branch_name: str | None = None
    commit_sha: str | None = None

    @property
    def failed_count(self) -> int:
        """Count of failed checks."""
        count = 0
        count += sum(1 for c in self.baseline_comparisons if not c.is_valid)
        count += sum(1 for i in self.identifier_validations if not i.is_valid)
        count += sum(1 for c in self.custom_checks if not c.get("passed", True))
        return count

    @property
    def passed_count(self) -> int:
        """Count of passed checks."""
        count = 0
        count += sum(1 for c in self.baseline_comparisons if c.is_valid)
        count += sum(1 for i in self.identifier_validations if i.is_valid)
        count += sum(1 for c in self.custom_checks if c.get("passed", True))
        return count


class ValidationReportGenerator:
    """Generates markdown reports from validation results."""

    def __init__(self):
        """Initialize report generator."""
        self.report = ValidationReport(title="Data Quality Validation Report")

    def set_metadata(
        self,
        branch_name: str | None = None,
        commit_sha: str | None = None,
    ):
        """Set report metadata."""
        self.report.branch_name = branch_name
        self.report.commit_sha = commit_sha

    def add_baseline_comparison(self, result: ComparisonResult):
        """Add baseline comparison result to report."""
        self.report.baseline_comparisons.append(result)
        if not result.is_valid:
            self.report.overall_passed = False

    def add_identifier_validation(self, result: IdentifierValidationResult):
        """Add identifier validation result to report."""
        self.report.identifier_validations.append(result)
        if not result.is_valid:
            self.report.overall_passed = False

    def add_custom_check(
        self,
        name: str,
        passed: bool,
        message: str,
        details: dict[str, Any] | None = None,
    ):
        """Add custom check result to report."""
        self.report.custom_checks.append(
            {
                "name": name,
                "passed": passed,
                "message": message,
                "details": details or {},
            }
        )
        if not passed:
            self.report.overall_passed = False

    def add_warning(self, message: str):
        """Add a warning message to report."""
        self.report.warnings.append(message)

    def generate_markdown(self) -> str:
        """Generate markdown-formatted report."""
        lines = []

        # Header
        status_emoji = "✅" if self.report.overall_passed else "❌"
        lines.append(f"## {status_emoji} {self.report.title}")
        lines.append("")

        # Summary
        lines.append("### Summary")
        lines.append("")
        lines.append("| Status | Passed | Failed |")
        lines.append("|--------|--------|--------|")
        status = "PASSED" if self.report.overall_passed else "**FAILED**"
        lines.append(
            f"| {status} | {self.report.passed_count} | {self.report.failed_count} |"
        )
        lines.append("")

        # Metadata
        if self.report.branch_name or self.report.commit_sha:
            lines.append("### Metadata")
            lines.append("")
            if self.report.branch_name:
                lines.append(f"- **Branch**: `{self.report.branch_name}`")
            if self.report.commit_sha:
                lines.append(f"- **Commit**: `{self.report.commit_sha[:8]}`")
            lines.append(f"- **Generated**: {self.report.generated_at}")
            lines.append("")

        # Baseline Comparisons
        if self.report.baseline_comparisons:
            lines.append("### Baseline Comparisons")
            lines.append("")
            lines.append("| Dataset | Status | Details |")
            lines.append("|---------|--------|---------|")

            for comparison in self.report.baseline_comparisons:
                status = "✅ PASS" if comparison.is_valid else "❌ FAIL"
                failed = comparison.failed_checks
                if failed:
                    details = "; ".join(c["message"] for c in failed[:2])
                    if len(failed) > 2:
                        details += f" (+{len(failed) - 2} more)"
                else:
                    details = f"All {len(comparison.checks)} checks passed"
                lines.append(f"| {comparison.dataset_name} | {status} | {details} |")

            lines.append("")

            # Detailed failures
            failed_comparisons = [
                c for c in self.report.baseline_comparisons if not c.is_valid
            ]
            if failed_comparisons:
                lines.append("#### Failed Baseline Checks")
                lines.append("")

                for comparison in failed_comparisons:
                    lines.append(f"**{comparison.dataset_name}**")
                    lines.append("")
                    for check in comparison.failed_checks:
                        lines.append(f"- {check['message']}")
                        if "baseline" in check and "current" in check:
                            lines.append(
                                f"  - Baseline: `{check['baseline']:,}` → Current: `{check['current']:,}`"
                            )
                    lines.append("")

        # Identifier Validations
        if self.report.identifier_validations:
            lines.append("### Identifier Format Validation")
            lines.append("")
            lines.append("| Type | Total | Valid | Invalid | Duplicates | Status |")
            lines.append("|------|-------|-------|---------|------------|--------|")

            for validation in self.report.identifier_validations:
                status = "✅" if validation.is_valid else "❌"
                lines.append(
                    f"| {validation.identifier_type} | "
                    f"{validation.total_count:,} | "
                    f"{validation.valid_count:,} | "
                    f"{validation.invalid_count:,} | "
                    f"{validation.duplicate_count:,} | "
                    f"{status} |"
                )

            lines.append("")

            # Show invalid samples
            failed_validations = [
                v for v in self.report.identifier_validations if not v.is_valid
            ]
            if failed_validations:
                lines.append("#### Invalid Identifier Samples")
                lines.append("")

                for validation in failed_validations:
                    if validation.invalid_samples:
                        lines.append(
                            f"**{validation.identifier_type}** invalid samples:"
                        )
                        lines.append("```")
                        for sample in validation.invalid_samples[:5]:
                            lines.append(f"  {sample}")
                        lines.append("```")
                        lines.append("")

        # Custom Checks
        if self.report.custom_checks:
            lines.append("### Additional Checks")
            lines.append("")
            lines.append("| Check | Status | Message |")
            lines.append("|-------|--------|---------|")

            for check in self.report.custom_checks:
                status = "✅" if check["passed"] else "❌"
                lines.append(f"| {check['name']} | {status} | {check['message']} |")

            lines.append("")

        # Warnings
        if self.report.warnings:
            lines.append("### Warnings")
            lines.append("")
            for warning in self.report.warnings:
                lines.append(f"⚠️ {warning}")
            lines.append("")

        # Footer
        lines.append("---")
        lines.append("")
        if self.report.overall_passed:
            lines.append(
                "✅ **All data quality checks passed.** This PR is safe to merge."
            )
        else:
            lines.append(
                "❌ **Data quality checks failed.** Please fix the issues before merging."
            )
            lines.append("")
            lines.append("Run locally to debug:")
            lines.append("```bash")
            lines.append("pytest backend/tests/validation/ -m pre_merge -v")
            lines.append("```")

        return "\n".join(lines)

    def generate_json(self) -> dict[str, Any]:
        """Generate JSON-formatted report for programmatic use."""
        return {
            "title": self.report.title,
            "generated_at": self.report.generated_at,
            "overall_passed": self.report.overall_passed,
            "passed_count": self.report.passed_count,
            "failed_count": self.report.failed_count,
            "branch_name": self.report.branch_name,
            "commit_sha": self.report.commit_sha,
            "baseline_comparisons": [
                {
                    "dataset": c.dataset_name,
                    "is_valid": c.is_valid,
                    "checks": c.checks,
                    "tolerance_pct": c.tolerance_pct,
                }
                for c in self.report.baseline_comparisons
            ],
            "identifier_validations": [
                {
                    "type": v.identifier_type,
                    "total": v.total_count,
                    "valid": v.valid_count,
                    "invalid": v.invalid_count,
                    "duplicates": v.duplicate_count,
                    "is_valid": v.is_valid,
                }
                for v in self.report.identifier_validations
            ],
            "custom_checks": self.report.custom_checks,
            "warnings": self.report.warnings,
        }

    def save_markdown(self, path: str):
        """Save markdown report to file."""
        file_path = Path(path)
        with file_path.open("w") as f:
            f.write(self.generate_markdown())
        logger.info(f"Saved validation report to {path}")

    def save_json(self, path: str):
        """Save JSON report to file."""
        import json

        file_path = Path(path)
        with file_path.open("w") as f:
            json.dump(self.generate_json(), f, indent=2)
        logger.info(f"Saved validation report to {path}")
