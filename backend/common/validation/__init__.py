"""
Data validation utilities for landbruget.dk pipelines.

This module provides:
- Baseline comparison for pre-merge validation
- Danish identifier validation (CVR, CHR, BFE)
- Area validation for field calculations
- Validation report generation for PR comments

Used to ensure data accuracy before merging changes that could corrupt output data.
"""

from .baseline_manager import BaselineManager, BaselineMetrics
from .identifier_validators import (
    validate_cvr_format,
    validate_chr_format,
    validate_bfe_format,
    CVRValidator,
    CHRValidator,
    IdentifierValidationResult,
)
from .report_generator import ValidationReportGenerator, ValidationReport
from .area_validator import FieldAreaValidator, AreaValidationResult

__all__ = [
    # Baseline management
    "BaselineManager",
    "BaselineMetrics",
    # Identifier validation
    "validate_cvr_format",
    "validate_chr_format",
    "validate_bfe_format",
    "CVRValidator",
    "CHRValidator",
    "IdentifierValidationResult",
    # Area validation
    "FieldAreaValidator",
    "AreaValidationResult",
    # Report generation
    "ValidationReportGenerator",
    "ValidationReport",
]
