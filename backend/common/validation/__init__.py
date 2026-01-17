"""
Data validation utilities for landbruget.dk pipelines.

This module provides:
- Baseline comparison for pre-merge validation
- Danish identifier validation (CVR, CHR, BFE)
- Area validation for field calculations
- Validation report generation for PR comments

Used to ensure data accuracy before merging changes that could corrupt output data.
"""

from .area_validator import AreaValidationResult, FieldAreaValidator
from .baseline_manager import BaselineManager, BaselineMetrics
from .identifier_validators import (
    CHRValidator,
    CVRValidator,
    IdentifierValidationResult,
    validate_bfe_format,
    validate_chr_format,
    validate_cvr_format,
)
from .report_generator import ValidationReport, ValidationReportGenerator

__all__ = [
    "AreaValidationResult",
    # Baseline management
    "BaselineManager",
    "BaselineMetrics",
    "CHRValidator",
    "CVRValidator",
    # Area validation
    "FieldAreaValidator",
    "IdentifierValidationResult",
    "ValidationReport",
    # Report generation
    "ValidationReportGenerator",
    "validate_bfe_format",
    "validate_chr_format",
    # Identifier validation
    "validate_cvr_format",
]
