"""Base validator for Silver layer."""

import abc
from dataclasses import dataclass
from typing import Any

from ...utils.logging import get_logger

# Get logger
logger = get_logger()


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    is_valid: bool
    errors: list[str] = None
    warnings: list[str] = None
    
    def __post_init__(self):
        """Initialize lists if None."""
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


class BaseValidator(abc.ABC):
    """Base class for all validators."""

    def __init__(self):
        """Initialize the validator."""
        logger.debug(f"Initialized {self.__class__.__name__}")
    
    @abc.abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Validate the data.

        Args:
            data: Data to validate

        Returns:
            ValidationResult with the result of the validation
        """
        pass
    
    def add_error(self, result: ValidationResult, error: str):
        """Add an error to the validation result.

        Args:
            result: ValidationResult to update
            error: Error message to add
        """
        result.errors.append(error)
        result.is_valid = False
        logger.warning(f"Validation error: {error}")
    
    def add_warning(self, result: ValidationResult, warning: str):
        """Add a warning to the validation result.

        Args:
            result: ValidationResult to update
            warning: Warning message to add
        """
        result.warnings.append(warning)
        logger.debug(f"Validation warning: {warning}")


class SchemaValidator(BaseValidator):
    """Validator for checking data schema."""

    def __init__(self, required_columns: set[str] | None = None):
        """Initialize the schema validator.

        Args:
            required_columns: Set of column names that must be present
        """
        super().__init__()
        self.required_columns = required_columns or set()
    
    def validate(self, data: Any) -> ValidationResult:
        """Validate the data schema.

        Args:
            data: Data to validate (e.g., DataFrame, Ibis table)

        Returns:
            ValidationResult with the result of the validation
        """
        result = ValidationResult(is_valid=True)
        
        # Get columns from data
        columns = self._get_columns(data)
        
        # Check required columns
        for col in self.required_columns:
            if col not in columns:
                self.add_error(
                    result, f"Required column '{col}' is missing"
                )
        
        return result
    
    def _get_columns(self, data: Any) -> list[str]:
        """Get column names from data.

        Args:
            data: Data to get columns from

        Returns:
            List of column names
        """
        # Handle different data types
        if hasattr(data, 'columns'):
            # This covers pandas DataFrames
            return data.columns.tolist()
        elif hasattr(data, 'column_names'):
            # This covers some Ibis tables
            return data.column_names
        else:
            logger.warning("Could not determine columns from data")
            return []


class DataTypeValidator(BaseValidator):
    """Validator for checking data types."""

    def __init__(self, expected_types: dict[str, str]):
        """Initialize the data type validator.

        Args:
            expected_types: Dictionary mapping column names to expected types
        """
        super().__init__()
        self.expected_types = expected_types
    
    def validate(self, data: Any) -> ValidationResult:
        """Validate the data types.

        Args:
            data: Data to validate (e.g., DataFrame)

        Returns:
            ValidationResult with the result of the validation
        """
        result = ValidationResult(is_valid=True)
        
        # This is a placeholder - actual implementation will depend on
        # whether we're using pandas, DuckDB, Ibis, or another library
        
        # Example implementation for pandas DataFrame
        if hasattr(data, 'dtypes'):
            for col, expected_type in self.expected_types.items():
                if col in data.columns:
                    actual_type = str(data[col].dtype)
                    if not self._is_compatible_type(actual_type, expected_type):
                        self.add_error(
                            result,
                            f"Column '{col}' has type '{actual_type}' "
                            f"but expected '{expected_type}'",
                        )
        
        return result
    
    def _is_compatible_type(self, actual: str, expected: str) -> bool:
        """Check if actual type is compatible with expected type.

        Args:
            actual: Actual data type
            expected: Expected data type

        Returns:
            True if compatible, False otherwise
        """
        # Simple string matching for now
        # More sophisticated type compatibility could be implemented
        return expected in actual 