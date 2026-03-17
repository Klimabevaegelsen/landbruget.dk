"""
Danish identifier validation utilities.

Validates format and integrity of:
- CVR (Central Business Register) - 8 digits
- CHR (Central Husbandry Register) - 6 digits
- BFE (Cadastral number) - kommune-ejerlav-matr pattern

Used to ensure identifier consistency across pipeline stages.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Regex patterns for Danish identifiers
CVR_PATTERN = re.compile(r"^\d{8}$")
CHR_PATTERN = re.compile(r"^\d{6}$")
# BFE format is variable: kommune-ejerlav-matr (e.g., "0101-123456-12a")
BFE_PATTERN = re.compile(r"^\d{4}-\d+-\d+[a-z]?$")


@dataclass
class IdentifierValidationResult:
    """Result of identifier validation."""

    identifier_type: str
    total_count: int
    valid_count: int
    invalid_count: int
    null_count: int
    duplicate_count: int
    is_valid: bool
    invalid_samples: list[str] = field(default_factory=list)
    validation_message: str = ""

    @property
    def valid_pct(self) -> float:
        """Percentage of valid identifiers."""
        if self.total_count == 0:
            return 100.0
        return (self.valid_count / self.total_count) * 100

    @property
    def summary(self) -> str:
        """Human-readable summary."""
        if self.is_valid:
            return f"✅ {self.identifier_type}: {self.valid_count:,} valid ({self.valid_pct:.1f}%)"
        return (
            f"❌ {self.identifier_type}: {self.invalid_count:,} invalid, "
            f"{self.duplicate_count:,} duplicates, {self.null_count:,} null"
        )


def validate_cvr_format(value: str | int | None) -> bool:
    """
    Validate CVR number format.

    CVR must be exactly 8 digits, stored as string (preserves leading zeros).

    Args:
        value: CVR number to validate

    Returns:
        True if valid format, False otherwise

    Example:
        >>> validate_cvr_format("31373077")
        True
        >>> validate_cvr_format("1234567")  # Only 7 digits
        False
        >>> validate_cvr_format("01234567")  # Leading zero preserved
        True
    """
    if value is None:
        return False
    str_value = str(value).zfill(8) if isinstance(value, int) else str(value)
    return bool(CVR_PATTERN.match(str_value))


def validate_chr_format(value: str | int | None) -> bool:
    """
    Validate CHR number format.

    CHR must be exactly 6 digits, stored as string.

    Args:
        value: CHR number to validate

    Returns:
        True if valid format, False otherwise

    Example:
        >>> validate_chr_format("123456")
        True
        >>> validate_chr_format("12345")  # Only 5 digits
        False
    """
    if value is None:
        return False
    str_value = str(value).zfill(6) if isinstance(value, int) else str(value)
    return bool(CHR_PATTERN.match(str_value))


def validate_bfe_format(value: str | None) -> bool:
    """
    Validate BFE (cadastral) number format.

    BFE format is: kommune-ejerlav-matr (e.g., "0101-123456-12a")

    Args:
        value: BFE number to validate

    Returns:
        True if valid format, False otherwise
    """
    if value is None:
        return False
    return bool(BFE_PATTERN.match(str(value)))


class CVRValidator:
    """Validates CVR numbers in datasets."""

    def __init__(self, conn: Any = None):
        """
        Initialize CVR validator.

        Args:
            conn: Optional DuckDB connection for table validation
        """
        self.conn = conn

    def validate_column(
        self,
        df: Any,
        column: str = "cvr",
        allow_nulls: bool = False,
    ) -> IdentifierValidationResult:
        """
        Validate CVR column in a pandas DataFrame.

        Args:
            df: DataFrame to validate
            column: Column name containing CVR numbers
            allow_nulls: Whether null values are acceptable

        Returns:
            IdentifierValidationResult with validation details
        """
        if column not in df.columns:
            return IdentifierValidationResult(
                identifier_type="CVR",
                total_count=0,
                valid_count=0,
                invalid_count=0,
                null_count=0,
                duplicate_count=0,
                is_valid=False,
                validation_message=f"Column '{column}' not found in DataFrame",
            )

        total_count = len(df)
        null_count = int(df[column].isna().sum())

        # Convert to string and validate format
        non_null = df[column].dropna()
        str_values = non_null.astype(str).str.zfill(8)
        valid_mask = str_values.str.match(CVR_PATTERN.pattern)
        valid_count = int(valid_mask.sum())
        invalid_count = total_count - valid_count - null_count

        # Check for duplicates
        duplicate_count = int(non_null.duplicated().sum())

        # Get sample of invalid values
        invalid_samples = []
        if invalid_count > 0:
            invalid_values = str_values[~valid_mask]
            invalid_samples = invalid_values.head(10).tolist()

        # Determine if validation passes
        is_valid = (
            invalid_count == 0
            and duplicate_count == 0
            and (null_count == 0 or allow_nulls)
        )

        return IdentifierValidationResult(
            identifier_type="CVR",
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            null_count=null_count,
            duplicate_count=duplicate_count,
            is_valid=is_valid,
            invalid_samples=invalid_samples,
            validation_message=self._build_message(
                "CVR", is_valid, invalid_count, duplicate_count, null_count
            ),
        )

    def validate_table(
        self,
        table_name: str,
        column: str = "cvr",
        allow_nulls: bool = False,
    ) -> IdentifierValidationResult:
        """
        Validate CVR column in a DuckDB table.

        Args:
            table_name: Name of table to validate
            column: Column name containing CVR numbers
            allow_nulls: Whether null values are acceptable

        Returns:
            IdentifierValidationResult with validation details
        """
        if self.conn is None:
            raise ValueError("DuckDB connection required for table validation")

        # Get counts via SQL
        result = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count,
                COUNT(*) - COUNT({column}) as null_count,
                SUM(CASE WHEN {column} IS NOT NULL
                         AND LENGTH(CAST({column} AS VARCHAR)) = 8
                         AND REGEXP_MATCHES(CAST({column} AS VARCHAR), '^\\d{{8}}$')
                    THEN 1 ELSE 0 END) as valid_count,
                COUNT({column}) - COUNT(DISTINCT {column}) as duplicate_count
            FROM {table_name}
        """).fetchone()

        total_count, non_null_count, null_count, valid_count, duplicate_count = result
        invalid_count = non_null_count - valid_count

        # Get sample of invalid values
        invalid_samples = []
        if invalid_count > 0:
            samples = self.conn.execute(f"""
                SELECT DISTINCT CAST({column} AS VARCHAR) as cvr_str
                FROM {table_name}
                WHERE {column} IS NOT NULL
                  AND NOT REGEXP_MATCHES(CAST({column} AS VARCHAR), '^\\d{{8}}$')
                LIMIT 10
            """).fetchall()
            invalid_samples = [s[0] for s in samples]

        is_valid = (
            invalid_count == 0
            and duplicate_count == 0
            and (null_count == 0 or allow_nulls)
        )

        return IdentifierValidationResult(
            identifier_type="CVR",
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            null_count=null_count,
            duplicate_count=duplicate_count,
            is_valid=is_valid,
            invalid_samples=invalid_samples,
            validation_message=self._build_message(
                "CVR", is_valid, invalid_count, duplicate_count, null_count
            ),
        )

    def _build_message(
        self,
        id_type: str,
        is_valid: bool,
        invalid_count: int,
        duplicate_count: int,
        null_count: int,
    ) -> str:
        """Build validation message."""
        if is_valid:
            return f"✅ {id_type} validation passed"
        issues = []
        if invalid_count > 0:
            issues.append(f"{invalid_count:,} invalid format")
        if duplicate_count > 0:
            issues.append(f"{duplicate_count:,} duplicates")
        if null_count > 0:
            issues.append(f"{null_count:,} null values")
        return f"❌ {id_type} validation failed: {', '.join(issues)}"


class CHRValidator:
    """Validates CHR numbers in datasets."""

    def __init__(self, conn: Any = None):
        """
        Initialize CHR validator.

        Args:
            conn: Optional DuckDB connection for table validation
        """
        self.conn = conn

    def validate_column(
        self,
        df: Any,
        column: str = "chr",
        allow_nulls: bool = False,
    ) -> IdentifierValidationResult:
        """
        Validate CHR column in a pandas DataFrame.

        Args:
            df: DataFrame to validate
            column: Column name containing CHR numbers
            allow_nulls: Whether null values are acceptable

        Returns:
            IdentifierValidationResult with validation details
        """
        if column not in df.columns:
            return IdentifierValidationResult(
                identifier_type="CHR",
                total_count=0,
                valid_count=0,
                invalid_count=0,
                null_count=0,
                duplicate_count=0,
                is_valid=False,
                validation_message=f"Column '{column}' not found in DataFrame",
            )

        total_count = len(df)
        null_count = int(df[column].isna().sum())

        # Convert to string and validate format
        non_null = df[column].dropna()
        str_values = non_null.astype(str).str.zfill(6)
        valid_mask = str_values.str.match(CHR_PATTERN.pattern)
        valid_count = int(valid_mask.sum())
        invalid_count = total_count - valid_count - null_count

        # Check for duplicates
        duplicate_count = int(non_null.duplicated().sum())

        # Get sample of invalid values
        invalid_samples = []
        if invalid_count > 0:
            invalid_values = str_values[~valid_mask]
            invalid_samples = invalid_values.head(10).tolist()

        is_valid = (
            invalid_count == 0
            and duplicate_count == 0
            and (null_count == 0 or allow_nulls)
        )

        return IdentifierValidationResult(
            identifier_type="CHR",
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            null_count=null_count,
            duplicate_count=duplicate_count,
            is_valid=is_valid,
            invalid_samples=invalid_samples,
            validation_message=self._build_message(
                "CHR", is_valid, invalid_count, duplicate_count, null_count
            ),
        )

    def validate_table(
        self,
        table_name: str,
        column: str = "chr",
        allow_nulls: bool = False,
    ) -> IdentifierValidationResult:
        """
        Validate CHR column in a DuckDB table.

        Args:
            table_name: Name of table to validate
            column: Column name containing CHR numbers
            allow_nulls: Whether null values are acceptable

        Returns:
            IdentifierValidationResult with validation details
        """
        if self.conn is None:
            raise ValueError("DuckDB connection required for table validation")

        # Get counts via SQL
        result = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count,
                COUNT(*) - COUNT({column}) as null_count,
                SUM(CASE WHEN {column} IS NOT NULL
                         AND LENGTH(CAST({column} AS VARCHAR)) = 6
                         AND REGEXP_MATCHES(CAST({column} AS VARCHAR), '^\\d{{6}}$')
                    THEN 1 ELSE 0 END) as valid_count,
                COUNT({column}) - COUNT(DISTINCT {column}) as duplicate_count
            FROM {table_name}
        """).fetchone()

        total_count, non_null_count, null_count, valid_count, duplicate_count = result
        invalid_count = non_null_count - valid_count

        # Get sample of invalid values
        invalid_samples = []
        if invalid_count > 0:
            samples = self.conn.execute(f"""
                SELECT DISTINCT CAST({column} AS VARCHAR) as chr_str
                FROM {table_name}
                WHERE {column} IS NOT NULL
                  AND NOT REGEXP_MATCHES(CAST({column} AS VARCHAR), '^\\d{{6}}$')
                LIMIT 10
            """).fetchall()
            invalid_samples = [s[0] for s in samples]

        is_valid = (
            invalid_count == 0
            and duplicate_count == 0
            and (null_count == 0 or allow_nulls)
        )

        return IdentifierValidationResult(
            identifier_type="CHR",
            total_count=total_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
            null_count=null_count,
            duplicate_count=duplicate_count,
            is_valid=is_valid,
            invalid_samples=invalid_samples,
            validation_message=self._build_message(
                "CHR", is_valid, invalid_count, duplicate_count, null_count
            ),
        )

    def _build_message(
        self,
        id_type: str,
        is_valid: bool,
        invalid_count: int,
        duplicate_count: int,
        null_count: int,
    ) -> str:
        """Build validation message."""
        if is_valid:
            return f"✅ {id_type} validation passed"
        issues = []
        if invalid_count > 0:
            issues.append(f"{invalid_count:,} invalid format")
        if duplicate_count > 0:
            issues.append(f"{duplicate_count:,} duplicates")
        if null_count > 0:
            issues.append(f"{null_count:,} null values")
        return f"❌ {id_type} validation failed: {', '.join(issues)}"


def validate_identifiers_in_dataframe(
    df: Any,
    cvr_column: str | None = "cvr",
    chr_column: str | None = "chr",
    allow_nulls: bool = False,
) -> dict[str, IdentifierValidationResult]:
    """
    Validate all identifier columns in a DataFrame.

    Args:
        df: DataFrame to validate
        cvr_column: Column name for CVR (None to skip)
        chr_column: Column name for CHR (None to skip)
        allow_nulls: Whether null values are acceptable

    Returns:
        Dictionary mapping identifier type to validation result
    """
    results = {}

    if cvr_column and cvr_column in df.columns:
        cvr_validator = CVRValidator()
        results["CVR"] = cvr_validator.validate_column(df, cvr_column, allow_nulls)

    if chr_column and chr_column in df.columns:
        chr_validator = CHRValidator()
        results["CHR"] = chr_validator.validate_column(df, chr_column, allow_nulls)

    return results


def normalize_cvr(value: str | int | None) -> str | None:
    """
    Normalize CVR number to 8-digit string format.

    Args:
        value: CVR number (string or int)

    Returns:
        8-digit string with leading zeros, or None if invalid

    Example:
        >>> normalize_cvr(1234567)
        '01234567'
        >>> normalize_cvr("31373077")
        '31373077'
    """
    if value is None:
        return None
    str_value = str(value).strip().zfill(8)
    if validate_cvr_format(str_value):
        return str_value
    return None


def normalize_chr(value: str | int | None) -> str | None:
    """
    Normalize CHR number to 6-digit string format.

    Args:
        value: CHR number (string or int)

    Returns:
        6-digit string with leading zeros, or None if invalid
    """
    if value is None:
        return None
    str_value = str(value).strip().zfill(6)
    if validate_chr_format(str_value):
        return str_value
    return None
