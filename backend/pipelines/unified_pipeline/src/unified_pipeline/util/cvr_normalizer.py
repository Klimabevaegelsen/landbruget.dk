"""
CVR Normalization Utility

Provides standardized CVR (Central Business Register) number normalization
for Danish agricultural data pipelines.

CVR Format:
- Exactly 8 digits
- Stored as string (to preserve leading zeros)
- Example: "31373077"

This module handles:
1. XML encoding in FVM data (e.g., "_x0034_" → "4")
2. Zero-padding short CVR numbers
3. Float-to-string conversion (from CSV imports)
4. Validation against Danish CVR format
"""

import contextlib
import re

# Regex pattern for valid 8-digit CVR
CVR_PATTERN = re.compile(r"^\d{8}$")

# XML hex encoding pattern (e.g., "_x0034_" represents "4")
XML_HEX_PATTERN = re.compile(r"_x([0-9A-Fa-f]{4})_")


def normalize_cvr(value: str | int | float | None) -> str | None:
    """
    Normalize a CVR number to 8-digit string format.

    Handles:
    - XML hex encoding from FVM Excel exports (e.g., "_x0034_" → "4")
    - Float values from CSV imports (e.g., 12345678.0 → "12345678")
    - Integer values (e.g., 12345678 → "12345678")
    - Short CVR numbers (e.g., "1234567" → "01234567")

    Args:
        value: CVR number in various formats

    Returns:
        8-digit CVR string, or None if invalid/empty

    Examples:
        >>> normalize_cvr("31373077")
        '31373077'
        >>> normalize_cvr(31373077.0)
        '31373077'
        >>> normalize_cvr("_x0033_1373077")
        '31373077'
        >>> normalize_cvr("1234567")
        '01234567'
        >>> normalize_cvr(None)
        None
    """
    if value is None:
        return None

    # Handle empty strings
    if isinstance(value, str) and value.strip() == "":
        return None

    # Convert to string
    s = str(value)

    # Handle float conversion (e.g., "12345678.0" → "12345678")
    if "." in s:
        with contextlib.suppress(ValueError, OverflowError):
            s = str(int(float(s)))

    # Decode XML hex encoding (e.g., "_x0034_" → "4")
    s = XML_HEX_PATTERN.sub(lambda m: chr(int(m.group(1), 16)), s)

    # Extract only digits
    digits = "".join(c for c in s if c.isdigit())

    # Validate and pad
    if len(digits) == 0:
        return None
    if len(digits) > 8:
        # Too many digits - might be concatenated or invalid
        return None
    if len(digits) <= 8:
        # Zero-pad to 8 digits
        return digits.zfill(8)

    return None


def validate_cvr(value: str) -> bool:
    """
    Validate that a CVR string matches the required format.

    Args:
        value: CVR string to validate

    Returns:
        True if valid 8-digit CVR, False otherwise

    Examples:
        >>> validate_cvr("31373077")
        True
        >>> validate_cvr("1234567")  # Only 7 digits
        False
        >>> validate_cvr("123456789")  # 9 digits
        False
    """
    if not isinstance(value, str):
        return False
    return bool(CVR_PATTERN.match(value))


def normalize_cvr_column_duckdb(
    conn, table_name: str, source_column: str, target_column: str = "cvr"
) -> int:
    """
    Normalize a CVR column in a DuckDB table in-place.

    Creates a new column with normalized CVR values. Handles:
    - XML hex decoding
    - Float-to-int conversion
    - Zero-padding

    Args:
        conn: DuckDB connection
        table_name: Name of table to modify
        source_column: Column containing raw CVR values
        target_column: Name for normalized CVR column (default: "cvr")

    Returns:
        Number of rows with valid normalized CVR

    Example:
        >>> normalize_cvr_column_duckdb(conn, "raw_subsidies", "CVR_NR", "cvr")
        24993
    """
    # Use Python UDF for normalization - cleaner than complex SQL regex
    conn.execute(f"""
    ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {target_column} VARCHAR;
    """)

    # Create Python UDF for normalization
    conn.create_function("normalize_cvr_udf", normalize_cvr, [str], str)

    conn.execute(f"""
    UPDATE {table_name}
    SET {target_column} = normalize_cvr_udf(CAST({source_column} AS VARCHAR))
    WHERE {source_column} IS NOT NULL;
    """)

    # Count valid CVRs
    result = conn.execute(f"""
    SELECT COUNT(*) FROM {table_name}
    WHERE {target_column} IS NOT NULL
    AND {target_column} ~ '^\\d{{8}}$'
    """).fetchone()

    return result[0] if result else 0


def get_cvr_quality_stats(conn, table_name: str, cvr_column: str = "cvr") -> dict:
    """
    Get quality statistics for a CVR column.

    Args:
        conn: DuckDB connection
        table_name: Table to analyze
        cvr_column: Column containing CVR values

    Returns:
        Dictionary with quality metrics
    """
    stats = conn.execute(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT({cvr_column}) AS non_null_cvrs,
        COUNT(CASE WHEN {cvr_column} ~ '^\\d{{8}}$' THEN 1 END) AS valid_8digit_cvrs,
        COUNT(DISTINCT {cvr_column}) AS unique_cvrs,
        COUNT(CASE WHEN {cvr_column} IS NULL THEN 1 END) AS null_cvrs
    FROM {table_name}
    """).fetchone()

    return {
        "total_rows": stats[0],
        "non_null_cvrs": stats[1],
        "valid_8digit_cvrs": stats[2],
        "unique_cvrs": stats[3],
        "null_cvrs": stats[4],
        "valid_pct": round(100 * stats[2] / stats[0], 2) if stats[0] > 0 else 0,
    }


# Mapping of historic CVR column names to standardized name
# Used when processing FVM data that evolved over time
CVR_COLUMN_ALIASES = {
    # FVM Marker columns (evolved 2008-2025)
    "Ansoeger": "cvr",  # 2008-2011 (applicant ID, often = CVR)
    "KUNDE_LB": "cvr",  # 2012-2013 (customer ID)
    "CVR": "cvr",  # 2016-2025 (official CVR)
    # støtteoplysninger columns
    "CVR-nummer": "cvr",
    "cvr_number": "cvr",
    "Cvr": "cvr",
    # De minimis columns
    "CVR_NR": "cvr",
    "cvr_nr": "cvr",
}


def find_cvr_column(columns: list) -> str | None:
    """
    Find the CVR column in a list of column names.

    Args:
        columns: List of column names

    Returns:
        Name of the CVR column, or None if not found
    """
    for col in columns:
        if col in CVR_COLUMN_ALIASES or col.upper() == "CVR":
            return col
    return None
