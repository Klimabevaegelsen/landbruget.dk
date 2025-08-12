"""PII (Personally Identifiable Information) validator for Silver layer using DuckDB."""

from enum import Enum
from typing import Any

# Handle imports for both standalone and package usage
try:
    from ...utils.logging import get_logger
    from ..duckdb_base import DuckDBProcessor
except ImportError:
    # Fallback for standalone usage
    import logging

    def get_logger():
        return logging.getLogger(__name__)

    from silver.duckdb_base import DuckDBProcessor
from .base import BaseValidator, ValidationResult

# Get logger
logger = get_logger()


class PIIAction(Enum):
    """Action to take when PII is detected."""

    REPORT = "report"  # Just report the PII
    MASK = "mask"  # Mask the PII (e.g., replace with ***)
    HASH = "hash"  # Hash the PII
    DELETE = "delete"  # Delete the column containing PII


class PIIType(Enum):
    """Types of PII that can be detected."""

    EMAIL = "email"
    PHONE = "phone"
    CPR = "cpr"  # Danish personal ID number (CPR-nummer)
    CVR = "cvr"  # Danish company ID number (CVR-nummer)
    ADDRESS = "address"
    NAME = "name"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"


class PIIValidator(BaseValidator, DuckDBProcessor):
    """Validator for detecting and handling PII using DuckDB."""

    # Regular expressions for different PII types
    PII_PATTERNS = {
        PIIType.EMAIL: r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        # Danish phone: ONLY with +45 country code to avoid false positives with dates/other numbers
        PIIType.PHONE: r"\b\+45[ -]?\d{2}[ -]?\d{2}[ -]?\d{2}[ -]?\d{2}\b",
        PIIType.CPR: r"\b\d{6}[-]?\d{4}\b",
        PIIType.CVR: r"\b\d{8}\b",
        PIIType.CREDIT_CARD: r"\b(?:\d{4}[ -]?){3}\d{4}\b",
        PIIType.IP_ADDRESS: r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    }

    def __init__(
        self,
        pii_types: set[PIIType] = None,
        action: PIIAction = PIIAction.REPORT,
        threshold: float = 0.3,
        column_name_hints: dict[PIIType, list[str]] = None,
    ) -> None:
        """Initialize the PII validator.

        Args:
            pii_types: Set of PII types to detect
            action: Action to take when PII is detected
            threshold: Threshold for detecting PII (0-1)
                      (e.g., 0.3 means 30% of values need to match to flag a column)
            column_name_hints: Dictionary mapping PII types to column name patterns
        """
        BaseValidator.__init__(self)
        DuckDBProcessor.__init__(self)

        self.pii_types = pii_types or {
            PIIType.EMAIL,
            PIIType.PHONE,
            PIIType.CPR,
            PIIType.CVR,
            PIIType.CREDIT_CARD,
            PIIType.IP_ADDRESS,
        }
        self.action = action
        self.threshold = threshold

        # Default column name hints
        self._default_name_hints = {
            PIIType.EMAIL: ["email", "e-mail", "mail"],
            PIIType.PHONE: ["phone", "mobil", "telefon", "tlf"],
            PIIType.CPR: ["cpr", "personnummer", "person_id", "ssn"],
            PIIType.CVR: [
                "cvr",
                "cvr_number",
                "virksomhedsnummer",
                "company_id",
                "companyregistrationnumber",
            ],
            PIIType.ADDRESS: ["address", "adresse", "street", "vej"],
            PIIType.NAME: ["name", "navn", "first_name", "last_name", "fornavn", "efternavn"],
            PIIType.CREDIT_CARD: ["credit_card", "creditcard", "card_number", "kortnummer"],
            PIIType.IP_ADDRESS: ["ip", "ip_address", "ipaddress"],
        }

        # Combine default hints with user-provided hints
        self.column_name_hints = self._default_name_hints
        if column_name_hints:
            for pii_type, hints in column_name_hints.items():
                if pii_type in self.column_name_hints:
                    self.column_name_hints[pii_type].extend(hints)
                else:
                    self.column_name_hints[pii_type] = hints

        logger.info("Initialized PIIValidator with DuckDB")

    def validate(self, table_name_or_data: Any) -> ValidationResult:
        """Validate data for PII using DuckDB.

        Args:
            table_name_or_data: DuckDB table name (str) or data to validate

        Returns:
            ValidationResult with PII detection results
        """
        result = ValidationResult(is_valid=True)

        try:
            # Handle different input types
            if isinstance(table_name_or_data, str):
                table_name = table_name_or_data
            else:
                # Register data as a table
                table_name = "pii_validation_data"
                self.register_table(table_name_or_data, table_name)

            # Get table information
            columns_info = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            if not columns_info:
                self.add_error(result, "No columns found in data")
                return result

            column_names = [col[0] for col in columns_info]

            # Track PII columns by type
            pii_columns = {}

            # Check column names first
            for pii_type in self.pii_types:
                if pii_type not in self.column_name_hints:
                    continue

                hints = self.column_name_hints[pii_type]

                for col in column_names:
                    col_lower = col.lower()
                    for hint in hints:
                        if hint.lower() in col_lower:
                            # Add to PII columns
                            if pii_type not in pii_columns:
                                pii_columns[pii_type] = []

                            pii_columns[pii_type].append(col)
                            self.add_warning(
                                result,
                                f"Column '{col}' might contain {pii_type.value} based on name",
                            )

            # Check column contents using DuckDB regex
            for pii_type in self.pii_types:
                if pii_type not in self.PII_PATTERNS:
                    continue

                pattern = self.PII_PATTERNS[pii_type]

                for col_name, col_type, *_ in columns_info:
                    # Skip non-string columns
                    if "VARCHAR" not in col_type.upper() and "TEXT" not in col_type.upper():
                        continue

                    # Skip columns already identified by name
                    if pii_type in pii_columns and col_name in pii_columns[pii_type]:
                        continue

                    # Skip date-related columns for phone detection to avoid false positives
                    if pii_type == PIIType.PHONE:
                        col_lower = col_name.lower()
                        date_keywords = [
                            "date",
                            "dato",
                            "time",
                            "tid",
                            "arrival",
                            "ankomst",
                            "departure",
                            "afgang",
                        ]
                        if any(keyword in col_lower for keyword in date_keywords):
                            logger.debug(f"Skipping phone detection for date column: {col_name}")
                            continue

                    # Check for PII in column values using DuckDB regex
                    try:
                        # Count total non-null values
                        total_count = self.conn.execute(f"""
                            SELECT COUNT(*) 
                            FROM {table_name} 
                            WHERE {col_name} IS NOT NULL AND {col_name} != ''
                        """).fetchone()[0]

                        if total_count == 0:
                            continue

                        # Count values that match the pattern
                        match_count = self.conn.execute(f"""
                            SELECT COUNT(*) 
                            FROM {table_name} 
                            WHERE {col_name} ~ '{pattern}'
                        """).fetchone()[0]

                        match_ratio = match_count / total_count if total_count > 0 else 0

                        if match_ratio >= self.threshold:
                            # Add to PII columns
                            if pii_type not in pii_columns:
                                pii_columns[pii_type] = []

                            pii_columns[pii_type].append(col_name)
                            self.add_warning(
                                result,
                                f"Column '{col_name}' contains {pii_type.value} "
                                f"({match_count} matches, {match_ratio:.1%})",
                            )

                    except Exception as e:
                        logger.debug(f"Error checking column '{col_name}' for PII: {str(e)}")

            # Mark as invalid if PII is found
            if pii_columns and self.action != PIIAction.REPORT:
                result.is_valid = False

                # Add metadata about PII columns
                pii_metadata = {}
                for pii_type, columns in pii_columns.items():
                    pii_metadata[pii_type.value] = columns

                result.metadata = {"pii_columns": pii_metadata}

            logger.info(f"PII validation completed. Found PII in {len(pii_columns)} column types")

        except Exception as e:
            self.add_error(result, f"PII validation failed: {str(e)}")

        return result

    def handle_pii(self, table_name_or_data: Any, result: ValidationResult) -> str:
        """Handle PII according to the configured action, returning DuckDB table name.

        Args:
            table_name_or_data: DuckDB table name (str) or data to process
            result: ValidationResult from validate()

        Returns:
            DuckDB table name with PII handled
        """
        try:
            # Handle different input types
            if isinstance(table_name_or_data, str):
                source_table = table_name_or_data
            else:
                # Register data as a table
                source_table = "pii_handling_data"
                self.register_table(table_name_or_data, source_table)

            # If no PII detected or action is REPORT, return original table
            if not hasattr(result, "metadata") or "pii_columns" not in result.metadata:
                return source_table

            if self.action == PIIAction.REPORT:
                return source_table

            pii_columns = result.metadata["pii_columns"]
            handled_table = f"{source_table}_pii_handled"

            # Get all columns
            columns_info = self.conn.execute(f"DESCRIBE {source_table}").fetchall()
            all_columns = [col[0] for col in columns_info]

            # Build select statement based on action
            select_parts = []

            for col_name in all_columns:
                # Check if this column contains PII
                is_pii_column = False
                for pii_type_columns in pii_columns.values():
                    if col_name in pii_type_columns:
                        is_pii_column = True
                        break

                if is_pii_column:
                    if self.action == PIIAction.MASK:
                        # Replace with masked value
                        select_parts.append(f"'***MASKED***' AS {col_name}")
                    elif self.action == PIIAction.HASH:
                        # Hash the value using DuckDB's hash function
                        select_parts.append(f"hash({col_name}) AS {col_name}")
                    elif self.action == PIIAction.DELETE:
                        # Skip this column (don't include in select)
                        continue
                else:
                    # Keep original column
                    select_parts.append(col_name)

            # Create the handled table
            if select_parts:  # Only if we have columns to select
                self.conn.execute(f"""
                    CREATE TABLE {handled_table} AS
                    SELECT {", ".join(select_parts)}
                    FROM {source_table}
                """)

                logger.info(
                    f"Applied PII handling ({self.action.value}) to create table {handled_table}"
                )
                return handled_table
            else:
                # All columns were deleted
                logger.warning("All columns contained PII and were deleted")
                return source_table

        except Exception as e:
            logger.error(f"Failed to handle PII: {str(e)}")
            return source_table if isinstance(table_name_or_data, str) else "pii_handling_data"
