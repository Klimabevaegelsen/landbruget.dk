"""PII validator for detecting and handling sensitive data."""

from enum import Enum
from typing import Any

import pandas as pd

from ...utils.logging import get_logger
from .base import BaseValidator, ValidationResult

# Get logger
logger = get_logger()


class PIIAction(Enum):
    """Action to take when PII is detected."""

    REPORT = "report"      # Just report the PII
    MASK = "mask"          # Mask the PII (e.g., replace with ***)
    HASH = "hash"          # Hash the PII
    DELETE = "delete"      # Delete the column containing PII


class PIIType(Enum):
    """Types of PII that can be detected."""

    EMAIL = "email"
    PHONE = "phone"
    CPR = "cpr"            # Danish personal ID number (CPR-nummer)
    CVR = "cvr"            # Danish company ID number (CVR-nummer)
    ADDRESS = "address"
    NAME = "name"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"


class PIIValidator(BaseValidator):
    """Validator for detecting and handling PII."""

    # Regular expressions for different PII types
    PII_PATTERNS = {
        PIIType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        PIIType.PHONE: r'\b(?:\+?45)?[ -]?\d{2}[ -]?\d{2}[ -]?\d{2}[ -]?\d{2}\b',
        PIIType.CPR: r'\b\d{6}[-]?\d{4}\b',
        PIIType.CVR: r'\b\d{8}\b',
        PIIType.CREDIT_CARD: r'\b(?:\d{4}[ -]?){3}\d{4}\b',
        PIIType.IP_ADDRESS: r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
    }

    def __init__(
        self,
        pii_types: set[PIIType] = None,
        action: PIIAction = PIIAction.REPORT,
        threshold: float = 0.3,
        column_name_hints: dict[PIIType, list[str]] = None,
    ):
        """Initialize the PII validator.

        Args:
            pii_types: Set of PII types to detect
            action: Action to take when PII is detected
            threshold: Threshold for detecting PII (0-1)
                      (e.g., 0.3 means 30% of values need to match to flag a column)
            column_name_hints: Dictionary mapping PII types to column name patterns
        """
        super().__init__()
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
            PIIType.CVR: ["cvr", "virksomhedsnummer", "company_id"],
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
    
    def validate(self, data: Any) -> ValidationResult:
        """Validate data for PII.

        Args:
            data: Data to validate (e.g., DataFrame)

        Returns:
            ValidationResult with PII detection results
        """
        result = ValidationResult(is_valid=True)
        
        if not isinstance(data, pd.DataFrame):
            self.add_error(result, "Data must be a pandas DataFrame")
            return result
        
        # Track PII columns by type
        pii_columns = {}
        
        # Check column names first
        for pii_type in self.pii_types:
            # Skip if no hints for this type
            if pii_type not in self.column_name_hints:
                continue
            
            hints = self.column_name_hints[pii_type]
            
            for col in data.columns:
                col_lower = col.lower()
                for hint in hints:
                    if hint.lower() in col_lower:
                        # Add to PII columns
                        if pii_type not in pii_columns:
                            pii_columns[pii_type] = []
                        
                        pii_columns[pii_type].append(col)
                        self.add_warning(
                            result,
                            f"Column '{col}' might contain {pii_type.value} based on name"
                        )
        
        # Check column contents
        for pii_type in self.pii_types:
            if pii_type not in self.PII_PATTERNS:
                continue
            
            pattern = self.PII_PATTERNS[pii_type]
            
            for col in data.columns:
                # Skip non-string columns
                if data[col].dtype != 'object':
                    continue
                
                # Skip columns already identified by name
                if pii_type in pii_columns and col in pii_columns[pii_type]:
                    continue
                
                # Check for PII in column values
                try:
                    # Count values that match the pattern
                    match_count = data[col].astype(str).str.match(pattern).sum()
                    match_ratio = match_count / len(data)
                    
                    if match_ratio >= self.threshold:
                        # Add to PII columns
                        if pii_type not in pii_columns:
                            pii_columns[pii_type] = []
                        
                        pii_columns[pii_type].append(col)
                        self.add_warning(
                            result,
                            f"Column '{col}' contains {pii_type.value} "
                            f"({match_count} matches, {match_ratio:.1%})"
                        )
                
                except Exception as e:
                    logger.debug(f"Error checking column '{col}' for PII: {str(e)}")
        
        # Mark as invalid if PII is found
        if pii_columns and self.action != PIIAction.REPORT:
            result.is_valid = False
            
            # Add metadata about PII columns
            pii_metadata = {}
            for pii_type, columns in pii_columns.items():
                pii_metadata[pii_type.value] = columns
            
            # Store PII metadata in result for handling
            if not hasattr(result, "metadata"):
                result.metadata = {}
            
            result.metadata["pii_columns"] = pii_metadata
            result.metadata["pii_action"] = self.action.value
        
        return result
    
    def handle_pii(self, data: pd.DataFrame, result: ValidationResult) -> pd.DataFrame:
        """Handle PII according to the configured action.

        Args:
            data: DataFrame to process
            result: ValidationResult from validate()

        Returns:
            Processed DataFrame with PII handled
        """
        # Check if we have PII information in the result
        if not hasattr(result, "metadata") or "pii_columns" not in result.metadata:
            return data
        
        # Create a copy to avoid modifying the original
        df = data.copy()
        
        pii_columns = result.metadata["pii_columns"]
        action = PIIAction(result.metadata["pii_action"])
        
        # Apply the action to each PII column
        for pii_type_str, columns in pii_columns.items():
            pii_type = PIIType(pii_type_str)
            
            for col in columns:
                if col not in df.columns:
                    continue
                
                if action == PIIAction.DELETE:
                    # Delete the column
                    df = df.drop(columns=[col])
                    logger.info(f"Deleted column '{col}' containing {pii_type.value}")
                
                elif action == PIIAction.MASK:
                    # Mask the PII
                    if df[col].dtype == 'object':
                        if pii_type in self.PII_PATTERNS:
                            pattern = self.PII_PATTERNS[pii_type]
                            df[col] = df[col].astype(str).replace(
                                pattern, "***", regex=True
                            )
                        else:
                            # If no pattern available, mask the entire value
                            df[col] = "***"
                        
                        logger.info(f"Masked values in column '{col}' containing {pii_type.value}")
                
                elif action == PIIAction.HASH:
                    # Hash the PII
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).apply(
                            lambda x: hash(x) if x else x
                        )
                        logger.info(f"Hashed values in column '{col}' containing {pii_type.value}")
        
        return df 