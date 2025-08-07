"""
CVR PII Filter Utility

This module provides utilities to filter personally identifiable information (PII)
from CVR register data while preserving business-relevant information.

Key filtering rules:
- Remove deltager (personal) addresses
- Remove deltager (personal) phone numbers and emails
- Keep entity numbers (they're not CPR numbers)
- Keep personal names (public business information)
- Keep company addresses and contact information
- Keep all business registration data
"""

import copy
from typing import Any, Dict, List

from unified_pipeline.util.log_util import Logger

logger = Logger.get_logger()


def filter_cvr_pii(raw_cvr_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter PII from raw CVR data while preserving business information.

    Args:
        raw_cvr_data: Raw CVR data from API

    Returns:
        Filtered CVR data with PII removed
    """
    logger.debug("Filtering PII from CVR data")

    # Create a deep copy to avoid modifying original data
    filtered_data = copy.deepcopy(raw_cvr_data)

    # Filter deltager relations (personal data)
    if "deltagerRelation" in filtered_data:
        filtered_data["deltagerRelation"] = filter_deltager_relations(
            filtered_data["deltagerRelation"]
        )

    # Company addresses and contact info are kept (business information)
    # Only deltager (personal) data is filtered

    logger.debug("CVR PII filtering completed")
    return filtered_data


def filter_deltager_relations(deltager_relations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter PII from deltager relations while keeping business-relevant information.

    Args:
        deltager_relations: List of deltager relation objects

    Returns:
        Filtered deltager relations with PII removed
    """
    filtered_relations = []

    for relation in deltager_relations:
        filtered_relation = copy.deepcopy(relation)

        # Filter the deltager (person) data
        if "deltager" in filtered_relation:
            filtered_relation["deltager"] = filter_deltager_data(filtered_relation["deltager"])

        filtered_relations.append(filtered_relation)

    return filtered_relations


def filter_deltager_data(deltager: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter PII from individual deltager (person) data.

    Args:
        deltager: Deltager data object

    Returns:
        Filtered deltager data with PII removed
    """
    filtered_deltager = copy.deepcopy(deltager)

    # Remove personal addresses
    pii_address_fields = [
        "beliggenhedsadresse",  # Personal addresses
        "postadresse",  # Personal postal addresses
    ]

    for field in pii_address_fields:
        if field in filtered_deltager:
            logger.debug(f"Removing personal address field: {field}")
            del filtered_deltager[field]

    # Remove personal contact information
    pii_contact_fields = [
        "telefonNummer",  # Personal phone numbers
        "telefaxNummer",  # Personal fax numbers
        "sekundaertTelefonNummer",  # Secondary phone numbers
        "sekundaertTelefaxNummer",  # Secondary fax numbers
        "elektroniskPost",  # Personal email addresses
        "obligatoriskEmail",  # Required email addresses
    ]

    for field in pii_contact_fields:
        if field in filtered_deltager:
            logger.debug(f"Removing personal contact field: {field}")
            del filtered_deltager[field]

    # Keep these fields (business-relevant, not PII):
    # - enhedsNummer (entity number - confirmed not CPR)
    # - enhedstype (entity type)
    # - navne (names - public business information)
    # - All other business registration data

    return filtered_deltager


def get_pii_filtering_summary(
    original_data: Dict[str, Any], filtered_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate a summary of what PII was filtered from the data.

    Args:
        original_data: Original raw CVR data
        filtered_data: Filtered CVR data

    Returns:
        Summary of filtering actions
    """
    summary = {
        "deltager_relations_processed": 0,
        "personal_addresses_removed": 0,
        "personal_contacts_removed": 0,
        "fields_removed": [],
    }

    # Count deltager relations
    original_relations = original_data.get("deltagerRelation", [])
    summary["deltager_relations_processed"] = len(original_relations)

    # Count removed fields
    for relation in original_relations:
        deltager = relation.get("deltager", {})

        # Count personal addresses
        address_fields = ["beliggenhedsadresse", "postadresse"]
        for field in address_fields:
            if field in deltager:
                addresses = deltager.get(field, [])
                summary["personal_addresses_removed"] += len(addresses)
                if field not in summary["fields_removed"]:
                    summary["fields_removed"].append(field)

        # Count personal contacts
        contact_fields = [
            "telefonNummer",
            "telefaxNummer",
            "sekundaertTelefonNummer",
            "sekundaertTelefaxNummer",
            "elektroniskPost",
            "obligatoriskEmail",
        ]
        for field in contact_fields:
            if field in deltager:
                contacts = deltager.get(field, [])
                summary["personal_contacts_removed"] += len(contacts)
                if field not in summary["fields_removed"]:
                    summary["fields_removed"].append(field)

    return summary


def validate_pii_filtering(filtered_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that PII has been properly filtered from CVR data.

    Args:
        filtered_data: Filtered CVR data

    Returns:
        Validation results
    """
    validation = {"is_valid": True, "issues": [], "warnings": []}

    # Check deltager relations for remaining PII
    deltager_relations = filtered_data.get("deltagerRelation", [])

    for i, relation in enumerate(deltager_relations):
        deltager = relation.get("deltager", {})

        # Check for personal addresses
        pii_address_fields = ["beliggenhedsadresse", "postadresse"]
        for field in pii_address_fields:
            if field in deltager:
                validation["is_valid"] = False
                validation["issues"].append(
                    f"Personal address field '{field}' found in deltager {i}"
                )

        # Check for personal contact information
        pii_contact_fields = [
            "telefonNummer",
            "telefaxNummer",
            "sekundaertTelefonNummer",
            "sekundaertTelefaxNummer",
            "elektroniskPost",
            "obligatoriskEmail",
        ]
        for field in pii_contact_fields:
            if field in deltager:
                validation["is_valid"] = False
                validation["issues"].append(
                    f"Personal contact field '{field}' found in deltager {i}"
                )

    # Company-level addresses and contacts should remain (business information)
    company_fields = ["beliggenhedsadresse", "postadresse", "telefonNummer", "elektroniskPost"]
    for field in company_fields:
        if field in filtered_data:
            validation["warnings"].append(f"Company {field} preserved (business information)")

    return validation
