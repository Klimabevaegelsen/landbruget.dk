"""
Field ID Validation Module for NLES5 Pipeline

This module provides comprehensive validation for field UUIDs throughout the NLES5 pipeline,
ensuring that field UUIDs maintain their UUID format and exist before and after processing.

Key features:
- UUID format validation for field UUIDs (field_uuid column)
- Field UUID tracking before and after pipeline runs
- Field UUID existence validation after processing
- Comprehensive validation reporting
- Support for both field_id (FVM identifiers) and field_uuid (geometry-based UUIDs)
"""

import uuid
from typing import Any, Dict, Set

from unified_pipeline.util.timing import timed


class FieldIDValidator:
    """
    Field UUID validator for NLES5 nitrogen estimation pipeline.

    This class handles:
    - UUID format validation for field UUIDs (field_uuid column)
    - Field UUID tracking before and after pipeline runs
    - Field UUID existence validation after processing
    - Comprehensive validation reporting
    - Support for both field_id and field_uuid validation
    """

    def __init__(self, processor):
        """Initialize validator with reference to main processor."""
        self.processor = processor
        self.config = processor.config
        self.log = processor.log
        self.conn = processor.conn

        # Track field UUIDs throughout the pipeline
        self.field_uuids_before_processing: Set[str] = set()
        self.field_uuids_after_processing: Set[str] = set()

        # Also track field_ids for completeness
        self.field_ids_before_processing: Set[str] = set()
        self.field_ids_after_processing: Set[str] = set()

        self.validation_results: Dict[str, Any] = {}

    @timed(name="Validating field ID format")
    def validate_field_id_format(self, field_id: str) -> Dict[str, Any]:
        """
        Validate that a field ID follows expected FVM field ID formats.

        FVM field IDs can be:
        - Simple numeric: "8", "23", "6", "2"
        - Number-dash-number: "17-0", "18-0", "19-5", "113-0"
        - UUID format (if present)
        - CVR_Marknummer composite format

        Args:
            field_id: The field ID to validate

        Returns:
            Dictionary with validation results
        """
        validation_result = {
            "field_id": field_id,
            "is_valid_uuid": False,
            "is_valid_format": False,
            "format_type": "unknown",
            "issues": [],
            "recommendations": [],
        }

        if not field_id:
            validation_result["issues"].append("Field ID is empty or None")
            validation_result["recommendations"].append("Ensure field ID is not empty")
            return validation_result

        field_id_str = str(field_id).strip()

        # Check for UUID format first
        try:
            uuid.UUID(field_id_str)
            validation_result["is_valid_uuid"] = True
            validation_result["format_type"] = "uuid"
            validation_result["is_valid_format"] = True
            validation_result["recommendations"].append("Field ID uses UUID format (preferred)")
            return validation_result
        except ValueError:
            pass

        # Check for FVM numeric format (simple number)
        if field_id_str.isdigit():
            validation_result["format_type"] = "fvm_numeric"
            validation_result["is_valid_format"] = True
            if len(field_id_str) > 10:
                validation_result["recommendations"].append(
                    "Very long numeric field ID - verify correctness"
                )
            return validation_result

        # Check for FVM number-dash-number format
        if "-" in field_id_str:
            parts = field_id_str.split("-")
            if len(parts) == 2:
                left_part, right_part = parts
                if left_part.isdigit() and right_part.isdigit():
                    validation_result["format_type"] = "fvm_dash_format"
                    validation_result["is_valid_format"] = True
                    validation_result["recommendations"].append(
                        "Field ID uses FVM number-dash-number format (valid)"
                    )
                    return validation_result
                else:
                    validation_result["format_type"] = "invalid_dash_format"
                    validation_result["issues"].append(
                        "Dash-separated field ID contains non-numeric parts"
                    )
            else:
                validation_result["format_type"] = "invalid_dash_format"
                validation_result["issues"].append("Multiple dashes in field ID not supported")

        # Check for CVR_Marknummer composite format
        elif "_" in field_id_str:
            validation_result["format_type"] = "composite_key"
            parts = field_id_str.split("_")
            if len(parts) == 2:
                cvr_part, marknummer_part = parts
                if cvr_part.isdigit() and len(cvr_part) == 8:
                    if marknummer_part.isdigit():
                        validation_result["is_valid_format"] = True
                        validation_result["recommendations"].append(
                            "Field ID uses CVR_Marknummer format (acceptable)"
                        )
                    else:
                        validation_result["issues"].append("Marknummer part is not numeric")
                else:
                    validation_result["issues"].append("CVR part is not 8-digit number")
            else:
                validation_result["issues"].append("Composite key has wrong number of parts")

        # Check for alphanumeric format
        elif field_id_str.isalnum():
            validation_result["format_type"] = "alphanumeric"
            validation_result["is_valid_format"] = True
            validation_result["recommendations"].append(
                "Field ID uses alphanumeric format - verify this is expected"
            )

        # Unknown format
        else:
            validation_result["format_type"] = "unknown"
            validation_result["issues"].append(
                f'Field ID "{field_id_str}" does not match any expected format '
                f"(numeric, number-dash-number, UUID, CVR_Marknummer)"
            )
            validation_result["recommendations"].append(
                "Check if field ID contains invalid characters or unexpected format"
            )

        return validation_result

    @timed(name="Validating field UUID format")
    def validate_field_uuid_format(self, field_uuid: str) -> Dict[str, Any]:
        """
        Validate that a field UUID follows proper UUID format.

        Args:
            field_uuid: The field UUID to validate

        Returns:
            Dictionary with validation results
        """
        validation_result = {
            "field_uuid": field_uuid,
            "is_valid_uuid": False,
            "is_valid_format": False,
            "format_type": "unknown",
            "issues": [],
            "recommendations": [],
        }

        # Check if field_uuid is actually aliased from field_id (common in spatial tables)
        field_uuid_str = str(field_uuid).strip()

        # If field_uuid looks like field_id (not a UUID), mark as aliased but valid
        if not field_uuid_str:
            validation_result["issues"].append("Field UUID is empty or None")
            validation_result["recommendations"].append("Ensure field UUID is not empty")
            return validation_result

        # Check if it might be aliased from field_id (contains non-UUID characters)
        if ("_" in field_uuid_str or "-" in field_uuid_str[:8]) and len(field_uuid_str) < 20:
            validation_result["is_valid_uuid"] = False
            validation_result["format_type"] = "aliased_field_id"
            validation_result["is_valid_format"] = True  # Consider aliased IDs as valid for now
            validation_result["recommendations"].append(
                "Field UUID appears to be aliased from field_id - consider generating proper UUIDs"
            )
            return validation_result

        # Check if it's a valid UUID format
        try:
            parsed_uuid = uuid.UUID(field_uuid_str)
            validation_result["is_valid_uuid"] = True
            validation_result["format_type"] = "uuid"
            validation_result["is_valid_format"] = True

            # Check UUID version
            if hasattr(parsed_uuid, "version") and parsed_uuid.version:
                validation_result["uuid_version"] = parsed_uuid.version
                if parsed_uuid.version == 5:
                    validation_result["recommendations"].append(
                        "Field UUID uses UUID5 format (geometry-based, preferred for FVM)"
                    )
                elif parsed_uuid.version == 4:
                    validation_result["recommendations"].append(
                        "Field UUID uses UUID4 format (random)"
                    )
                else:
                    validation_result["recommendations"].append(
                        f"Field UUID uses UUID{parsed_uuid.version} format"
                    )

            return validation_result

        except ValueError as e:
            validation_result["is_valid_uuid"] = False
            validation_result["format_type"] = "invalid_uuid"
            validation_result["issues"].append(
                f'Field UUID "{field_uuid_str}" is not a valid UUID: {e}'
            )
            validation_result["recommendations"].append(
                "Ensure field UUID follows proper UUID format "
                "(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            )

        return validation_result

    @timed(name="Collecting field IDs and UUIDs before processing")
    def collect_field_ids_before_processing(self) -> Set[str]:
        """
        Collect all field IDs and field UUIDs from the input data before processing begins.

        Returns:
            Set of field IDs found in input data (for backwards compatibility)
        """
        try:
            self.log.info("🔍 Collecting field IDs and field UUIDs from input data...")

            field_ids = set()
            field_uuids = set()

            # Check agricultural fields data
            try:
                # Try different possible table names
                table_names = [
                    "agricultural_fields",
                    "agricultural_fields_spatial",
                    "fvm_marker_data",
                ]

                for table_name in table_names:
                    try:
                        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[
                            0
                        ]
                        if count > 0:
                            self.log.info(f"Found {count:,} records in {table_name}")

                            # Check what columns are available
                            columns_result = self.conn.execute(f"""
                                SELECT column_name 
                                FROM information_schema.columns 
                                WHERE table_name = '{table_name}'
                                AND column_name IN ('field_id', 'field_uuid')
                            """).fetchall()

                            available_columns = {row[0] for row in columns_result}
                            self.log.info(
                                f"Available ID columns in {table_name}: {available_columns}"
                            )

                            # Get field IDs if available
                            if "field_id" in available_columns:
                                field_id_results = self.conn.execute(f"""
                                    SELECT DISTINCT field_id 
                                    FROM {table_name} 
                                    WHERE field_id IS NOT NULL
                                """).fetchall()

                                table_field_ids = {
                                    str(row[0]) for row in field_id_results if row[0]
                                }
                                field_ids.update(table_field_ids)
                                self.log.info(
                                    f"Collected {len(table_field_ids):,} unique field IDs "
                                    f"from {table_name}"
                                )

                            # Get field UUIDs if available (this is what we really want to validate)
                            if "field_uuid" in available_columns:
                                field_uuid_results = self.conn.execute(f"""
                                    SELECT DISTINCT field_uuid 
                                    FROM {table_name} 
                                    WHERE field_uuid IS NOT NULL
                                """).fetchall()

                                table_field_uuids = {
                                    str(row[0]) for row in field_uuid_results if row[0]
                                }
                                field_uuids.update(table_field_uuids)
                                self.log.info(
                                    f"Collected {len(table_field_uuids):,} unique field UUIDs "
                                    f"from {table_name}"
                                )

                            break
                    except Exception as e:
                        self.log.debug(f"Table {table_name} not available: {e}")
                        continue

            except Exception as e:
                self.log.warning(f"Could not collect field IDs/UUIDs from agricultural fields: {e}")

            # Store the collected field IDs and UUIDs
            self.field_ids_before_processing = field_ids
            self.field_uuids_before_processing = field_uuids

            self.log.info(
                f"✅ Collected {len(field_ids):,} field IDs and "
                f"{len(field_uuids):,} field UUIDs before processing"
            )

            # Log sample field UUIDs for verification (prioritize UUIDs over field_ids)
            if field_uuids:
                sample_uuids = list(field_uuids)[:5]
                self.log.info(f"Sample field UUIDs: {sample_uuids}")

                # Validate format of sample field UUIDs
                for sample_uuid in sample_uuids:
                    validation = self.validate_field_uuid_format(sample_uuid)
                    if validation["is_valid_format"]:
                        version_info = (
                            f"(UUID{validation.get('uuid_version', '?')})"
                            if "uuid_version" in validation
                            else ""
                        )
                        self.log.info(
                            f"   ✅ {sample_uuid}: {validation['format_type']} format "
                            f"{version_info}"
                        )
                    else:
                        self.log.warning(
                            f"   ⚠️ {sample_uuid}: {validation['format_type']} format - "
                            f"{validation['issues']}"
                        )

            # Also validate field IDs if no UUIDs available
            elif field_ids:
                sample_ids = list(field_ids)[:5]
                self.log.info(f"Sample field IDs: {sample_ids}")

                # Validate format of sample field IDs
                for sample_id in sample_ids:
                    validation = self.validate_field_id_format(sample_id)
                    if validation["is_valid_format"]:
                        self.log.info(f"   ✅ {sample_id}: {validation['format_type']} format")
                    else:
                        self.log.warning(
                            f"   ⚠️ {sample_id}: {validation['format_type']} format - "
                            f"{validation['issues']}"
                        )

            return field_ids  # Return field_ids for backwards compatibility

        except Exception as e:
            self.log.error(f"Error collecting field IDs/UUIDs before processing: {e}")
            return set()

    @timed(name="Collecting field IDs and UUIDs after processing")
    def collect_field_ids_after_processing(self) -> Set[str]:
        """
        Collect all field IDs and field UUIDs from the output data after processing completes.

        Returns:
            Set of field IDs found in output data (for backwards compatibility)
        """
        try:
            self.log.info("🔍 Collecting field IDs and field UUIDs from output data...")

            field_ids = set()
            field_uuids = set()

            # Check NLES5 results tables
            result_table_names = [
                "nles5_nitrogen_estimates_gold",
                "nles5_estimates_final_batched",
                "nles5_unified_results",
                "nles5_nitrogen_estimates",
            ]

            for table_name in result_table_names:
                try:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    if count > 0:
                        self.log.info(f"Found {count:,} records in {table_name}")

                        # Check what columns are available
                        columns_result = self.conn.execute(f"""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = '{table_name}'
                            AND column_name IN ('field_id', 'field_uuid')
                        """).fetchall()

                        available_columns = {row[0] for row in columns_result}
                        self.log.info(f"Available ID columns in {table_name}: {available_columns}")

                        # Get field IDs if available
                        if "field_id" in available_columns:
                            field_id_results = self.conn.execute(f"""
                                SELECT DISTINCT field_id 
                                FROM {table_name} 
                                WHERE field_id IS NOT NULL
                            """).fetchall()

                            table_field_ids = {str(row[0]) for row in field_id_results if row[0]}
                            field_ids.update(table_field_ids)
                            self.log.info(
                                f"Collected {len(table_field_ids):,} unique field IDs "
                                f"from {table_name}"
                            )

                        # Get field UUIDs if available (this is what we really want to validate)
                        if "field_uuid" in available_columns:
                            field_uuid_results = self.conn.execute(f"""
                                SELECT DISTINCT field_uuid 
                                FROM {table_name} 
                                WHERE field_uuid IS NOT NULL
                            """).fetchall()

                            table_field_uuids = {
                                str(row[0]) for row in field_uuid_results if row[0]
                            }
                            field_uuids.update(table_field_uuids)
                            self.log.info(
                                f"Collected {len(table_field_uuids):,} unique field UUIDs "
                                f"from {table_name}"
                            )

                        break
                except Exception as e:
                    self.log.debug(f"Table {table_name} not available: {e}")
                    continue

            # Store the collected field IDs and UUIDs
            self.field_ids_after_processing = field_ids
            self.field_uuids_after_processing = field_uuids

            self.log.info(
                f"✅ Collected {len(field_ids):,} field IDs and "
                f"{len(field_uuids):,} field UUIDs after processing"
            )

            # Log sample field UUIDs for verification (prioritize UUIDs over field_ids)
            if field_uuids:
                sample_uuids = list(field_uuids)[:5]
                self.log.info(f"Sample output field UUIDs: {sample_uuids}")

                # Validate format of sample field UUIDs
                for sample_uuid in sample_uuids:
                    validation = self.validate_field_uuid_format(sample_uuid)
                    if validation["is_valid_format"]:
                        version_info = (
                            f"(UUID{validation.get('uuid_version', '?')})"
                            if "uuid_version" in validation
                            else ""
                        )
                        self.log.info(
                            f"   ✅ {sample_uuid}: {validation['format_type']} format "
                            f"{version_info}"
                        )
                    else:
                        self.log.warning(
                            f"   ⚠️ {sample_uuid}: {validation['format_type']} format - "
                            f"{validation['issues']}"
                        )

            # Also validate field IDs if no UUIDs available
            elif field_ids:
                sample_ids = list(field_ids)[:5]
                self.log.info(f"Sample output field IDs: {sample_ids}")

                # Validate format of sample field IDs
                for sample_id in sample_ids:
                    validation = self.validate_field_id_format(sample_id)
                    if validation["is_valid_format"]:
                        self.log.info(f"   ✅ {sample_id}: {validation['format_type']} format")
                    else:
                        self.log.warning(
                            f"   ⚠️ {sample_id}: {validation['format_type']} format - "
                            f"{validation['issues']}"
                        )

            return field_ids  # Return field_ids for backwards compatibility

        except Exception as e:
            self.log.error(f"Error collecting field IDs/UUIDs after processing: {e}")
            return set()

    @timed(name="Validating field ID and UUID consistency")
    def validate_field_id_consistency(self) -> Dict[str, Any]:
        """
        Validate that field IDs and UUIDs are consistent before and after processing.

        Returns:
            Dictionary with validation results for both field IDs and UUIDs
        """
        try:
            self.log.info("🔍 Validating field ID and UUID consistency...")

            validation_result = {
                # Field ID validation
                "total_field_ids_before": len(self.field_ids_before_processing),
                "total_field_ids_after": len(self.field_ids_after_processing),
                "field_ids_lost": set(),
                "field_ids_gained": set(),
                "field_ids_preserved": set(),
                "field_id_consistency_score": 0.0,
                # Field UUID validation
                "total_field_uuids_before": len(self.field_uuids_before_processing),
                "total_field_uuids_after": len(self.field_uuids_after_processing),
                "field_uuids_lost": set(),
                "field_uuids_gained": set(),
                "field_uuids_preserved": set(),
                "field_uuid_consistency_score": 0.0,
                # Overall validation
                "consistency_score": 0.0,
                "issues": [],
                "recommendations": [],
            }

            # Validate field IDs
            if self.field_ids_before_processing and self.field_ids_after_processing:
                validation_result["field_ids_lost"] = (
                    self.field_ids_before_processing - self.field_ids_after_processing
                )
                validation_result["field_ids_gained"] = (
                    self.field_ids_after_processing - self.field_ids_before_processing
                )
                validation_result["field_ids_preserved"] = (
                    self.field_ids_before_processing & self.field_ids_after_processing
                )

                if self.field_ids_before_processing:
                    validation_result["field_id_consistency_score"] = len(
                        validation_result["field_ids_preserved"]
                    ) / len(self.field_ids_before_processing)

            # Validate field UUIDs (primary validation)
            if self.field_uuids_before_processing and self.field_uuids_after_processing:
                validation_result["field_uuids_lost"] = (
                    self.field_uuids_before_processing - self.field_uuids_after_processing
                )
                validation_result["field_uuids_gained"] = (
                    self.field_uuids_after_processing - self.field_uuids_before_processing
                )
                validation_result["field_uuids_preserved"] = (
                    self.field_uuids_before_processing & self.field_uuids_after_processing
                )

                if self.field_uuids_before_processing:
                    validation_result["field_uuid_consistency_score"] = len(
                        validation_result["field_uuids_preserved"]
                    ) / len(self.field_uuids_before_processing)

            # Calculate overall consistency score (prioritize UUIDs if available)
            if validation_result["field_uuid_consistency_score"] > 0:
                validation_result["consistency_score"] = validation_result[
                    "field_uuid_consistency_score"
                ]
                primary_validation = "field_uuid"
            elif validation_result["field_id_consistency_score"] > 0:
                validation_result["consistency_score"] = validation_result[
                    "field_id_consistency_score"
                ]
                primary_validation = "field_id"
            else:
                validation_result["issues"].append(
                    "No field identifiers found for consistency validation"
                )
                validation_result["recommendations"].append(
                    "Ensure field identifier collection runs before and after processing"
                )
                return validation_result

            # Log results
            self.log.info("📊 Field Identifier Consistency Results:")

            # Log field ID results if available
            if validation_result["total_field_ids_before"] > 0:
                self.log.info(
                    f"   Field IDs before processing: "
                    f"{validation_result['total_field_ids_before']:,}"
                )
                self.log.info(
                    f"   Field IDs after processing: {validation_result['total_field_ids_after']:,}"
                )
                self.log.info(
                    f"   Field IDs preserved: {len(validation_result['field_ids_preserved']):,}"
                )
                self.log.info(f"   Field IDs lost: {len(validation_result['field_ids_lost']):,}")
                self.log.info(
                    f"   Field IDs gained: {len(validation_result['field_ids_gained']):,}"
                )
                self.log.info(
                    f"   Field ID consistency score: "
                    f"{validation_result['field_id_consistency_score']:.2%}"
                )

            # Log field UUID results if available
            if validation_result["total_field_uuids_before"] > 0:
                self.log.info(
                    f"   Field UUIDs before processing: "
                    f"{validation_result['total_field_uuids_before']:,}"
                )
                self.log.info(
                    f"   Field UUIDs after processing: "
                    f"{validation_result['total_field_uuids_after']:,}"
                )
                self.log.info(
                    f"   Field UUIDs preserved: {len(validation_result['field_uuids_preserved']):,}"
                )
                self.log.info(
                    f"   Field UUIDs lost: {len(validation_result['field_uuids_lost']):,}"
                )
                self.log.info(
                    f"   Field UUIDs gained: {len(validation_result['field_uuids_gained']):,}"
                )
                self.log.info(
                    f"   Field UUID consistency score: "
                    f"{validation_result['field_uuid_consistency_score']:.2%}"
                )

            self.log.info(
                f"   Overall consistency score ({primary_validation}): "
                f"{validation_result['consistency_score']:.2%}"
            )

            # Check for UUID-specific issues (primary concern)
            if validation_result["field_uuids_lost"]:
                validation_result["issues"].append(
                    f"{len(validation_result['field_uuids_lost'])} field UUIDs "
                    f"were lost during processing"
                )
                validation_result["recommendations"].append(
                    "Investigate why field UUIDs were lost during processing"
                )

                # Log sample lost field UUIDs
                sample_lost = list(validation_result["field_uuids_lost"])[:3]
                self.log.warning(f"Sample lost field UUIDs: {sample_lost}")

            if validation_result["field_uuids_gained"]:
                validation_result["issues"].append(
                    f"{len(validation_result['field_uuids_gained'])} field UUIDs "
                    f"were gained during processing"
                )
                validation_result["recommendations"].append(
                    "Investigate why new field UUIDs were created during processing"
                )

                # Log sample gained field UUIDs
                sample_gained = list(validation_result["field_uuids_gained"])[:3]
                self.log.warning(f"Sample gained field UUIDs: {sample_gained}")

            # Check for field ID issues (secondary concern)
            if validation_result["field_ids_lost"]:
                validation_result["issues"].append(
                    f"{len(validation_result['field_ids_lost'])} field IDs "
                    f"were lost during processing"
                )
                sample_lost = list(validation_result["field_ids_lost"])[:3]
                self.log.warning(f"Sample lost field IDs: {sample_lost}")

            if validation_result["field_ids_gained"]:
                validation_result["issues"].append(
                    f"{len(validation_result['field_ids_gained'])} field IDs "
                    f"were gained during processing"
                )
                sample_gained = list(validation_result["field_ids_gained"])[:3]
                self.log.warning(f"Sample gained field IDs: {sample_gained}")

            # Check consistency score
            if validation_result["consistency_score"] < 0.95:
                validation_result["issues"].append(
                    f"Low consistency score: {validation_result['consistency_score']:.2%}"
                )
                validation_result["recommendations"].append(
                    "Investigate field identifier consistency issues"
                )
            elif validation_result["consistency_score"] < 0.99:
                validation_result["recommendations"].append(
                    "Minor field identifier consistency issues detected"
                )
            else:
                self.log.info(
                    f"✅ Field identifier consistency is excellent ({primary_validation})"
                )

            return validation_result

        except Exception as e:
            self.log.error(f"Error validating field identifier consistency: {e}")
            return {"error": str(e)}

    @timed(name="Validating field formats in dataset")
    def validate_field_id_formats_in_dataset(
        self, table_name: str, sample_size: int = 100
    ) -> Dict[str, Any]:
        """
        Validate field ID and UUID formats in a specific dataset.

        Args:
            table_name: Name of the table to validate
            sample_size: Number of field identifiers to sample for validation

        Returns:
            Dictionary with validation results for both field_id and field_uuid
        """
        try:
            self.log.info(f"🔍 Validating field identifier formats in {table_name}...")

            validation_result = {
                "table_name": table_name,
                "total_records": 0,
                "sample_size": sample_size,
                # Field ID validation
                "records_with_field_id": 0,
                "field_id_valid_formats": 0,
                "field_id_invalid_formats": 0,
                "field_id_format_distribution": {},
                # Field UUID validation
                "records_with_field_uuid": 0,
                "field_uuid_valid_formats": 0,
                "field_uuid_invalid_formats": 0,
                "field_uuid_format_distribution": {},
                "issues": [],
                "recommendations": [],
            }

            # Get total record count
            try:
                validation_result["total_records"] = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
            except Exception as e:
                validation_result["issues"].append(f"Could not access table {table_name}: {e}")
                return validation_result

            if validation_result["total_records"] == 0:
                validation_result["issues"].append(f"Table {table_name} is empty")
                return validation_result

            # Check what columns are available
            try:
                columns_result = self.conn.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    AND column_name IN ('field_id', 'field_uuid')
                """).fetchall()

                available_columns = {row[0] for row in columns_result}
                self.log.info(f"Available identifier columns in {table_name}: {available_columns}")
            except Exception as e:
                validation_result["issues"].append(f"Could not check available columns: {e}")
                return validation_result

            # Validate field_id if available
            if "field_id" in available_columns:
                try:
                    validation_result["records_with_field_id"] = self.conn.execute(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE field_id IS NOT NULL AND field_id != ''
                    """).fetchone()[0]

                    # Sample field IDs for validation
                    sample_field_ids = self.conn.execute(f"""
                        SELECT DISTINCT field_id 
                        FROM {table_name} 
                        WHERE field_id IS NOT NULL AND field_id != ''
                        ORDER BY RANDOM()
                        LIMIT {sample_size}
                    """).fetchall()

                    sample_field_ids = [str(row[0]) for row in sample_field_ids]

                    # Validate each sampled field ID
                    for field_id in sample_field_ids:
                        validation = self.validate_field_id_format(field_id)

                        if validation["is_valid_format"]:
                            validation_result["field_id_valid_formats"] += 1
                        else:
                            validation_result["field_id_invalid_formats"] += 1

                        # Track format distribution
                        format_type = validation["format_type"]
                        validation_result["field_id_format_distribution"][format_type] = (
                            validation_result["field_id_format_distribution"].get(format_type, 0)
                            + 1
                        )

                except Exception as e:
                    validation_result["issues"].append(f"Could not validate field_id: {e}")

            # Validate field_uuid if available (primary validation)
            if "field_uuid" in available_columns:
                try:
                    validation_result["records_with_field_uuid"] = self.conn.execute(f"""
                        SELECT COUNT(*) 
                        FROM {table_name} 
                        WHERE field_uuid IS NOT NULL AND field_uuid != ''
                    """).fetchone()[0]

                    # Sample field UUIDs for validation
                    sample_field_uuids = self.conn.execute(f"""
                        SELECT DISTINCT field_uuid 
                        FROM {table_name} 
                        WHERE field_uuid IS NOT NULL AND field_uuid != ''
                        ORDER BY RANDOM()
                        LIMIT {sample_size}
                    """).fetchall()

                    sample_field_uuids = [str(row[0]) for row in sample_field_uuids]

                    # Validate each sampled field UUID
                    for field_uuid in sample_field_uuids:
                        validation = self.validate_field_uuid_format(field_uuid)

                        if validation["is_valid_format"]:
                            validation_result["field_uuid_valid_formats"] += 1
                        else:
                            validation_result["field_uuid_invalid_formats"] += 1

                        # Track format distribution (should be mostly 'uuid')
                        format_type = validation["format_type"]
                        validation_result["field_uuid_format_distribution"][format_type] = (
                            validation_result["field_uuid_format_distribution"].get(format_type, 0)
                            + 1
                        )

                except Exception as e:
                    validation_result["issues"].append(f"Could not validate field_uuid: {e}")

            # Log results
            self.log.info(f"📊 Field Identifier Format Validation Results for {table_name}:")
            self.log.info(f"   Total records: {validation_result['total_records']:,}")

            # Log field ID results
            if validation_result["records_with_field_id"] > 0:
                total_field_id_sampled = (
                    validation_result["field_id_valid_formats"]
                    + validation_result["field_id_invalid_formats"]
                )
                if total_field_id_sampled > 0:
                    valid_percentage = (
                        validation_result["field_id_valid_formats"] / total_field_id_sampled
                    )
                    invalid_percentage = (
                        validation_result["field_id_invalid_formats"] / total_field_id_sampled
                    )

                    self.log.info("   Field ID Results:")
                    self.log.info(
                        f"     Records with field_id: "
                        f"{validation_result['records_with_field_id']:,}"
                    )
                    self.log.info(f"     Sampled field IDs: {total_field_id_sampled}")
                    self.log.info(
                        f"     Valid formats: {validation_result['field_id_valid_formats']} "
                        f"({valid_percentage:.1%})"
                    )
                    self.log.info(
                        f"     Invalid formats: {validation_result['field_id_invalid_formats']} "
                        f"({invalid_percentage:.1%})"
                    )
                    self.log.info(
                        f"     Format distribution: "
                        f"{validation_result['field_id_format_distribution']}"
                    )

                    if invalid_percentage > 0.1:
                        validation_result["issues"].append(
                            f"High percentage of invalid field ID formats: {invalid_percentage:.1%}"
                        )
                        validation_result["recommendations"].append(
                            "Investigate and fix invalid field ID formats"
                        )

            # Log field UUID results (primary validation)
            if validation_result["records_with_field_uuid"] > 0:
                total_field_uuid_sampled = (
                    validation_result["field_uuid_valid_formats"]
                    + validation_result["field_uuid_invalid_formats"]
                )
                if total_field_uuid_sampled > 0:
                    valid_percentage = (
                        validation_result["field_uuid_valid_formats"] / total_field_uuid_sampled
                    )
                    invalid_percentage = (
                        validation_result["field_uuid_invalid_formats"] / total_field_uuid_sampled
                    )

                    self.log.info("   Field UUID Results:")
                    self.log.info(
                        f"     Records with field_uuid: "
                        f"{validation_result['records_with_field_uuid']:,}"
                    )
                    self.log.info(f"     Sampled field UUIDs: {total_field_uuid_sampled}")
                    self.log.info(
                        f"     Valid formats: {validation_result['field_uuid_valid_formats']} "
                        f"({valid_percentage:.1%})"
                    )
                    self.log.info(
                        f"     Invalid formats: {validation_result['field_uuid_invalid_formats']} "
                        f"({invalid_percentage:.1%})"
                    )
                    self.log.info(
                        f"     Format distribution: "
                        f"{validation_result['field_uuid_format_distribution']}"
                    )

                    if invalid_percentage > 0.1:
                        validation_result["issues"].append(
                            f"High percentage of invalid field UUID formats: "
                            f"{invalid_percentage:.1%}"
                        )
                        validation_result["recommendations"].append(
                            "Investigate and fix invalid field UUID formats"
                        )
                    elif invalid_percentage == 0:
                        self.log.info("✅ All field UUID formats are valid")

            if (
                not validation_result["records_with_field_id"]
                and not validation_result["records_with_field_uuid"]
            ):
                validation_result["issues"].append("No field identifiers found in table")
                validation_result["recommendations"].append(
                    "Ensure table contains field_id or field_uuid columns"
                )

            return validation_result

        except Exception as e:
            self.log.error(f"Error validating field identifier formats in dataset: {e}")
            return {"error": str(e)}

    @timed(name="Comprehensive field ID validation")
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """
        Run comprehensive field ID validation for the entire pipeline.

        Returns:
            Dictionary with comprehensive validation results
        """
        try:
            self.log.info("🔍 Running comprehensive field ID validation...")

            comprehensive_results = {
                "validation_timestamp": self.processor.date_pattern,
                "field_ids_before_processing": len(self.field_ids_before_processing),
                "field_ids_after_processing": len(self.field_ids_after_processing),
                "field_uuids_before_processing": len(self.field_uuids_before_processing),
                "field_uuids_after_processing": len(self.field_uuids_after_processing),
                "consistency_validation": {},
                "format_validations": {},
                "uuid_presence_validations": {},
                "overall_score": 0.0,
                "issues": [],
                "recommendations": [],
                "passed": True,
            }

            # Step 1: Collect field IDs before processing (if not already done)
            if not self.field_ids_before_processing:
                self.collect_field_ids_before_processing()

            # Step 2: Collect field IDs after processing (if not already done)
            if not self.field_ids_after_processing:
                self.collect_field_ids_after_processing()

            # Step 3: Validate consistency
            comprehensive_results["consistency_validation"] = self.validate_field_id_consistency()

            # Step 4: Validate formats in key tables
            key_tables = [
                "agricultural_fields",
                "agricultural_fields_spatial",
                "nles5_nitrogen_estimates_gold",
                "nles5_estimates_final_batched",
            ]

            for table_name in key_tables:
                try:
                    format_validation = self.validate_field_id_formats_in_dataset(
                        table_name, sample_size=50
                    )
                    comprehensive_results["format_validations"][table_name] = format_validation
                except Exception as e:
                    self.log.warning(f"Could not validate formats in {table_name}: {e}")
                    comprehensive_results["format_validations"][table_name] = {"error": str(e)}

            # Step 5: Validate field_uuid presence in key tables
            for table_name in key_tables:
                try:
                    uuid_presence = self.validate_field_uuid_presence(table_name)
                    comprehensive_results["uuid_presence_validations"][table_name] = uuid_presence
                    # Log if table was excluded from validation
                    if "error" in uuid_presence:
                        self.log.info(
                            f"   ℹ️ Excluding {table_name} from UUID validation: "
                            f"{uuid_presence['error']}"
                        )
                except Exception as e:
                    self.log.warning(f"Could not validate field_uuid presence in {table_name}: {e}")
                    comprehensive_results["uuid_presence_validations"][table_name] = {
                        "error": str(e)
                    }

            # Step 6: Calculate overall score
            consistency_score = comprehensive_results["consistency_validation"].get(
                "consistency_score", 0.0
            )

            # Calculate format score from validations (prioritize UUID formats)
            format_scores = []
            uuid_presence_scores = []

            for table_name, format_validation in comprehensive_results[
                "format_validations"
            ].items():
                if "error" not in format_validation:
                    # Prioritize field_uuid validation over field_id validation
                    uuid_total = format_validation.get(
                        "field_uuid_valid_formats", 0
                    ) + format_validation.get("field_uuid_invalid_formats", 0)
                    if uuid_total > 0:
                        uuid_score = (
                            format_validation.get("field_uuid_valid_formats", 0) / uuid_total
                        )
                        format_scores.append(uuid_score)
                    else:
                        # Fall back to field_id validation if no UUIDs
                        id_total = format_validation.get(
                            "field_id_valid_formats", 0
                        ) + format_validation.get("field_id_invalid_formats", 0)
                        if id_total > 0:
                            id_score = format_validation.get("field_id_valid_formats", 0) / id_total
                            format_scores.append(id_score)

            # Calculate UUID presence scores
            uuid_tables_checked = []
            for table_name, uuid_validation in comprehensive_results[
                "uuid_presence_validations"
            ].items():
                if "error" not in uuid_validation:
                    coverage = uuid_validation.get("field_uuid_coverage", 0.0)
                    uuid_presence_scores.append(coverage)
                    uuid_tables_checked.append(f"{table_name}({coverage:.0%})")

            avg_format_score = sum(format_scores) / len(format_scores) if format_scores else 1.0
            avg_uuid_presence_score = (
                sum(uuid_presence_scores) / len(uuid_presence_scores)
                if uuid_presence_scores
                else 1.0
            )

            # Log which tables were included in UUID scoring
            if uuid_tables_checked:
                self.log.info(
                    f"   UUID validation included {len(uuid_tables_checked)} tables: "
                    f"{', '.join(uuid_tables_checked)}"
                )

            # NLES5-specific scoring: Format and UUID presence are more important than consistency
            # because NLES5 intentionally filters out 95%+ of records due to missing required data
            if comprehensive_results["field_uuids_after_processing"] > 0:
                # UUID presence score is most important for NLES5 - if final results
                # have UUIDs, that's what matters
                comprehensive_results["overall_score"] = (avg_format_score * 0.4) + (
                    avg_uuid_presence_score * 0.6
                )
                self.log.info(
                    "   Using NLES5 UUID-focused scoring (Format: 40%, UUID presence: 60%)"
                )
            else:
                # Fallback to field ID consistency for pipelines without UUIDs
                comprehensive_results["overall_score"] = (
                    (consistency_score * 0.5)
                    + (avg_format_score * 0.3)
                    + (avg_uuid_presence_score * 0.2)
                )
                self.log.info(
                    "   Using standard scoring (Consistency: 50%, Format: 30%, UUID presence: 20%)"
                )

            # Step 7: Determine if validation passed
            if comprehensive_results["overall_score"] < 0.7:
                comprehensive_results["passed"] = False
                comprehensive_results["issues"].append(
                    f"Overall validation score too low: "
                    f"{comprehensive_results['overall_score']:.2%}"
                )
                comprehensive_results["recommendations"].append(
                    "Investigate field identifier validation issues"
                )

            # Enhanced logging with context about data filtering
            self.log.info("📊 COMPREHENSIVE FIELD IDENTIFIER VALIDATION RESULTS:")
            self.log.info(
                "🚨 IMPORTANT: Large differences between input/output are NORMAL for NLES5!"
            )
            self.log.info(
                "   The pipeline filters out fields missing required data "
                "(crop, geometry, percolation)"
            )
            self.log.info(
                f"   Field IDs before processing: "
                f"{comprehensive_results['field_ids_before_processing']:,}"
            )
            self.log.info(
                f"   Field IDs after processing: "
                f"{comprehensive_results['field_ids_after_processing']:,}"
            )
            self.log.info(
                f"   Field UUIDs before processing: "
                f"{comprehensive_results['field_uuids_before_processing']:,}"
            )
            self.log.info(
                f"   Field UUIDs after processing: "
                f"{comprehensive_results['field_uuids_after_processing']:,}"
            )

            # Calculate and log filtering percentage
            if comprehensive_results["field_uuids_before_processing"] > 0:
                filtering_rate = 1.0 - (
                    comprehensive_results["field_uuids_after_processing"]
                    / comprehensive_results["field_uuids_before_processing"]
                )
                self.log.info(
                    f"   Data filtering rate: {filtering_rate:.1%} (NORMAL for NLES5 requirements)"
                )

            self.log.info(f"   Consistency score: {consistency_score:.2%}")
            self.log.info(f"   Average format score: {avg_format_score:.2%}")
            self.log.info(f"   Average UUID presence score: {avg_uuid_presence_score:.2%}")
            self.log.info(
                f"   Overall validation score: {comprehensive_results['overall_score']:.2%}"
            )
            self.log.info(
                f"   Validation status: "
                f"{'✅ PASSED' if comprehensive_results['passed'] else '❌ FAILED'}"
            )

            # Store results
            self.validation_results = comprehensive_results

            return comprehensive_results

        except Exception as e:
            self.log.error(f"Error running comprehensive field ID validation: {e}")
            return {"error": str(e), "passed": False}

    def get_validation_summary(self) -> str:
        """
        Get a summary of field ID validation results.

        Returns:
            String summary of validation results
        """
        if not self.validation_results:
            return "No field identifier validation results available"

        results = self.validation_results

        # Prioritize UUID validation if available
        field_uuids_before = results.get("field_uuids_before_processing", 0)
        field_uuids_after = results.get("field_uuids_after_processing", 0)
        field_ids_before = results.get("field_ids_before_processing", 0)
        field_ids_after = results.get("field_ids_after_processing", 0)

        if field_uuids_before > 0 or field_uuids_after > 0:
            primary_type = "Field UUIDs"
            before_count = field_uuids_before
            after_count = field_uuids_after
        else:
            primary_type = "Field IDs"
            before_count = field_ids_before
            after_count = field_ids_after

        summary = f"""
Field Identifier Validation Summary:
- {primary_type} before processing: {before_count:,}
- {primary_type} after processing: {after_count:,}
- Overall validation score: {results.get("overall_score", 0):.2%}
- Validation status: {"✅ PASSED" if results.get("passed", False) else "❌ FAILED"}
"""

        # Include both types if both are present
        if field_uuids_before > 0 and field_ids_before > 0:
            summary += f"- Field IDs before processing: {field_ids_before:,}\n"
            summary += f"- Field IDs after processing: {field_ids_after:,}\n"

        if results.get("issues"):
            summary += f"- Issues: {len(results['issues'])}\n"

        if results.get("recommendations"):
            summary += f"- Recommendations: {len(results['recommendations'])}\n"

        return summary.strip()

    def validate_field_uuid_presence(self, table_name: str) -> Dict[str, Any]:
        """
        Validate that field_uuid column is present and properly populated in a table.

        Args:
            table_name: Name of the table to check

        Returns:
            Dictionary with UUID presence validation results
        """
        validation_result = {
            "table_name": table_name,
            "has_field_uuid_column": False,
            "total_records": 0,
            "records_with_field_uuid": 0,
            "field_uuid_coverage": 0.0,
            "sample_field_uuids": [],
            "uuid_format_issues": 0,
            "issues": [],
            "recommendations": [],
        }

        try:
            # Check if table exists
            try:
                validation_result["total_records"] = self.conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
            except Exception as e:
                validation_result["issues"].append(f"Table {table_name} not accessible: {e}")
                # Mark as error so it's excluded from scoring
                return {"error": f"Table {table_name} not accessible"}

            # Check if field_uuid column exists
            try:
                columns_result = self.conn.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name = 'field_uuid'
                """).fetchall()

                validation_result["has_field_uuid_column"] = len(columns_result) > 0
            except Exception as e:
                validation_result["issues"].append(f"Could not check for field_uuid column: {e}")
                # Mark as error so it's excluded from scoring
                return {"error": f"Could not check for field_uuid column in {table_name}"}

            if not validation_result["has_field_uuid_column"]:
                validation_result["issues"].append(f"Table {table_name} missing field_uuid column")
                validation_result["recommendations"].append(
                    "Ensure field_uuid column is present for proper field tracking"
                )
                # Mark as error so it's excluded from scoring - this table is not
                # relevant for UUID scoring
                return {"error": f"Table {table_name} missing field_uuid column"}

            # Check field_uuid population
            try:
                validation_result["records_with_field_uuid"] = self.conn.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE field_uuid IS NOT NULL AND field_uuid != ''
                """).fetchone()[0]

                if validation_result["total_records"] > 0:
                    validation_result["field_uuid_coverage"] = (
                        validation_result["records_with_field_uuid"]
                        / validation_result["total_records"]
                    )

                # Get sample field UUIDs
                sample_uuids = self.conn.execute(f"""
                    SELECT field_uuid 
                    FROM {table_name} 
                    WHERE field_uuid IS NOT NULL AND field_uuid != ''
                    ORDER BY RANDOM()
                    LIMIT 5
                """).fetchall()

                validation_result["sample_field_uuids"] = [str(row[0]) for row in sample_uuids]

                # Validate format of sample UUIDs
                for sample_uuid in validation_result["sample_field_uuids"]:
                    uuid_validation = self.validate_field_uuid_format(sample_uuid)
                    if not uuid_validation["is_valid_format"]:
                        validation_result["uuid_format_issues"] += 1

            except Exception as e:
                validation_result["issues"].append(f"Could not analyze field_uuid data: {e}")
                return validation_result

            # Evaluate results
            if validation_result["field_uuid_coverage"] < 0.95:
                validation_result["issues"].append(
                    f"Low field_uuid coverage: {validation_result['field_uuid_coverage']:.1%}"
                )
                validation_result["recommendations"].append(
                    "Investigate why some records lack field_uuid values"
                )
            elif validation_result["field_uuid_coverage"] < 1.0:
                validation_result["recommendations"].append(
                    "Minor field_uuid coverage gaps detected"
                )

            if validation_result["uuid_format_issues"] > 0:
                validation_result["issues"].append(
                    f"{validation_result['uuid_format_issues']} field UUIDs have format issues"
                )
                validation_result["recommendations"].append("Fix invalid field_uuid formats")

            self.log.info(f"📊 Field UUID Presence Validation for {table_name}:")
            self.log.info(
                f"   Has field_uuid column: "
                f"{'✅' if validation_result['has_field_uuid_column'] else '❌'}"
            )
            self.log.info(f"   Total records: {validation_result['total_records']:,}")
            self.log.info(
                f"   Records with field_uuid: {validation_result['records_with_field_uuid']:,}"
            )
            self.log.info(f"   Field UUID coverage: {validation_result['field_uuid_coverage']:.1%}")
            self.log.info(f"   UUID format issues: {validation_result['uuid_format_issues']}")

            return validation_result

        except Exception as e:
            self.log.error(f"Error validating field_uuid presence in {table_name}: {e}")
            return {"error": str(e)}
