"""
Centralized UUID generation utilities for all landbrugsdata pipelines.

This module provides consistent UUID generation across all pipelines to ensure
data integrity and proper relationships between entities.
"""

import hashlib
import logging
import os
import uuid
from typing import Optional, Union

logger = logging.getLogger(__name__)


class LandbrugsdataUUID:
    """
    Centralized UUID generation for all landbrugsdata entities.

    This class ensures consistent UUID generation across all pipelines using
    UUID5 with a configurable namespace from environment variables.

    Required environment variables:
    - LANDBRUGSDATA_UUID_NAMESPACE: UUID namespace for deterministic generation
    """

    _namespace = None

    @classmethod
    def _get_namespace(cls) -> uuid.UUID:
        """Get the UUID namespace from environment variable."""
        if cls._namespace is None:
            namespace_str = os.getenv("LANDBRUGSDATA_UUID_NAMESPACE")
            if not namespace_str:
                raise ValueError(
                    "LANDBRUGSDATA_UUID_NAMESPACE environment variable is required. "
                    "Generate one with: python -c 'import uuid; print(uuid.uuid4())'"
                )

            try:
                cls._namespace = uuid.UUID(namespace_str)
                logger.info("UUID namespace loaded from environment")
            except ValueError as e:
                raise ValueError(f"Invalid UUID namespace in LANDBRUGSDATA_UUID_NAMESPACE: {e}")

        return cls._namespace

    @classmethod
    def company_uuid(cls, cvr_number: Union[str, int]) -> Optional[str]:
        """
        Generate deterministic company UUID from CVR number.

        Args:
            cvr_number: CVR number (8 digits)

        Returns:
            UUID string or None if invalid CVR

        Examples:
            >>> LandbrugsdataUUID.company_uuid("12345678")
            'a1b2c3d4-e5f6-5789-8abc-def012345678'
        """
        if not cvr_number:
            return None

        cvr_str = str(cvr_number).strip()

        # Validate CVR format (8 digits, starting with 1-9)
        if len(cvr_str) != 8 or not cvr_str.isdigit() or cvr_str[0] == "0":
            logger.warning(f"Invalid CVR number format: {cvr_number}")
            return None

        # Use MD5-based UUID generation to match DuckDB implementation
        namespace_str = str(cls._get_namespace())
        input_str = f"{namespace_str}company-cvr-{cvr_str}"
        md5_hash = hashlib.md5(input_str.encode()).hexdigest()

        # Format as UUID5 (version 5, variant 10)
        uuid_str = (
            f"{md5_hash[0:8]}-{md5_hash[8:12]}-5{md5_hash[12:15]}-"
            f"8{md5_hash[15:18]}-{md5_hash[18:30]}"
        )
        return uuid_str

    @classmethod
    def field_uuid(cls, geometry_wkb: bytes) -> str:
        """
        Generate deterministic field UUID from geometry WKB.

        Args:
            geometry_wkb: Geometry in Well-Known Binary format

        Returns:
            UUID string
        """
        if not geometry_wkb:
            raise ValueError("Geometry WKB cannot be empty")

        geometry_hex = geometry_wkb.hex()

        # Use MD5-based UUID generation to match DuckDB implementation
        namespace_str = str(cls._get_namespace())
        input_str = f"{namespace_str}field-geometry-{geometry_hex}"
        md5_hash = hashlib.md5(input_str.encode()).hexdigest()

        # Format as UUID5 (version 5, variant 10)
        uuid_str = (
            f"{md5_hash[0:8]}-{md5_hash[8:12]}-5{md5_hash[12:15]}-"
            f"8{md5_hash[15:18]}-{md5_hash[18:30]}"
        )
        return uuid_str

    @classmethod
    def pnumber_uuid(cls, p_number: Union[str, int]) -> Optional[str]:
        """
        Generate deterministic P-number UUID.

        Args:
            p_number: Production unit number

        Returns:
            UUID string or None if invalid
        """
        if not p_number:
            return None

        p_str = str(p_number).strip()
        if not p_str.isdigit():
            logger.warning(f"Invalid P-number format: {p_number}")
            return None

        # Use MD5-based UUID generation to match DuckDB implementation
        namespace_str = str(cls._get_namespace())
        input_str = f"{namespace_str}pnumber-{p_str}"
        md5_hash = hashlib.md5(input_str.encode()).hexdigest()

        # Format as UUID5 (version 5, variant 10)
        uuid_str = (
            f"{md5_hash[0:8]}-{md5_hash[8:12]}-5{md5_hash[12:15]}-"
            f"8{md5_hash[15:18]}-{md5_hash[18:30]}"
        )
        return uuid_str

    @classmethod
    def generate_deterministic_uuid(cls, entity_type: str, identifier: str) -> str:
        """
        Generate deterministic UUID for any entity type with custom identifier.

        Args:
            entity_type: Type of entity (e.g., 'employment', 'transaction', etc.)
            identifier: Unique identifier string for this entity

        Returns:
            UUID string
        """
        if not identifier:
            raise ValueError("Identifier cannot be empty")

        # Use MD5-based UUID generation to match DuckDB implementation
        namespace_str = str(cls._get_namespace())
        input_str = f"{namespace_str}{entity_type}-{identifier}"
        md5_hash = hashlib.md5(input_str.encode()).hexdigest()

        # Format as UUID5 (version 5, variant 10)
        uuid_str = (
            f"{md5_hash[0:8]}-{md5_hash[8:12]}-5{md5_hash[12:15]}-"
            f"8{md5_hash[15:18]}-{md5_hash[18:30]}"
        )
        return uuid_str

    @classmethod
    def chr_uuid(cls, chr_number: Union[str, int]) -> Optional[str]:
        """
        Generate deterministic CHR UUID.

        Args:
            chr_number: CHR number (without CHR prefix)

        Returns:
            UUID string or None if invalid
        """
        if not chr_number:
            return None

        # Remove CHR prefix if present
        chr_str = str(chr_number).strip()
        if chr_str.startswith("CHR"):
            chr_str = chr_str[3:]

        if not chr_str.isdigit():
            logger.warning(f"Invalid CHR number format: {chr_number}")
            return None

        # Use MD5-based UUID generation to match DuckDB implementation
        namespace_str = str(cls._get_namespace())
        input_str = f"{namespace_str}chr-{chr_str}"
        md5_hash = hashlib.md5(input_str.encode()).hexdigest()

        # Format as UUID5 (version 5, variant 10)
        uuid_str = (
            f"{md5_hash[0:8]}-{md5_hash[8:12]}-5{md5_hash[12:15]}-"
            f"8{md5_hash[15:18]}-{md5_hash[18:30]}"
        )
        return uuid_str

    @classmethod
    def setup_duckdb_functions(cls, conn) -> None:
        """
        Set up UUID generation functions in DuckDB connection.

        Args:
            conn: DuckDB connection object
        """
        try:
            # Install crypto extension
            conn.execute("INSTALL crypto FROM community")
            conn.execute("LOAD crypto")

            # Create UUID5 function using MD5 hash (DuckDB doesn't have built-in uuid5)
            namespace = cls._get_namespace()
            # Create UUID5 function with shorter lines
            hash_expr = f"crypto_hash('md5', CONCAT('{namespace}', entity_type, '-', identifier))"
            conn.execute(f"""
                CREATE OR REPLACE FUNCTION landbrugsdata_uuid5(entity_type, identifier) AS (
                    SELECT CASE
                        WHEN entity_type IS NULL OR identifier IS NULL THEN NULL
                        ELSE CONCAT(
                            SUBSTR({hash_expr}, 1, 8), '-',
                            SUBSTR({hash_expr}, 9, 4), '-',
                            '5', SUBSTR({hash_expr}, 13, 3), '-',
                            CONCAT('8', SUBSTR({hash_expr}, 16, 3)), '-',
                            SUBSTR({hash_expr}, 19, 12)
                        )
                    END
                )
            """)

            # Create specific helper functions
            conn.execute("""
                CREATE OR REPLACE FUNCTION company_uuid(cvr_number) AS (
                    SELECT CASE
                        WHEN cvr_number IS NULL
                             OR LENGTH(TRIM(CAST(cvr_number AS VARCHAR))) != 8
                             OR NOT REGEXP_MATCHES(
                                 TRIM(CAST(cvr_number AS VARCHAR)), '^[1-9][0-9]{7}$'
                             )
                        THEN NULL
                        ELSE landbrugsdata_uuid5('company-cvr', TRIM(CAST(cvr_number AS VARCHAR)))
                    END
                )
            """)

            conn.execute("""
                CREATE OR REPLACE FUNCTION field_uuid(geometry_data) AS (
                    SELECT CASE
                        WHEN geometry_data IS NULL THEN NULL
                        ELSE landbrugsdata_uuid5('field-geometry', hex(ST_AsWKB(geometry_data)))
                    END
                )
            """)

            conn.execute("""
                CREATE OR REPLACE FUNCTION pnumber_uuid(p_number) AS (
                    SELECT CASE
                        WHEN p_number IS NULL THEN NULL
                        ELSE landbrugsdata_uuid5('pnumber', TRIM(CAST(p_number AS VARCHAR)))
                    END
                )
            """)

            conn.execute("""
                CREATE OR REPLACE FUNCTION chr_uuid(chr_number) AS (
                    SELECT CASE
                        WHEN chr_number IS NULL THEN NULL
                        ELSE landbrugsdata_uuid5('chr',
                            CASE
                                WHEN TRIM(CAST(chr_number AS VARCHAR)) LIKE 'CHR%'
                                THEN SUBSTR(TRIM(CAST(chr_number AS VARCHAR)), 4)
                                ELSE TRIM(CAST(chr_number AS VARCHAR))
                            END
                        )
                    END
                )
            """)

            logger.info("✅ Landbrugsdata UUID functions created successfully")

        except Exception as e:
            logger.error(f"Failed to set up UUID functions: {e}")
            raise

    @classmethod
    def generate_postgresql_functions(cls) -> str:
        """
        Generate PostgreSQL function definitions for database migrations.

        Returns:
            SQL string with function definitions
        """
        namespace = cls._get_namespace()

        return f"""
-- Landbrugsdata UUID generation functions for PostgreSQL
-- Generated automatically - do not edit manually

-- Main company UUID function
CREATE OR REPLACE FUNCTION landbrugsdata_company_uuid(cvr_number_input BIGINT)
RETURNS UUID AS $$
DECLARE
    namespace_uuid UUID := '{namespace}'::UUID;
    cvr_str TEXT;
BEGIN
    -- Validate CVR number format
    IF cvr_number_input IS NULL THEN
        RETURN NULL;
    END IF;

    cvr_str := TRIM(cvr_number_input::TEXT);

    -- Validate CVR format (8 digits, starting with 1-9)
    IF LENGTH(cvr_str) != 8 OR NOT cvr_str ~ '^[1-9][0-9]{{7}}$' THEN
        RAISE WARNING 'Invalid CVR number format: %', cvr_number_input;
        RETURN NULL;
    END IF;

    -- Generate UUID5 using the same format as Python
    RETURN uuid_generate_v5(namespace_uuid, 'company-cvr-' || cvr_str);
END;
$$ LANGUAGE plpgsql;

-- P-number UUID function
CREATE OR REPLACE FUNCTION landbrugsdata_pnumber_uuid(p_number_input TEXT)
RETURNS UUID AS $$
DECLARE
    namespace_uuid UUID := '{namespace}'::UUID;
    p_str TEXT;
BEGIN
    IF p_number_input IS NULL THEN
        RETURN NULL;
    END IF;

    p_str := TRIM(p_number_input);

    IF NOT p_str ~ '^[0-9]+$' THEN
        RAISE WARNING 'Invalid P-number format: %', p_number_input;
        RETURN NULL;
    END IF;

    RETURN uuid_generate_v5(namespace_uuid, 'pnumber-' || p_str);
END;
$$ LANGUAGE plpgsql;

-- CHR UUID function
CREATE OR REPLACE FUNCTION landbrugsdata_chr_uuid(chr_number_input TEXT)
RETURNS UUID AS $$
DECLARE
    namespace_uuid UUID := '{namespace}'::UUID;
    chr_str TEXT;
BEGIN
    IF chr_number_input IS NULL THEN
        RETURN NULL;
    END IF;

    chr_str := TRIM(chr_number_input);

    -- Remove CHR prefix if present
    IF chr_str ILIKE 'CHR%' THEN
        chr_str := SUBSTR(chr_str, 4);
    END IF;

    IF NOT chr_str ~ '^[0-9]+$' THEN
        RAISE WARNING 'Invalid CHR number format: %', chr_number_input;
        RETURN NULL;
    END IF;

    RETURN uuid_generate_v5(namespace_uuid, 'chr-' || chr_str);
END;
$$ LANGUAGE plpgsql;

-- Field UUID function (for geometry-based UUIDs)
CREATE OR REPLACE FUNCTION landbrugsdata_field_uuid(geometry_wkb BYTEA)
RETURNS UUID AS $$
DECLARE
    namespace_uuid UUID := '{namespace}'::UUID;
    geometry_hex TEXT;
BEGIN
    IF geometry_wkb IS NULL THEN
        RETURN NULL;
    END IF;

    geometry_hex := encode(geometry_wkb, 'hex');
    RETURN uuid_generate_v5(namespace_uuid, 'field-geometry-' || geometry_hex);
END;
$$ LANGUAGE plpgsql;
"""


def validate_uuid_consistency(conn, entity_type: str, sample_identifiers: list) -> dict:
    """
    Validate that UUID generation is consistent between Python and DuckDB.

    Args:
        conn: DuckDB connection
        entity_type: Type of entity ('company', 'field', 'pnumber', 'chr')
        sample_identifiers: List of sample identifiers to test

    Returns:
        Dict with validation results
    """
    results = {
        "total_tested": len(sample_identifiers),
        "consistent": 0,
        "inconsistent": 0,
        "errors": [],
    }

    uuid_methods = {
        "company": (LandbrugsdataUUID.company_uuid, "company_uuid"),
        "pnumber": (LandbrugsdataUUID.pnumber_uuid, "pnumber_uuid"),
        "chr": (LandbrugsdataUUID.chr_uuid, "chr_uuid"),
    }

    if entity_type not in uuid_methods:
        results["errors"].append(f"Unknown entity type: {entity_type}")
        return results

    python_method, duckdb_function = uuid_methods[entity_type]

    for identifier in sample_identifiers:
        try:
            # Generate UUID using Python
            python_uuid = python_method(identifier)

            # Generate UUID using DuckDB
            duckdb_result = conn.execute(f"SELECT {duckdb_function}(?)", [identifier]).fetchone()
            duckdb_uuid = duckdb_result[0] if duckdb_result else None

            if python_uuid == duckdb_uuid:
                results["consistent"] += 1
            else:
                results["inconsistent"] += 1
                results["errors"].append(
                    f"Mismatch for {identifier}: Python={python_uuid}, DuckDB={duckdb_uuid}"
                )

        except Exception as e:
            results["errors"].append(f"Error testing {identifier}: {e}")

    return results
