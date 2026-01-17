"""
Centralized CRS handling utilities for landbruget.dk pipelines.

This module provides constants, detection functions, and validation utilities
for working with Coordinate Reference Systems across all data pipelines.

Based on DuckDB-spatial documentation:
https://duckdb.org/docs/stable/core_extensions/spatial/functions

Key patterns:
- ST_Transform(geometry, 'source_crs', 'target_crs') for CRS conversion
- ST_Buffer requires metric CRS (use EPSG:25832 for Denmark)
- ST_Area_Spheroid expects LAT/LON order (not lon/lat)
"""

import logging

logger = logging.getLogger(__name__)

# =============================================================================
# Standard CRS Constants
# =============================================================================

DANISH_UTM = "EPSG:25832"  # UTM Zone 32N / ETRS89 (Danish government standard)
WGS84 = "EPSG:4326"  # World Geodetic System 1984 (storage standard)
WEB_MERCATOR = "EPSG:3857"  # Web mapping projection (display only)

# Aliases for clarity
SOURCE_CRS_DANISH = DANISH_UTM
TARGET_CRS_STORAGE = WGS84

# NEW: Optimal CRS strategy constants
# Process in EPSG:25832 throughout Bronze/Silver/Gold, transform to EPSG:4326 only at Supabase upload
TARGET_CRS_PROCESSING = DANISH_UTM  # Use throughout pipeline processing
TARGET_CRS_SUPABASE = WGS84  # Use ONLY at final Supabase upload

# =============================================================================
# Denmark Bounding Boxes for CRS Detection
# =============================================================================

DENMARK_BOUNDS_WGS84 = {
    "min_x": 7.5,  # min longitude
    "max_x": 15.5,  # max longitude
    "min_y": 54.5,  # min latitude
    "max_y": 58.0,  # max latitude
}

DENMARK_BOUNDS_UTM = {
    "min_x": 400000,  # min easting
    "max_x": 900000,  # max easting
    "min_y": 6000000,  # min northing
    "max_y": 6500000,  # max northing
}

# =============================================================================
# Conversion Factors at Denmark Latitude (~56°N)
# =============================================================================

# At 56°N latitude:
# - 1 degree longitude ≈ 62 km (cos(56°) * 111km)
# - 1 degree latitude ≈ 111 km
DEGREES_TO_METERS_LON = 62000  # meters per degree longitude
DEGREES_TO_METERS_LAT = 111000  # meters per degree latitude
DEGREES_TO_METERS_AVG = 86500  # average for rough calculations


def meters_to_degrees(meters: float) -> float:
    """
    Convert meters to approximate degrees for Denmark latitude.

    This is useful for ST_DWithin operations when working with EPSG:4326 data.

    Args:
        meters: Distance in meters

    Returns:
        Approximate distance in degrees

    Example:
        >>> meters_to_degrees(100000)  # 100km
        1.156...
    """
    return meters / DEGREES_TO_METERS_AVG


def degrees_to_meters(degrees: float) -> float:
    """
    Convert degrees to approximate meters for Denmark latitude.

    Args:
        degrees: Distance in degrees

    Returns:
        Approximate distance in meters
    """
    return degrees * DEGREES_TO_METERS_AVG


# =============================================================================
# CRS Detection Functions
# =============================================================================


def detect_crs_from_bounds(min_x: float, max_x: float, min_y: float, max_y: float) -> tuple[str | None, str]:
    """
    Detect CRS from coordinate bounds.

    Analyzes coordinate ranges to determine the likely CRS and coordinate order.

    Args:
        min_x: Minimum X coordinate
        max_x: Maximum X coordinate
        min_y: Minimum Y coordinate
        max_y: Maximum Y coordinate

    Returns:
        Tuple of (crs, coordinate_order) where:
        - crs: Detected CRS string (e.g., "EPSG:4326") or None if unknown
        - coordinate_order: One of "lon_lat", "lat_lon_swapped", "easting_northing", "unknown"

    Example:
        >>> detect_crs_from_bounds(8.0, 15.0, 54.5, 57.5)
        ('EPSG:4326', 'lon_lat')

        >>> detect_crs_from_bounds(500000, 800000, 6100000, 6400000)
        ('EPSG:25832', 'easting_northing')
    """
    # Check WGS84 with standard lon/lat order (X=longitude, Y=latitude)
    if (
        DENMARK_BOUNDS_WGS84["min_x"] <= min_x <= DENMARK_BOUNDS_WGS84["max_x"]
        and DENMARK_BOUNDS_WGS84["min_x"] <= max_x <= DENMARK_BOUNDS_WGS84["max_x"]
        and DENMARK_BOUNDS_WGS84["min_y"] <= min_y <= DENMARK_BOUNDS_WGS84["max_y"]
        and DENMARK_BOUNDS_WGS84["min_y"] <= max_y <= DENMARK_BOUNDS_WGS84["max_y"]
    ):
        return WGS84, "lon_lat"

    # Check WGS84 with swapped lat/lon order (X=latitude, Y=longitude)
    # This is a common issue with some data sources
    if (
        DENMARK_BOUNDS_WGS84["min_y"] <= min_x <= DENMARK_BOUNDS_WGS84["max_y"]
        and DENMARK_BOUNDS_WGS84["min_y"] <= max_x <= DENMARK_BOUNDS_WGS84["max_y"]
        and DENMARK_BOUNDS_WGS84["min_x"] <= min_y <= DENMARK_BOUNDS_WGS84["max_x"]
        and DENMARK_BOUNDS_WGS84["min_x"] <= max_y <= DENMARK_BOUNDS_WGS84["max_x"]
    ):
        return WGS84, "lat_lon_swapped"

    # Check UTM Zone 32N (Danish standard)
    if (
        DENMARK_BOUNDS_UTM["min_x"] <= min_x <= DENMARK_BOUNDS_UTM["max_x"]
        and DENMARK_BOUNDS_UTM["min_x"] <= max_x <= DENMARK_BOUNDS_UTM["max_x"]
        and DENMARK_BOUNDS_UTM["min_y"] <= min_y <= DENMARK_BOUNDS_UTM["max_y"]
        and DENMARK_BOUNDS_UTM["min_y"] <= max_y <= DENMARK_BOUNDS_UTM["max_y"]
    ):
        return DANISH_UTM, "easting_northing"

    return None, "unknown"


def is_utm_coordinate_range(x: float, y: float) -> bool:
    """
    Check if a single coordinate pair is in UTM range for Denmark.

    Args:
        x: X coordinate (easting)
        y: Y coordinate (northing)

    Returns:
        True if coordinates are in Danish UTM range
    """
    return (
        DENMARK_BOUNDS_UTM["min_x"] <= x <= DENMARK_BOUNDS_UTM["max_x"]
        and DENMARK_BOUNDS_UTM["min_y"] <= y <= DENMARK_BOUNDS_UTM["max_y"]
    )


def is_wgs84_coordinate_range(x: float, y: float) -> bool:
    """
    Check if a single coordinate pair is in WGS84 range for Denmark.

    Args:
        x: X coordinate (longitude)
        y: Y coordinate (latitude)

    Returns:
        True if coordinates are in Danish WGS84 range
    """
    return (
        DENMARK_BOUNDS_WGS84["min_x"] <= x <= DENMARK_BOUNDS_WGS84["max_x"]
        and DENMARK_BOUNDS_WGS84["min_y"] <= y <= DENMARK_BOUNDS_WGS84["max_y"]
    )


# =============================================================================
# CRS Validation Functions
# =============================================================================


def validate_crs_before_transform(
    conn, table_name: str, geometry_col: str, expected_crs: str, sample_size: int = 1000
) -> tuple[str, str]:
    """
    Validate geometry CRS before transformation.

    Samples geometries from a table and validates they match the expected CRS.
    Raises ValueError if there's a mismatch.

    Args:
        conn: DuckDB connection
        table_name: Name of table to validate
        geometry_col: Name of geometry column
        expected_crs: Expected CRS (e.g., "EPSG:4326")
        sample_size: Number of rows to sample for validation

    Returns:
        Tuple of (detected_crs, coordinate_order)

    Raises:
        ValueError: If CRS doesn't match expected or cannot be determined
    """
    # Get coordinate bounds from sample
    query = f"""
        SELECT
            MIN(ST_XMin({geometry_col})) as min_x,
            MAX(ST_XMax({geometry_col})) as max_x,
            MIN(ST_YMin({geometry_col})) as min_y,
            MAX(ST_YMax({geometry_col})) as max_y
        FROM (
            SELECT {geometry_col}
            FROM {table_name}
            WHERE {geometry_col} IS NOT NULL
            LIMIT {sample_size}
        )
    """

    result = conn.execute(query).fetchone()

    if not result or result[0] is None:
        raise ValueError(f"No valid geometries found in {table_name}.{geometry_col}")

    min_x, max_x, min_y, max_y = result
    detected_crs, order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

    if detected_crs is None:
        raise ValueError(
            f"Cannot detect CRS for {table_name}.{geometry_col}: "
            f"bounds X=[{min_x:.2f}, {max_x:.2f}], Y=[{min_y:.2f}, {max_y:.2f}] "
            f"don't match any known Danish CRS"
        )

    if detected_crs != expected_crs:
        raise ValueError(
            f"CRS mismatch in {table_name}.{geometry_col}: "
            f"expected {expected_crs}, detected {detected_crs} "
            f"(bounds: X=[{min_x:.2f}, {max_x:.2f}], Y=[{min_y:.2f}, {max_y:.2f}])"
        )

    logger.info(f"CRS validation passed for {table_name}.{geometry_col}: {detected_crs} ({order})")

    return detected_crs, order


def log_geometry_bounds(conn, table_name: str, geometry_col: str, sample_size: int = 100) -> dict:
    """
    Log geometry bounds for debugging CRS issues.

    Args:
        conn: DuckDB connection
        table_name: Name of table
        geometry_col: Name of geometry column
        sample_size: Number of rows to sample

    Returns:
        Dictionary with bounds and detected CRS info
    """
    query = f"""
        SELECT
            MIN(ST_XMin({geometry_col})) as min_x,
            MAX(ST_XMax({geometry_col})) as max_x,
            MIN(ST_YMin({geometry_col})) as min_y,
            MAX(ST_YMax({geometry_col})) as max_y,
            COUNT(*) as count
        FROM (
            SELECT {geometry_col}
            FROM {table_name}
            WHERE {geometry_col} IS NOT NULL
            LIMIT {sample_size}
        )
    """

    result = conn.execute(query).fetchone()

    if not result or result[0] is None:
        logger.warning(f"No geometries found in {table_name}.{geometry_col}")
        return {"error": "no_geometries"}

    min_x, max_x, min_y, max_y, count = result
    detected_crs, order = detect_crs_from_bounds(min_x, max_x, min_y, max_y)

    info = {
        "table": table_name,
        "geometry_column": geometry_col,
        "sample_count": count,
        "bounds": {
            "min_x": min_x,
            "max_x": max_x,
            "min_y": min_y,
            "max_y": max_y,
        },
        "detected_crs": detected_crs,
        "coordinate_order": order,
    }

    logger.info(
        f"Geometry bounds for {table_name}.{geometry_col}: "
        f"X=[{min_x:.4f}, {max_x:.4f}], Y=[{min_y:.4f}, {max_y:.4f}] "
        f"-> {detected_crs or 'UNKNOWN'} ({order})"
    )

    return info


# =============================================================================
# SQL Generation Helpers
# =============================================================================


def sql_transform_to_utm(geometry_expr: str, source_crs: str = WGS84) -> str:
    """
    Generate SQL to transform geometry to Danish UTM for metric operations.

    Args:
        geometry_expr: SQL expression for geometry
        source_crs: Source CRS (default WGS84)

    Returns:
        SQL expression for transformed geometry
    """
    return f"ST_Transform({geometry_expr}, '{source_crs}', '{DANISH_UTM}')"


def sql_transform_to_wgs84(geometry_expr: str, source_crs: str = DANISH_UTM) -> str:
    """
    Generate SQL to transform geometry to WGS84 for storage.

    Args:
        geometry_expr: SQL expression for geometry
        source_crs: Source CRS (default Danish UTM)

    Returns:
        SQL expression for transformed geometry
    """
    return f"ST_Transform({geometry_expr}, '{source_crs}', '{WGS84}')"


def sql_buffer_meters(geometry_expr: str, meters: float, source_crs: str = WGS84) -> str:
    """
    Generate SQL for buffer operation in meters.

    Transforms to UTM, applies buffer, transforms back.

    Args:
        geometry_expr: SQL expression for geometry (in source_crs)
        meters: Buffer distance in meters
        source_crs: CRS of input geometry

    Returns:
        SQL expression for buffered geometry (in source_crs)

    Example:
        >>> sql_buffer_meters("geometry", 100)
        "ST_Transform(ST_Buffer(ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832'), 100), 'EPSG:25832', 'EPSG:4326')"
    """
    if source_crs == DANISH_UTM:
        # Already in UTM, just buffer
        return f"ST_Buffer({geometry_expr}, {meters})"
    # Transform to UTM, buffer, transform back
    return (
        f"ST_Transform("
        f"ST_Buffer(ST_Transform({geometry_expr}, '{source_crs}', '{DANISH_UTM}'), {meters}), "
        f"'{DANISH_UTM}', '{source_crs}')"
    )


def sql_intersects_with_buffer_meters(geom1_expr: str, geom2_expr: str, meters: float, source_crs: str = WGS84) -> str:
    """
    Generate SQL for ST_Intersects with a metric buffer.

    Args:
        geom1_expr: SQL expression for first geometry
        geom2_expr: SQL expression for geometry to buffer
        meters: Buffer distance in meters
        source_crs: CRS of input geometries

    Returns:
        SQL expression for intersection test
    """
    if source_crs == DANISH_UTM:
        return f"ST_Intersects({geom1_expr}, ST_Buffer({geom2_expr}, {meters}))"
    # Transform both to UTM for the operation
    geom1_utm = f"ST_Transform({geom1_expr}, '{source_crs}', '{DANISH_UTM}')"
    geom2_utm = f"ST_Transform({geom2_expr}, '{source_crs}', '{DANISH_UTM}')"
    return f"ST_Intersects({geom1_utm}, ST_Buffer({geom2_utm}, {meters}))"


def sql_transform_for_supabase(geometry_expr: str, source_crs: str = DANISH_UTM) -> str:
    """
    Generate SQL to transform geometry for final Supabase upload.

    This should be the ONLY place where data is transformed to WGS84.
    Use at the final upload step, not during processing.

    Args:
        geometry_expr: SQL expression for geometry (should be in EPSG:25832)
        source_crs: Source CRS (default Danish UTM - the processing standard)

    Returns:
        SQL expression for geometry in EPSG:4326 (Supabase storage format)

    Example:
        >>> sql_transform_for_supabase("geometry")
        "ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')"
    """
    if source_crs == WGS84:
        # Already in WGS84, no transform needed
        return geometry_expr
    return f"ST_Transform({geometry_expr}, '{source_crs}', '{WGS84}')"


def sql_transform_to_processing_crs(geometry_expr: str, source_crs: str) -> str:
    """
    Generate SQL to transform geometry to processing CRS (EPSG:25832).

    Use this in Silver layer to normalize all data to EPSG:25832.
    Only needed for sources that don't provide data in UTM (e.g., DAGI, H3).

    Args:
        geometry_expr: SQL expression for geometry
        source_crs: Source CRS of the data

    Returns:
        SQL expression for geometry in EPSG:25832 (processing standard)

    Example:
        >>> sql_transform_to_processing_crs("geometry", "EPSG:4326")
        "ST_Transform(geometry, 'EPSG:4326', 'EPSG:25832')"
    """
    if source_crs == DANISH_UTM:
        # Already in processing CRS, no transform needed
        return geometry_expr
    return f"ST_Transform({geometry_expr}, '{source_crs}', '{DANISH_UTM}')"
