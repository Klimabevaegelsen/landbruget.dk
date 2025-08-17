"""
Sample geometry data for testing coordinate order verification.

This module provides realistic sample data that mimics the structure
of actual pipeline datasets without requiring access to production data.
"""

import duckdb
from typing import Dict, List


def create_sample_fvm_marker_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample FVM marker data with realistic Danish field geometries."""
    conn.execute("""
        CREATE TABLE fvm_marker_sample AS
        SELECT 
            'DK001' as field_id,
            'CVR12345678' as cvr_number,
            'BLOCK001' as block_id,
            ST_GeomFromText('POLYGON((12.0 55.0, 12.1 55.0, 12.1 55.1, 12.0 55.1, 12.0 55.0))') as geom,
            2024 as year
        UNION ALL
        SELECT 
            'DK002' as field_id,
            'CVR12345678' as cvr_number, 
            'BLOCK002' as block_id,
            ST_GeomFromText('POLYGON((10.5 56.2, 10.6 56.2, 10.6 56.3, 10.5 56.3, 10.5 56.2))') as geom,
            2024 as year
        UNION ALL
        SELECT
            'DK003' as field_id,
            'CVR87654321' as cvr_number,
            'BLOCK003' as block_id, 
            ST_GeomFromText('POLYGON((11.8 55.8, 11.9 55.8, 11.9 55.9, 11.8 55.9, 11.8 55.8))') as geom,
            2024 as year
    """)


def create_sample_bnbo_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample BNBO status data."""
    conn.execute("""
        CREATE TABLE bnbo_status_sample AS
        SELECT 
            'Natura2000' as status_category,
            ST_GeomFromText('MULTIPOLYGON(((12.2 55.2, 12.3 55.2, 12.3 55.3, 12.2 55.3, 12.2 55.2)))') as geometry
        UNION ALL
        SELECT
            'Natura2000' as status_category,
            ST_GeomFromText('MULTIPOLYGON(((10.7 56.4, 10.8 56.4, 10.8 56.5, 10.7 56.5, 10.7 56.4)))') as geometry
        UNION ALL
        SELECT
            'Protected' as status_category,
            ST_GeomFromText('MULTIPOLYGON(((11.5 55.5, 11.6 55.5, 11.6 55.6, 11.5 55.6, 11.5 55.5)))') as geometry
    """)


def create_sample_wetlands_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample wetlands data."""
    conn.execute("""
        CREATE TABLE wetlands_sample AS
        SELECT
            'WETLAND_001' as id,
            'Marsh' as wetland_type,
            ST_GeomFromText('POLYGON((12.4 55.4, 12.5 55.4, 12.5 55.5, 12.4 55.5, 12.4 55.4))') as geometry
        UNION ALL
        SELECT
            'WETLAND_002' as id,
            'Bog' as wetland_type,
            ST_GeomFromText('POLYGON((10.9 56.6, 11.0 56.6, 11.0 56.7, 10.9 56.7, 10.9 56.6))') as geometry
    """)


def create_sample_soil_types_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample soil types data."""
    conn.execute("""
        CREATE TABLE soil_types_sample AS
        SELECT
            'CLAY' as soil_type,
            'Heavy Clay' as description,
            ST_GeomFromText('POLYGON((12.6 55.6, 12.7 55.6, 12.7 55.7, 12.6 55.7, 12.6 55.6))') as geometry
        UNION ALL
        SELECT
            'SAND' as soil_type,
            'Sandy Loam' as description,
            ST_GeomFromText('POLYGON((11.1 56.8, 11.2 56.8, 11.2 56.9, 11.1 56.9, 11.1 56.8))') as geometry
    """)


def create_sample_properties_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample cadastral properties data.""" 
    conn.execute("""
        CREATE TABLE properties_sample AS
        SELECT
            'BFE123456' as bfe_number,
            'Property A' as property_name,
            ST_GeomFromText('POLYGON((12.8 55.8, 12.9 55.8, 12.9 55.9, 12.8 55.9, 12.8 55.8))') as geometry
        UNION ALL
        SELECT
            'BFE789012' as bfe_number,
            'Property B' as property_name,
            ST_GeomFromText('POLYGON((11.3 57.0, 11.4 57.0, 11.4 57.1, 11.3 57.1, 11.3 57.0))') as geometry
    """)


def create_sample_water_projects_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample water projects data."""
    conn.execute("""
        CREATE TABLE water_projects_sample AS
        SELECT
            'WP001' as project_id,
            'Stream Restoration' as project_type,
            ST_GeomFromText('POLYGON((13.0 56.0, 13.1 56.0, 13.1 56.1, 13.0 56.1, 13.0 56.0))') as geometry
        UNION ALL
        SELECT
            'WP002' as project_id,
            'Wetland Creation' as project_type,
            ST_GeomFromText('POLYGON((11.5 57.2, 11.6 57.2, 11.6 57.3, 11.5 57.3, 11.5 57.2))') as geometry
    """)


# Sample data with WRONG coordinate order for testing detection
def create_wrong_coordinate_samples(conn: duckdb.DuckDBPyConnection) -> None:
    """Create sample data with incorrect (LAT, LON) order for testing."""
    conn.execute("""
        CREATE TABLE wrong_coordinates_sample AS
        SELECT
            'WRONG_001' as id,
            ST_GeomFromText('POINT(55.6761 12.5681)') as geometry  -- LAT, LON - WRONG!
        UNION ALL
        SELECT
            'WRONG_002' as id,
            ST_GeomFromText('POINT(56.1629 10.2039)') as geometry  -- LAT, LON - WRONG!
        UNION ALL
        SELECT
            'WRONG_003' as id,
            ST_GeomFromText('POINT(57.0488 9.9187)') as geometry   -- LAT, LON - WRONG!
    """)


# Utility function to create all sample datasets
def setup_all_sample_datasets(conn: duckdb.DuckDBPyConnection) -> Dict[str, str]:
    """
    Create all sample datasets for testing.
    
    Returns:
        Dict mapping dataset names to table names
    """
    # Ensure spatial extension is loaded
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    
    # Create all sample datasets
    create_sample_fvm_marker_data(conn)
    create_sample_bnbo_data(conn)
    create_sample_wetlands_data(conn)
    create_sample_soil_types_data(conn)
    create_sample_properties_data(conn)
    create_sample_water_projects_data(conn)
    create_wrong_coordinate_samples(conn)
    
    return {
        "FVM Marker Fields": "fvm_marker_sample",
        "BNBO Status": "bnbo_status_sample", 
        "Wetlands": "wetlands_sample",
        "Soil Types": "soil_types_sample",
        "Properties": "properties_sample",
        "Water Projects": "water_projects_sample",
        "Wrong Coordinates": "wrong_coordinates_sample"
    }


def get_expected_coordinate_orders() -> Dict[str, bool]:
    """
    Return expected coordinate order results for sample datasets.
    
    Returns:
        Dict mapping dataset names to expected is_correct_order boolean
    """
    return {
        "FVM Marker Fields": True,   # (LON, LAT) - correct
        "BNBO Status": True,         # (LON, LAT) - correct
        "Wetlands": True,            # (LON, LAT) - correct
        "Soil Types": True,          # (LON, LAT) - correct
        "Properties": True,          # (LON, LAT) - correct
        "Water Projects": True,      # (LON, LAT) - correct
        "Wrong Coordinates": False   # (LAT, LON) - incorrect
    }
