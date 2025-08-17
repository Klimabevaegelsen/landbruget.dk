"""
Coordinate Order Verification Tests

These tests ensure that all geometries in the pipeline maintain consistent
coordinate order (LON, LAT) as expected by our ST_FlipCoordinates wrappers
around ST_Area_Spheroid functions.

This is critical for accurate area calculations and spatial operations.
"""

import pytest
import duckdb
from pathlib import Path
from typing import List, Tuple, Optional
import tempfile
import os


class CoordinateOrderVerifier:
    """Utility class to verify coordinate order in geometry data."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        
    def verify_coordinate_order(
        self, 
        table_name: str, 
        geometry_column: str = "geometry",
        sample_size: int = 10
    ) -> Tuple[bool, str, List[Tuple[float, float]]]:
        """
        Verify coordinate order in a geometry table.
        
        Returns:
            - bool: True if coordinates are in LON/LAT order
            - str: Status message
            - List[Tuple[float, float]]: Sample coordinate pairs
        """
        try:
            # Get sample WKT centroids
            sample_wkt = self.conn.execute(f"""
                SELECT 
                    ST_AsText(ST_Centroid({geometry_column})) as wkt_centroid
                FROM {table_name} 
                WHERE {geometry_column} IS NOT NULL 
                LIMIT {sample_size}
            """).fetchall()
            
            if not sample_wkt:
                return False, "No geometries found", []
            
            coord_pairs = []
            for wkt, in sample_wkt:
                if wkt and 'POINT(' in wkt:
                    coords_str = wkt.replace('POINT(', '').replace(')', '')
                    try:
                        first_val, second_val = map(float, coords_str.split())
                        coord_pairs.append((first_val, second_val))
                    except Exception:
                        continue
            
            if not coord_pairs:
                return False, "Could not parse coordinate pairs", []
            
            # Analyze coordinate ranges
            first_vals = [pair[0] for pair in coord_pairs]
            second_vals = [pair[1] for pair in coord_pairs]
            first_range = (min(first_vals), max(first_vals))
            second_range = (min(second_vals), max(second_vals))
            
            # Check if first values are longitude (8-15°) and second are latitude (54-58°)
            is_lon_lat = (8 <= first_range[0] <= 15 and 8 <= first_range[1] <= 15 and 
                         54 <= second_range[0] <= 58 and 54 <= second_range[1] <= 58)
            
            # Check if first values are latitude (54-58°) and second are longitude (8-15°)
            is_lat_lon = (54 <= first_range[0] <= 58 and 54 <= first_range[1] <= 58 and 
                         8 <= second_range[0] <= 15 and 8 <= second_range[1] <= 15)
            
            if is_lon_lat:
                status = f"✅ CONFIRMED: (LON, LAT) order - ({first_range[0]:.2f}-{first_range[1]:.2f}, {second_range[0]:.2f}-{second_range[1]:.2f})"
                return True, status, coord_pairs
            elif is_lat_lon:
                status = f"❌ INCORRECT: (LAT, LON) order - ({first_range[0]:.2f}-{first_range[1]:.2f}, {second_range[0]:.2f}-{second_range[1]:.2f})"
                return False, status, coord_pairs
            else:
                status = f"❓ UNCLEAR: Coordinate order unclear - ({first_range[0]:.2f}-{first_range[1]:.2f}, {second_range[0]:.2f}-{second_range[1]:.2f})"
                return False, status, coord_pairs
                
        except Exception as e:
            return False, f"Error verifying coordinates: {e}", []


@pytest.fixture
def duck_conn():
    """Create a DuckDB connection with spatial extension."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    return conn


@pytest.fixture
def sample_geometry_data(duck_conn):
    """Create sample geometry data in correct (LON, LAT) format for testing."""
    duck_conn.execute("""
        CREATE TABLE test_geometries AS
        SELECT 
            'denmark_point_1' as name,
            ST_GeomFromText('POINT(12.5681 55.6761)') as geometry  -- Copenhagen (LON, LAT)
        UNION ALL
        SELECT 
            'denmark_point_2' as name,
            ST_GeomFromText('POINT(10.2039 56.1629)') as geometry  -- Aarhus (LON, LAT)
        UNION ALL
        SELECT 
            'denmark_point_3' as name,
            ST_GeomFromText('POINT(9.9187 57.0488)') as geometry   -- Aalborg (LON, LAT)
    """)
    return duck_conn


@pytest.fixture 
def incorrect_geometry_data(duck_conn):
    """Create sample geometry data in incorrect (LAT, LON) format for testing."""
    duck_conn.execute("""
        CREATE TABLE test_geometries_wrong AS
        SELECT 
            'denmark_point_1' as name,
            ST_GeomFromText('POINT(55.6761 12.5681)') as geometry  -- Copenhagen (LAT, LON) - WRONG!
        UNION ALL
        SELECT 
            'denmark_point_2' as name,
            ST_GeomFromText('POINT(56.1629 10.2039)') as geometry  -- Aarhus (LAT, LON) - WRONG!
        UNION ALL
        SELECT 
            'denmark_point_3' as name,
            ST_GeomFromText('POINT(57.0488 9.9187)') as geometry   -- Aalborg (LAT, LON) - WRONG!
    """)
    return duck_conn


class TestCoordinateOrderVerification:
    """Test suite for coordinate order verification logic."""
    
    def test_coordinate_detection_algorithm(self, duck_conn):
        """Test the core coordinate order detection algorithm."""
        # Test with known LON/LAT coordinates
        duck_conn.execute("""
            CREATE TABLE test_lon_lat AS
            SELECT ST_GeomFromText('POINT(12.5 55.6)') as geometry  -- Copenhagen: LON, LAT
            UNION ALL
            SELECT ST_GeomFromText('POINT(10.2 56.1)') as geometry  -- Aarhus: LON, LAT
        """)
        
        verifier = CoordinateOrderVerifier(duck_conn)
        is_correct, status, coords = verifier.verify_coordinate_order("test_lon_lat")
        
        # Test the detection logic
        assert is_correct == True, f"Algorithm should detect LON/LAT as correct: {status}"
        assert "✅ CONFIRMED" in status
        
        # Verify the algorithm correctly identified the coordinate ranges
        first_vals = [pair[0] for pair in coords]  # Should be longitudes
        second_vals = [pair[1] for pair in coords]  # Should be latitudes
        
        assert all(8 <= val <= 15 for val in first_vals), "First values should be in longitude range"
        assert all(54 <= val <= 58 for val in second_vals), "Second values should be in latitude range"
    
    def test_incorrect_lat_lon_order(self, incorrect_geometry_data):
        """Test that incorrect (LAT, LON) order is detected."""
        verifier = CoordinateOrderVerifier(incorrect_geometry_data)
        is_correct, status, coords = verifier.verify_coordinate_order("test_geometries_wrong")
        
        assert not is_correct, f"Expected LAT/LON order to be detected as incorrect: {status}"
        assert "❌ INCORRECT" in status
        assert len(coords) == 3
        
        # Check that first values are latitude (54-58°) and second are longitude (8-15°)  
        for lat, lon in coords:
            assert 54 <= lat <= 58, f"First value {lat} should be latitude in range 54-58°"
            assert 8 <= lon <= 15, f"Second value {lon} should be longitude in range 8-15°"
    
    def test_empty_table(self, duck_conn):
        """Test handling of empty geometry table."""
        duck_conn.execute("CREATE TABLE empty_geometries (name TEXT, geometry GEOMETRY)")
        verifier = CoordinateOrderVerifier(duck_conn)
        is_correct, status, coords = verifier.verify_coordinate_order("empty_geometries")
        
        assert not is_correct
        assert "No geometries found" in status
        assert len(coords) == 0
    
    def test_null_geometries(self, duck_conn):
        """Test handling of table with only null geometries."""
        duck_conn.execute("""
            CREATE TABLE null_geometries AS
            SELECT 'test' as name, NULL::GEOMETRY as geometry
        """)
        verifier = CoordinateOrderVerifier(duck_conn)
        is_correct, status, coords = verifier.verify_coordinate_order("null_geometries")
        
        assert not is_correct
        assert "No geometries found" in status
        assert len(coords) == 0


class TestPipelineCoordinateConsistency:
    """Integration tests to verify coordinate consistency across pipeline components."""
    
    def test_geometry_validator_produces_correct_order(self, duck_conn):
        """Test that geometry_validator.py produces geometries in correct (LON, LAT) order."""
        # This would be a more complex integration test that:
        # 1. Simulates geometry_validator processing
        # 2. Verifies output coordinate order
        # For now, we'll create a simple test with expected behavior
        
        # Simulate UTM to WGS84 transformation (common in geometry_validator)
        duck_conn.execute("""
            CREATE TABLE utm_test AS
            SELECT 
                ST_GeomFromText('POINT(725369 6176652)', 25832) as geometry_utm  -- UTM Zone 32N
        """)
        
        # Transform to WGS84 (this should produce LON, LAT order)
        duck_conn.execute("""
            CREATE TABLE wgs84_test AS
            SELECT 
                ST_Transform(geometry_utm, 'EPSG:25832', 'EPSG:4326') as geometry
            FROM utm_test
        """)
        
        verifier = CoordinateOrderVerifier(duck_conn)
        is_correct, status, coords = verifier.verify_coordinate_order("wgs84_test")
        
        assert is_correct, f"Transformed geometry should be in LON/LAT order: {status}"
        assert len(coords) == 1
        
        # Verify the transformed point is reasonable for Denmark
        lon, lat = coords[0]
        assert 8 <= lon <= 15, f"Transformed longitude {lon} should be in Denmark range"
        assert 54 <= lat <= 58, f"Transformed latitude {lat} should be in Denmark range"
    
    def test_st_area_spheroid_with_flip_coordinates(self, sample_geometry_data):
        """Test that ST_Area_Spheroid with ST_FlipCoordinates produces reasonable results."""
        # Create a small polygon for area testing
        sample_geometry_data.execute("""
            CREATE TABLE test_polygon AS
            SELECT 
                ST_GeomFromText('POLYGON((12.5 55.6, 12.6 55.6, 12.6 55.7, 12.5 55.7, 12.5 55.6))') as geometry
        """)
        
        # Test area calculation with and without flip
        result = sample_geometry_data.execute("""
            SELECT 
                ST_Area_Spheroid(geometry) as area_direct,
                ST_Area_Spheroid(ST_FlipCoordinates(geometry)) as area_flipped
            FROM test_polygon
        """).fetchone()
        
        area_direct, area_flipped = result
        
        # The flipped version should give a reasonable area for a small polygon in Denmark
        # (approximately 0.1° x 0.1° should be roughly 100-200 km²)
        assert area_flipped > 0, "Area should be positive"
        assert 50_000_000 < area_flipped < 500_000_000, f"Area {area_flipped} m² should be reasonable for small Denmark polygon"
        
        # The direct calculation (without flip) might be incorrect due to coordinate order
        # We don't assert specific values here since the behavior depends on DuckDB version


@pytest.mark.integration
class TestSampleDatasetCoordinates:
    """Integration tests using realistic sample data that mimics production datasets."""
    
    def test_all_sample_datasets_coordinate_order(self, duck_conn):
        """Test coordinate order verification across all sample datasets."""
        # Import here to avoid import issues in CI
        from tests.fixtures.sample_geometries import setup_all_sample_datasets, get_expected_coordinate_orders
        
        # Create all sample datasets
        dataset_tables = setup_all_sample_datasets(duck_conn)
        expected_results = get_expected_coordinate_orders()
        
        verifier = CoordinateOrderVerifier(duck_conn)
        
        for dataset_name, table_name in dataset_tables.items():
            is_correct, status, coords = verifier.verify_coordinate_order(table_name)
            expected_correct = expected_results[dataset_name]
            
            assert is_correct == expected_correct, (
                f"Dataset '{dataset_name}' coordinate order verification failed.\n"
                f"Expected: {'correct' if expected_correct else 'incorrect'}\n"
                f"Got: {'correct' if is_correct else 'incorrect'}\n"
                f"Status: {status}"
            )
            
            # Verify we got some coordinate pairs
            assert len(coords) > 0, f"No coordinates found for {dataset_name}"
            
            # For correct datasets, verify coordinates are in Denmark bounds
            if expected_correct:
                for lon, lat in coords:
                    assert 8 <= lon <= 15, f"Longitude {lon} out of Denmark range for {dataset_name}"
                    assert 54 <= lat <= 58, f"Latitude {lat} out of Denmark range for {dataset_name}"
    
    def test_wrong_coordinates_detection(self, duck_conn):
        """Test that datasets with wrong coordinate order are properly detected."""
        from tests.fixtures.sample_geometries import create_wrong_coordinate_samples
        
        create_wrong_coordinate_samples(duck_conn)
        verifier = CoordinateOrderVerifier(duck_conn)
        
        is_correct, status, coords = verifier.verify_coordinate_order("wrong_coordinates_sample")
        
        assert not is_correct, f"Wrong coordinates should be detected as incorrect: {status}"
        assert "❌ INCORRECT" in status or "❓ UNCLEAR" in status
        assert len(coords) > 0


def test_coordinate_verification_utility():
    """Test the coordinate verification utility itself."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")
    
    # Test with known good data
    conn.execute("""
        CREATE TABLE good_coords AS
        SELECT ST_GeomFromText('POINT(12.5 55.6)') as geometry  -- LON, LAT
    """)
    
    verifier = CoordinateOrderVerifier(conn)
    is_correct, status, coords = verifier.verify_coordinate_order("good_coords")
    
    assert is_correct
    assert len(coords) == 1
    assert 12.4 < coords[0][0] < 12.6  # Longitude
    assert 55.5 < coords[0][1] < 55.7  # Latitude


if __name__ == "__main__":
    # Allow running this test file directly for debugging
    pytest.main([__file__, "-v"])
