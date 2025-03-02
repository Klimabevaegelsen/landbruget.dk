import duckdb
import tempfile
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def geo_validate_geometries(data, *args, **kwargs):
    """
    Global geometry validator for spatial features in GeoParquet format.
    This is a standardized validator that all geospatial pipelines should use.
    
    Requirements:
    - Input must be a GeoParquet file with a 'geometry' column in WKB format
    - All geometries must be in EPSG:4326 (WGS84)
    - Input path must be provided either as 'input_path' or 'formatted_data_path'
    
    Validation and Processing Steps:
    1. Validates WKB parsing and basic geometry validity
    2. Repairs invalid geometries where possible using ST_MakeValid
    3. Simplifies geometries while preserving topology
    4. Removes duplicate vertices
    5. Creates spatial index hints (bbox, centroid)
    6. Validates against minimum quality standards
    
    Args:
        data: Dictionary containing:
            Required:
                - input_path: Path to input GeoParquet file
                OR
                - formatted_data_path: Alternative path format
            Optional:
                - metadata: Dictionary containing validation rules:
                    - min_points: Minimum points per geometry (default: 4)
                    - min_area_m2: Minimum area in m² (default: 1.0)
                    - max_area_m2: Maximum area in m² (default: 10000000.0)
    
    Returns:
        Dictionary containing:
            - validated_data_path: Path to output GeoParquet with validated geometries
            - validated_count: Number of valid features
            - original_feature_count: Number of input features
            - metadata: Processing statistics and validation results
    
    Raises:
        ValueError: If input requirements are not met
    """
    
    # 1. Input Validation
    input_path = data.get('input_path') or data.get('formatted_data_path')
    if not input_path:
        raise ValueError("No input path provided. Must provide 'input_path' or 'formatted_data_path'")
    
    if not input_path.endswith('.parquet'):
        raise ValueError("Input must be a GeoParquet file")
    
    # Get validation rules with strict defaults
    validation_rules = data.get('metadata', {}).get('validation_rules', {})
    min_points = validation_rules.get('min_points', 4)
    min_area_m2 = validation_rules.get('min_area_m2', 1.0)
    max_area_m2 = validation_rules.get('max_area_m2', 100000000.0)
    
    # Create temporary output file
    fd, output_path = tempfile.mkstemp(suffix='.parquet')
    os.close(fd)
    
    # Initialize DuckDB with spatial extension
    con = duckdb.connect()
    con.install_extension('spatial')
    con.load_extension('spatial')
    
    try:
        # 2. Verify Input Data Structure
        try:
            # Check if we can read the file and it has a geometry column
            test_read = con.execute(f"""
                SELECT name, type
                FROM parquet_schema('{input_path}')
                WHERE name = 'geometry';
            """).fetchone()
            
            if not test_read:
                raise ValueError("Input file must have a 'geometry' column")
            
        except Exception as e:
            raise ValueError(f"Invalid input file: {str(e)}")
        
        # 3. Get Initial Statistics
        input_stats = con.execute(f"""
            SELECT 
                COUNT(*) as total_features,
                COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END) as non_null_geoms,
                COUNT(CASE WHEN ST_GeomFromWKB(geometry) IS NOT NULL THEN 1 END) as valid_wkb
            FROM read_parquet('{input_path}');
        """).fetchone()
        
        feature_count = input_stats[0]
        if feature_count == 0:
            raise ValueError("Input file contains no features")
        
        print("\n📊 Input Statistics:")
        print(f"  • Total features: {input_stats[0]:,}")
        print(f"  • Non-null geometries: {input_stats[1]:,}")
        print(f"  • Valid WKB geometries: {input_stats[2]:,}")
        
        # 4. Process and Validate Geometries
        con.execute(f"""
            CREATE TEMP TABLE valid_features AS
            WITH 
            -- Stage 1: Parse WKB and validate basic geometry
            parsed_geoms AS (
                SELECT 
                    *,
                    ST_GeomFromWKB(geometry) as geom
                FROM read_parquet('{input_path}')
                WHERE geometry IS NOT NULL
                  AND ST_GeomFromWKB(geometry) IS NOT NULL
            ),
            -- Stage 2: Fix invalid geometries and simplify
            cleaned_geoms AS (
                SELECT 
                    * EXCLUDE (geometry, geom),
                    CASE 
                        WHEN ST_IsValid(geom) THEN geom
                        ELSE ST_MakeValid(geom)
                    END as clean_geom
                FROM parsed_geoms
                WHERE geom IS NOT NULL
            ),
            -- Stage 3: Optimize and add spatial hints
            optimized_geoms AS (
                SELECT 
                    * EXCLUDE (clean_geom),
                    ST_RemoveRepeatedPoints(
                        ST_SimplifyPreserveTopology(clean_geom, 0.000001)
                    ) as geometry,
                    -- Add spatial indexing hints
                    ST_Envelope(clean_geom) as bbox,
                    ST_X(ST_Centroid(clean_geom)) as center_x,
                    ST_Y(ST_Centroid(clean_geom)) as center_y,
                    -- Validation metrics
                    ST_NPoints(clean_geom) as num_points,
                    -- Calculate area in UTM zone 32N for accurate measurements
                    ST_Area(ST_Transform(clean_geom, 'EPSG:4326', 'EPSG:25832')) as area_m2
                FROM cleaned_geoms
                WHERE clean_geom IS NOT NULL
            )
            -- Stage 4: Apply validation rules
            SELECT 
                * EXCLUDE (bbox),
                ST_AsWKB(bbox) as bbox
            FROM optimized_geoms
            WHERE num_points >= {min_points}
              AND area_m2 >= {min_area_m2}
              AND area_m2 <= {max_area_m2}
              AND ST_IsValid(geometry)
              AND ST_IsSimple(geometry);
        """)
        
        # 5. Get Validation Statistics
        validation_stats = con.execute("""
            SELECT 
                COUNT(*) as valid_count,
                AVG(num_points) as avg_points,
                MIN(num_points) as min_points,
                MAX(num_points) as max_points,
                AVG(area_m2) as avg_area,
                MIN(area_m2) as min_area,
                MAX(area_m2) as max_area
            FROM valid_features;
        """).fetchone()
        
        # 6. Write Validated Features to Output
        con.execute(f"""
            COPY (
                SELECT 
                    * EXCLUDE (num_points, area_m2),
                    ST_AsWKB(geometry) as geometry
                FROM valid_features
            ) TO '{output_path}' (FORMAT PARQUET);
        """)
        
        # 7. Prepare Output Statistics
        metadata = {
            'validation_rules': {
                'min_points': min_points,
                'min_area_m2': min_area_m2,
                'max_area_m2': max_area_m2
            },
            'stats': {
                'input_features': feature_count,
                'valid_features': int(validation_stats[0]),
                'avg_points': float(validation_stats[1]),
                'min_points': int(validation_stats[2]),
                'max_points': int(validation_stats[3]),
                'avg_area_m2': float(validation_stats[4]),
                'min_area_m2': float(validation_stats[5]),
                'max_area_m2': float(validation_stats[6])
            }
        }
        
        # Print Summary
        print("\n📊 Validation Results:")
        print(f"  • Input features: {feature_count:,}")
        print(f"  • Valid features: {metadata['stats']['valid_features']:,}")
        print(f"  • Success rate: {100.0 * metadata['stats']['valid_features']/feature_count:.1f}%")
        
        print("\n📐 Geometry Statistics:")
        print(f"  • Points per feature: {metadata['stats']['min_points']:,} to {metadata['stats']['max_points']:,}")
        print(f"  • Average points: {metadata['stats']['avg_points']:.1f}")
        print(f"  • Area range: {metadata['stats']['min_area_m2']:.1f} to {metadata['stats']['max_area_m2']:.1f} m²")
        
        return {
            'validated_data_path': output_path,
            'validated_count': metadata['stats']['valid_features'],
            'original_feature_count': feature_count,
            'metadata': metadata
        }
        
    except Exception as e:
        if os.path.exists(output_path):
            os.remove(output_path)
        raise RuntimeError(f"Validation failed: {str(e)}") from e
    finally:
        try:
            con.execute("DROP TABLE IF EXISTS valid_features")
        except Exception:
            pass
        con.close()


@test
def test_output(data):
    """Test the output of the transformer."""
    assert data is not None, 'The output is undefined'
    assert 'validated_data_path' in data, 'Output must contain validated_data_path'
    assert 'validated_count' in data, 'Output must contain validated_count'
    assert 'original_feature_count' in data, 'Output must contain original_feature_count'
    assert 'metadata' in data, 'Output must contain metadata'
    assert os.path.exists(data['validated_data_path']), 'Output file must exist'


if __name__ == "__main__":
    # Test with both input formats
    test_cases = [
        {
            'input_path': 'path/to/test.parquet',  # Traditional format
            'metadata': {
                'validation_rules': {
                    'min_points': 4,
                    'min_area_m2': 1.0,
                    'max_area_m2': 10000000.0
                }
            }
        },
        {
            'formatted_data_path': 'path/to/test.parquet',  # Alternative format from wetlands_format_geojson
            'feature_count': 100,  # Additional metadata that will be preserved
            'metadata': {
                'validation_rules': {
                    'min_points': 4,
                    'min_area_m2': 1.0,
                    'max_area_m2': 10000000.0
                }
            }
        }
    ]
    
    for i, test_input in enumerate(test_cases, 1):
        try:
            print(f"\nTesting input format {i}:")
            result = geo_validate_geometries(test_input)
            print(f"Success - Validated {result['validated_count']} features")
            print(f"Stats: {result['metadata']['stats']}")
        except Exception as e:
            print(f"Error validating geometries: {str(e)}") 