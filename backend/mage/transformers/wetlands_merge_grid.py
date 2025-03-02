import time
import psutil
from typing import Dict, Any, List
import shapely
import geopandas as gpd
import pandas as pd
import os
import tempfile
import libpysal
import numpy as np
from shapely.geometry import box
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import wkb

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def process_chunk(chunk_gdf: gpd.GeoDataFrame, gridcode: int) -> gpd.GeoDataFrame:
    """Process a spatial chunk to merge adjacent polygons with the same gridcode."""
    
    # Filter by gridcode
    filtered_df = chunk_gdf[chunk_gdf['gridcode'] == gridcode].copy()
    
    if len(filtered_df) == 0:
        return None
    
    # Handle CRS - should be EPSG:4326 from upstream
    if filtered_df.crs is None:
        print("⚠️ No CRS found in chunk, setting to EPSG:4326 as per upstream data")
        filtered_df.set_crs(epsg=4326, inplace=True)
    elif filtered_df.crs.to_epsg() != 4326:
        raise ValueError(f"Unexpected CRS EPSG:{filtered_df.crs.to_epsg()} - Pipeline requires EPSG:4326")
    
    # Fix any invalid geometries
    filtered_df['geometry'] = filtered_df.geometry.apply(
        lambda g: shapely.make_valid(g) if g is not None and not shapely.is_valid(g) else g
    )
    
    # Create a temporary ID for tracking components
    filtered_df['temp_id'] = range(len(filtered_df))
    
    # Create spatial weights matrix using Queen contiguity
    W = libpysal.weights.Queen.from_dataframe(filtered_df)
    
    # Get component labels
    components = W.component_labels
    filtered_df['component_id'] = components
    
    # Group by component and dissolve to merge adjacent polygons
    merged_df = filtered_df.dissolve(by='component_id', aggfunc={
        'gridcode': 'first',
        'temp_id': 'count'  # Count will give us the number of merged polygons
    }).reset_index()
    
    # Rename the count column
    merged_df = merged_df.rename(columns={'temp_id': 'merged_count'})
    
    # Ensure all geometries are valid and CRS is maintained
    merged_df['geometry'] = merged_df.geometry.apply(
        lambda g: shapely.make_valid(g) if g is not None else g
    )
    merged_df.set_crs(epsg=4326, inplace=True)
    
    return merged_df


def create_spatial_chunks(gdf: gpd.GeoDataFrame, num_chunks: int = 4) -> List[gpd.GeoDataFrame]:
    """Split a GeoDataFrame into spatial chunks based on a grid."""
    # Remove unused logger
    
    # Get the total bounds of the dataset
    minx, miny, maxx, maxy = gdf.total_bounds
    
    # Calculate the number of chunks in each dimension
    # Use square root to make chunks roughly square
    chunks_per_side = int(np.ceil(np.sqrt(num_chunks)))
    
    # Calculate the size of each chunk
    chunk_width = (maxx - minx) / chunks_per_side
    chunk_height = (maxy - miny) / chunks_per_side
    
    chunks = []
    for i in range(chunks_per_side):
        for j in range(chunks_per_side):
            # Calculate the bounds of this chunk
            chunk_minx = minx + i * chunk_width
            chunk_miny = miny + j * chunk_height
            chunk_maxx = chunk_minx + chunk_width
            chunk_maxy = chunk_miny + chunk_height
            
            # Create a box for this chunk
            chunk_box = box(chunk_minx, chunk_miny, chunk_maxx, chunk_maxy)
            
            # Select features that intersect with this chunk
            chunk_gdf = gdf[gdf.intersects(chunk_box)].copy()
            
            if len(chunk_gdf) > 0:
                chunks.append(chunk_gdf)
    
    return chunks


def write_duckdb_compatible_geoparquet(gdf: gpd.GeoDataFrame, output_path: str) -> None:
    """
    Write a GeoDataFrame to a GeoParquet file in a format compatible with DuckDB's spatial extension.
    DuckDB expects WKB format for geometries. The output will maintain EPSG:4326 CRS.
    """
    print("💾 Converting geometries to WKB format...")
    
    # Convert geometries to WKB format
    df = gdf.copy()
    
    # Ensure CRS is EPSG:4326
    if df.crs is None:
        print("⚠️ No CRS found, setting to EPSG:4326 as per upstream data")
        df.set_crs(epsg=4326, inplace=True)
    elif df.crs.to_epsg() != 4326:
        raise ValueError(f"Unexpected CRS EPSG:{df.crs.to_epsg()} - Pipeline requires EPSG:4326")
    else:
        print("✅ Confirmed CRS is EPSG:4326")
    
    # Count initial features
    initial_count = len(df)
    
    # Remove null geometries
    df = df[df['geometry'].notna()].copy()
    null_geoms = initial_count - len(df)
    if null_geoms > 0:
        print(f"⚠️ Removed {null_geoms} null geometries")
    
    # Validate and fix geometries before WKB conversion
    df['geometry'] = df['geometry'].apply(lambda geom: 
        shapely.make_valid(geom) if geom is not None and not shapely.is_valid(geom) else geom
    )
    
    # Convert to WKB, handling any conversion errors
    def safe_to_wkb(geom):
        try:
            if geom is not None:
                if not shapely.is_valid(geom):
                    return None
                return wkb.dumps(geom)
            return None
        except Exception:
            return None
    
    df['geometry'] = df['geometry'].apply(safe_to_wkb)
    
    # Remove any rows where WKB conversion failed
    valid_mask = df['geometry'].notna()
    invalid_count = (~valid_mask).sum()
    if invalid_count > 0:
        print(f"⚠️ Removed {invalid_count} features with invalid geometries")
        df = df[valid_mask]
    
    print(f"✅ Successfully converted {len(df):,} features to WKB format")
    
    # Convert to PyArrow table
    table = pa.Table.from_pandas(df)
    
    # Add GeoParquet metadata
    metadata = table.schema.metadata
    
    # Add GeoParquet metadata if not already present
    if b'geo' not in metadata:
        geo_metadata = {
            'columns': {
                'geometry': {
                    'encoding': 'WKB',
                    'geometry_type': 'GeometryCollection',  # Generic type to handle mixed geometries
                    'crs': 'EPSG:4326'  # Explicitly set EPSG:4326
                }
            },
            'primary_column': 'geometry',
            'version': '1.0.0-beta.1'
        }
        
        # Convert to JSON and add to metadata
        import json
        metadata_updated = {**{k.decode('utf8'): v.decode('utf8') 
                              for k, v in metadata.items()}, 
                           'geo': json.dumps(geo_metadata)}
        
        # Convert keys back to bytes
        metadata_bytes = {k.encode('utf8'): v.encode('utf8') 
                         for k, v in metadata_updated.items()}
        
        # Create new table with updated metadata
        table = table.replace_schema_metadata(metadata_bytes)
    
    # Write to parquet file
    pq.write_table(table, output_path)
    print(f"💾 Wrote {len(df):,} features to GeoParquet file in DuckDB-compatible format (EPSG:4326)")


@transformer
def merge_grid(data_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge adjacent polygons that share the same peat percentage category (gridcode).
    Processes data in spatial chunks to manage memory usage.
    
    The input data must be in EPSG:25832 (ETRS89 / UTM zone 32N) CRS, which is
    maintained throughout the merging process.
    
    Args:
        data_item: Dictionary containing:
            - input_path: Path to GeoParquet file with features in EPSG:25832
            - metadata: Processing metadata
            - num_chunks: (optional) Number of spatial chunks to process
        
    Returns:
        Dictionary containing:
            - input_path: Path to GeoParquet file with merged features (in EPSG:25832)
            - metadata: Processing metadata and statistics
            - performance_metrics: Detailed metrics about processing efficiency
    """
    if not data_item.get("input_path"):
        raise ValueError("No input path provided for merging")

    print("🚀 Starting polygon merging process")
    
    # Record start time for performance tracking
    start_time = time.time()
    initial_memory_used = psutil.virtual_memory().used / (1024**3)  # GB
    
    input_path = data_item["input_path"]
    num_chunks = data_item.get("num_chunks", 4)  # Default to 4 chunks
    
    # Read the GeoParquet file
    print(f"📥 Reading GeoParquet file: {input_path}")
    gdf = gpd.read_parquet(input_path)
    
    # Debug CRS information
    print("\n🔍 CRS Information:")
    print(f"  • Raw CRS object: {gdf.crs}")
    if gdf.crs is not None:
        print(f"  • CRS type: {type(gdf.crs)}")
        if hasattr(gdf.crs, 'to_epsg'):
            print(f"  • EPSG code: {gdf.crs.to_epsg()}")
    
    # Handle CRS - should be EPSG:4326 from upstream
    if gdf.crs is None:
        print("⚠️ No CRS found, setting to EPSG:4326 as per upstream data")
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        print(f"❌ ERROR: Found unexpected CRS EPSG:{gdf.crs.to_epsg()}")
        print("   Pipeline requires EPSG:4326 throughout")
        print("   Please check the upstream transformers")
        raise ValueError(f"Unexpected CRS EPSG:{gdf.crs.to_epsg()} - Pipeline requires EPSG:4326")
    else:
        print("✅ Confirmed CRS is EPSG:4326")
    
    print(f"\n📊 Total features read: {len(gdf)}")
    
    # Initialize results
    all_merged_dfs = []
    merge_stats = {12: {'original_features': 0, 'merged_features': 0},
                  60: {'original_features': 0, 'merged_features': 0}}
    
    # Set original counts
    total_original_features = 0
    for code in [12, 60]:
        count = len(gdf[gdf['gridcode'] == code])
        merge_stats[code]['original_features'] = count
        total_original_features += count
        print(f"📈 Found {count} features with gridcode {code}")
    
    # Create spatial chunks to process
    print(f"🔄 Creating {num_chunks} spatial chunks for processing")
    chunks = create_spatial_chunks(gdf, num_chunks)
    print(f"✅ Created {len(chunks)} spatial chunks")
    
    # Process each gridcode (only 12 and 60)
    for code in [12, 60]:
        if merge_stats[code]['original_features'] == 0:
            print(f"⚠️ No features found for gridcode {code}")
            continue
        
        print(f"\n🔍 Processing gridcode {code} with {merge_stats[code]['original_features']} features")
        
        # Process each chunk
        chunk_results = []
        for i, chunk in enumerate(chunks):
            print(f"  ⚙️ Processing chunk {i+1}/{len(chunks)} for gridcode {code}")
            
            # Check memory usage
            mem_used = psutil.virtual_memory().percent
            print(f"Memory usage before processing chunk: {mem_used}%")
            
            # Process the chunk
            result = process_chunk(chunk, code)
            
            if result is not None and len(result) > 0:
                chunk_results.append(result)
                print(f"  ✨ Chunk {i+1} produced {len(result)} merged features")
            
            # Force garbage collection
            import gc
            gc.collect()
        
        # Combine chunk results
        if chunk_results:
            merged_df = pd.concat(chunk_results, ignore_index=True)
            
            # Now we need to merge features that might span chunk boundaries
            # Create a temporary ID for tracking components
            merged_df['temp_id'] = range(len(merged_df))
            
            # Create spatial weights matrix using Queen contiguity
            try:
                W = libpysal.weights.Queen.from_dataframe(merged_df)
                
                # Get component labels
                components = W.component_labels
                merged_df['component_id'] = components
                
                # Group by component and dissolve to merge adjacent polygons
                final_merged = merged_df.dissolve(by='component_id', aggfunc={
                    'gridcode': 'first',
                    'merged_count': 'sum'  # Sum the merged counts from chunks
                }).reset_index()
                
                print(f"  🎯 Final merge for gridcode {code}: {len(merged_df)} features into {len(final_merged)}")
                
                # Store the processed results
                all_merged_dfs.append(final_merged)
                merge_stats[code]['merged_features'] = len(final_merged)
            except Exception as e:
                print(f"  ⚠️ Error in final merge for gridcode {code}: {str(e)}")
                # If final merge fails, just use the chunk results
                all_merged_dfs.append(merged_df)
                merge_stats[code]['merged_features'] = len(merged_df)
    
    # Create a temporary output file for the next step
    fd, output_path = tempfile.mkstemp(suffix='.parquet')
    os.close(fd)
    
    # Combine all results and write to GeoParquet
    print(f"\n💾 Writing merged results to GeoParquet file: {output_path}")
    
    # Combine all the merged dataframes
    if all_merged_dfs:
        combined_gdf = pd.concat(all_merged_dfs, ignore_index=True)
        
        # Write in DuckDB-compatible format
        write_duckdb_compatible_geoparquet(combined_gdf, output_path)
        print(f"✅ Successfully wrote {len(combined_gdf)} features to GeoParquet")
    else:
        # Create an empty file with correct structure
        empty_gdf = gpd.GeoDataFrame(
            columns=['gridcode', 'merged_count', 'component_id', 'geometry'],
            geometry='geometry',
            crs=gdf.crs
        )
        # Write in DuckDB-compatible format
        write_duckdb_compatible_geoparquet(empty_gdf, output_path)
        print("⚠️ No features to write, created empty GeoParquet file")
    
    # Calculate final performance metrics
    end_time = time.time()
    total_processing_time = end_time - start_time
    final_memory_used = psutil.virtual_memory().used / (1024**3)
    
    # Calculate total merged features
    total_merged_features = sum(merge_stats[code]['merged_features'] for code in merge_stats)
    
    # Calculate efficiency metrics
    reduction_percent = 0
    compression_ratio = 1
    if total_original_features > 0 and total_merged_features > 0:
        reduction_percent = round(100.0 * (total_original_features - total_merged_features) / total_original_features, 1)
        compression_ratio = round(total_original_features / total_merged_features, 1)
    
    # Calculate processing speed
    features_per_second = 0
    if total_processing_time > 0:
        features_per_second = round(total_original_features / total_processing_time, 1)
    
    # Print final summary
    print("\n📊 Final Summary:")
    print(f"  • Processing time: {round(total_processing_time, 1)} seconds")
    print(f"  • Features/second: {features_per_second}")
    print(f"  • Memory delta: {round(final_memory_used - initial_memory_used, 2)} GB")
    print(f"  • Reduction: {reduction_percent}%")
    print(f"  • Compression ratio: {compression_ratio}:1")
    print("\n🔍 Results by Gridcode:")
    for gridcode, stats in merge_stats.items():
        print(f"  Gridcode {gridcode}:")
        print(f"    - Original: {stats['original_features']}")
        print(f"    - Merged: {stats['merged_features']}")
        if stats['original_features'] > 0:
            reduction = round(100.0 * (stats['original_features'] - stats['merged_features']) / stats['original_features'], 1)
            print(f"    - Reduction: {reduction}%")
    
    # Detailed performance metrics
    performance_metrics = {
        "time_metrics": {
            "total_seconds": round(total_processing_time, 2),
            "features_per_second": features_per_second
        },
        "memory_metrics": {
            "initial_used_gb": round(initial_memory_used, 2),
            "final_used_gb": round(final_memory_used, 2),
            "memory_delta_gb": round(final_memory_used - initial_memory_used, 2),
            "peak_memory_percent": round(psutil.virtual_memory().percent, 1)
        },
        "efficiency_metrics": {
            "original_features": total_original_features,
            "merged_features": total_merged_features,
            "reduction_percent": reduction_percent,
            "compression_ratio": compression_ratio
        },
        "processing": {
            "num_chunks": len(chunks)
        }
    }
    
    # Update metadata
    metadata = data_item.get("metadata", {})
    metadata["merged_features_count"] = total_merged_features
    metadata["merge_stats"] = merge_stats
    metadata["processing_time_seconds"] = round(total_processing_time, 1)
    
    return {
        "input_path": output_path,
        "metadata": metadata,
        "performance_metrics": performance_metrics
    }


@test
def test_output(output, *args) -> None:
    """Test the output of the transformer."""
    assert output is not None, 'The output is undefined'
    assert 'input_path' in output, 'Output should contain input_path with path to GeoParquet file'
    assert 'metadata' in output, 'Output should contain metadata'
    assert 'performance_metrics' in output, 'Output should contain performance metrics'
    
    # Check that the output file exists
    assert os.path.exists(output['input_path']), 'Output GeoParquet file should exist'

if __name__ == "__main__":
    # For local testing
    test_data = {
        'input_path': 'path/to/test.parquet',
        'metadata': {
            'source_crs': 4326
        }
    }
    
    result = merge_grid(test_data)
    print(f"Success - Merged features: {result['metadata']['merged_features_count']}")
    
    # Print detailed performance metrics
    perf = result['performance_metrics']
    print("\nPerformance Metrics:")
    print(f"- Processing time: {perf['time_metrics']['total_seconds']} seconds")
    print(f"- Features/second: {perf['time_metrics']['features_per_second']}")
    print(f"- Memory delta: {perf['memory_metrics']['memory_delta_gb']} GB")
    print(f"- Reduction: {perf['efficiency_metrics']['reduction_percent']}%")
    print(f"- Compression ratio: {perf['efficiency_metrics']['compression_ratio']}:1")
    
    # Print per-gridcode results
    print("\nDetailed Results by Gridcode:")
    for gridcode, stats in result['metadata']['merge_stats'].items():
        print(f"Gridcode {gridcode}:")
        print(f"  - Original: {stats['original_features']}")
        print(f"  - Merged: {stats['merged_features']}")
        if 'reduction_percent' in stats:
            print(f"  - Reduction: {stats['reduction_percent']}%")
    
    print(f"\nTotal processing time: {result['metadata']['processing_time_seconds']} seconds") 