"""BBR Building processor using DuckDB-spatial."""

import duckdb
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

class BuildingProcessor:
    """Process BBR building data using DuckDB-spatial."""
    
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
    
    def process_buildings(self, input_path: str, output_path: str) -> str:
        """Process building data with spatial operations."""
        table_name = f"bbr_buildings_{int(time.time())}"
        
        # Load spatial data
        self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM ST_Read('{input_path}')
        """)
        
        # Process buildings with spatial operations
        processed_table = f"processed_{table_name}"
        self.conn.execute(f"""
            CREATE TABLE {processed_table} AS
            SELECT 
                *,
                ST_Area(geom) as building_area,
                ST_Centroid(geom) as building_centroid,
                current_timestamp as processed_at
            FROM {table_name}
            WHERE ST_IsValid(geom)
        """)
        
        # Save results
        self.conn.execute(f"""
            COPY {processed_table} TO '{output_path}' (FORMAT PARQUET)
        """)
        
        return processed_table
    
    def validate_geometries(self, table_name: str) -> Dict[str, int]:
        """Validate building geometries."""
        results = {}
        
        # Count total buildings
        total = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        results['total'] = total
        
        # Count valid geometries
        valid = self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ST_IsValid(geom)").fetchone()[0]
        results['valid'] = valid
        
        # Count invalid geometries
        results['invalid'] = total - valid
        
        return results
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
