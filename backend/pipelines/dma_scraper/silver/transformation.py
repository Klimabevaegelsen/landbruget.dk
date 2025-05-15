import os
import sys
import json
import pyarrow as pa
import duckdb
import pandas as pd

def transform_dma_json(data):
    """
    Load JSON, sanitize nested arrays to remove empty keys, flatten nested arrays, and return as a Pandas DataFrame.
    """
    for rec in data:
        for sec in ['Tilsyn', 'Håndhævelser', 'Afgørelser']:
            items = rec.get(sec) or []
            rec[sec] = [{k: v for k, v in item.items() if k and k.strip()} for item in items]
    # Create Arrow table and register in DuckDB
    table = pa.Table.from_pylist(data)
    con = duckdb.connect()
    con.register('dma_raw', table)
    con.execute("""
        CREATE TABLE dma_flattened AS
        SELECT d.miljoeaktoerUrl AS id,
               'Tilsyn' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw d, UNNEST(d.Tilsyn) AS t(detail)
        UNION ALL
        SELECT d.miljoeaktoerUrl AS id,
               'Haandhaevelser' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw d, UNNEST(d.Håndhævelser) AS h(detail)
        UNION ALL
        SELECT d.miljoeaktoerUrl AS id,
               'Afgørelser' AS section,
               to_json(detail) AS detail_json
        FROM dma_raw d, UNNEST(d.Afgørelser) AS a(detail)
    """)
    # Fetch as Arrow table and convert to Pandas
    arrow_table = con.execute("SELECT id, section, detail_json FROM dma_flattened").arrow()
    con.close()
    return arrow_table.to_pandas()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage: python transformation.py <input_json>')
        sys.exit(1)
    df = transform_dma_json(sys.argv[1])
    print(df)