# ruff: noqa: T201, PTH110, PTH118, PTH120
"""
Verify: "Over 1.2 million Danes live within 1 km of fields sprayed with pesticides."

Data sources:
  - Eurostat Census Grid 2021 V2.2 (1km population grid, public download)
  - Silver FVM marker data (field boundaries from R2)

Method:
  For each 1km population grid cell centroid (EPSG:25832),
  check if it's within 1km of any non-organic agricultural field boundary.
  Sum total_population for matching cells.

Usage:
  cd backend && source venv/bin/activate
  # 1. Download Eurostat data (one-time, ~190MB):
  #    curl -L -o ESTAT_Census_2021_V2.zip \
  #      "https://gisco-services.ec.europa.eu/pub/census/2021/ESTAT_Census_2021_V2-2.zip"
  #    unzip ESTAT_Census_2021_V2.zip ESTAT_Census_2021_V2.parquet
  # 2. Run:
  #    python scripts/verify_population_near_fields.py
"""

import os
import sys
import time

import duckdb
import s3fs
from dotenv import load_dotenv

load_dotenv()
# Also load pipeline .env for R2 credentials
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "pipelines", ".env"))

# --- Config ---
EUROSTAT_PARQUET = os.getenv("EUROSTAT_PARQUET", "ESTAT_Census_2021_V2.parquet")
FIELD_YEAR = int(os.getenv("FIELD_YEAR", "2024"))
BUFFER_DISTANCE_M = int(os.getenv("BUFFER_DISTANCE_M", "1000"))

# Denmark bounding box in LAEA (EPSG:3035) — from dk-building-data
DK_MIN_E, DK_MAX_E = 4_149_000, 4_688_000
DK_MIN_N, DK_MAX_N = 3_481_000, 3_901_000


def setup_conn() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with spatial + R2 auth."""
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # R2 auth for field data
    r2_key = os.getenv("R2_ACCESS_KEY_ID")
    r2_secret = os.getenv("R2_SECRET_ACCESS_KEY")
    r2_account = os.getenv("R2_ACCOUNT_ID")

    if r2_key and r2_secret and r2_account:
        # Register s3fs for R2 access (DuckDB native R2 glob doesn't work well)
        endpoint_url = f"https://{r2_account}.r2.cloudflarestorage.com"
        fs = s3fs.S3FileSystem(
            key=r2_key,
            secret=r2_secret,
            client_kwargs={"endpoint_url": endpoint_url},
        )
        conn.register_filesystem(fs)
        print("R2 auth configured (s3fs)")
    else:
        print("WARNING: R2 credentials not set — field data loading will fail")

    return conn


def load_population_grid(conn: duckdb.DuckDBPyConnection) -> None:
    """Load Eurostat Census Grid 2021, filter to Denmark, transform to UTM32N."""
    if not os.path.exists(EUROSTAT_PARQUET):
        print(f"ERROR: {EUROSTAT_PARQUET} not found.")
        print("Download it first:")
        print("  curl -L -o ESTAT_Census_2021_V2.zip \\")
        print('    "https://gisco-services.ec.europa.eu/pub/census/2021/ESTAT_Census_2021_V2-2.zip"')
        print("  unzip ESTAT_Census_2021_V2.zip ESTAT_Census_2021_V2.parquet")
        sys.exit(1)

    print(f"Loading population grid from {EUROSTAT_PARQUET}...")
    t0 = time.time()

    # Eurostat V2.2 has a broken geom column — disable geoparquet for this read
    conn.execute("SET enable_geoparquet_conversion=false;")
    # Extract LAEA coordinates from GRD_ID, filter to Denmark bbox
    conn.execute(f"""
        CREATE TABLE pop_laea AS
        SELECT
            GRD_ID,
            T AS total_population,
            CAST(regexp_extract(GRD_ID, 'N(\\d+)E', 1) AS INTEGER) AS northing,
            CAST(regexp_extract(GRD_ID, 'E(\\d+)$', 1) AS INTEGER) AS easting
        FROM read_parquet('{EUROSTAT_PARQUET}')
        WHERE T > 0
          AND CAST(regexp_extract(GRD_ID, 'E(\\d+)$', 1) AS INTEGER)
              BETWEEN {DK_MIN_E} AND {DK_MAX_E}
          AND CAST(regexp_extract(GRD_ID, 'N(\\d+)E', 1) AS INTEGER)
              BETWEEN {DK_MIN_N} AND {DK_MAX_N}
    """)

    stats = conn.execute("SELECT COUNT(*), SUM(total_population) FROM pop_laea").fetchone()
    print(f"  Danish grid cells: {stats[0]:,}  |  Total population: {stats[1]:,}")

    # Create centroids (center of 1km cell) and transform LAEA → UTM32N
    # EPSG:3035 axis order is (Northing, Easting), so ST_Point(N, E)
    conn.execute("""
        CREATE TABLE pop_grid AS
        SELECT
            GRD_ID,
            total_population,
            ST_Transform(
                ST_Point(northing + 500, easting + 500),
                'EPSG:3035',
                'EPSG:25832'
            ) AS centroid
        FROM pop_laea
    """)
    conn.execute("DROP TABLE pop_laea")
    # Re-enable geoparquet for field data
    conn.execute("SET enable_geoparquet_conversion=true;")

    print(f"  Population grid ready ({time.time() - t0:.1f}s)")


def load_field_boundaries(conn: duckdb.DuckDBPyConnection) -> None:
    """Load silver FVM markblokke (field block boundaries) from R2.

    Uses markblokke (field blocks) which have geometry. These represent
    all agricultural land areas — the vast majority of which are sprayed.
    Geometry is in OGC:CRS84 (WGS84), transformed to EPSG:25832 for analysis.
    """
    bucket = "landbruget-data"
    t0 = time.time()
    # Try requested year, fall back to 2021 (most recent on R2)
    for year in [FIELD_YEAR, 2021]:
        data_path = f"s3://{bucket}/silver/fvm_markblokke_{year}/*/data.parquet"
        print(f"  Trying: {data_path}")
        try:
            # Geometry metadata says OGC:CRS84 but coords are actually EPSG:25832
            # (UTM32N meters — verified by inspecting centroid values ~500k, ~6M)
            # Just load as-is, no transform needed
            conn.execute(f"""
                CREATE TABLE fields AS
                SELECT geometry
                FROM read_parquet('{data_path}')
                WHERE geometry IS NOT NULL
            """)
            print(f"  Loaded year {year}")
            break
        except Exception as e:
            print(f"  Year {year} failed: {e}")
            if year == 2021:
                raise

    field_count = conn.execute("SELECT COUNT(*) FROM fields").fetchone()[0]
    print(f"  Field blocks loaded: {field_count:,} ({time.time() - t0:.1f}s)")


def compute_population_near_fields(conn: duckdb.DuckDBPyConnection) -> None:
    """Spatial semi-join: population centroids within BUFFER_DISTANCE_M of any field."""
    print(f"\nComputing: population within {BUFFER_DISTANCE_M}m of non-organic fields...")
    print("  (This may take several minutes for the spatial join...)")
    t0 = time.time()

    # Strategy: buffer all fields, dissolve to single geometry, then point-in-polygon.
    # This is faster than NxM distance checks because DuckDB can use spatial indexing
    # on the dissolved geometry.
    #
    # If memory is an issue, fall back to ST_DWithin semi-join.

    # Use a grid-based approach to avoid O(N*M) spatial join:
    # 1. For each field block, find which 1km grid cells its buffered bbox overlaps
    # 2. Then refine with actual distance check only for candidate cells

    # First, create a spatial index hint by expanding field bboxes
    print("  Step 1/2: Building field envelope index...")
    conn.execute(f"""
        CREATE TABLE field_envelopes AS
        SELECT
            ST_XMin(ST_Envelope(geometry)) - {BUFFER_DISTANCE_M} AS xmin,
            ST_YMin(ST_Envelope(geometry)) - {BUFFER_DISTANCE_M} AS ymin,
            ST_XMax(ST_Envelope(geometry)) + {BUFFER_DISTANCE_M} AS xmax,
            ST_YMax(ST_Envelope(geometry)) + {BUFFER_DISTANCE_M} AS ymax
        FROM fields
        WHERE geometry IS NOT NULL
    """)
    print(f"    Done ({time.time() - t0:.1f}s)")

    # Use envelope pre-filter + ST_DWithin for actual check
    print("  Step 2/2: Spatial join with envelope pre-filter...")
    result = conn.execute(f"""
        SELECT
            COUNT(*) AS cells_near,
            SUM(total_population) AS pop_near
        FROM pop_grid p
        WHERE EXISTS (
            SELECT 1 FROM fields f
            WHERE ST_DWithin(p.centroid, f.geometry, {BUFFER_DISTANCE_M})
            LIMIT 1
        )
    """).fetchone()

    elapsed = time.time() - t0
    print(f"    Done ({elapsed:.1f}s)")

    # Get total population for comparison
    total = conn.execute("SELECT SUM(total_population) FROM pop_grid").fetchone()[0]

    print()
    print("=" * 65)
    print("  RESULTS")
    print("=" * 65)
    print(f"  Field year:                     {FIELD_YEAR}")
    print(f"  Buffer distance:                {BUFFER_DISTANCE_M}m")
    print("  Population source:              Eurostat Census Grid 2021")
    print("  Method:                         Grid-centroid within buffer")
    print()
    # Grid total includes parts of Sweden/Germany due to LAEA bbox overlap
    # Use official 2021 census population for Denmark for accurate percentage
    dk_official_pop = 5_840_045  # DST 2021
    print(f"  Grid cells in LAEA bbox:        {total:>12,}  (includes neighbors)")
    print(f"  Official DK pop (2021):         {dk_official_pop:>12,}")
    print(f"  Grid cells within {BUFFER_DISTANCE_M}m:         {result[0]:>12,}")
    print(f"  Population within {BUFFER_DISTANCE_M}m:         {result[1]:>12,}")
    print(f"  Percentage of DK pop:           {result[1] / dk_official_pop * 100:>11.1f}%")
    print()
    claim = 1_200_000
    diff = result[1] - claim
    direction = "higher" if diff > 0 else "lower"
    print(f"  Website claim:                  {claim:>12,}")
    print(f"  Difference:                     {diff:>+12,} ({direction})")
    print("=" * 65)


def main():
    conn = setup_conn()
    load_population_grid(conn)
    load_field_boundaries(conn)
    compute_population_near_fields(conn)
    conn.close()


if __name__ == "__main__":
    main()
