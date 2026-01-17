# Data Quality Rules

## This is a Public Transparency Project

Data accuracy is paramount. Double-check all transformations. Be honest about data limitations.

## Danish Identifier Formats

### CVR Number (Company ID)
- **Format**: Exactly 8 digits
- **Validation**: `^\d{8}$`
- **Storage**: String (preserve leading zeros)
- **Example**: `31373077`

```python
df['cvr'] = df['cvr'].astype(str).str.zfill(8)
assert df['cvr'].str.match(r'^\d{8}$').all()
```

### CHR Number (Herd ID)
- **Format**: Exactly 6 digits
- **Validation**: `^\d{6}$`
- **Storage**: String
- **Example**: `123456`

### BFE Number (Cadastral ID)
- **Format**: Variable (kommune-ejerlav-matr)
- **Storage**: String
- **Example**: `0101-123456-12a`

## Geospatial Standards

### Coordinate Reference Systems
| EPSG | Name | Use |
|------|------|-----|
| 25832 | UTM 32N | **Processing** (Bronze/Silver/Gold) |
| 4326 | WGS84 | **Final storage** (Supabase only) |
| 3857 | Web Mercator | Display/maps |

### CRS Strategy (Optimal)

**Process in EPSG:25832, transform to EPSG:4326 only at final Supabase upload.**

This eliminates unnecessary back-and-forth transforms:
- ❌ Old: Source(25832) → Silver(4326) → Gold(25832 for calc) → Supabase(4326) = 2-3 transforms
- ✅ New: Source(25832) → Process(25832) → Supabase(4326) = 1 transform

### Validate Within Denmark
```sql
-- For EPSG:25832 (UTM)
ST_Within(geom, ST_MakeEnvelope(400000, 6000000, 900000, 6500000, 25832))

-- For EPSG:4326 (WGS84)
ST_Within(geom, ST_MakeEnvelope(7.5, 54.5, 15.5, 58, 4326))
```

### Buffer/Distance Operations

**With EPSG:25832, buffer/distance work natively in meters:**
```sql
-- EPSG:25832 data - buffer works directly in meters
ST_Buffer(geometry, 1000)  -- 1000 meters ✓

-- EPSG:4326 data - WRONG! This is 1000 degrees!
ST_Buffer(geometry, 1000)  -- BUG: wraps the planet!
```

See `backend/common/crs_utils.py` for utilities when working with EPSG:4326 data.

## Medallion Architecture

### Bronze (Raw)
- Preserve exactly as received
- Add metadata: `_fetch_timestamp`, `_source`, **`_source_crs`**
- Never modify geometry or CRS
- Never overwrite
- **CRS**: Keep native (usually EPSG:25832 from Danish sources)

```python
# Track source CRS in metadata
_source_crs = detect_crs_from_response(wfs_capabilities)  # e.g., "EPSG:25832"
```

### Silver (Cleaned)
- Type coercion
- Format validation
- Deduplication
- **CRS: Keep EPSG:25832** (no transformation yet!)
- Transform non-25832 sources (DAGI, H3) to 25832 here

```python
# Only transform sources that aren't already EPSG:25832
if source_crs != "EPSG:25832":
    ST_Transform(geometry, source_crs, 'EPSG:25832')
```

### Gold (Analysis-Ready)
- Join on CVR/CHR/BFE
- Derived metrics (area, distance, buffer work natively in meters!)
- **CRS: Keep EPSG:25832** for processing
- Transform to EPSG:4326 **only** at final Supabase upload

```python
# Area/buffer/distance work directly - no transforms needed!
ST_Area(geometry) / 10000  # hectares (geometry already in meters)
ST_Buffer(geometry, 1000)  # 1km buffer (meters work directly)

# Transform ONCE at final upload
ST_Transform(geometry, 'EPSG:25832', 'EPSG:4326')  # for Supabase
```

## Data Joinability

All data must be joinable on at least one of:
- CVR (company)
- CHR (herd)
- BFE (cadastral)
- Geospatial coordinates

## Quality Checks Before Upload

- [ ] CVR format valid (8 digits)
- [ ] CHR format valid (6 digits)
- [ ] Processing CRS is EPSG:25832 (Bronze/Silver/Gold)
- [ ] Final Supabase upload transformed to EPSG:4326
- [ ] No duplicate primary keys
- [ ] Required fields not null
- [ ] Values within expected ranges
