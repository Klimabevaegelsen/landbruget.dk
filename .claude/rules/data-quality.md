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
| 4326 | WGS84 | **Storage** (required) |
| 25832 | UTM 32N | Danish data input |
| 3857 | Web Mercator | Display/maps |

### Before Storing Geometry
```python
# ALWAYS convert to WGS84
gdf = gdf.to_crs('EPSG:4326')
```

### Validate Within Denmark
```sql
ST_Within(geom, ST_MakeEnvelope(7.5, 54.5, 15.5, 58, 4326))
```

## Medallion Architecture

### Bronze (Raw)
- Preserve exactly as received
- Add metadata: `_fetch_timestamp`, `_source`
- Never modify
- Never overwrite

### Silver (Cleaned)
- Type coercion
- Format validation
- Deduplication
- CRS conversion

### Gold (Analysis-Ready)
- Join on CVR/CHR/BFE
- Derived metrics
- Upload to Supabase

## Data Joinability

All data must be joinable on at least one of:
- CVR (company)
- CHR (herd)
- BFE (cadastral)
- Geospatial coordinates

## Quality Checks Before Upload

- [ ] CVR format valid (8 digits)
- [ ] CHR format valid (6 digits)
- [ ] Geospatial CRS is EPSG:4326
- [ ] No duplicate primary keys
- [ ] Required fields not null
- [ ] Values within expected ranges
