# Kemidata Surface Water Pesticides

## Overview

Ingests pesticide detection data from the **Kemidata REST API** (Danmarks Miljøportal) for surface water monitoring stations — rivers (Vandløb) and lakes (Sø).

## Data Source

| Field | Value |
|-------|-------|
| Authority | Danmarks Miljøportal |
| API Base | `https://kemidata.miljoeportal.dk/api/` |
| Coverage | 2,900+ chemical parameters, 70M+ observations, 10,000+ stations |
| Media Types | Vandløb (rivers), Sø (lakes) |
| CRS | EPSG:25832 (UTM32 EUREF89) |
| Update Frequency | Quarterly |

## API Endpoints

### Search (`POST /api/search`)

Discovers stations with chemistry data. Request body:

```json
{
  "language": "da",
  "searchBy": "Chemistry",
  "searchParameters": [],
  "period": {"showLastResult": false, "fromDate": null, "toDate": null},
  "area": {
    "type": "Rectangle",
    "geoJsonString": "<GeoJSON FeatureCollection with Polygon in EPSG:25832>"
  },
  "mediaTypes": ["Vandløb", "Sø"],
  "sessionId": "<uuid>"
}
```

Response includes `stations[]` with `id`, `name`, `location` ("x, y" in EPSG:25832), `mediaName`, and `results[]`.

### Download (`POST /api/download`)

Same body as search + `"isDownloadAll": true`. Returns CSV with actual measurement values.

### Metadata (`GET /api/metadata?language=da`)

Returns all parameter IDs, examination types, media types, and responsible organizations.

## Pipeline Stages

### Bronze

- Fetches metadata catalogue
- Searches for all surface water stations with chemistry data
- Downloads full CSV export
- Saves raw CSV, station JSON, and metadata to GCS

```bash
python -m unified_pipeline bronze --source kemidata_surface_water_pesticides
```

### Silver

- Reads CSV from bronze via DuckDB `read_csv_auto`
- Joins station coordinates from search results → `ST_Point(x, y)` geometry
- Filters to surface water only (Vandløb, Sø)
- Validates coordinates within Denmark bounds
- Saves as parquet to GCS

```bash
python -m unified_pipeline silver --source kemidata_surface_water_pesticides
```

### Full Pipeline

```bash
python -m unified_pipeline all --source kemidata_surface_water_pesticides
```

## Key Design Decisions

1. **No parameter filter in search**: The bronze layer fetches ALL chemistry data for surface water, not just specific pesticides. This makes it trivial to expand to other parameters later.
2. **Media type filter**: Applied at both API level (`mediaTypes` parameter) and silver layer (post-processing filter) to ensure only river/lake data is included.
3. **Station coordinate join**: The CSV export doesn't always include coordinates directly. Station coordinates are extracted from the search response and joined in silver.

## Expanding to Other Parameters

To add non-pesticide parameters (e.g., nutrients, metals, oxygen):
1. No bronze changes needed — it already fetches all chemistry data
2. Silver layer may need additional column mappings depending on the CSV schema
3. Consider creating separate silver datasets per parameter group for cleaner analysis
