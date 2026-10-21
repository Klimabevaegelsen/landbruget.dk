# DAGI workflow

DAGI boundaries come from the Datafordeler DAGI WFS. The job fetches municipalities,
regions, landsdele, and postal codes, then publishes the existing DAWA-compatible
GeoJSON contract for silver and downstream consumers.

## Source and bronze output

- **WFS:** `https://wfs.datafordeler.dk/DAGIM/DAGI_10MULTIGEOM_GMLSFP/1.0.0/WFS`
- **Authentication:** query parameter `apiKey`, read from `DATAFORDELER_API_KEY` or
  `DATAFORDELER_GRAPHQL_API_KEY`. The key is not written to logs.
- **Request format:** WFS 2.0 GetFeature, GML 3.2, `dagi10` feature types, and
  `EPSG:25832` coordinates. Responses are paged and retried.
- **Raw archive:** every response page is saved unchanged at
  `bronze/dagi_{layer}/{date}/raw/dagi_{layer}_pageNNN.gml`.
- **Compatibility output:** a WGS84 GeoJSON FeatureCollection is written to the
  existing `bronze/dagi_{layer}/{date}/dagi_{layer}.json` path and returned in memory.
  Its property names and coordinate order match the former DAWA output.

| Layer | WFS type | Compatibility properties |
|---|---|---|
| `kommuner` | `Kommuneinddeling` | `kode` (4 digits), `navn`, `regionskode`, `regionsnavn`, `udenforkommuneinddeling` (bool), `dagi_id` |
| `regioner` | `Regionsinddeling` | `kode`, `navn`, `nuts2`, `dagi_id` |
| `landsdele` | `Landsdel` | `nuts3`, `navn`, `regionskode`, `regionsnavn`, `dagi_id` |
| `postnumre` | `Postnummerinddeling` | `nr` (4-character string), `navn`, `ergadepostnummer` (bool), `stormodtager` (false), `dagi_id` |

All layers also carry `_source`, `_source_crs`, and `_fetch_timestamp`. Landsdele are
assigned to the containing region using their representative point in EPSG:25832.
Before publishing, the job validates row counts, unique codes, required code formats,
valid geometries, and the Denmark WGS84 bounds.

## Silver processing

Silver keeps the existing columns and transforms compatibility GeoJSON geometry from
WGS84 to EPSG:25832. `geometry` and `area_m2` use EPSG:25832; `centroid_x`,
`centroid_y`, and `geometry_wkt` remain WGS84. This preserves consumers that read
administrative codes, region names, centroids, and bronze GeoJSON directly.
