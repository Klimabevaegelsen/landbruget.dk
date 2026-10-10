# Child Receptors Pipeline

Builds one national EPSG:25832 point layer of daycare and after-school sites, schools, and playgrounds for downstream pesticide exposure analysis.

## Sources

- Dagtilbudsregisteret (STIL): active site and alternative-address exports. Dagpleje is excluded.
- STIL Institutionsregisteret: active primary, special, and boarding schools.
- BBR technical installations: playgrounds classified as `1905`.
- GeoFA `t_5800_fac_pkt`: current playground, nature-playground, and water-playground features.
- OpenStreetMap Overpass: playgrounds, with the ODbL-1.0 licence recorded in bronze manifests.

## Run

From this directory, run `python main.py --layer all`. Use `--layer bronze` or `--layer silver` for one layer, `--sources dagtilbud,stil` to select sources, and `--bronze-timestamp YYYYMMDD_HHMMSS` to select a bronze version. Add `--local-dir PATH` to keep local copies. Add `--no-upload` for local-only runs; it requires `--local-dir`.

Bronze files and manifests are written under `bronze/child_receptors/{source}/{timestamp}/`. Silver Parquet and its QA report are written under `silver/child_receptors/{timestamp}/`. Silver uses the latest bronze run for each source when no timestamp is supplied.

## Known gaps

- SFO is not in Dagtilbudsregisteret; school sites cover the school locations where SFO operates.
- Dagpleje homes are intentionally excluded to avoid publishing childminders' private addresses.
- Playground coverage varies substantially by kommune. BBR registration is voluntary; København has no BBR playground records and relies on GeoFA and OSM coverage.
- Children's døgninstitutioner from Tilbudsportalen are not included because a data agreement is needed.
