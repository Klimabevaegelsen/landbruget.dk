# Dataset publication

Publishes the public pesticide **field-use allocation** bundle to Zenodo, Figshare, and Dataverse.
Two stages: (1) generate the bundle from gold data, (2) publish it.

## License

**CC-BY-4.0.** The bundle embeds Landbrugsstyrelsen/Geodatastyrelsen field geodata ("frie
geografiske data"), which carries *mandatory attribution* — that legally rules out CC0 for the
derived dataset. CC-BY-4.0 is the lowest-common-denominator that also covers the freer Miljøstyrelsen
SJI and BMD sources. The bundle's `README.md` carries the required attribution block; keep
`--license-id cc-by-4.0`.

## 1. Generate the bundle

The `pesticide_bulk` exporter reads R2 gold `pesticide_disaggregation_*` and silver `fvm_marker_*`.
It is **not** in the weekly api_export matrix, so run it explicitly.

Local (needs R2 read creds in `backend/pipelines/api_export/.env`:
`R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`):

```bash
cd backend/pipelines/api_export
uv run python main.py --only pesticide_bulk --local ./export
# -> ./export/datasets/pesticide-field-use-allocations/v1/
```

Or via CI, then download the R2 prefix locally:

```bash
gh workflow run api_export_pipeline.yml -f only=pesticide_bulk
```

## 2. Publish

Dry run (validate + zip + privacy scan, no network):

```bash
uv run --all-packages --group dev python backend/pipelines/api_export/publish_dataset.py \
  --source-dir backend/pipelines/api_export/export/datasets/pesticide-field-use-allocations/v1 \
  --target zenodo --target figshare --target dataverse \
  --dry-run
```

Create draft/private records:

```bash
export ZENODO_TOKEN=...
export FIGSHARE_TOKEN=...
export DATAVERSE_TOKEN=...
export DATAVERSE_SERVER=https://dataverse.harvard.edu
export DATAVERSE_COLLECTION_ALIAS=<your-collection>   # root rejects non-admin dataset creation
export DATAVERSE_CONTACT_EMAIL=kontakt@landbruget.dk

uv run --all-packages --group dev python backend/pipelines/api_export/publish_dataset.py \
  --source-dir backend/pipelines/api_export/export/datasets/pesticide-field-use-allocations/v1 \
  --target zenodo --target figshare --target dataverse \
  --license-id cc-by-4.0 \
  --figshare-license-id <id> \
  --figshare-private-link \
  --creator "Klimabevægelsen / Landbruget.dk" --creator "Martin Collignon" \
  --create-draft
```

- Zenodo token scopes: `deposit:write deposit:actions`. Test the full flow first with
  `--zenodo-sandbox`.
- Figshare CC-BY-4.0 license id: `GET https://api.figshare.com/v2/licenses` → pass `--figshare-license-id`.
- Dataverse: the CLI does not set a license field — set CC-BY-4.0 in the Harvard UI on the draft
  before publishing.

Publish: re-run with `--confirm-publish` instead of `--create-draft`. **Do Zenodo first** to mint the
canonical DOI, then publish the Figshare + Dataverse drafts with
`--confirm-publish --canonical-doi <zenodo-doi>` so the mirrors cross-reference it. (Re-running creates
fresh records — prefer publishing the existing Zenodo draft from its web UI, then mirror.)

## Safety checks

- Requires `use_allocations/`; rejects a legacy `applications/` directory.
- Verifies `checksums.txt`.
- Scans public Parquet schemas and refuses CVR-like columns.
- Uploads one versioned zip archive so repository records contain a stable immutable bundle.
