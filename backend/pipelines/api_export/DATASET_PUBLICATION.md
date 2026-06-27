# Dataset publication

Use `publish_dataset.py` after exporting the public pesticide use-allocation bundle locally.
The command validates the bundle, creates a versioned zip archive, and can create draft records
or publish records on Zenodo, Figshare, and Dataverse.

Example dry run:

```bash
uv run --all-packages --group dev python backend/pipelines/api_export/publish_dataset.py \
  --source-dir /path/to/export/datasets/pesticide-field-use-allocations/v1 \
  --target zenodo \
  --target figshare \
  --target dataverse \
  --dry-run
```

Create draft/private records:

```bash
export ZENODO_TOKEN=...
export FIGSHARE_TOKEN=...
export DATAVERSE_TOKEN=...
export DATAVERSE_SERVER=https://dataverse.harvard.edu
export DATAVERSE_COLLECTION_ALIAS=root
export DATAVERSE_CONTACT_EMAIL=kontakt@landbruget.dk

uv run --all-packages --group dev python backend/pipelines/api_export/publish_dataset.py \
  --source-dir /path/to/export/datasets/pesticide-field-use-allocations/v1 \
  --target zenodo \
  --target figshare \
  --target dataverse \
  --license-id cc-by-4.0 \
  --figshare-license-id <figshare-license-id> \
  --figshare-private-link \
  --create-draft
```

Final publication uses the same command with `--confirm-publish` instead of `--create-draft`.

Safety checks:

- Requires `use_allocations/`; rejects a legacy `applications/` directory.
- Verifies `checksums.txt`.
- Scans public Parquet schemas and refuses CVR-like columns.
- Uploads one versioned zip archive so repository records contain a stable immutable bundle.

Confirm upstream reuse terms before selecting the final license ID.
