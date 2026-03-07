# GCS → Cloudflare R2 Migration Progress

## Phase 1: Core Module (`gcsfs` → `s3fs`)
- [x] 1a. Write R2 filesystem tests — RED (10 tests)
- [x] 1a. Implement R2 filesystem — GREEN (10/10 pass)
- [x] 1b. Write R2 core tests — RED (12 tests)
- [x] 1b. Implement R2 core — GREEN (12/12 pass, 29/29 existing GCS tests pass)
- [x] 1c. Storage interface — removed google.cloud.storage fallback
- [x] 1d. DuckDB processor — R2 TYPE r2 with GCS fallback
- [x] 1e. Backward-compat shim — aliases in filesystem.py (get_gcs_filesystem → get_r2_filesystem)
- [x] Verify: 22 new R2 tests pass + 29 existing GCS tests pass

## Phase 2: Dependencies
- [x] Update 13 pyproject.toml files (gcsfs/google-cloud-storage → s3fs)
- [x] Verify: 22 R2 tests + 29 GCS compat tests pass (51/51)

## Phase 3: Frontend PMTiles
- [x] Update frontend-pesticide basemaps.ts → data.pesticidkortet.dk
- [x] Update frontend-pesticide pmtiles-discovery.ts → data.pesticidkortet.dk
- [x] Update frontend-pesticide metadata route.ts → data.pesticidkortet.dk
- [ ] Verify: PMTiles render correctly (manual check needed)

## Phase 4: GitHub Actions Workflows
- [x] Update 23 workflow files (GCS env vars → R2, gsutil → aws s3 CLI)
- [x] Remove all google-github-actions/auth and setup-gcloud steps
- [x] Replace gsutil commands with aws s3 + R2 endpoint
- [x] Replace inline Python gcsfs with s3fs (cvr_enrichment)
- [ ] Verify: CI passes for each workflow (manual check needed)
- Note: sync-r2.yml left as-is (will be retired in Phase 6)

## Phase 5: Pipeline Path Strings
- [x] Migrate 14 files with direct google.cloud.storage imports to s3fs/get_r2_filesystem
- [x] Migrate gsutil subprocess calls to s3fs (dma_scraper)
- [x] All gs:// paths handled by _strip_protocol() backward compat
- [x] Verify: 51 tests pass
- Note: ~300 gs:// path strings remain but work via _strip_protocol() — cleanup in Phase 6

## Phase 6: Cleanup
- [ ] Retire sync-r2.yml
- [ ] Remove backward-compat shim (gs:// prefix support, get_gcs_* aliases)
- [ ] Remove GCS secrets from GitHub
- [ ] Delete sync scripts
- [ ] Rename gs:// path strings to r2:// across ~100 files (cosmetic)
- [ ] Remove prototype gsutil references (dma_scraper/prototypes/)
