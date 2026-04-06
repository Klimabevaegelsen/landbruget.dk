# Environment Setup

## Frontend (`.env.local`)

```bash
NEXT_PUBLIC_DATA_URL=<r2_cdn_public_url>
```

## Backend (`.env`)

```bash
R2_BUCKET=<r2_bucket_name>
R2_ACCESS_KEY_ID=<r2_access_key>
R2_SECRET_ACCESS_KEY=<r2_secret_key>
R2_ACCOUNT_ID=<cloudflare_account_id>
GCS_BUCKET=<gcs_bucket_name>
GCS_CREDENTIALS=<path_to_service_account_json>
```

## Validation

```bash
cd frontend && npm run dev                    # Should start on :3000
cd backend && source venv/bin/activate && python -c "import duckdb; print('OK')"
```

## Common Failures

### Frontend Not Starting
- Check Node version: `node --version` (18+)
- Clear cache: `rm -rf .next node_modules && npm install`
- Verify `.env.local` exists with `NEXT_PUBLIC_DATA_URL`
- Check port: `lsof -i :3000`

### R2 Data Access Issues
- Verify `NEXT_PUBLIC_DATA_URL` points to the correct R2 public URL
- Check R2 bucket permissions (public read access required)
- Test: `curl -s $NEXT_PUBLIC_DATA_URL/homepage-rankings.json | head`

### Pipeline Environment
- Always activate: `cd backend && source venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- R2: Set `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`
- GCS: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"`
