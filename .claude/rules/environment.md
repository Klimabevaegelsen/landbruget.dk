# Environment Setup

## Frontend (`.env.local`)

```bash
NEXT_PUBLIC_API_URL=<supabase_project_url>
NEXT_PUBLIC_API_KEY=<supabase_anon_key>
```

## Backend (`.env`)

```bash
SUPABASE_URL=<supabase_project_url>
SUPABASE_KEY=<supabase_service_role_key>
GCS_BUCKET=<gcs_bucket_name>
GCS_CREDENTIALS=<path_to_service_account_json>
```

## Validation

```bash
cd frontend && npm run dev                    # Should start on :3000
cd backend && source venv/bin/activate && python -c "import supabase; print('OK')"
supabase status                               # Should show linked project
```

## Common Failures

### Frontend Not Starting
- Check Node version: `node --version` (18+)
- Clear cache: `rm -rf .next node_modules && npm install`
- Verify `.env.local` exists with Supabase credentials
- Check port: `lsof -i :3000`

### Supabase Connection Issues
- `supabase status` to check link
- `SUPABASE_URL` should end with `.supabase.co`
- `SUPABASE_KEY` should be a long string (not empty)
- Test: `supabase db pull`

### Migration Failures
- Check SQL syntax in migration file
- Verify order: `ls supabase/migrations/`
- Reset local (destructive): `supabase db reset`

### Pipeline Environment
- Always activate: `cd backend && source venv/bin/activate`
- Install deps: `pip install -r requirements.txt`
- GCS: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"`
