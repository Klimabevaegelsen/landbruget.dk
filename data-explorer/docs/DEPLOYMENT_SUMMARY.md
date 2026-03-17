# Deployment Summary - Quick Reference

This document provides a quick reference for deploying the data explorer to production. For comprehensive details, see [DEPLOYMENT.md](../DEPLOYMENT.md).

---

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                                                 │
│  User → Cloudflare CDN → Vercel Edge           │
│         ↓                                       │
│  Next.js 15 App (Serverless)                   │
│         ↓                    ↓                  │
│  DuckDB WASM            Google Gemini           │
│         ↓                                       │
│  Cloudflare R2 (Parquet Files)                 │
│                                                 │
└─────────────────────────────────────────────────┘
```

**Production URL**: https://data.landbruget.dk

**Staging URL**: https://data-staging.landbruget.dk (optional)

**R2 Storage**: https://r2.landbruget.dk

---

## Quick Start Deployment

### 1. Prerequisites

```bash
# Install CLI tools
npm install -g vercel wrangler

# Login to services
vercel login
wrangler login
```

### 2. Setup Cloudflare R2

```bash
# Create bucket
wrangler r2 bucket create landbruget-data-explorer --jurisdiction eu

# Configure CORS
wrangler r2 bucket cors put landbruget-data-explorer \
  --config r2-cors-config.json

# Upload data
./scripts/upload-to-r2.sh ./backend/data

# Setup custom domain
wrangler r2 bucket domain add landbruget-data-explorer r2.landbruget.dk
```

### 3. Configure Vercel

```bash
cd data-explorer

# Link project
vercel link

# Set environment variables
vercel env add NEXT_PUBLIC_R2_URL production
# Enter: https://r2.landbruget.dk

vercel env add GOOGLE_API_KEY production
# Enter: [Your Google API Key]

# Deploy
vercel --prod
```

### 4. Configure DNS

In Cloudflare Dashboard → DNS:

```
Type: CNAME | Name: data | Target: cname.vercel-dns.com
Type: CNAME | Name: r2 | Target: landbruget-data-explorer.r2.cloudflarestorage.com
```

### 5. Verify Deployment

```bash
./scripts/test-deployment.sh https://data.landbruget.dk
```

---

## Environment Variables

| Variable             | Value                                                               | Where to Set     |
| -------------------- | ------------------------------------------------------------------- | ---------------- |
| `NEXT_PUBLIC_R2_URL` | `https://r2.landbruget.dk`                                          | Vercel Dashboard |
| `GOOGLE_API_KEY`     | Get from [Google AI Studio](https://aistudio.google.com/app/apikey) | Vercel Dashboard |

---

## Cost Estimate

| Service       | Plan          | Monthly Cost |
| ------------- | ------------- | ------------ |
| Vercel        | Pro           | $20          |
| Cloudflare R2 | Pay-as-you-go | ~$1          |
| Google Gemini | Free Tier     | $0           |
| **Total**     |               | **~$21**     |

---

## Key Files

| File                           | Purpose                   |
| ------------------------------ | ------------------------- |
| `DEPLOYMENT.md`                | Complete deployment guide |
| `DEPLOYMENT_CHECKLIST.md`      | Pre-deployment checklist  |
| `r2-cors-config.json`          | CORS configuration for R2 |
| `vercel.json`                  | Vercel configuration      |
| `.github/workflows/deploy.yml` | GitHub Actions CI/CD      |
| `scripts/upload-to-r2.sh`      | Upload data to R2         |
| `scripts/test-deployment.sh`   | Test deployment health    |

---

## Common Commands

### Deployment

```bash
# Deploy to production
vercel --prod

# Deploy to preview
vercel

# View deployments
vercel ls

# View logs
vercel logs landbruget-data-explorer --follow
```

### R2 Management

```bash
# List files
wrangler r2 object list landbruget-data-explorer

# Upload file
wrangler r2 object put landbruget-data-explorer/bronze/file.parquet \
  --file=./path/to/file.parquet

# Download file
wrangler r2 object get landbruget-data-explorer/bronze/file.parquet \
  --file=./output.parquet

# Delete file
wrangler r2 object delete landbruget-data-explorer/bronze/file.parquet
```

### Testing

```bash
# Health check
curl https://data.landbruget.dk/api/health

# Full deployment test
./scripts/test-deployment.sh https://data.landbruget.dk

# API test
curl https://data.landbruget.dk/api/ask \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"question":"Show me the first 5 rows"}'
```

---

## Rollback Procedure

### Quick Rollback (< 2 minutes)

**Option 1: Via Dashboard**

1. Go to [Vercel Dashboard](https://vercel.com/dashboard)
2. Select project → Deployments
3. Find last known good deployment
4. Click ⋮ → "Promote to Production"

**Option 2: Via CLI**

```bash
# List recent deployments
vercel ls landbruget-data-explorer

# Promote specific deployment
vercel promote [deployment-url] --prod
```

**Option 3: Via Git**

```bash
# Revert last commit
git revert HEAD
git push origin main
# Automatic deployment triggered
```

---

## Monitoring

### Uptime Monitoring

**UptimeRobot** (Recommended - Free):

- URL: https://data.landbruget.dk/api/health
- Interval: 5 minutes
- Alerts: Email + SMS

### Performance Monitoring

**Vercel Analytics**:

- Core Web Vitals
- Real User Monitoring (RUM)
- Function execution times

**Access**: [Vercel Dashboard](https://vercel.com/dashboard) → Project → Analytics

### Error Tracking

```bash
# View error logs
vercel logs landbruget-data-explorer --level=error

# Filter by time
vercel logs landbruget-data-explorer --since=1h
```

---

## Troubleshooting Quick Reference

### Build Failures

```bash
# Test build locally first
npm run build

# Check Node version
node --version  # Should be 20+

# Clear cache and rebuild
rm -rf .next node_modules
npm install
npm run build
```

### CORS Errors

```bash
# Verify CORS configuration
wrangler r2 bucket cors get landbruget-data-explorer

# Re-apply CORS config
wrangler r2 bucket cors put landbruget-data-explorer \
  --config r2-cors-config.json

# Check allowed origins include your domain
```

### Environment Variables Not Working

```bash
# Pull current environment variables
vercel env pull .env.local

# Verify they're set correctly
cat .env.local

# Force redeploy after changing env vars
vercel --prod --force
```

### Slow Performance

```bash
# Check bundle size
npm run build

# Analyze bundle
npx @next/bundle-analyzer

# Check Cloudflare cache
curl -I https://data.landbruget.dk/
# Look for: cf-cache-status header
```

---

## Security Checklist

- [ ] HTTPS enforced
- [ ] Environment variables not exposed
- [ ] CORS properly configured
- [ ] API keys rotated every 90 days
- [ ] Security headers present
- [ ] Rate limiting enabled
- [ ] No secrets in code

---

## Support Contacts

| Issue Type        | Contact                       |
| ----------------- | ----------------------------- |
| Deployment Issues | devops@landbruget.dk          |
| Platform Outage   | platform-alerts@landbruget.dk |
| Security Issues   | security@landbruget.dk        |
| General Questions | team@landbruget.dk            |

---

## Additional Resources

- [Complete Deployment Guide](../DEPLOYMENT.md)
- [Pre-Deployment Checklist](../DEPLOYMENT_CHECKLIST.md)
- [Testing Documentation](../TESTING.md)
- [Quick Start Guide](../QUICK_START.md)
- [Vercel Documentation](https://vercel.com/docs)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)

---

**Last Updated**: 2026-01-10

**Version**: 1.0.0
