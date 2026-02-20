# Deployment Guide - Landbruget.dk Data Explorer

Production deployment guide for the data explorer application to Vercel with Cloudflare R2 backend.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Infrastructure Setup](#infrastructure-setup)
  - [Cloudflare R2 Configuration](#cloudflare-r2-configuration)
  - [Domain Configuration](#domain-configuration)
  - [Vercel Project Setup](#vercel-project-setup)
- [Environment Variables](#environment-variables)
- [Deployment Process](#deployment-process)
- [Post-Deployment Testing](#post-deployment-testing)
- [Monitoring & Maintenance](#monitoring--maintenance)
- [Cost Breakdown](#cost-breakdown)
- [Troubleshooting](#troubleshooting)
- [Rollback Procedures](#rollback-procedures)

---

## Overview

### Architecture

```
User → Cloudflare CDN → Vercel Edge → Next.js 15 App
                                       ↓
                                   DuckDB WASM
                                       ↓
                         Cloudflare R2 (Parquet files)
                                       ↓
                                Google Gemini API
```

### Key Components

| Component | Purpose | Provider |
|-----------|---------|----------|
| **Frontend** | Next.js 15 application | Vercel |
| **Data Storage** | Parquet files (Bronze/Silver/Gold) | Cloudflare R2 |
| **Data Engine** | In-browser SQL queries | DuckDB WASM |
| **AI** | Natural language to SQL | Google Gemini API |
| **CDN** | Global content delivery | Cloudflare |
| **Domain** | data.landbruget.dk | Cloudflare DNS |

### Deployment Targets

- **Production**: `data.landbruget.dk`
- **Staging**: `data-staging.landbruget.dk` (optional)
- **Preview**: Automatic Vercel preview URLs for PRs

---

## Prerequisites

### Required Accounts

- [ ] Cloudflare account with R2 enabled
- [ ] Vercel account (team or personal)
- [ ] Google Cloud account for Gemini API
- [ ] GitHub repository access (for CI/CD)

### Required CLI Tools

```bash
# Vercel CLI
npm install -g vercel

# Wrangler (Cloudflare CLI)
npm install -g wrangler

# Google Cloud SDK (for Gemini API management)
curl https://sdk.cloud.google.com | bash
```

### Access Requirements

- Admin access to Cloudflare account
- Owner/member access to Vercel team
- Write access to GitHub repository
- Domain registrar access (for DNS configuration)

---

## Infrastructure Setup

### Cloudflare R2 Configuration

#### 1. Create R2 Bucket

```bash
# Login to Cloudflare
wrangler login

# Create bucket for data files
wrangler r2 bucket create landbruget-data-explorer --jurisdiction eu

# Verify creation
wrangler r2 bucket list
```

#### 2. Configure CORS for Browser Access

Create `r2-cors-config.json`:

```json
{
  "CORSRules": [
    {
      "AllowedOrigins": [
        "https://data.landbruget.dk",
        "https://data-staging.landbruget.dk",
        "http://localhost:3000"
      ],
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedHeaders": [
        "Content-Type",
        "Range",
        "If-Modified-Since",
        "If-None-Match"
      ],
      "ExposeHeaders": [
        "Content-Range",
        "Accept-Ranges",
        "ETag",
        "Last-Modified",
        "Content-Length"
      ],
      "MaxAgeSeconds": 3600
    }
  ]
}
```

Apply CORS configuration:

```bash
wrangler r2 bucket cors put landbruget-data-explorer --config r2-cors-config.json

# Verify CORS settings
wrangler r2 bucket cors get landbruget-data-explorer
```

#### 3. Set Up Custom Domain for R2

**Create subdomain**: `r2.landbruget.dk`

```bash
# Generate R2 bucket domain
wrangler r2 bucket domain add landbruget-data-explorer r2.landbruget.dk

# Verify DNS records were created
dig r2.landbruget.dk
```

**Expected DNS Record**:
```
r2.landbruget.dk. 300 IN CNAME landbruget-data-explorer.r2.cloudflarestorage.com.
```

#### 4. Configure Cloudflare Access (Optional but Recommended)

For private beta or internal use:

```bash
# Create access application
wrangler access app create \
  --name "Data Explorer R2" \
  --domain "r2.landbruget.dk" \
  --session-duration "24h"

# Add allowed emails
wrangler access policy create \
  --app-id <app-id> \
  --name "Allowed Users" \
  --decision allow \
  --include email:user@landbruget.dk
```

#### 5. Upload Data Files

```bash
# Upload Bronze layer (raw Parquet files)
wrangler r2 object put landbruget-data-explorer/bronze/agriculture_data.parquet \
  --file=./backend/data/bronze/agriculture_data.parquet \
  --content-type=application/octet-stream

# Upload Silver layer (cleaned data)
wrangler r2 object put landbruget-data-explorer/silver/agriculture_data.parquet \
  --file=./backend/data/silver/agriculture_data.parquet \
  --content-type=application/octet-stream

# Upload Gold layer (analysis-ready)
wrangler r2 object put landbruget-data-explorer/gold/agriculture_data.parquet \
  --file=./backend/data/gold/agriculture_data.parquet \
  --content-type=application/octet-stream

# Verify uploads
wrangler r2 object list landbruget-data-explorer --prefix=bronze/
wrangler r2 object list landbruget-data-explorer --prefix=silver/
wrangler r2 object list landbruget-data-explorer --prefix=gold/
```

**Automated Upload Script** (`scripts/upload-to-r2.sh`):

```bash
#!/bin/bash
set -euo pipefail

BUCKET="landbruget-data-explorer"
DATA_DIR="${1:-./backend/data}"

echo "Uploading data files to R2..."

for layer in bronze silver gold; do
  echo "Processing $layer layer..."
  find "$DATA_DIR/$layer" -name "*.parquet" | while read -r file; do
    key="${layer}/$(basename "$file")"
    echo "  Uploading: $key"
    wrangler r2 object put "${BUCKET}/${key}" \
      --file="$file" \
      --content-type=application/octet-stream
  done
done

echo "Upload complete!"
```

Usage:
```bash
chmod +x scripts/upload-to-r2.sh
./scripts/upload-to-r2.sh ./backend/data
```

---

### Domain Configuration

#### 1. Configure DNS Records

Add the following DNS records in Cloudflare:

```
# Main application domain
Type: CNAME
Name: data
Target: cname.vercel-dns.com
Proxy: Enabled (orange cloud)
TTL: Auto

# R2 storage subdomain
Type: CNAME
Name: r2
Target: landbruget-data-explorer.r2.cloudflarestorage.com
Proxy: Enabled (orange cloud)
TTL: Auto

# Optional staging environment
Type: CNAME
Name: data-staging
Target: cname.vercel-dns.com
Proxy: Enabled (orange cloud)
TTL: Auto
```

#### 2. SSL/TLS Configuration

In Cloudflare dashboard:

1. Navigate to **SSL/TLS** → **Overview**
2. Set encryption mode to **Full (strict)**
3. Enable **Always Use HTTPS**
4. Enable **Automatic HTTPS Rewrites**
5. Enable **Minimum TLS Version**: TLS 1.2

#### 3. Cloudflare Page Rules (Optional Performance)

```
Rule 1: Cache Everything for Static Assets
URL: data.landbruget.dk/_next/static/*
Settings:
  - Cache Level: Cache Everything
  - Edge Cache TTL: 1 year

Rule 2: Bypass Cache for API Routes
URL: data.landbruget.dk/api/*
Settings:
  - Cache Level: Bypass
```

---

### Vercel Project Setup

#### 1. Connect GitHub Repository

```bash
# Login to Vercel
vercel login

# Link project (run from data-explorer/ directory)
cd data-explorer
vercel link

# Follow prompts:
# - Set up and deploy? Yes
# - Scope: [Your team/account]
# - Link to existing project? No
# - Project name: landbruget-data-explorer
# - Directory: ./
```

#### 2. Configure Project Settings

**Build Configuration**:

```bash
# Framework: Next.js
# Build Command: npm run build
# Output Directory: .next
# Install Command: npm install
# Root Directory: data-explorer/
```

**Environment Variables** (see next section)

#### 3. Configure Domains

```bash
# Add production domain
vercel domains add data.landbruget.dk --project=landbruget-data-explorer

# Add staging domain (optional)
vercel domains add data-staging.landbruget.dk --project=landbruget-data-explorer

# Verify domains
vercel domains ls
```

#### 4. Configure Git Integration

In Vercel dashboard:

1. Go to **Settings** → **Git**
2. Configure branch deployments:
   - **Production Branch**: `main`
   - **Preview Branches**: All branches
   - **Automatic Deployments**: Enabled

3. Enable **Vercel for GitHub** features:
   - Deploy on push
   - Preview deployments on PRs
   - Deployment status checks

---

## Environment Variables

### Required Variables

Set these in Vercel dashboard or via CLI:

```bash
# Production environment
vercel env add NEXT_PUBLIC_R2_URL production
# Value: https://r2.landbruget.dk

vercel env add GOOGLE_API_KEY production
# Value: [Your Google API Key - keep secret!]

# Preview environment (for PR previews)
vercel env add NEXT_PUBLIC_R2_URL preview
# Value: https://r2.landbruget.dk

vercel env add GOOGLE_API_KEY preview
# Value: [Same or separate API key]

# Development environment (optional)
vercel env add NEXT_PUBLIC_R2_URL development
# Value: http://localhost:3000/api/mock-data (or R2 URL)

vercel env add GOOGLE_API_KEY development
# Value: [Development API key]
```

### Environment Variable Reference

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `NEXT_PUBLIC_R2_URL` | Cloudflare R2 bucket URL | `https://r2.landbruget.dk` | **Yes** |
| `GOOGLE_API_KEY` | Google Gemini API key | `AIzaSy...` | **Yes** |
| `NODE_ENV` | Node environment | `production` | Auto-set |
| `NEXT_PUBLIC_VERCEL_ENV` | Deployment environment | `production` | Auto-set |
| `VERCEL_URL` | Deployment URL | `*.vercel.app` | Auto-set |

### Obtaining API Keys

#### Google Gemini API Key

1. Visit [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Sign in with Google account
3. Click **"Create API Key"**
4. Select project or create new one
5. Copy API key (starts with `AIza...`)
6. Set usage limits (optional but recommended):
   - Navigate to [Google Cloud Console](https://console.cloud.google.com)
   - Go to **APIs & Services** → **Credentials**
   - Click your API key → **API restrictions**
   - Restrict to "Generative Language API"
   - Set quota limits under **Quotas & System Limits**

**Security Best Practices**:
- Use separate API keys for production/staging/development
- Rotate keys every 90 days
- Monitor usage in Google Cloud Console
- Set budget alerts to avoid unexpected costs

---

## Deployment Process

### Initial Deployment

```bash
# 1. Ensure you're in the data-explorer directory
cd data-explorer

# 2. Install dependencies
npm install

# 3. Build locally to verify (optional)
npm run build

# 4. Deploy to production
vercel --prod

# 5. Verify deployment
vercel ls
```

### Subsequent Deployments

**Automatic (Recommended)**:
- Push to `main` branch triggers automatic production deployment
- Push to other branches creates preview deployments
- Pull requests get unique preview URLs automatically

**Manual**:
```bash
# Deploy to production
vercel --prod

# Deploy to preview
vercel

# Deploy specific branch to production
vercel --prod --branch=release/v1.0
```

### Deployment Pipeline (GitHub Actions)

Create `.github/workflows/deploy-data-explorer.yml`:

```yaml
name: Deploy Data Explorer

on:
  push:
    branches: [main, staging]
    paths:
      - 'data-explorer/**'
  pull_request:
    paths:
      - 'data-explorer/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'npm'
          cache-dependency-path: data-explorer/package-lock.json

      - name: Install dependencies
        working-directory: data-explorer
        run: npm ci

      - name: Build
        working-directory: data-explorer
        run: npm run build
        env:
          NEXT_PUBLIC_R2_URL: ${{ secrets.NEXT_PUBLIC_R2_URL }}
          GOOGLE_API_KEY: ${{ secrets.GOOGLE_API_KEY }}

      - name: Deploy to Vercel
        working-directory: data-explorer
        run: |
          if [ "${{ github.ref }}" = "refs/heads/main" ]; then
            vercel --prod --token=${{ secrets.VERCEL_TOKEN }}
          else
            vercel --token=${{ secrets.VERCEL_TOKEN }}
          fi
        env:
          VERCEL_ORG_ID: ${{ secrets.VERCEL_ORG_ID }}
          VERCEL_PROJECT_ID: ${{ secrets.VERCEL_PROJECT_ID }}
```

**Required GitHub Secrets**:
- `VERCEL_TOKEN`: Vercel authentication token
- `VERCEL_ORG_ID`: Your Vercel organization ID
- `VERCEL_PROJECT_ID`: Project ID from Vercel
- `NEXT_PUBLIC_R2_URL`: R2 bucket URL
- `GOOGLE_API_KEY`: Google Gemini API key

---

## Post-Deployment Testing

### Automated Testing Checklist

Run these tests after every deployment:

```bash
# 1. Health check
curl https://data.landbruget.dk/

# 2. API endpoint check
curl https://data.landbruget.dk/api/ask \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me the first 5 rows"}'

# 3. Static asset loading
curl -I https://data.landbruget.dk/_next/static/

# 4. CORS headers check
curl -I https://r2.landbruget.dk/bronze/agriculture_data.parquet \
  -H "Origin: https://data.landbruget.dk"
```

### Manual Testing Checklist

- [ ] **Homepage loads** (`/`)
  - [ ] UI renders correctly
  - [ ] No console errors
  - [ ] Assets load (images, fonts, icons)

- [ ] **Data explorer loads** (`/explore`)
  - [ ] Dataset list appears
  - [ ] SQL editor renders
  - [ ] Query execution works

- [ ] **Natural language queries work**
  - [ ] Input field accepts text
  - [ ] AI generates SQL (1-3 second response)
  - [ ] Generated SQL is valid
  - [ ] Results display correctly

- [ ] **Data loading from R2**
  - [ ] Parquet files load successfully
  - [ ] No CORS errors
  - [ ] Query results display

- [ ] **Responsive design**
  - [ ] Mobile layout (< 768px)
  - [ ] Tablet layout (768px - 1024px)
  - [ ] Desktop layout (> 1024px)

- [ ] **Performance**
  - [ ] First load < 3 seconds
  - [ ] Query execution < 5 seconds
  - [ ] Page navigation smooth

### Testing Script

Create `scripts/test-deployment.sh`:

```bash
#!/bin/bash
set -euo pipefail

BASE_URL="${1:-https://data.landbruget.dk}"

echo "Testing deployment at: $BASE_URL"
echo

# Test 1: Homepage
echo "✓ Testing homepage..."
if curl -sf "$BASE_URL" > /dev/null; then
  echo "  ✓ Homepage accessible"
else
  echo "  ✗ Homepage failed"
  exit 1
fi

# Test 2: API endpoint
echo "✓ Testing API endpoint..."
RESPONSE=$(curl -sf "$BASE_URL/api/ask" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"question":"test"}' || echo "failed")

if [ "$RESPONSE" != "failed" ]; then
  echo "  ✓ API endpoint responding"
else
  echo "  ✗ API endpoint failed"
  exit 1
fi

# Test 3: Static assets
echo "✓ Testing static assets..."
if curl -sf "$BASE_URL/_next/static/" > /dev/null; then
  echo "  ✓ Static assets accessible"
else
  echo "  ✗ Static assets failed"
  exit 1
fi

echo
echo "All tests passed! ✓"
```

Usage:
```bash
chmod +x scripts/test-deployment.sh
./scripts/test-deployment.sh https://data.landbruget.dk
```

---

## Monitoring & Maintenance

### Vercel Monitoring

**Built-in Metrics**:
- **Deployment Status**: Real-time build/deploy tracking
- **Performance**: Core Web Vitals (LCP, FID, CLS)
- **Function Logs**: Serverless function execution logs
- **Analytics**: Page views, user geography, device types

Access via: [Vercel Dashboard](https://vercel.com/dashboard) → Your Project → Analytics

**Key Metrics to Monitor**:
| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Uptime | 99.9% | < 99% |
| P95 Response Time | < 1s | > 3s |
| Error Rate | < 0.1% | > 1% |
| Build Success Rate | 100% | < 95% |

### Cloudflare Analytics

**R2 Bucket Metrics**:
- Storage used (GB)
- Class A operations (writes)
- Class B operations (reads)
- Egress bandwidth

Access via: Cloudflare Dashboard → R2 → landbruget-data-explorer → Metrics

**CDN Metrics**:
- Requests per second
- Bandwidth usage
- Cache hit ratio
- Error rates (4xx, 5xx)

Access via: Cloudflare Dashboard → Analytics & Logs

### Google Gemini API Monitoring

**Usage Tracking**:
```bash
# Install Google Cloud SDK
gcloud init

# Set project
gcloud config set project YOUR_PROJECT_ID

# View API usage
gcloud monitoring time-series list \
  --filter='metric.type="serviceruntime.googleapis.com/api/request_count" AND resource.labels.service="generativelanguage.googleapis.com"' \
  --format=json
```

**Key Metrics**:
- Requests per day (free tier: 1,500/day)
- Requests per minute (free tier: 15/min)
- Average response time
- Error rate

### Error Tracking

**Vercel Error Logs**:
```bash
# View real-time logs
vercel logs landbruget-data-explorer --follow

# Filter by severity
vercel logs landbruget-data-explorer --level=error

# View specific deployment
vercel logs [deployment-url]
```

**Client-Side Error Tracking** (Optional - Sentry Integration):

1. Install Sentry:
```bash
npm install @sentry/nextjs
```

2. Configure `sentry.client.config.ts`:
```typescript
import * as Sentry from "@sentry/nextjs";

Sentry.init({
  dsn: process.env.NEXT_PUBLIC_SENTRY_DSN,
  environment: process.env.NEXT_PUBLIC_VERCEL_ENV || "development",
  tracesSampleRate: 0.1,
});
```

3. Add to `next.config.ts`:
```typescript
const { withSentryConfig } = require("@sentry/nextjs");

module.exports = withSentryConfig(nextConfig, {
  silent: true,
});
```

### Uptime Monitoring

**External Monitoring** (Recommended):

Use services like:
- **UptimeRobot**: Free tier (5-minute intervals)
- **Better Uptime**: Advanced alerting
- **Pingdom**: Comprehensive monitoring

**Simple Health Check Endpoint**:

Create `src/app/api/health/route.ts`:
```typescript
import { NextResponse } from 'next/server';

export async function GET() {
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    checks: {
      r2: process.env.NEXT_PUBLIC_R2_URL ? 'ok' : 'missing',
      gemini: process.env.GOOGLE_API_KEY ? 'ok' : 'missing',
    }
  };

  return NextResponse.json(health);
}
```

Monitor: `https://data.landbruget.dk/api/health`

### Maintenance Schedule

**Daily**:
- [ ] Check Vercel deployment status
- [ ] Review error logs
- [ ] Monitor API usage (Gemini quota)

**Weekly**:
- [ ] Review performance metrics
- [ ] Check R2 storage usage
- [ ] Update dependencies (security patches)

**Monthly**:
- [ ] Review and optimize costs
- [ ] Update data files in R2
- [ ] Review and rotate API keys
- [ ] Performance optimization review

**Quarterly**:
- [ ] Major dependency updates
- [ ] Security audit
- [ ] Load testing
- [ ] Disaster recovery test

---

## Cost Breakdown

### Monthly Cost Estimate (Production)

#### Vercel (Pro Plan)

| Resource | Included | Overage Cost | Estimated Use | Monthly Cost |
|----------|----------|--------------|---------------|--------------|
| **Bandwidth** | 1 TB | $40/TB | 500 GB | $0 |
| **Function Executions** | 1M | $40/1M | 500K | $0 |
| **Function Duration** | 100 GB-hours | $60/100 GB-hours | 50 GB-hours | $0 |
| **Build Minutes** | 6,000 | - | 200 | $0 |
| **Pro Plan** | - | - | - | **$20** |

**Total Vercel**: ~$20/month (within Pro plan limits)

#### Cloudflare R2

| Resource | Pricing | Estimated Use | Monthly Cost |
|----------|---------|---------------|--------------|
| **Storage** | $0.015/GB | 50 GB | $0.75 |
| **Class A Operations** | $4.50/million | 10K | $0.05 |
| **Class B Operations** | $0.36/million | 1M | $0.36 |
| **Egress** | Free (zero egress fees!) | Unlimited | $0 |

**Total R2**: ~$1.16/month

#### Google Gemini API

| Tier | Requests/Day | Requests/Month | Cost |
|------|--------------|----------------|------|
| **Free** | 1,500 | 45,000 | $0 |
| **Pay-as-you-go** | Unlimited | - | $0.001/request |

Estimated usage for 1,000 active users:
- Average: 10 queries/user/month = 10,000 queries
- Within free tier: **$0**

**Total Gemini API**: $0/month (free tier sufficient for MVP)

#### Cloudflare (DNS + CDN)

| Service | Cost |
|---------|------|
| **DNS** | Free |
| **CDN** | Free (no usage limits) |
| **SSL/TLS** | Free |

**Total Cloudflare**: $0/month

### Total Monthly Cost: ~$21/month

### Cost Optimization Tips

1. **Reduce Vercel Bandwidth**:
   - Enable Cloudflare caching
   - Optimize image sizes
   - Use WebP format
   - Implement lazy loading

2. **Reduce R2 Operations**:
   - Cache Parquet file metadata
   - Batch queries when possible
   - Use HTTP caching headers

3. **Optimize Gemini API Usage**:
   - Cache common SQL patterns
   - Implement client-side rate limiting
   - Use smaller model variant if available

4. **Alternative: Hobby Plan**:
   - Vercel Hobby: $0/month (limited to personal projects)
   - All other costs remain same
   - **Total: ~$1/month**

### Scaling Costs

**At 10,000 active users**:
| Resource | Monthly Cost |
|----------|--------------|
| Vercel (Pro) | $20-50 |
| Cloudflare R2 | $5-10 |
| Gemini API | $50-100 |
| **Total** | **$75-160/month** |

**At 100,000 active users**:
| Resource | Monthly Cost |
|----------|--------------|
| Vercel (Enterprise) | $500-1,000 |
| Cloudflare R2 | $50-100 |
| Gemini API | $500-1,000 |
| **Total** | **$1,050-2,100/month** |

---

## Troubleshooting

### Common Deployment Issues

#### 1. Build Failures

**Symptom**: Vercel build fails with error message

**Causes & Solutions**:

```bash
# Error: "Cannot find module 'X'"
# Solution: Install missing dependency
npm install X

# Error: "Type error in file Y.tsx"
# Solution: Fix TypeScript errors locally first
npm run build  # Test locally

# Error: "Out of memory"
# Solution: Increase Node memory limit
# In package.json:
"scripts": {
  "build": "NODE_OPTIONS=--max_old_space_size=4096 next build"
}

# Error: "WASM compilation failed"
# Solution: Ensure asyncWebAssembly is enabled in next.config.ts
webpack: (config) => {
  config.experiments = {
    ...config.experiments,
    asyncWebAssembly: true,
  };
  return config;
}
```

#### 2. CORS Errors Loading Parquet Files

**Symptom**: Browser console shows `Access to fetch at 'https://r2.landbruget.dk/...' has been blocked by CORS policy`

**Solution**:

```bash
# 1. Verify CORS configuration
wrangler r2 bucket cors get landbruget-data-explorer

# 2. Re-apply CORS config
wrangler r2 bucket cors put landbruget-data-explorer --config r2-cors-config.json

# 3. Check allowed origins include your domain
# Ensure r2-cors-config.json includes:
{
  "AllowedOrigins": [
    "https://data.landbruget.dk",
    "https://*.vercel.app"
  ]
}

# 4. Clear browser cache and test
# Open DevTools → Application → Clear storage
```

#### 3. Environment Variables Not Working

**Symptom**: Application can't connect to R2 or Gemini API

**Solutions**:

```bash
# 1. Verify variables are set
vercel env ls

# 2. Check variable names (must be exact)
# ❌ WRONG: R2_URL
# ✅ CORRECT: NEXT_PUBLIC_R2_URL

# 3. Redeploy after adding/changing variables
vercel --prod --force

# 4. Check variable values (no typos)
vercel env pull .env.local
cat .env.local

# 5. Ensure NEXT_PUBLIC_ prefix for client-side variables
# Client-side (browser): NEXT_PUBLIC_*
# Server-side only: No prefix
```

#### 4. Slow Query Performance

**Symptom**: Queries take > 10 seconds to execute

**Causes & Solutions**:

```typescript
// 1. Large Parquet files
// Solution: Implement file size limits
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100 MB
if (fileSize > MAX_FILE_SIZE) {
  throw new Error('File too large for browser processing');
}

// 2. Missing indexes in DuckDB
// Solution: Create indexes after loading
await conn.run(`
  CREATE INDEX idx_cvr ON agriculture_data(cvr);
  CREATE INDEX idx_date ON agriculture_data(date);
`);

// 3. Inefficient SQL queries
// Solution: Optimize generated SQL
- Add LIMIT clauses
- Use specific columns (not SELECT *)
- Add WHERE filters early
- Use appropriate indexes

// 4. Network latency to R2
// Solution: Enable Cloudflare caching
// In Cloudflare Dashboard:
// - Cache Level: Standard
// - Browser Cache TTL: 1 hour
// - Edge Cache TTL: 1 day
```

#### 5. API Rate Limiting (Gemini)

**Symptom**: `429 Too Many Requests` error

**Solutions**:

```typescript
// 1. Implement exponential backoff
async function retryWithBackoff(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (error.status === 429 && i < maxRetries - 1) {
        const delay = Math.pow(2, i) * 1000; // 1s, 2s, 4s
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      throw error;
    }
  }
}

// 2. Implement client-side rate limiting
const rateLimiter = {
  requests: [],
  maxRequests: 15,
  windowMs: 60000, // 1 minute

  async throttle() {
    const now = Date.now();
    this.requests = this.requests.filter(t => now - t < this.windowMs);

    if (this.requests.length >= this.maxRequests) {
      const oldestRequest = this.requests[0];
      const waitTime = this.windowMs - (now - oldestRequest);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }

    this.requests.push(now);
  }
};

// 3. Cache frequent queries
const queryCache = new Map();
const cacheKey = `sql:${question}`;
if (queryCache.has(cacheKey)) {
  return queryCache.get(cacheKey);
}

// 4. Upgrade API plan if needed
// Visit: https://console.cloud.google.com/billing
```

#### 6. Domain Not Resolving

**Symptom**: `data.landbruget.dk` shows "DNS_PROBE_FINISHED_NXDOMAIN"

**Solutions**:

```bash
# 1. Verify DNS records
dig data.landbruget.dk

# 2. Check Vercel domain status
vercel domains ls

# 3. Verify Cloudflare proxy is enabled
# In Cloudflare Dashboard → DNS:
# - Orange cloud icon should be on (Proxied)

# 4. Wait for DNS propagation (up to 48 hours)
# Check propagation status:
# https://www.whatsmydns.net/#CNAME/data.landbruget.dk

# 5. Flush local DNS cache
# macOS:
sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder

# Windows:
ipconfig /flushdns

# Linux:
sudo systemd-resolve --flush-caches
```

#### 7. Deployment Succeeds but Site Shows 404

**Symptom**: Deployment successful but accessing URL shows 404

**Solutions**:

```bash
# 1. Check root directory configuration
vercel projects ls
# Verify "Root Directory" is set to "data-explorer"

# 2. Verify build output
vercel logs [deployment-url]
# Look for ".next directory created successfully"

# 3. Check deployment URL directly (bypass CDN)
# Use the .vercel.app URL first
# Example: https://landbruget-data-explorer-abc123.vercel.app

# 4. Re-link domain if needed
vercel domains remove data.landbruget.dk
vercel domains add data.landbruget.dk

# 5. Check Vercel configuration
# Visit: https://vercel.com/[team]/landbruget-data-explorer/settings
```

### Debug Mode

Enable verbose logging:

```typescript
// src/lib/debug.ts
export const DEBUG = process.env.NODE_ENV === 'development' ||
                     process.env.NEXT_PUBLIC_DEBUG === 'true';

export function debugLog(...args: any[]) {
  if (DEBUG) {
    console.log('[DEBUG]', new Date().toISOString(), ...args);
  }
}

// Usage in components:
import { debugLog } from '@/lib/debug';

debugLog('Loading parquet file:', fileUrl);
debugLog('Query executed in:', executionTime, 'ms');
```

Set in Vercel:
```bash
vercel env add NEXT_PUBLIC_DEBUG development
# Value: true
```

---

## Rollback Procedures

### Quick Rollback via Vercel Dashboard

1. Navigate to [Vercel Dashboard](https://vercel.com/dashboard)
2. Select **landbruget-data-explorer** project
3. Go to **Deployments** tab
4. Find last known good deployment
5. Click **⋮** (three dots) → **Promote to Production**
6. Confirm promotion

**Time to rollback**: ~30 seconds

### Rollback via CLI

```bash
# 1. List recent deployments
vercel ls landbruget-data-explorer

# 2. Find deployment URL of last known good version
# Example: https://landbruget-data-explorer-abc123.vercel.app

# 3. Promote to production
vercel promote [deployment-url] --prod

# Verify
curl -I https://data.landbruget.dk
```

### Rollback via Git

```bash
# 1. Identify problematic commit
git log --oneline

# 2. Revert the commit
git revert [commit-hash]

# 3. Push to trigger redeployment
git push origin main

# Alternative: Force rollback to specific commit
git reset --hard [good-commit-hash]
git push --force origin main  # ⚠️ Use with caution!
```

### Emergency: Maintenance Mode

Create `src/middleware.ts`:

```typescript
import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

export function middleware(request: NextRequest) {
  const maintenanceMode = process.env.MAINTENANCE_MODE === 'true';

  if (maintenanceMode && !request.nextUrl.pathname.startsWith('/maintenance')) {
    return NextResponse.redirect(new URL('/maintenance', request.url));
  }

  return NextResponse.next();
}
```

Create `src/app/maintenance/page.tsx`:

```typescript
export default function MaintenancePage() {
  return (
    <div className="flex min-h-screen items-center justify-center">
      <div className="text-center">
        <h1 className="text-3xl font-bold mb-4">Under Maintenance</h1>
        <p className="text-gray-600">
          We're making improvements. Back soon!
        </p>
      </div>
    </div>
  );
}
```

Enable maintenance mode:
```bash
vercel env add MAINTENANCE_MODE production
# Value: true

# Redeploy
vercel --prod
```

Disable:
```bash
vercel env rm MAINTENANCE_MODE production
vercel --prod
```

---

## Security Checklist

Before going live:

- [ ] **Environment Variables**
  - [ ] No hardcoded secrets in code
  - [ ] API keys use `NEXT_PUBLIC_` prefix only when needed
  - [ ] Separate keys for prod/staging/dev
  - [ ] API keys rotated in last 90 days

- [ ] **CORS Configuration**
  - [ ] R2 CORS only allows specific origins
  - [ ] No wildcard (`*`) origins in production
  - [ ] CORS headers tested and working

- [ ] **Domain & SSL**
  - [ ] HTTPS enforced (no HTTP access)
  - [ ] SSL certificate valid and auto-renewing
  - [ ] TLS 1.2+ required
  - [ ] HSTS header enabled

- [ ] **Access Control**
  - [ ] Cloudflare Access configured (if applicable)
  - [ ] Rate limiting enabled
  - [ ] DDoS protection active (Cloudflare)

- [ ] **Code Security**
  - [ ] Dependencies updated (no critical vulnerabilities)
  - [ ] TypeScript strict mode enabled
  - [ ] ESLint security rules passing
  - [ ] No sensitive data in error messages

- [ ] **Monitoring**
  - [ ] Error tracking configured
  - [ ] Uptime monitoring active
  - [ ] Alert notifications set up
  - [ ] Log retention configured

---

## Support & Resources

### Documentation
- [Next.js Deployment](https://nextjs.org/docs/deployment)
- [Vercel Documentation](https://vercel.com/docs)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [DuckDB WASM Documentation](https://duckdb.org/docs/api/wasm/)

### Dashboards
- **Vercel**: https://vercel.com/dashboard
- **Cloudflare**: https://dash.cloudflare.com
- **Google Cloud Console**: https://console.cloud.google.com
- **GitHub Actions**: https://github.com/[org]/[repo]/actions

### Community
- [Next.js Discord](https://discord.gg/nextjs)
- [Vercel Community](https://github.com/vercel/vercel/discussions)
- [DuckDB Discord](https://discord.duckdb.org)

### Emergency Contacts
- DevOps Team: devops@landbruget.dk
- Platform Issues: platform-alerts@landbruget.dk
- Security Issues: security@landbruget.dk

---

**Last Updated**: 2026-01-10

**Version**: 1.0.0

**Maintained By**: Landbruget.dk DevOps Team
