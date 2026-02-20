# Deploy Now - Quick Start Guide

Deploy the data explorer to production in under 30 minutes.

---

## Prerequisites (5 minutes)

Install required tools:

```bash
# Vercel CLI
npm install -g vercel

# Wrangler (Cloudflare CLI)
npm install -g wrangler

# Login to services
vercel login
wrangler login
```

Get API keys:
- Google Gemini API: https://aistudio.google.com/app/apikey

---

## Step 1: Setup Cloudflare R2 (10 minutes)

```bash
# Navigate to project
cd data-explorer

# Create R2 bucket
wrangler r2 bucket create landbruget-data-explorer --jurisdiction eu

# Configure CORS
wrangler r2 bucket cors put landbruget-data-explorer \
  --config r2-cors-config.json

# Setup custom domain
wrangler r2 bucket domain add landbruget-data-explorer r2.landbruget.dk

# Upload test data (if available)
./scripts/upload-to-r2.sh ../backend/data
```

**Verify**: `wrangler r2 bucket list` should show `landbruget-data-explorer`

---

## Step 2: Configure Vercel (10 minutes)

```bash
# Link project (creates .vercel directory)
vercel link
# Follow prompts:
# - Setup new project? Yes
# - Project name: landbruget-data-explorer
# - Directory: ./ (current)

# Add environment variables
vercel env add NEXT_PUBLIC_R2_URL production
# Enter: https://r2.landbruget.dk

vercel env add GOOGLE_API_KEY production
# Enter: [Your Google API Key]

# Also add for preview environment
vercel env add NEXT_PUBLIC_R2_URL preview
# Enter: https://r2.landbruget.dk

vercel env add GOOGLE_API_KEY preview
# Enter: [Your Google API Key]
```

**Verify**: `vercel env ls` should show 4 variables

---

## Step 3: Configure DNS (5 minutes)

In Cloudflare Dashboard → DNS, add these records:

```
Type: CNAME
Name: data
Target: cname.vercel-dns.com
Proxy: ON (orange cloud)

Type: CNAME
Name: r2
Target: landbruget-data-explorer.r2.cloudflarestorage.com
Proxy: ON (orange cloud)
```

**Verify**: `dig data.landbruget.dk` should show CNAME record

---

## Step 4: Add Domain to Vercel (2 minutes)

```bash
# Add production domain
vercel domains add data.landbruget.dk

# Verify
vercel domains ls
```

**Verify**: Domain status should be "Ready"

---

## Step 5: Deploy (3 minutes)

```bash
# Build and deploy to production
vercel --prod

# Wait for deployment to complete...
# Note the deployment URL
```

**Example output**:
```
✔ Production: https://data.landbruget.dk [copied to clipboard]
```

---

## Step 6: Verify Deployment (5 minutes)

```bash
# Run automated tests
./scripts/test-deployment.sh https://data.landbruget.dk
```

**Expected**: All tests should pass ✓

Manual verification:
1. Visit https://data.landbruget.dk
2. Navigate to `/explore`
3. Try a natural language query: "Show me the first 10 rows"
4. Verify results display

---

## Complete!

Your data explorer is now live at: **https://data.landbruget.dk**

### Next Steps

1. **Setup Monitoring**:
   - Configure uptime monitoring (UptimeRobot recommended)
   - Enable Vercel Analytics
   - Set up error alerts

2. **Setup CI/CD**:
   ```bash
   # Add GitHub secrets for automated deployments
   # See: .github/workflows/deploy.yml
   ```

3. **Review Security**:
   - Rotate API keys every 90 days
   - Enable rate limiting
   - Configure budget alerts

4. **Documentation**:
   - Read [DEPLOYMENT.md](./DEPLOYMENT.md) for complete details
   - Review [DEPLOYMENT_CHECKLIST.md](./DEPLOYMENT_CHECKLIST.md)
   - Bookmark [docs/DEPLOYMENT_SUMMARY.md](./docs/DEPLOYMENT_SUMMARY.md) for quick reference

---

## Troubleshooting

### Deployment fails

```bash
# Test build locally first
npm run build

# Check logs
vercel logs --level=error
```

### Domain not resolving

```bash
# Check DNS propagation
dig data.landbruget.dk

# Verify Vercel domain status
vercel domains ls

# Wait up to 48 hours for DNS propagation
```

### CORS errors

```bash
# Verify CORS configuration
wrangler r2 bucket cors get landbruget-data-explorer

# Re-apply if needed
wrangler r2 bucket cors put landbruget-data-explorer \
  --config r2-cors-config.json
```

### Environment variables not working

```bash
# Pull and verify
vercel env pull .env.local
cat .env.local

# Force redeploy
vercel --prod --force
```

---

## Rollback

If something goes wrong:

```bash
# List recent deployments
vercel ls landbruget-data-explorer

# Promote previous deployment
vercel promote [previous-deployment-url] --prod
```

Or via dashboard:
1. Go to https://vercel.com/dashboard
2. Select project → Deployments
3. Find last known good deployment
4. Click ⋮ → "Promote to Production"

---

## Cost Estimate

| Service | Monthly Cost |
|---------|--------------|
| Vercel Pro | $20 |
| Cloudflare R2 | ~$1 |
| Google Gemini | $0 (free tier) |
| **Total** | **~$21/month** |

---

## Support

- **Deployment Issues**: See [DEPLOYMENT.md](./DEPLOYMENT.md#troubleshooting)
- **Quick Reference**: See [docs/DEPLOYMENT_SUMMARY.md](./docs/DEPLOYMENT_SUMMARY.md)
- **Complete Checklist**: See [DEPLOYMENT_CHECKLIST.md](./DEPLOYMENT_CHECKLIST.md)

---

**Total Time**: ~30-40 minutes

**Difficulty**: Beginner-friendly

**Last Updated**: 2026-01-10
