# Deployment Checklist

Use this checklist to ensure a smooth deployment to production.

---

## Pre-Deployment

### Code Quality

- [ ] All TypeScript errors resolved (`npx tsc --noEmit`)
- [ ] ESLint passes with no errors (`npm run lint`)
- [ ] All tests passing locally
- [ ] No console.log statements in production code
- [ ] No commented-out code blocks
- [ ] No TODO comments for critical issues

### Build Verification

- [ ] Production build completes successfully (`npm run build`)
- [ ] No build warnings that indicate problems
- [ ] Bundle size is reasonable (< 1MB initial load)
- [ ] Source maps generated correctly
- [ ] Environment variables are set correctly

### Dependencies

- [ ] All dependencies up to date with security patches
- [ ] No critical vulnerabilities (`npm audit`)
- [ ] Package-lock.json is committed
- [ ] No unused dependencies

---

## Infrastructure Setup

### Cloudflare R2

- [ ] Bucket created: `landbruget-data-explorer`
- [ ] Custom domain configured: `r2.landbruget.dk`
- [ ] CORS configuration applied (see `r2-cors-config.json`)
- [ ] Test Parquet files uploaded
- [ ] Public read access configured
- [ ] Storage quota appropriate for data size

#### CORS Verification

```bash
# Verify CORS settings
wrangler r2 bucket cors get landbruget-data-explorer

# Test CORS from production domain
curl -I https://r2.landbruget.dk/bronze/test.parquet \
  -H "Origin: https://data.landbruget.dk"
```

### Vercel

- [ ] Project created: `landbruget-data-explorer`
- [ ] GitHub integration enabled
- [ ] Production domain added: `data.landbruget.dk`
- [ ] Staging domain added (optional): `data-staging.landbruget.dk`
- [ ] Build settings configured correctly
- [ ] Node.js version: 20.x
- [ ] Root directory: `data-explorer/`

### Domain & DNS

- [ ] DNS records created in Cloudflare:
  - [ ] `data.landbruget.dk` → Vercel
  - [ ] `r2.landbruget.dk` → R2 bucket
- [ ] SSL/TLS mode: Full (strict)
- [ ] Always Use HTTPS: Enabled
- [ ] Automatic HTTPS Rewrites: Enabled
- [ ] Minimum TLS Version: 1.2
- [ ] HSTS enabled (after DNS propagation confirmed)

---

## Environment Variables

### Vercel Environment Variables

Configure in Vercel Dashboard → Settings → Environment Variables:

#### Production
- [ ] `NEXT_PUBLIC_R2_URL` = `https://r2.landbruget.dk`
- [ ] `GOOGLE_API_KEY` = `[Your production API key]`

#### Preview
- [ ] `NEXT_PUBLIC_R2_URL` = `https://r2.landbruget.dk`
- [ ] `GOOGLE_API_KEY` = `[Your preview API key]`

#### Development (Optional)
- [ ] `NEXT_PUBLIC_R2_URL` = `https://r2.landbruget.dk`
- [ ] `GOOGLE_API_KEY` = `[Your development API key]`

### Verification

```bash
# Pull environment variables locally
vercel env pull .env.local

# Verify all required variables are set
cat .env.local
```

---

## API Keys & Secrets

### Google Gemini API

- [ ] API key created at [Google AI Studio](https://aistudio.google.com/app/apikey)
- [ ] API restrictions configured:
  - [ ] Restricted to "Generative Language API"
  - [ ] Application restrictions set (optional)
- [ ] Quota limits set:
  - [ ] Daily limit: 1,500 requests (free tier) or higher
  - [ ] Rate limit: 15 requests/minute (free tier) or higher
- [ ] Budget alerts configured
- [ ] Usage monitoring enabled

### Secret Rotation Plan

- [ ] API key rotation schedule: Every 90 days
- [ ] Documented procedure for key rotation
- [ ] Emergency key revocation procedure documented

---

## Deployment Process

### Initial Deployment

```bash
# 1. Login to Vercel
vercel login

# 2. Link project
cd data-explorer
vercel link

# 3. Deploy to production
vercel --prod

# 4. Verify deployment
vercel ls
```

### GitHub Actions (Automated)

- [ ] `.github/workflows/deploy.yml` configured
- [ ] GitHub secrets configured:
  - [ ] `VERCEL_TOKEN`
  - [ ] `VERCEL_ORG_ID`
  - [ ] `VERCEL_PROJECT_ID`
  - [ ] `NEXT_PUBLIC_R2_URL`
  - [ ] `GOOGLE_API_KEY`
- [ ] Workflow triggers on push to `main` branch
- [ ] Post-deployment tests configured

---

## Post-Deployment Testing

### Automated Tests

```bash
# Run deployment health checks
chmod +x scripts/test-deployment.sh
./scripts/test-deployment.sh https://data.landbruget.dk
```

### Manual Tests

#### Basic Functionality
- [ ] Homepage loads (`/`)
- [ ] No console errors
- [ ] All assets load (CSS, JS, fonts, images)
- [ ] Responsive design works (mobile, tablet, desktop)

#### Data Explorer
- [ ] Explorer page loads (`/explore`)
- [ ] Dataset list appears
- [ ] SQL editor renders correctly
- [ ] Sample query executes successfully
- [ ] Results display in table

#### Natural Language Queries
- [ ] NL input field accepts text
- [ ] Simple query works: "Show me the first 10 rows"
- [ ] Response time < 3 seconds
- [ ] Generated SQL is valid
- [ ] Error handling works for invalid queries

#### Data Loading
- [ ] Parquet files load from R2
- [ ] No CORS errors in console
- [ ] File download progress shown
- [ ] Large files (> 50MB) load successfully
- [ ] Query results display correctly

#### Performance
- [ ] First contentful paint < 2s
- [ ] Time to interactive < 3s
- [ ] Query execution < 5s for typical queries
- [ ] No memory leaks during extended use

### Cross-Browser Testing

- [ ] Chrome (latest)
- [ ] Firefox (latest)
- [ ] Safari (latest)
- [ ] Edge (latest)
- [ ] Mobile Safari (iOS 15+)
- [ ] Chrome Mobile (Android)

### Load Testing

```bash
# Simple load test using Apache Bench (optional)
ab -n 100 -c 10 https://data.landbruget.dk/

# Or use a load testing service:
# - Loader.io
# - k6.io
# - Artillery
```

---

## Monitoring Setup

### Vercel Analytics

- [ ] Analytics enabled in Vercel dashboard
- [ ] Core Web Vitals monitoring active
- [ ] Real User Monitoring (RUM) configured

### Error Tracking

- [ ] Sentry configured (optional)
- [ ] Error notifications enabled
- [ ] Error grouping configured
- [ ] Source maps uploaded

### Uptime Monitoring

- [ ] External uptime monitor configured:
  - [ ] UptimeRobot (free tier)
  - [ ] Better Uptime
  - [ ] Pingdom
- [ ] Monitor URL: `https://data.landbruget.dk/api/health`
- [ ] Check interval: 5 minutes
- [ ] Alert contacts configured
- [ ] SMS/email alerts enabled

### Log Monitoring

```bash
# View real-time logs
vercel logs landbruget-data-explorer --follow

# Filter by severity
vercel logs landbruget-data-explorer --level=error
```

---

## Security Checklist

### HTTPS & SSL
- [ ] HTTPS enforced (no HTTP access)
- [ ] Valid SSL certificate
- [ ] Certificate auto-renewal enabled
- [ ] HSTS header present
- [ ] TLS 1.2+ required

### Headers
- [ ] `X-Content-Type-Options: nosniff`
- [ ] `X-Frame-Options: DENY`
- [ ] `X-XSS-Protection: 1; mode=block`
- [ ] `Referrer-Policy: strict-origin-when-cross-origin`
- [ ] Content Security Policy configured (optional)

### API Security
- [ ] API keys not exposed in client code
- [ ] Rate limiting configured
- [ ] CORS properly configured
- [ ] No sensitive data in error messages

### Data Security
- [ ] No PII in logs
- [ ] No credentials in code/config
- [ ] Environment variables properly scoped
- [ ] Secrets stored securely in Vercel

---

## Performance Optimization

### CDN & Caching
- [ ] Cloudflare proxy enabled (orange cloud)
- [ ] Static assets cached (1 year)
- [ ] API routes not cached
- [ ] Browser caching configured

### Bundle Optimization
- [ ] Code splitting enabled
- [ ] Lazy loading implemented
- [ ] Tree shaking working
- [ ] Dynamic imports for large components

### Image Optimization
- [ ] Next.js Image component used
- [ ] Images compressed
- [ ] WebP format used where supported
- [ ] Lazy loading for below-fold images

### Database/Storage
- [ ] Parquet files optimized
- [ ] File sizes reasonable (< 100MB recommended)
- [ ] Partitioning strategy for large datasets
- [ ] Compression enabled

---

## Documentation

- [ ] README.md updated with production URLs
- [ ] DEPLOYMENT.md reviewed and accurate
- [ ] Environment variable documentation complete
- [ ] API documentation up to date
- [ ] Troubleshooting guide reviewed
- [ ] Runbook created for common operations

---

## Rollback Plan

### Preparation
- [ ] Last known good deployment URL documented
- [ ] Rollback procedure tested
- [ ] Team trained on rollback process
- [ ] Communication plan for rollback

### Rollback Procedure

```bash
# Via Vercel Dashboard:
# 1. Go to Deployments tab
# 2. Find last known good deployment
# 3. Click "Promote to Production"

# Via CLI:
vercel ls landbruget-data-explorer
vercel promote [good-deployment-url] --prod
```

---

## Communication

### Stakeholders
- [ ] Deployment scheduled and communicated
- [ ] Status page updated (if applicable)
- [ ] Team notified of deployment window
- [ ] Users informed of any expected downtime

### Post-Deployment
- [ ] Deployment announcement sent
- [ ] Known issues documented
- [ ] Support team briefed
- [ ] Monitoring dashboard shared

---

## Cost Monitoring

### Initial Setup
- [ ] Budget alerts configured in Google Cloud
- [ ] Vercel usage alerts enabled
- [ ] Cloudflare usage tracked
- [ ] Monthly cost review scheduled

### Expected Costs
- Vercel Pro: $20/month
- Cloudflare R2: ~$1/month (50GB)
- Google Gemini: $0 (free tier)
- **Total: ~$21/month**

---

## Maintenance Schedule

### Daily
- [ ] Check Vercel deployment status
- [ ] Review error logs
- [ ] Monitor API usage (Gemini quota)
- [ ] Verify uptime monitoring

### Weekly
- [ ] Review performance metrics
- [ ] Check R2 storage usage
- [ ] Update dependencies (security patches)
- [ ] Review and respond to user feedback

### Monthly
- [ ] Review and optimize costs
- [ ] Update data files in R2
- [ ] Review and rotate API keys (every 90 days)
- [ ] Performance optimization review
- [ ] Security audit

### Quarterly
- [ ] Major dependency updates
- [ ] Full security audit
- [ ] Load testing
- [ ] Disaster recovery test
- [ ] Documentation review

---

## Sign-off

### Deployment Team

- [ ] **Developer**: Code reviewed and tested
  - Name: ________________
  - Date: ________________

- [ ] **DevOps**: Infrastructure configured and verified
  - Name: ________________
  - Date: ________________

- [ ] **QA**: Testing complete and passing
  - Name: ________________
  - Date: ________________

- [ ] **Product Owner**: Approved for production
  - Name: ________________
  - Date: ________________

---

## Post-Deployment Notes

**Deployment Date**: ________________

**Deployment URL**: ________________

**Issues Encountered**:

_____________________________________

_____________________________________

**Resolutions Applied**:

_____________________________________

_____________________________________

**Performance Metrics**:
- First load time: ______ seconds
- API response time: ______ seconds
- Build time: ______ seconds

**Next Steps**:

_____________________________________

_____________________________________

---

**Last Updated**: 2026-01-10

**Version**: 1.0.0
