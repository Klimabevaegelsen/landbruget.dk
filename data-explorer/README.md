# Landbruget.dk Data Explorer

A modern, browser-based data exploration tool for Danish agricultural data. Built with Next.js 16, DuckDB WASM, and AI-powered natural language queries.

## Features

- **In-Browser SQL Queries**: Query Parquet files directly in your browser using DuckDB WASM
- **Natural Language Interface**: Ask questions in Danish or English, get SQL automatically generated
- **Interactive Tables**: Sort, filter, and explore results with TanStack Table
- **SQL Editor**: Full-featured CodeMirror editor with syntax highlighting
- **Zero Backend**: All data processing happens in the browser
- **Fast & Efficient**: Cloudflare R2 + Vercel Edge Network for global performance

---

## Quick Start

### Prerequisites

- Node.js 18+ installed
- Google API key for Gemini (free tier available)

### Installation

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.local.example .env.local

# 3. Add your Google API key to .env.local
# Get a free key at: https://aistudio.google.com/app/apikey
echo 'GOOGLE_API_KEY=your-api-key-here' >> .env.local

# 4. Start development server
npm run dev
```

Open [http://localhost:3000](http://localhost:3000)

### First Query

1. Navigate to `/explore`
2. Select a dataset from the sidebar
3. Type: "Show me the first 10 rows"
4. Click "Ask" and watch the magic happen

For detailed setup instructions, see [QUICK_START.md](./QUICK_START.md).

---

## Project Structure

```
data-explorer/
├── src/
│   ├── app/                    # Next.js App Router
│   │   ├── page.tsx           # Landing page
│   │   ├── explore/           # Main data exploration interface
│   │   ├── example/           # Example/demo page
│   │   └── api/
│   │       └── ask/           # Natural language to SQL API
│   ├── components/            # React components
│   │   ├── data-explorer/     # Core exploration UI
│   │   ├── sql-editor/        # SQL editing components
│   │   └── ui/                # Reusable UI primitives
│   └── lib/                   # Utilities and helpers
│       ├── duckdb/            # DuckDB WASM integration
│       └── gemini/            # Google Gemini API client
├── public/                    # Static assets
├── docs/                      # Documentation
├── DEPLOYMENT.md              # Production deployment guide
├── TESTING.md                 # Testing documentation
└── package.json
```

---

## Tech Stack

| Layer           | Technology        | Purpose                         |
| --------------- | ----------------- | ------------------------------- |
| **Framework**   | Next.js 16        | React framework with App Router |
| **Language**    | TypeScript        | Type-safe development           |
| **Styling**     | Tailwind CSS v4   | Utility-first CSS               |
| **Build Tool**  | Turbopack         | Fast bundler (dev mode)         |
| **Data Engine** | DuckDB WASM       | In-browser SQL database         |
| **AI**          | Google Gemini API | Natural language to SQL         |
| **Storage**     | Cloudflare R2     | Parquet file hosting            |
| **Deployment**  | Vercel            | Serverless hosting              |

### Key Dependencies

```json
{
  "dependencies": {
    "next": "16.x",
    "react": "19.x",
    "@duckdb/duckdb-wasm": "^1.33",
    "@google/generative-ai": "^0.24",
    "@tanstack/react-table": "^8.21",
    "codemirror": "^6.0"
  }
}
```

---

## Development

### Available Scripts

| Command         | Description                               |
| --------------- | ----------------------------------------- |
| `npm run dev`   | Start development server (with Turbopack) |
| `npm run build` | Build for production                      |
| `npm start`     | Start production server locally           |
| `npm run lint`  | Run ESLint                                |

### Development Workflow

1. **Make changes** to components in `src/`
2. **Hot reload** automatically updates in browser
3. **Check types**: `npx tsc --noEmit`
4. **Build locally**: `npm run build` (verify before deploying)

### Environment Variables

Required for development:

```bash
# .env.local
NEXT_PUBLIC_R2_URL=https://your-r2-bucket.r2.dev
GOOGLE_API_KEY=your-google-api-key-here
```

See [`.env.local.example`](./.env.local.example) for complete reference.

---

## Testing

### Manual Testing

```bash
# Start dev server
npm run dev

# Test checklist:
# ✓ Homepage loads (/)
# ✓ Explorer loads (/explore)
# ✓ SQL query executes
# ✓ Natural language query works
# ✓ Results display correctly
```

### Production Build Test

```bash
# Build and start production server
npm run build && npm start

# Verify:
# ✓ No build errors
# ✓ Assets optimized
# ✓ All routes accessible
```

For comprehensive testing procedures, see [TESTING.md](./TESTING.md) and [TESTING_CHECKLIST.md](./TESTING_CHECKLIST.md).

---

## Deployment

### Quick Deploy to Vercel

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy
vercel --prod
```

### Production Checklist

- [ ] Environment variables configured in Vercel
- [ ] Cloudflare R2 bucket created and configured
- [ ] Custom domain configured (`data.landbruget.dk`)
- [ ] CORS headers configured for R2
- [ ] Google API key set and quota monitored
- [ ] Post-deployment tests passed

For complete deployment instructions, see [DEPLOYMENT.md](./DEPLOYMENT.md).

### Deployment Architecture

```
┌─────────────────────────────────────────────────┐
│ User Browser                                    │
│  ↓                                             │
│ Cloudflare CDN (data.landbruget.dk)           │
│  ↓                                             │
│ Vercel Edge Network                            │
│  ↓                                             │
│ Next.js 15 App (Serverless)                   │
│  ↓              ↓                              │
│ DuckDB WASM   Google Gemini API               │
│  ↓                                             │
│ Cloudflare R2 (r2.landbruget.dk)              │
│  ↓                                             │
│ Parquet Files (Bronze/Silver/Gold)            │
└─────────────────────────────────────────────────┘
```

---

## Key Features

### 1. SQL Editor

Full-featured SQL editor powered by CodeMirror:

- Syntax highlighting
- Auto-completion
- Multi-line editing
- Query history
- Error feedback

### 2. Natural Language Queries

Ask questions in plain language:

- **Danish**: "Vis mig de første 10 bedrifter"
- **English**: "Show me the first 10 farms"

Powered by Google Gemini Pro with custom prompting for agricultural domain knowledge.

### 3. Data Explorer

Interactive interface for exploring datasets:

- Dataset browser (Bronze/Silver/Gold layers)
- Column metadata viewer
- Sample data preview
- Query results table with sorting/filtering

### 4. Parquet Support

Direct querying of Parquet files via DuckDB WASM:

- No server-side processing required
- Efficient columnar storage
- Supports complex queries (joins, aggregations, window functions)
- Handles files up to 100MB efficiently

---

## Documentation

| Document                                                               | Description                       |
| ---------------------------------------------------------------------- | --------------------------------- |
| [README.md](./README.md)                                               | This file - project overview      |
| [QUICK_START.md](./QUICK_START.md)                                     | 5-minute getting started guide    |
| [DEPLOYMENT.md](./DEPLOYMENT.md)                                       | Production deployment guide       |
| [TESTING.md](./TESTING.md)                                             | Testing strategies and procedures |
| [TESTING_CHECKLIST.md](./TESTING_CHECKLIST.md)                         | Pre-deployment testing checklist  |
| [IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md)               | Technical implementation details  |
| [README_COMPONENTS.md](./README_COMPONENTS.md)                         | Component architecture            |
| [FEATURE_FLOW.md](./FEATURE_FLOW.md)                                   | Feature documentation             |
| [docs/NATURAL_LANGUAGE_QUERIES.md](./docs/NATURAL_LANGUAGE_QUERIES.md) | NL query implementation           |

---

## Architecture Decisions

### Why DuckDB WASM?

- **Zero backend**: All processing in browser
- **Fast**: Columnar processing optimized for analytics
- **Parquet native**: Direct Parquet file reading
- **SQL standard**: Full SQL support (joins, window functions, CTEs)

### Why Cloudflare R2?

- **Zero egress fees**: Unlimited data transfer
- **S3 compatible**: Easy migration path
- **Fast**: Global CDN included
- **Affordable**: $0.015/GB storage

### Why Vercel?

- **Next.js optimized**: First-class Next.js support
- **Edge network**: Fast global delivery
- **Zero config**: Git-based deployments
- **Serverless**: Scales automatically

### Why Google Gemini?

- **Free tier**: 1,500 requests/day free
- **Fast**: 1-3 second response times
- **Accurate**: Good SQL generation quality
- **Simple API**: Easy integration

---

## Performance

### Benchmarks

| Operation              | Time    | Notes                         |
| ---------------------- | ------- | ----------------------------- |
| Initial page load      | < 2s    | First visit                   |
| SQL query execution    | < 1s    | 10MB Parquet file             |
| Natural language query | 1-3s    | API call + SQL generation     |
| Parquet file download  | 2-5s    | 50MB file over Cloudflare CDN |
| DuckDB initialization  | < 500ms | WASM load and init            |

### Optimization Tips

1. **Use LIMIT clauses** in SQL queries
2. **Query specific columns** instead of `SELECT *`
3. **Use partitioned Parquet files** for large datasets (split by date/region)
4. **Enable browser caching** for Parquet files
5. **Compress large result sets** before display

---

## Cost Estimation

### Development (Free)

- Vercel: Hobby plan (free)
- Cloudflare R2: Free tier (10GB)
- Google Gemini: Free tier (1,500 requests/day)
- **Total: $0/month**

### Production (Small Scale)

- Vercel Pro: $20/month
- Cloudflare R2: ~$1/month (50GB storage)
- Google Gemini: $0 (within free tier)
- **Total: ~$21/month**

See [DEPLOYMENT.md](./DEPLOYMENT.md#cost-breakdown) for detailed cost analysis.

---

## Browser Support

| Browser       | Version | Status                               |
| ------------- | ------- | ------------------------------------ |
| Chrome        | 90+     | ✅ Fully supported                   |
| Firefox       | 90+     | ✅ Fully supported                   |
| Safari        | 15+     | ✅ Fully supported                   |
| Edge          | 90+     | ✅ Fully supported                   |
| Mobile Safari | iOS 15+ | ⚠️ Limited (WASM performance)        |
| Chrome Mobile | Latest  | ⚠️ Limited (large files may timeout) |

**Note**: DuckDB WASM requires modern browser with WebAssembly support.

---

## Contributing

This project is part of the [Landbruget.dk](https://landbruget.dk) platform:

- **Main App**: [`frontend/`](../frontend/)
- **Backend Pipelines**: [`backend/`](../backend/)
- **Documentation**: [`docs/`](../docs/)

For contribution guidelines, see the main repository README.

---

## Troubleshooting

### Common Issues

**Q: Natural language queries not working**

```bash
# Check API key is set
cat .env.local | grep GOOGLE_API_KEY

# Restart dev server
npm run dev
```

**Q: CORS errors loading Parquet files**

```bash
# Verify R2 URL is correct
cat .env.local | grep NEXT_PUBLIC_R2_URL

# Check browser console for exact error
```

**Q: Build fails with WASM errors**

```bash
# Ensure asyncWebAssembly is enabled in next.config.ts
# See DEPLOYMENT.md troubleshooting section
```

For more troubleshooting, see [DEPLOYMENT.md](./DEPLOYMENT.md#troubleshooting).

---

## Support & Resources

### Documentation

- [Next.js Documentation](https://nextjs.org/docs)
- [DuckDB WASM Documentation](https://duckdb.org/docs/api/wasm/)
- [Google Gemini API Documentation](https://ai.google.dev/docs)
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)

### Community

- [Next.js Discord](https://discord.gg/nextjs)
- [DuckDB Discord](https://discord.duckdb.org)

### Internal

- Main Repository: [landbruget.dk](https://github.com/landbruget/landbruget.dk)
- Issue Tracker: [GitHub Issues](https://github.com/landbruget/landbruget.dk/issues)
- Internal Docs: [`docs/`](../docs/)

---

## License

This project is part of Landbruget.dk and follows the same license as the main repository.

---

## Notes

- **React 19**: This project uses React 19 (not 18)
- **Tailwind CSS v4**: Uses latest Tailwind (different from v3)
- **TypeScript Strict Mode**: All files must pass strict type checking
- **No Custom Aliases**: Uses default `@/*` for `src/*` imports

---

**Version**: 0.1.0

**Last Updated**: 2026-01-10

**Maintained By**: Landbruget.dk Team
