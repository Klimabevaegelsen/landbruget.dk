---
name: vercel-expert
description: Vercel deployment patterns — environments, env vars, preview deployments, ISR, and edge config for Next.js
---

# Vercel Expert

## Environments

Vercel has three built-in environments:

| Environment | Trigger | URL |
|-------------|---------|-----|
| **Production** | Push/merge to `main` | Your domain |
| **Preview** | Push to any other branch / PR | Auto-generated URL |
| **Local** | `npm run dev` | `localhost:3000` |

Custom environments (`staging`, `qa`) available on Pro+ plans.

## Environment Variables

```bash
# Pull env vars for local development
vercel link          # link local dir to Vercel project
vercel env pull      # populates .env.local

# Add env var via CLI
vercel env add MY_SECRET production
vercel env add MY_SECRET preview
vercel env add MY_SECRET development

# Deploy to custom environment
vercel deploy --target=staging
vercel env pull --environment=staging
```

In code, always access via `process.env.MY_VAR`. Client-safe vars must be prefixed `NEXT_PUBLIC_`.

## Deployment

```bash
# Preview deploy (default)
vercel

# Production deploy
vercel --prod
```

Every push to a non-main branch automatically creates a Preview deployment with:
- A unique URL (commit-specific)
- A branch URL (always points to latest on that branch)
- PR comments with the preview link (when connected to GitHub)

## ISR with Next.js

```typescript
// Tag-based revalidation (preferred)
const res = await fetch('https://api.example.com/posts', {
  next: { tags: ['posts'], revalidate: 3600 }
})

// Revalidate on-demand in a Server Action
import { revalidateTag } from 'next/cache'
revalidateTag('posts')

// Time-based ISR
const res = await fetch('https://api.example.com/posts', {
  next: { revalidate: 60 } // seconds
})
```

On Vercel, ISR pages are:
- Cached globally on the CDN
- Persisted to durable storage (not just in-memory)
- Updated in ~300ms globally on revalidation

## Vercel-Specific Headers

```typescript
// Route Handler reading Vercel geo headers
export function GET(request: NextRequest) {
  const country = request.headers.get('x-vercel-ip-country')
  const city = request.headers.get('x-vercel-ip-city')
  return NextResponse.json({ country, city })
}
```

## next.config.ts Patterns

```typescript
import type { NextConfig } from 'next'

const config: NextConfig = {
  // Redirect
  async redirects() {
    return [{ source: '/old', destination: '/new', permanent: true }]
  },
  // Rewrite (proxy)
  async rewrites() {
    return [{ source: '/api/:path*', destination: 'https://backend.example.com/:path*' }]
  },
  // Security headers
  async headers() {
    return [{
      source: '/(.*)',
      headers: [
        { key: 'X-Frame-Options', value: 'DENY' },
        { key: 'X-Content-Type-Options', value: 'nosniff' },
        { key: 'Referrer-Policy', value: 'strict-origin-when-cross-origin' },
      ],
    }]
  },
  // Image domains
  images: {
    remotePatterns: [{ protocol: 'https', hostname: '**.supabase.co' }],
  },
}

export default config
```

## Proxy / Middleware

The project uses `proxy.ts` (Next.js 16 naming) for session refresh. In Vercel, proxy/middleware runs at the Edge globally before any cache:

```typescript
export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)'],
}
```

## Analytics & Speed Insights

```tsx
// app/layout.tsx
import { Analytics } from '@vercel/analytics/next'
import { SpeedInsights } from '@vercel/speed-insights/next'

export default function RootLayout({ children }) {
  return (
    <html>
      <body>
        {children}
        <Analytics />
        <SpeedInsights />
      </body>
    </html>
  )
}
```

## OG Image (built into Next.js 16)

```typescript
// app/opengraph-image.tsx — no extra package needed
import { ImageResponse } from 'next/og'
// @vercel/og is included in next/og automatically
```

## Rules

- Set env vars per-environment in the Vercel dashboard or via CLI
- NEVER commit `.env.local` — it's gitignored
- Use `vercel env pull` to sync env vars to local
- Preview deploys are automatic — every branch push triggers one
- ISR on Vercel is globally distributed; self-hosted ISR is single-region
- Proxy/middleware runs at Edge, before cache — keep it lightweight (no Node.js APIs)
- Use `NEXT_PUBLIC_` prefix only for values safe to expose to the browser
