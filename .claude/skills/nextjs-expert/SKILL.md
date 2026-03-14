---
name: nextjs-expert
description: Next.js 15 App Router patterns — file conventions, data fetching, caching, metadata, streaming, and Server Actions
---

# Next.js 15 Expert

> Docs version: 15.x. Turbopack available via `--turbopack` flag.

## File Conventions

```
app/
  layout.tsx          # Required root layout — must include <html> and <body>
  page.tsx            # Route page (default export only)
  loading.tsx         # Suspense boundary for the whole segment
  error.tsx           # Error boundary (must be 'use client')
  not-found.tsx       # 404 for the segment
  route.ts            # API Route Handler (GET, POST, etc.)
  opengraph-image.tsx # OG image (static file or ImageResponse)
  sitemap.ts          # Dynamic sitemap
  robots.ts           # robots.txt
```

Named exports everywhere **except** the above file conventions (which must be default exports).

## Params (Next.js 15)

```typescript
// page.tsx, layout.tsx — params is a Promise in Next.js 15
export default async function Page({
  params,
  searchParams,
}: {
  params: Promise<{ slug: string }>
  searchParams: Promise<{ q?: string }>
}) {
  const { slug } = await params
  const { q } = await searchParams
  // ...
}
```

## Data Fetching

### Server Components (preferred)

```typescript
// fetch — NOT cached by default in Next.js 15
const res = await fetch('https://api.example.com/data')
// Explicit cache
const res = await fetch('https://api.example.com/data', { cache: 'force-cache' })
// ISR — revalidate every 60s
const res = await fetch('https://api.example.com/data', { next: { revalidate: 60 } })
// Tagged for on-demand revalidation
const res = await fetch('https://api.example.com/data', { next: { tags: ['posts'] } })
```

### ORM/DB — deduplicate with React cache

```typescript
import { cache } from 'react'
import 'server-only'

export const getPost = cache(async (id: string) => {
  return db.query.posts.findFirst({ where: eq(posts.id, id) })
})
```

### Parallel fetching

```typescript
const [user, posts] = await Promise.all([getUser(id), getPosts(id)])
// Use Promise.allSettled to prevent one failure blocking all
```

### Client Components — use React `use()` hook

```typescript
// Server Component passes promise as prop
const postsPromise = getPosts() // do NOT await
return (
  <Suspense fallback={<Skeleton />}>
    <PostList posts={postsPromise} />
  </Suspense>
)

// Client Component unwraps with use()
'use client'
import { use } from 'react'
export function PostList({ posts }: { posts: Promise<Post[]> }) {
  const data = use(posts)
  return <ul>{data.map(p => <li key={p.id}>{p.title}</li>)}</ul>
}
```

## Streaming

```typescript
// Coarse: loading.tsx wraps entire page in Suspense automatically

// Fine-grained: Suspense around slow component
import { Suspense } from 'react'

export default function Page() {
  return (
    <>
      <StaticHeader />
      <Suspense fallback={<Skeleton />}>
        <SlowComponent />
      </Suspense>
    </>
  )
}
```

## Caching & Revalidation

```typescript
import { revalidatePath, revalidateTag } from 'next/cache'
import { refresh } from 'next/cache'

// In Server Action after mutation:
revalidatePath('/posts')           // revalidate a path
revalidateTag('posts')            // revalidate by tag
refresh()                          // refresh current page without revalidating cache
```

## Metadata

```typescript
// Static metadata
export const metadata: Metadata = {
  title: 'My Page',
  description: '...',
  openGraph: {
    title: 'My Page',
    images: [{ url: '/og.png', width: 1200, height: 630 }],
  },
}

// Dynamic metadata
export async function generateMetadata({
  params,
}: {
  params: Promise<{ slug: string }>
}): Promise<Metadata> {
  const { slug } = await params
  const post = await getPost(slug)
  return { title: post.title }
}
```

## OG Image Generation

```typescript
// app/opengraph-image.tsx
import { ImageResponse } from 'next/og'

export const size = { width: 1200, height: 630 }
export const contentType = 'image/png'
export const alt = 'Page title'

export default function Image() {
  return new ImageResponse(
    <div style={{ display: 'flex', fontSize: 64, background: 'white', width: '100%', height: '100%', alignItems: 'center', justifyContent: 'center' }}>
      Hello
    </div>,
    { ...size }
  )
}
```

## Route Handlers

```typescript
// app/api/posts/route.ts
import { NextRequest, NextResponse } from 'next/server'

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const id = searchParams.get('id')
  return NextResponse.json({ id })
}

export async function POST(request: NextRequest) {
  const body = await request.json()
  return NextResponse.json({ ok: true }, { status: 201 })
}
```

## Image & Font

```typescript
import Image from 'next/image'
import { Inter } from 'next/font/google'

const inter = Inter({ subsets: ['latin'], display: 'swap' })

// Image always needs width + height (or fill + container with position:relative)
<Image src="/hero.png" alt="Hero" width={1200} height={630} priority />
```

## Rules

- `params` and `searchParams` are Promises in Next.js 15 — always `await` them
- `fetch` is NOT cached by default — be explicit with `cache` or `next.revalidate`
- Server Components are default — only add `'use client'` for interactivity
- `next build` does NOT run linting automatically in Next.js 15 — run separately
- Use `loading.tsx` for route-level skeletons, `<Suspense>` for component-level
- Use `Promise.all` for parallel data fetching within a component
