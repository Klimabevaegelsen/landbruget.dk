# PMTiles Caching Optimization Strategy

## Current Setup Analysis

- **Storage**: Cloudflare R2 bucket
- **CDN**: `https://data.pesticidkortet.dk` (Cloudflare CDN)
- **Update Frequency**: Max once per week (mostly immutable)
- **File Sizes**: 7MB - 907MB per PMTiles file
- **Usage Pattern**: Same files accessed repeatedly by many users

## 🚀 Recommended Caching Strategy

### 1. Cloudflare R2 + CDN Configuration (Server-Side)

#### A. Cloudflare Cache Rules

```javascript
// Cloudflare Workers or Cache Rules
{
  "cache": {
    "ttl": 31536000, // 1 year (365 days)
    "browser_ttl": 31536000,
    "edge_ttl": 31536000,
    "cache_by_device_type": false
  },
  "headers": {
    "Cache-Control": "public, max-age=31536000, immutable",
    "CDN-Cache-Control": "public, max-age=31536000",
    "Vary": "Accept-Encoding"
  }
}
```

#### B. R2 Bucket Headers Configuration

```bash
# Set metadata on R2 objects
aws s3api put-object-metadata \
  --bucket your-r2-bucket \
  --key "pmtiles/*.pmtiles" \
  --metadata-directive REPLACE \
  --cache-control "public, max-age=31536000, immutable" \
  --content-type "application/octet-stream"
```

### 2. Application-Level Caching

#### A. Service Worker for PMTiles Caching

```typescript
// public/sw-pmtiles.js
const PMTILES_CACHE = "pmtiles-cache-v1";
const PMTILES_PATTERNS = [
  /https:\/\/data\.pesticidkortet\.dk\/pmtiles\/.+\.pmtiles/,
];

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // Cache PMTiles files aggressively
  if (PMTILES_PATTERNS.some((pattern) => pattern.test(url.href))) {
    event.respondWith(
      caches.open(PMTILES_CACHE).then((cache) => {
        return cache.match(event.request).then((response) => {
          if (response) {
            console.log("PMTiles cache hit:", url.pathname);
            return response;
          }

          // Fetch and cache
          return fetch(event.request).then((fetchResponse) => {
            if (fetchResponse.ok) {
              cache.put(event.request, fetchResponse.clone());
            }
            return fetchResponse;
          });
        });
      })
    );
  }
});
```

#### B. Next.js API Route for PMTiles Proxy with Caching

```typescript
// src/app/api/pmtiles/[...path]/route.ts
import { NextRequest, NextResponse } from "next/server";

const PMTILES_BASE_URL = "https://data.pesticidkortet.dk";
const CACHE_DURATION = 31536000; // 1 year

export async function GET(
  request: NextRequest,
  { params }: { params: { path: string[] } }
) {
  const pmtilesPath = params.path.join("/");
  const pmtilesUrl = `${PMTILES_BASE_URL}/${pmtilesPath}`;

  try {
    // Check if it's a range request (PMTiles uses these)
    const range = request.headers.get("range");
    const headers: Record<string, string> = {};

    if (range) {
      headers.Range = range;
    }

    const response = await fetch(pmtilesUrl, { headers });

    if (!response.ok) {
      return NextResponse.json(
        { error: "PMTiles file not found" },
        { status: 404 }
      );
    }

    // Clone response to read headers
    const responseHeaders = new Headers();

    // Copy essential headers
    [
      "content-type",
      "content-length",
      "content-range",
      "accept-ranges",
    ].forEach((header) => {
      const value = response.headers.get(header);
      if (value) responseHeaders.set(header, value);
    });

    // Add aggressive caching headers
    responseHeaders.set(
      "Cache-Control",
      `public, max-age=${CACHE_DURATION}, immutable`
    );
    responseHeaders.set(
      "CDN-Cache-Control",
      `public, max-age=${CACHE_DURATION}`
    );
    responseHeaders.set(
      "Vercel-CDN-Cache-Control",
      `public, max-age=${CACHE_DURATION}`
    );

    return new NextResponse(response.body, {
      status: response.status,
      headers: responseHeaders,
    });
  } catch (error) {
    console.error("PMTiles proxy error:", error);
    return NextResponse.json(
      { error: "Failed to fetch PMTiles" },
      { status: 500 }
    );
  }
}
```

### 3. Client-Side Optimizations

#### A. PMTiles URL Service with Built-in Caching

```typescript
// src/services/pmtiles-cache-service.ts
interface CachedPMTilesUrl {
  url: string;
  timestamp: number;
  etag?: string;
}

class PMTilesCacheService {
  private cache = new Map<string, CachedPMTilesUrl>();
  private readonly CACHE_DURATION = 7 * 24 * 60 * 60 * 1000; // 1 week

  async getPMTilesUrl(filename: string): Promise<string> {
    const cacheKey = filename;
    const cached = this.cache.get(cacheKey);

    // Return cached URL if still valid
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      return cached.url;
    }

    // Use proxy route for better caching
    const url = `/api/pmtiles/pmtiles/${filename}`;

    this.cache.set(cacheKey, {
      url,
      timestamp: Date.now(),
    });

    return url;
  }

  // Preload commonly used PMTiles
  async preloadPMTiles(filenames: string[]): Promise<void> {
    const preloadPromises = filenames.map(async (filename) => {
      const url = await this.getPMTilesUrl(filename);

      // Use link preload for better browser caching
      const link = document.createElement("link");
      link.rel = "preload";
      link.href = url;
      link.as = "fetch";
      link.crossOrigin = "anonymous";
      document.head.appendChild(link);
    });

    await Promise.all(preloadPromises);
  }

  clearCache(): void {
    this.cache.clear();
  }
}

export const pmtilesCacheService = new PMTilesCacheService();
```

#### B. Updated Field Analysis Components

```typescript
// Update FieldAnalysisVisualization.tsx
import { pmtilesCacheService } from "@/services/pmtiles-cache-service";

export default function FieldAnalysisVisualization() {
  // ... existing state ...

  // Generate PMTiles URLs with caching
  const [pmtilesUrls, setPmtilesUrls] = useState<Record<string, string>>({});

  useEffect(() => {
    const loadPMTilesUrls = async () => {
      const urls = {
        fields: await pmtilesCacheService.getPMTilesUrl(
          `field_analysis_${yearSelection.selectedYear}.pmtiles`
        ),
        bnbo: await pmtilesCacheService.getPMTilesUrl("bnbo_areas.pmtiles"),
        wetlands: await pmtilesCacheService.getPMTilesUrl(
          "wetlands_all_2024.pmtiles"
        ),
        water_projects: await pmtilesCacheService.getPMTilesUrl(
          "water_projects_2024.pmtiles"
        ),
        buildings: await pmtilesCacheService.getPMTilesUrl(
          "buildings_proximity_2024.pmtiles"
        ),
      };

      setPmtilesUrls(urls);
    };

    loadPMTilesUrls();
  }, [yearSelection.selectedYear]);

  // Preload next/previous year PMTiles
  useEffect(() => {
    const preloadAdjacentYears = async () => {
      const currentYear = yearSelection.selectedYear;
      const adjacentYears = [currentYear - 1, currentYear + 1].filter((year) =>
        yearSelection.availableYears.includes(year)
      );

      const filenames = adjacentYears.map(
        (year) => `field_analysis_${year}.pmtiles`
      );
      await pmtilesCacheService.preloadPMTiles(filenames);
    };

    preloadAdjacentYears();
  }, [yearSelection]);

  // ... rest of component
}
```

### 4. Infrastructure Optimizations

#### A. Vercel Configuration Update

```json
// vercel.json
{
  "headers": [
    {
      "source": "/api/pmtiles/(.*)",
      "headers": [
        {
          "key": "Cache-Control",
          "value": "public, max-age=31536000, immutable"
        },
        {
          "key": "CDN-Cache-Control",
          "value": "public, max-age=31536000"
        },
        {
          "key": "Vercel-CDN-Cache-Control",
          "value": "public, max-age=31536000"
        }
      ]
    }
  ],
  "functions": {
    "app/api/pmtiles/[...path]/route.ts": {
      "maxDuration": 60
    }
  }
}
```

#### B. Next.js Configuration

```typescript
// next.config.ts
const nextConfig: NextConfig = {
  async headers() {
    return [
      {
        source: "/api/pmtiles/:path*",
        headers: [
          {
            key: "Cache-Control",
            value: "public, max-age=31536000, immutable",
          },
          {
            key: "Access-Control-Allow-Origin",
            value: "*",
          },
        ],
      },
    ];
  },
};
```

## 📊 Expected Performance Improvements

### Before Optimization

- **First Load**: 2-5 seconds per PMTiles file
- **Year Switching**: 2-4 seconds (full download)
- **Repeat Visits**: Same as first load
- **Bandwidth Usage**: Full file size each time

### After Optimization

- **First Load**: 2-5 seconds (unchanged)
- **Year Switching**: 50-200ms (cache hit)
- **Repeat Visits**: 50-200ms (cache hit)
- **Bandwidth Usage**: 95% reduction for cached files

### Cache Hit Rates

- **Browser Cache**: 90%+ for returning users
- **CDN Cache**: 95%+ globally
- **Service Worker**: 99%+ for active users

## 🔧 Implementation Priority

1. **Immediate** (Low effort, high impact):

   - Update Cloudflare cache headers
   - Add Vercel CDN configuration

2. **Short-term** (Medium effort, high impact):

   - Implement PMTiles proxy API route
   - Update frontend to use cached URLs

3. **Long-term** (High effort, medium impact):
   - Service Worker implementation
   - Intelligent preloading system

## 🎯 Monitoring & Analytics

```typescript
// PMTiles performance monitoring
class PMTilesAnalytics {
  static trackCacheHit(filename: string, source: "browser" | "cdn" | "origin") {
    console.log(`PMTiles cache hit: ${filename} from ${source}`);
    // Add to analytics service
  }

  static trackLoadTime(filename: string, duration: number) {
    console.log(`PMTiles load time: ${filename} - ${duration}ms`);
    // Add to analytics service
  }
}
```

This comprehensive caching strategy will dramatically improve PMTiles loading performance while reducing bandwidth costs and server load.
