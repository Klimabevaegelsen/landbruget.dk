// Cloudflare Worker to proactively warm PMTiles cache
// Deploy this as a Cloudflare Worker on your data.pesticidkortet.dk domain

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Only handle PMTiles requests
    if (!url.pathname.includes("/pmtiles/")) {
      return fetch(request);
    }

    const cacheKey = new Request(url.toString(), request);
    const cache = caches.default;

    // Check if already cached
    let response = await cache.match(cacheKey);

    if (response) {
      console.log(`🎯 PMTiles cache HIT: ${url.pathname}`);
      return response;
    }

    console.log(`📥 PMTiles cache MISS: ${url.pathname} - fetching from R2`);

    // Fetch from R2 bucket
    response = await fetch(request);

    if (response.ok) {
      // Clone response for caching
      const cacheResponse = new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: {
          ...response.headers,
          // Aggressive caching headers
          "Cache-Control": "public, max-age=31536000, immutable",
          "CDN-Cache-Control": "public, max-age=31536000",
          "Cloudflare-CDN-Cache-Control": "public, max-age=31536000",
          // Add cache status
          "X-Cache-Status": "MISS",
          "X-Cached-At": new Date().toISOString(),
        },
      });

      // Cache for all users
      ctx.waitUntil(cache.put(cacheKey, cacheResponse.clone()));

      // Update headers for response
      cacheResponse.headers.set("X-Cache-Status", "MISS");
      return cacheResponse;
    }

    return response;
  },

  // Scheduled event to warm cache proactively
  async scheduled(controller, env, ctx) {
    console.log("🔥 Starting PMTiles cache warming...");

    const pmtilesFiles = [
      // Current year files (highest priority)
      "field_analysis_2023.pmtiles",
      "field_analysis_2024.pmtiles",

      // Background layers (used by all users)
      "bnbo_areas.pmtiles",
      "buildings_proximity_2024.pmtiles",

      // Historical years (lower priority)
      "field_analysis_2022.pmtiles",
      "field_analysis_2021.pmtiles",
      "field_analysis_2020.pmtiles",

      // Large but less frequently accessed
      "wetlands_all_2024.pmtiles",
      "water_projects_2024.pmtiles",
    ];

    const baseUrl = "https://data.pesticidkortet.dk/pmtiles/";
    const warmingPromises = [];

    for (const filename of pmtilesFiles) {
      const url = `${baseUrl}${filename}`;

      // Check if already cached
      const cacheKey = new Request(url);
      const cached = await caches.default.match(cacheKey);

      if (!cached) {
        console.log(`🔥 Warming cache for: ${filename}`);

        // Warm cache by fetching file
        const warmPromise = fetch(url, {
          headers: {
            "User-Agent": "PMTiles-Cache-Warmer/1.0",
            "X-Cache-Warming": "true",
          },
        })
          .then((response) => {
            if (response.ok) {
              console.log(`✅ Cache warmed: ${filename}`);
              // Cache the response
              return caches.default.put(cacheKey, response.clone());
            } else {
              console.error(
                `❌ Failed to warm cache: ${filename} - ${response.status}`
              );
            }
          })
          .catch((error) => {
            console.error(`❌ Cache warming error for ${filename}:`, error);
          });

        warmingPromises.push(warmPromise);
      } else {
        console.log(`✅ Already cached: ${filename}`);
      }
    }

    await Promise.allSettled(warmingPromises);
    console.log("🎯 PMTiles cache warming completed");
  },
};

// Cloudflare Worker configuration (wrangler.toml)
/*
name = "pmtiles-cache-warmer"
main = "pmtiles-cache-warmer.js"
compatibility_date = "2024-01-01"

[triggers]
crons = ["0 2 * * 1"] # Every Monday at 2 AM (after data updates)

[[routes]]
pattern = "data.pesticidkortet.dk/pmtiles/*"
zone_name = "pesticidkortet.dk"
*/
