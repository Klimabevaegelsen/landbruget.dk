import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Enable experimental features for better caching
  experimental: {
    // Enable server-side caching improvements
    staleTimes: {
      dynamic: 0, // Dynamic routes cached for 0 seconds (always fresh)
      static: 7 * 24 * 60 * 60, // Static content cached for 1 week (in seconds)
    },
  },

  // Configure caching headers for static assets
  async headers() {
    return [
      {
        source: '/api/homepage-statistics',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, max-age=3600, stale-while-revalidate=86400', // 1 hour fresh, 24 hours stale
          },
        ],
      },
      {
        source: '/api/supabase/functions/homepage-rankings',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, max-age=7200, stale-while-revalidate=86400', // 2 hours fresh, 24 hours stale
          },
        ],
      },
      {
        source: '/api/supabase/functions/kommuner',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, max-age=7200, stale-while-revalidate=86400', // 2 hours fresh, 24 hours stale
          },
        ],
      },
    ];
  },
};

export default nextConfig;
