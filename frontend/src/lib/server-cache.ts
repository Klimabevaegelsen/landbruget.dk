/**
 * Server-side caching utilities using Next.js unstable_cache
 * These functions cache data on the server until Tuesday updates
 */

import { unstable_cache } from 'next/cache';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;

/**
 * Cached server-side fetch for homepage statistics
 * Revalidates every Tuesday when data updates
 */
export const getCachedHomepageStatistics = unstable_cache(
  async () => {
    console.log('🔄 Fetching fresh homepage statistics from Supabase...');

    try {
      const response = await fetch(
        `${SUPABASE_URL}/functions/v1/homepage-statistics`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
            'Content-Type': 'application/json',
          },
        }
      );

      if (!response.ok) {
        throw new Error(`Supabase error: ${response.status}`);
      }

      const data = await response.json();
      console.log('✅ Fresh homepage statistics cached on server');
      return data;
    } catch (error) {
      console.error('❌ Failed to fetch homepage statistics:', error);

      // Return fallback data
      return {
        total_data_points: 29104178,
        total_companies: 46126,
        last_updated: new Date().toISOString(),
        formatted: {
          data_points: '29.104.178',
          companies: '46.126',
        },
        fallback: true,
      };
    }
  },
  ['homepage-statistics'], // Cache key
  {
    revalidate: 604800, // 7 days - manual invalidation on Tuesdays via /api/revalidate-cache
    tags: ['homepage-stats'], // Cache tags for manual invalidation
  }
);

/**
 * Cached server-side fetch for homepage rankings
 * Revalidates every Tuesday when data updates
 */
export const getCachedHomepageRankings = unstable_cache(
  async (category: string = 'all', limit: string = '20') => {
    console.log(
      `🔄 Fetching fresh homepage rankings (${category}) from Supabase...`
    );

    try {
      const functionUrl = new URL(
        '/functions/v1/homepage-rankings',
        SUPABASE_URL
      );
      functionUrl.searchParams.set('category', category);
      functionUrl.searchParams.set('limit', limit);

      const response = await fetch(functionUrl.toString(), {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          apikey: SUPABASE_ANON_KEY,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(
          `Supabase error: ${response.status} ${response.statusText}`
        );
      }

      const data = await response.json();
      console.log(`✅ Fresh homepage rankings (${category}) cached on server`);
      return data;
    } catch (error) {
      console.error(
        `❌ Failed to fetch homepage rankings (${category}):`,
        error
      );
      throw error; // Re-throw to let API route handle the error
    }
  },
  ['homepage-rankings'], // Cache key
  {
    revalidate: 604800, // 7 days - manual invalidation on Tuesdays via /api/revalidate-cache
    tags: ['homepage-rankings'], // Cache tags for manual invalidation
  }
);

/**
 * Cached server-side fetch for municipality rankings
 * Revalidates every Tuesday when data updates
 */
export const getCachedMunicipalityRankings = unstable_cache(
  async (
    category: string = 'all',
    year: string = '2024',
    limit: string = '100'
  ) => {
    console.log(
      `🔄 Fetching fresh municipality rankings (${category}, ${year}) from Supabase...`
    );

    try {
      const functionUrl = new URL('/functions/v1/kommuner', SUPABASE_URL);
      functionUrl.searchParams.set('category', category);
      functionUrl.searchParams.set('year', year);
      functionUrl.searchParams.set('limit', limit);

      const response = await fetch(functionUrl.toString(), {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          apikey: SUPABASE_ANON_KEY,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(
          `Supabase error: ${response.status} ${response.statusText}`
        );
      }

      const data = await response.json();
      console.log(
        `✅ Fresh municipality rankings (${category}, ${year}) cached on server`
      );
      return data;
    } catch (error) {
      console.error(
        `❌ Failed to fetch municipality rankings (${category}, ${year}):`,
        error
      );
      throw error; // Re-throw to let API route handle the error
    }
  },
  ['municipality-rankings'], // Cache key
  {
    revalidate: 604800, // 7 days - manual invalidation on Tuesdays via /api/revalidate-cache
    tags: ['municipality-rankings'], // Cache tags for manual invalidation
  }
);

/**
 * Manual cache invalidation functions for Tuesday data updates
 * Call these when you update data on Tuesdays
 */
export const invalidateAllCaches = async () => {
  const { revalidateTag } = await import('next/cache');

  console.log('🔄 Invalidating all server caches for Tuesday update...');
  revalidateTag('homepage-stats');
  revalidateTag('homepage-rankings');
  revalidateTag('municipality-rankings');
  console.log('✅ All server caches invalidated');
};

export const invalidateHomepageCache = async () => {
  const { revalidateTag } = await import('next/cache');

  console.log('🔄 Invalidating homepage caches...');
  revalidateTag('homepage-stats');
  revalidateTag('homepage-rankings');
  console.log('✅ Homepage caches invalidated');
};
