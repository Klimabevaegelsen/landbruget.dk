/**
 * Server-side caching utilities using Next.js unstable_cache
 * These functions cache data on the server until Tuesday updates
 */

import { unstable_cache } from 'next/cache';

import { SUPABASE_URL, SUPABASE_ANON_KEY } from '@/lib/env';

/**
 * Cached server-side fetch for homepage statistics
 * Revalidates every Tuesday when data updates
 */
export const getCachedHomepageStatistics = unstable_cache(
  async () => {
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
      return data;
    } catch (error) {
      console.error('Failed to fetch homepage statistics:', error);

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
  async (
    category: string = 'all',
    limit: string = '20',
    rankingId: string = ''
  ) => {
    try {
      const functionUrl = new URL(
        '/functions/v1/homepage-rankings',
        SUPABASE_URL
      );
      functionUrl.searchParams.set('category', category);
      functionUrl.searchParams.set('limit', limit);
      if (rankingId) {
        functionUrl.searchParams.set('rankingId', rankingId);
      }

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
      return data;
    } catch (error) {
      console.error(
        `Failed to fetch homepage rankings (${category}${rankingId ? `, ranking: ${rankingId}` : ''}):`,
        error
      );
      throw error; // Re-throw to let API route handle the error
    }
  },
  ['homepage-rankings'], // Cache key - will be combined with parameters
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
      return data;
    } catch (error) {
      console.error(
        `Failed to fetch municipality rankings (${category}, ${year}):`,
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
 * Cached server-side fetch for pesticide analysis
 * Revalidates every Tuesday when data updates
 */
export const getCachedPesticideAnalysis = unstable_cache(
  async (searchParams: Record<string, string> = {}) => {
    try {
      const functionUrl = new URL(
        '/functions/v1/pesticide-analysis',
        SUPABASE_URL
      );

      // Add all search parameters
      for (const [key, value] of Object.entries(searchParams)) {
        functionUrl.searchParams.set(key, value);
      }

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
      return data;
    } catch (error) {
      console.error('Failed to fetch pesticide analysis:', error);
      throw error; // Re-throw to let API route handle the error
    }
  },
  ['pesticide-analysis'], // Cache key
  {
    revalidate: 604800, // 7 days - manual invalidation on Tuesdays via /api/revalidate-cache
    tags: ['pesticide-analysis'], // Cache tags for manual invalidation
  }
);

/**
 * Cached server-side fetch for pesticide company details
 * Revalidates every Tuesday when data updates
 */
export const getCachedPesticideCompanyDetails = unstable_cache(
  async (searchParams: Record<string, string> = {}) => {
    try {
      const functionUrl = new URL(
        '/functions/v1/pesticide-company-details',
        SUPABASE_URL
      );

      // Add all search parameters
      for (const [key, value] of Object.entries(searchParams)) {
        functionUrl.searchParams.set(key, value);
      }

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
      return data;
    } catch (error) {
      console.error('Failed to fetch pesticide company details:', error);
      throw error; // Re-throw to let API route handle the error
    }
  },
  ['pesticide-company-details'], // Cache key
  {
    revalidate: 604800, // 7 days - manual invalidation on Tuesdays via /api/revalidate-cache
    tags: ['pesticide-company-details'], // Cache tags for manual invalidation
  }
);

/**
 * Cached server-side fetch for national burden histogram.
 * Calls the get_burden_histogram RPC via PostgREST.
 */
export const getCachedBurdenHistogram = unstable_cache(
  async (year: number) => {
    try {
      const response = await fetch(
        `${SUPABASE_URL}/rest/v1/rpc/get_burden_histogram`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
            apikey: SUPABASE_ANON_KEY,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ p_year: year }),
        }
      );

      if (!response.ok) {
        throw new Error(
          `Supabase error: ${response.status} ${response.statusText}`
        );
      }

      return (await response.json()) as {
        bin_start: number;
        field_count: number;
      }[];
    } catch (error) {
      console.error('Failed to fetch burden histogram:', error);
      return [];
    }
  },
  ['burden-histogram'],
  {
    revalidate: 604800,
    tags: ['burden-histogram'],
  }
);

/**
 * Manual cache invalidation functions for Tuesday data updates
 * Call these when you update data on Tuesdays
 */
export const invalidateAllCaches = async () => {
  const { revalidateTag } = await import('next/cache');

  revalidateTag('homepage-stats', 'max');
  revalidateTag('homepage-rankings', 'max');
  revalidateTag('municipality-rankings', 'max');
  revalidateTag('pesticide-analysis', 'max');
  revalidateTag('pesticide-company-details', 'max');
};

export const invalidateHomepageCache = async () => {
  const { revalidateTag } = await import('next/cache');

  revalidateTag('homepage-stats', 'max');
  revalidateTag('homepage-rankings', 'max');
};
