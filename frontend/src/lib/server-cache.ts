/**
 * Server-side data fetching from R2 CDN.
 * Replaces server-cache.ts (Supabase edge functions) with direct R2 JSON fetches.
 * Data is static, updated weekly by the api_export pipeline.
 */

import { unstable_cache } from 'next/cache';

import { DATA_URL } from '@/lib/env';

async function fetchR2Json<T>(path: string): Promise<T> {
  const url = `${DATA_URL}${path}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`R2 fetch failed: ${url} (${response.status})`);
  }
  return response.json() as Promise<T>;
}

export const getCachedHomepageStatistics = unstable_cache(
  async () => {
    return await fetchR2Json('/homepage/statistics.json');
  },
  ['homepage-statistics'],
  { revalidate: 604800, tags: ['homepage-stats'] }
);

export const getCachedHomepageRankings = unstable_cache(
  async (
    category: string = 'all',
    _limit: string = '20',
    _rankingId: string = ''
  ) => {
    // Pre-computed JSON per category; limit/rankingId filtering done client-side
    const data = await fetchR2Json<{
      rankings: {
        items: { cvr_number: string; company_id?: string }[];
        [k: string]: unknown;
      }[];
      [k: string]: unknown;
    }>(`/homepage/rankings/${category}.json`);
    // Map cvr_number to company_id for frontend compatibility
    for (const ranking of data.rankings ?? []) {
      for (const item of ranking.items ?? []) {
        item.company_id = item.cvr_number;
      }
    }
    return data;
  },
  ['homepage-rankings'],
  { revalidate: 604800, tags: ['homepage-rankings'] }
);

export const getCachedMunicipalityRankings = unstable_cache(
  async (
    category: string = 'all',
    _year: string = '2024',
    _limit: string = '100'
  ) => {
    return fetchR2Json(`/municipalities/rankings/${category}.json`);
  },
  ['municipality-rankings'],
  { revalidate: 604800, tags: ['municipality-rankings'] }
);

export const getCachedMunicipalityDetails = unstable_cache(
  async (municipality: string, category: string = 'land_use') => {
    const safeMuni = encodeURIComponent(municipality);
    return fetchR2Json(`/municipalities/details/${safeMuni}_${category}.json`);
  },
  ['municipality-details'],
  { revalidate: 604800, tags: ['municipality-rankings'] }
);

export const getCachedPesticideAnalysis = unstable_cache(
  async (searchParams: Record<string, string> = {}) => {
    const municipality = searchParams.geography;
    const path = municipality
      ? `/pesticides/analysis/${encodeURIComponent(municipality)}.json`
      : '/pesticides/analysis/index.json';
    return fetchR2Json(path);
  },
  ['pesticide-analysis'],
  { revalidate: 604800, tags: ['pesticide-analysis'] }
);

export const getCachedPesticideCompanyDetails = unstable_cache(
  async (searchParams: Record<string, string> = {}) => {
    const cvr = searchParams.cvr;
    if (!cvr) throw new Error('CVR required');
    return fetchR2Json(`/pesticides/companies/${cvr}.json`);
  },
  ['pesticide-company-details'],
  { revalidate: 604800, tags: ['pesticide-company-details'] }
);

export const getCachedBurdenHistogram = unstable_cache(
  async (year: number) => {
    return await fetchR2Json<{ bin_start: number; field_count: number }[]>(
      `/pesticides/burden-histogram-${year}.json`
    );
  },
  ['burden-histogram'],
  {
    revalidate: 604800,
    tags: ['burden-histogram'],
  }
);
export const invalidateAllCaches = async () => {
  const { revalidateTag } = await import('next/cache');
  revalidateTag('homepage-stats', 'page');
  revalidateTag('homepage-rankings', 'page');
  revalidateTag('municipality-rankings', 'page');
  revalidateTag('pesticide-analysis', 'page');
  revalidateTag('pesticide-company-details', 'page');
  revalidateTag('burden-histogram', 'page');
};

export const invalidateHomepageCache = async () => {
  const { revalidateTag } = await import('next/cache');
  revalidateTag('homepage-stats', 'page');
  revalidateTag('homepage-rankings', 'page');
};
