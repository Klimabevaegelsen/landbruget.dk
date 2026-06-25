/**
 * Server-side data fetching from R2 CDN.
 * Replaces server-cache.ts (Supabase edge functions) with direct R2 JSON fetches.
 * Data is static, updated weekly by the api_export pipeline.
 */

import { unstable_cache } from 'next/cache';

import { DATA_URL } from '@/lib/env';

type HomepageRanking = {
  items: { cvr_number: string; company_id?: string }[];
  [k: string]: unknown;
};

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
      rankings: (HomepageRanking | null)[];
      [k: string]: unknown;
    }>(`/homepage/rankings/${category}.json`);
    const rankings = (data.rankings ?? []).filter(
      (ranking): ranking is HomepageRanking =>
        ranking != null && Array.isArray(ranking.items)
    );
    // Map cvr_number to company_id for frontend compatibility
    for (const ranking of rankings) {
      for (const item of ranking.items) {
        item.company_id = item.cvr_number;
      }
    }
    return { ...data, rankings };
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
    const isNational = !municipality || municipality === 'country';
    const path = isNational
      ? '/pesticides/analysis/index.json'
      : `/pesticides/analysis/${encodeURIComponent(municipality)}.json`;
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

export interface DriftExposureIndex {
  pesticide_year: number | null;
  national_avg_drift_dose_kg: number | null;
  building_count: number;
  tile_zoom: number;
  tile_count: number;
}

export interface DriftExposureBuilding {
  uid: string;
  lat: number;
  lng: number;
  pct: number;
  dose: number;
}

export const getCachedDriftExposureIndex = unstable_cache(
  async () =>
    fetchR2Json<DriftExposureIndex>('/pesticides/drift-exposure/index.json'),
  ['drift-exposure-index'],
  { revalidate: 604800, tags: ['drift-exposure'] }
);

export const getCachedDriftExposureTile = unstable_cache(
  async (z: number, x: number, y: number) =>
    fetchR2Json<DriftExposureBuilding[]>(
      `/pesticides/drift-exposure/tiles/${z}/${x}/${y}.json`
    ),
  ['drift-exposure-tile'],
  { revalidate: 604800, tags: ['drift-exposure'] }
);
export const invalidateAllCaches = async () => {
  const { revalidateTag } = await import('next/cache');
  revalidateTag('homepage-stats', 'page');
  revalidateTag('homepage-rankings', 'page');
  revalidateTag('municipality-rankings', 'page');
  revalidateTag('pesticide-analysis', 'page');
  revalidateTag('pesticide-company-details', 'page');
  revalidateTag('burden-histogram', 'page');
  revalidateTag('drift-exposure', 'page');
};

export const invalidateHomepageCache = async () => {
  const { revalidateTag } = await import('next/cache');
  revalidateTag('homepage-stats', 'page');
  revalidateTag('homepage-rankings', 'page');
};
