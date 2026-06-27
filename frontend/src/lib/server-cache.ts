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

type PesticideAnalysisFilters = {
  available_years: number[];
  available_municipalities: string[];
  total_companies: number;
  companies_with_pfas: number;
  companies_with_diquat: number;
  companies_with_glyphosate: number;
};

type PesticideAnalysisResponse = {
  companies: unknown[];
  total_count: number;
  page: number;
  limit: number;
  filters: PesticideAnalysisFilters;
  summary?: Record<string, unknown>;
  top_pesticides?: unknown[];
  metadata?: Record<string, unknown>;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asFiniteNumber(value: unknown, fallback = 0): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function parsePositiveInt(value: unknown, fallback: number): number {
  const parsed =
    typeof value === 'string' || typeof value === 'number'
      ? Number.parseInt(String(value), 10)
      : Number.NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function yearsFromPeriod(period: unknown): number[] {
  if (typeof period !== 'string') return [];
  const years = period
    .match(/\b20\d{2}\b/g)
    ?.map((year) => Number.parseInt(year, 10))
    .filter((year) => Number.isFinite(year));
  return Array.from(new Set(years ?? [])).sort((a, b) => b - a);
}

function normalizePesticideAnalysis(
  rawData: unknown,
  searchParams: Record<string, string>
): PesticideAnalysisResponse {
  const data = asRecord(rawData);
  const existingFilters = asRecord(data.filters);
  const companies = Array.isArray(data.companies) ? data.companies : [];
  const summary = asRecord(data.summary);
  const metadata = asRecord(data.metadata);

  const filters: PesticideAnalysisFilters = {
    available_years: Array.isArray(existingFilters.available_years)
      ? existingFilters.available_years.filter(
          (year): year is number =>
            typeof year === 'number' && Number.isFinite(year)
        )
      : yearsFromPeriod(metadata.period),
    available_municipalities: Array.isArray(
      existingFilters.available_municipalities
    )
      ? existingFilters.available_municipalities.filter(
          (municipality): municipality is string =>
            typeof municipality === 'string'
        )
      : typeof data.municipality === 'string'
        ? [data.municipality]
        : [],
    total_companies: asFiniteNumber(
      existingFilters.total_companies,
      asFiniteNumber(summary.total_companies, companies.length)
    ),
    companies_with_pfas: asFiniteNumber(existingFilters.companies_with_pfas),
    companies_with_diquat: asFiniteNumber(
      existingFilters.companies_with_diquat
    ),
    companies_with_glyphosate: asFiniteNumber(
      existingFilters.companies_with_glyphosate
    ),
  };

  return {
    ...data,
    companies,
    total_count: asFiniteNumber(data.total_count, companies.length),
    page: parsePositiveInt(data.page ?? searchParams.page, 1),
    limit: parsePositiveInt(data.limit ?? searchParams.limit, 50),
    filters,
    summary,
    top_pesticides: Array.isArray(data.top_pesticides)
      ? data.top_pesticides
      : [],
    metadata,
  };
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
    const isNational =
      !municipality || municipality === 'country' || municipality === 'all';
    const path = !isNational
      ? `/pesticides/analysis/${encodeURIComponent(municipality)}.json`
      : '/pesticides/analysis/index.json';
    const data = await fetchR2Json(path);
    return normalizePesticideAnalysis(data, searchParams);
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
