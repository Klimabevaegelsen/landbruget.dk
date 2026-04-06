import { dataFetch } from '@/services/data/config';

export interface SearchIndexEntry {
  cvr: number;
  name: string;
  municipality: string;
  type: string;
}

interface SearchIndex {
  companies: SearchIndexEntry[];
  total_companies: number;
}

export interface SearchResult {
  name: string;
  cvr: string;
  address: string;
  type: string;
  id: string;
}

// Module-level cache for the search index
let cachedIndex: SearchIndexEntry[] | null = null;
let indexLoadPromise: Promise<SearchIndexEntry[]> | null = null;

export function loadSearchIndex(): Promise<SearchIndexEntry[]> {
  if (cachedIndex) return Promise.resolve(cachedIndex);
  if (indexLoadPromise) return indexLoadPromise;

  indexLoadPromise = dataFetch<SearchIndex>('/search/index.json')
    .then((data) => {
      cachedIndex = data.companies;
      return cachedIndex;
    })
    .catch((err) => {
      indexLoadPromise = null;
      throw err;
    });

  return indexLoadPromise;
}

export function getCachedIndex(): SearchIndexEntry[] | null {
  return cachedIndex;
}

export function searchIndex(
  entries: SearchIndexEntry[],
  query: string,
  searchType: string,
  limit: number = 20
): SearchResult[] {
  const q = query.toLowerCase();

  const matches: SearchResult[] = [];
  for (const entry of entries) {
    if (matches.length >= limit) break;

    const cvrStr = String(entry.cvr);
    let match = false;

    if (searchType === 'cvr') {
      match = cvrStr.startsWith(q);
    } else if (searchType === 'company_name') {
      match = entry.name?.toLowerCase().includes(q) ?? false;
    } else {
      match =
        cvrStr.startsWith(q) ||
        (entry.name?.toLowerCase().includes(q) ?? false);
    }

    if (match) {
      matches.push({
        name: entry.name || '',
        cvr: cvrStr,
        address: entry.municipality || '',
        type: entry.type || '',
        id: cvrStr,
      });
    }
  }

  return matches;
}
