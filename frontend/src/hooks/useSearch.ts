import { useEffect, useState, useCallback, useRef } from 'react';
import { useLoadingToast } from '@/hooks/useLoadingToast';
import {
  SearchIndexEntry,
  loadSearchIndex,
  searchIndex,
  getCachedIndex,
} from '@/hooks/search-index';

export type { SearchResult } from '@/hooks/search-index';

interface UseSearchOptions {
  debounceMs?: number;
}

interface UseSearchReturn {
  searchResults: {
    name: string;
    cvr: string;
    address: string;
    type: string;
    id: string;
  }[];
  isLoading: boolean;
  error: string | null;
}

export function useSearch(
  query: string,
  activeTab: number,
  options: UseSearchOptions = {}
): UseSearchReturn {
  const { debounceMs = 300 } = options;
  const [searchResults, setSearchResults] = useState<
    UseSearchReturn['searchResults']
  >([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const indexRef = useRef<SearchIndexEntry[] | null>(getCachedIndex());

  const { showLoadingToast, hideLoadingToast } = useLoadingToast();

  // Preload search index on mount
  useEffect(() => {
    loadSearchIndex()
      .then((entries) => {
        indexRef.current = entries;
      })
      .catch(() => {
        // Will retry on search
      });
  }, []);

  const performSearch = useCallback(
    async (q: string) => {
      if (!q || q.trim().length < 2) {
        setSearchResults([]);
        hideLoadingToast();
        return;
      }

      setIsLoading(true);
      setError(null);

      try {
        if (!indexRef.current) {
          showLoadingToast('Indlæser', 'Henter søgeindeks...');
          indexRef.current = await loadSearchIndex();
        }

        const searchTypeMap: { [key: number]: string } = {
          0: 'auto',
          1: 'cvr',
          2: 'company_name',
        };

        const searchType = searchTypeMap[activeTab] || 'auto';
        const results = searchIndex(indexRef.current, q.trim(), searchType);
        setSearchResults(results);
      } catch (err) {
        console.error('Search error:', err);
        setError('Søgning fejlede. Prøv igen.');
        setSearchResults([]);
      } finally {
        setIsLoading(false);
        hideLoadingToast();
      }
    },
    [activeTab, showLoadingToast, hideLoadingToast]
  );

  useEffect(() => {
    const timeoutId = setTimeout(() => {
      performSearch(query);
    }, debounceMs);

    return () => clearTimeout(timeoutId);
  }, [query, performSearch, debounceMs]);

  useEffect(() => {
    if (query && query.trim().length >= 2) {
      performSearch(query);
    }
  }, [activeTab, query, performSearch]);

  return { searchResults, isLoading, error };
}
