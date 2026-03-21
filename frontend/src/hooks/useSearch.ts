import { useEffect, useState, useCallback } from 'react';
import { apiFetch } from '@/services/supabase/config';
import { useLoadingToast } from '@/hooks/useLoadingToast';

interface SearchResult {
  name: string;
  cvr: string;
  address: string;
  type: string;
  id: string;
}

interface SearchResponse {
  results: SearchResult[];
  total: number;
  query: string;
  searchType?: string;
}

interface UseSearchOptions {
  debounceMs?: number;
}

interface UseSearchReturn {
  searchResults: SearchResult[];
  isLoading: boolean;
  error: string | null;
}

export type { SearchResult };

export function useSearch(
  query: string,
  activeTab: number,
  options: UseSearchOptions = {}
): UseSearchReturn {
  const { debounceMs = 300 } = options;
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { showLoadingToast, hideLoadingToast } = useLoadingToast();

  const performSearch = useCallback(
    async (q: string) => {
      if (!q || q.trim().length < 2) {
        setSearchResults([]);
        hideLoadingToast();
        return;
      }

      showLoadingToast('Søger', `Søger efter "${q.trim()}"...`);
      setIsLoading(true);
      setError(null);

      try {
        const searchTypeMap: { [key: number]: string } = {
          0: 'auto',
          1: 'cvr',
          2: 'company_name',
        };

        const searchType = searchTypeMap[activeTab] || 'auto';
        const response = await apiFetch(
          `/functions/v1/search?q=${encodeURIComponent(q.trim())}&type=${searchType}&limit=20`
        );

        if (!response.ok) {
          throw new Error('Search failed');
        }

        const data: SearchResponse = await response.json();
        setSearchResults(data.results || []);
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

  // Debounced search
  useEffect(() => {
    const timeoutId = setTimeout(() => {
      performSearch(query);
    }, debounceMs);

    return () => clearTimeout(timeoutId);
  }, [query, performSearch, debounceMs]);

  // Re-search when tab changes
  useEffect(() => {
    if (query && query.trim().length >= 2) {
      performSearch(query);
    }
  }, [activeTab, query, performSearch]);

  return { searchResults, isLoading, error };
}
