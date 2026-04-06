/* oxlint-disable landbruget/require-test-coverage */
import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';
import type {
  PesticideAnalysisFilters,
  PesticideAnalysisResponse,
} from '@/components/pesticide-analysis/types';

export function usePesticideAnalysis(filters: PesticideAnalysisFilters) {
  const [data, setData] = useState<PesticideAnalysisResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);

    const toastId = toast.loading('Indlæser pesticiddata', {
      description: 'Henter analysedata...',
    });

    try {
      const params = new URLSearchParams();
      Object.entries(filters).forEach(([key, value]) => {
        if (key === 'years') {
          const yearsArray = value as number[];
          if (yearsArray.length > 0) {
            yearsArray.forEach((year) =>
              params.append('years', year.toString())
            );
          }
        } else if (value !== null && value !== undefined && value !== '') {
          params.append(key, value.toString());
        }
      });

      const response = await fetch(`/api/data/pesticide-analysis?${params}`);

      if (!response.ok) {
        throw new Error(`API request failed: ${response.status}`);
      }

      const result: PesticideAnalysisResponse = await response.json();
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
      console.error('API Error:', err);
    } finally {
      setLoading(false);
      toast.dismiss(toastId);
    }
  }, [filters]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
