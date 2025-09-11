'use client';

import { useState, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Loader2, Filter, RefreshCw, Clock, Database } from 'lucide-react';
import RankingTable from './RankingTable';
import { useRankingsCache } from '@/hooks/useRankingsCache';
import { useCompanyCache } from '@/hooks/useCompanyCache';

interface RankingItem {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  rank: number;
  value: number;
  formatted_value: string;
  year?: number;
}

interface RankingTable {
  id: string;
  title: string;
  category: string;
  description: string;
  unit: string;
  items: RankingItem[];
  company_count: number;
  last_updated: string;
}

interface HomepageRankingsResponse {
  rankings: RankingTable[];
  metadata: {
    generated_at: string;
    total_tables: number;
  };
}

const CATEGORY_FILTERS = [
  { key: 'all', label: 'Alle kategorier', count: 0 },
  { key: 'financial', label: 'Økonomi', count: 0 },
  { key: 'field', label: 'Landbrugsareal', count: 0 },
  { key: 'environment', label: 'Miljø', count: 0 },
  { key: 'animal', label: 'Husdyr', count: 0 },
  { key: 'worker', label: 'Medarbejdere', count: 0 },
];

export default function HomepageRankings() {
  const [rankings, setRankings] = useState<RankingTable[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedCategory, setSelectedCategory] = useState('all');
  const [metadata, setMetadata] = useState<{
    generated_at: string;
    total_tables: number;
  } | null>(null);
  const [usingCache, setUsingCache] = useState(false);

  const { getCachedData, setCachedData, clearCache } = useRankingsCache();
  const { cacheCompaniesFromRankings, getCacheStats } = useCompanyCache();

  const fetchRankings = useCallback(
    async (category: string = 'all', forceRefresh: boolean = false) => {
      // Check cache first if not forcing refresh
      if (!forceRefresh) {
        const cachedData = getCachedData(category);
        if (cachedData) {
          // Also cache company information from cached rankings
          cacheCompaniesFromRankings(cachedData.rankings);

          setRankings(cachedData.rankings);
          setMetadata(cachedData.metadata);
          setUsingCache(true);
          setLoading(false);
          return;
        }
      }

      setLoading(true);
      setError(null);
      setUsingCache(false);

      try {
        const params = new URLSearchParams({
          category,
          limit: '20',
        });

        const response = await fetch(
          `/api/supabase/functions/homepage-rankings?${params}`
        );

        if (!response.ok) {
          throw new Error(
            `API error: ${response.status} ${response.statusText}`
          );
        }

        const data: HomepageRankingsResponse = await response.json();

        // Cache the fresh data
        setCachedData(category, data);

        // Cache company information from the rankings for faster access
        cacheCompaniesFromRankings(data.rankings);

        setRankings(data.rankings);
        setMetadata(data.metadata);
      } catch (err) {
        console.error('Error fetching rankings:', err);
        setError(
          err instanceof Error ? err.message : 'Failed to load rankings'
        );
      } finally {
        setLoading(false);
      }
    },
    [getCachedData, setCachedData, cacheCompaniesFromRankings]
  );

  useEffect(() => {
    fetchRankings(selectedCategory);
  }, [selectedCategory, fetchRankings]);

  const handleCategoryChange = (category: string) => {
    setSelectedCategory(category);
  };

  const handleRefresh = () => {
    fetchRankings(selectedCategory, true); // Force refresh
  };

  const handleClearCache = () => {
    clearCache();
    setUsingCache(false);
    fetchRankings(selectedCategory, true);
  };

  // Calculate category counts
  const categoryFilters = CATEGORY_FILTERS.map((filter) => ({
    ...filter,
    count:
      filter.key === 'all'
        ? rankings.length
        : rankings.filter((ranking) => ranking.category === filter.key).length,
  }));

  const filteredRankings =
    selectedCategory === 'all'
      ? rankings
      : rankings.filter((ranking) => ranking.category === selectedCategory);

  return (
    <div className="w-full space-y-6">
      {/* Category Filters */}
      <div className="flex flex-wrap justify-center gap-2 pb-4">
        {categoryFilters.map((filter) => (
          <Button
            key={filter.key}
            variant={selectedCategory === filter.key ? 'default' : 'outline'}
            size="sm"
            onClick={() => handleCategoryChange(filter.key)}
            className="flex items-center space-x-2"
          >
            <Filter className="h-3 w-3" />
            <span>{filter.label}</span>
            {filter.count > 0 && (
              <Badge variant="secondary" className="ml-1 text-xs">
                {filter.count}
              </Badge>
            )}
          </Button>
        ))}

        <Button
          variant="outline"
          size="sm"
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center space-x-2"
        >
          <RefreshCw className={`h-3 w-3 ${loading ? 'animate-spin' : ''}`} />
          <span>Opdater</span>
        </Button>

        {usingCache && (
          <Button
            variant="ghost"
            size="sm"
            onClick={handleClearCache}
            className="text-conventional hover:text-conventional/80 flex items-center space-x-2"
          >
            <Clock className="h-3 w-3" />
            <span>Ryd cache</span>
          </Button>
        )}
      </div>

      {/* Loading State */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="text-muted-foreground h-8 w-8 animate-spin" />
          <span className="text-muted-foreground ml-3">
            Indlæser ranglister...
          </span>
        </div>
      )}

      {/* Error State */}
      {error && !loading && (
        <div className="py-12 text-center">
          <div className="border-destructive/20 bg-destructive/10 mx-auto max-w-md rounded-lg border p-6">
            <p className="text-destructive font-medium">
              Fejl ved indlæsning af data
            </p>
            <p className="text-destructive/80 mt-2 text-sm">{error}</p>
            <Button
              variant="outline"
              size="sm"
              onClick={handleRefresh}
              className="mt-4"
            >
              Prøv igen
            </Button>
          </div>
        </div>
      )}

      {/* Rankings Grid */}
      {!loading && !error && filteredRankings.length > 0 && (
        <div className="grid grid-cols-1 gap-6">
          {filteredRankings.map((ranking) => (
            <RankingTable
              key={ranking.id}
              id={ranking.id}
              title={ranking.title}
              category={ranking.category}
              description={ranking.description}
              unit={ranking.unit}
              items={ranking.items}
              last_updated={ranking.last_updated}
              showTop={10} // Show top 10 on homepage
            />
          ))}
        </div>
      )}

      {/* No Results */}
      {!loading && !error && filteredRankings.length === 0 && (
        <div className="py-12 text-center">
          <p className="text-muted-foreground">
            Ingen ranglister fundet for den valgte kategori.
          </p>
        </div>
      )}

      {/* Metadata Footer */}
      {metadata && !loading && (
        <div className="border-border space-y-2 border-t pt-8 text-center">
          <p className="text-muted-foreground text-xs">
            Data opdateret:{' '}
            {new Date(metadata.generated_at).toLocaleString('da-DK')} •{' '}
            {metadata.total_tables} ranglister tilgængelige
          </p>
          <div className="flex items-center justify-center space-x-4">
            {usingCache && (
              <div className="flex items-center space-x-2">
                <Clock className="text-conventional h-3 w-3" />
                <p className="text-conventional/80 text-xs">
                  Data fra cache • Opdateres automatisk hver uge
                </p>
              </div>
            )}
            <div className="flex items-center space-x-2">
              <Database className="h-3 w-3 text-green-500" />
              <p className="text-xs text-green-600">
                {getCacheStats().total_companies} virksomheder cachet for hurtig
                adgang
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
