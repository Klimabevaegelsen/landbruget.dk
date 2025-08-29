'use client';

import { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Loader2, Filter, RefreshCw } from 'lucide-react';
import RankingTable from './RankingTable';

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
  last_updated?: string;
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

  const fetchRankings = async (category: string = 'all') => {
    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams({
        category,
        limit: '20',
      });

      const response = await fetch(
        `/api/supabase/functions/homepage-rankings?${params}`
      );

      if (!response.ok) {
        throw new Error(`API error: ${response.status} ${response.statusText}`);
      }

      const data: HomepageRankingsResponse = await response.json();
      setRankings(data.rankings);
      setMetadata(data.metadata);
    } catch (err) {
      console.error('Error fetching rankings:', err);
      setError(err instanceof Error ? err.message : 'Failed to load rankings');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRankings(selectedCategory);
  }, [selectedCategory]);

  const handleCategoryChange = (category: string) => {
    setSelectedCategory(category);
  };

  const handleRefresh = () => {
    fetchRankings(selectedCategory);
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
      {/* Header */}
      <div className="space-y-4 text-center">
        <h2 className="text-3xl font-bold text-gray-900">
          Top 20 Danske Landbrugsvirksomheder
        </h2>
        <p className="mx-auto max-w-3xl text-lg text-gray-600">
          Ranglisterne viser de førende virksomheder inden for økonomi,
          landbrugsareal, miljøpåvirkning, husdyrproduktion og beskæftigelse
          baseret på officielle data.
        </p>
      </div>

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
      </div>

      {/* Loading State */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-gray-400" />
          <span className="ml-3 text-gray-600">Indlæser ranglister...</span>
        </div>
      )}

      {/* Error State */}
      {error && !loading && (
        <div className="py-12 text-center">
          <div className="mx-auto max-w-md rounded-lg border border-red-200 bg-red-50 p-6">
            <p className="font-medium text-red-700">
              Fejl ved indlæsning af data
            </p>
            <p className="mt-2 text-sm text-red-600">{error}</p>
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
          <p className="text-gray-600">
            Ingen ranglister fundet for den valgte kategori.
          </p>
        </div>
      )}

      {/* Metadata Footer */}
      {metadata && !loading && (
        <div className="border-t border-gray-200 pt-8 text-center">
          <p className="text-xs text-gray-500">
            Data opdateret:{' '}
            {new Date(metadata.generated_at).toLocaleString('da-DK')} •{' '}
            {metadata.total_tables} ranglister tilgængelige
          </p>
        </div>
      )}
    </div>
  );
}
