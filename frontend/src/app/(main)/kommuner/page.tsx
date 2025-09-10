'use client';

import React, { useState, useEffect, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { TrendingUp, Loader2 } from 'lucide-react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';

interface MunicipalityRanking {
  municipality: string;
  rank: number;
  value: number;
  metric: string;
  additional_data?: {
    unique_companies?: number;
    organic_percentage?: number;
  };
}

interface MunicipalityRankingResponse {
  rankings: {
    land_use?: MunicipalityRanking[];
    production?: MunicipalityRanking[];
    environmental?: MunicipalityRanking[];
    animal_health?: MunicipalityRanking[];
    worker_safety?: MunicipalityRanking[];
  };
  metadata: {
    year: number;
    total_municipalities: number;
    generated_at: string;
    categories_included: string[];
  };
}

const formatValue = (value: number, metric: string): string => {
  switch (metric) {
    case 'total_agricultural_area_ha':
      return `${value.toLocaleString('da-DK')} ha`;
    case 'total_production_capacity':
      return value.toLocaleString('da-DK');
    case 'organic_farming_percentage':
      return `${value.toFixed(1)}%`;
    case 'avg_antibiotic_usage_add_per_100_animals_per_day':
      return `${value.toFixed(2)} ADD/100 dyr/dag`;
    case 'worker_safety_burden_score':
      return value.toFixed(1);
    default:
      return value.toLocaleString('da-DK');
  }
};

// Simple ranking table component like the old front page
function SimpleRankingTable({
  title,
  description,
  items,
  showTop = 20,
}: {
  title: string;
  description: string;
  items: MunicipalityRanking[];
  showTop?: number;
}) {
  const displayItems = items.slice(0, showTop);

  return (
    <Card className="w-full">
      <CardHeader className="pb-3">
        <CardTitle className="text-lg font-semibold">{title}</CardTitle>
        <CardDescription className="text-sm text-gray-600">
          {description}
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0">
        <div className="overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="border-b border-gray-200 bg-gray-50">
                <th className="w-16 px-4 py-3 text-left text-xs font-medium tracking-wider text-gray-500 uppercase">
                  Rang
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium tracking-wider text-gray-500 uppercase">
                  Kommune
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium tracking-wider text-gray-500 uppercase">
                  Værdi
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 bg-white">
              {displayItems.map((item) => (
                <tr key={item.municipality} className="hover:bg-gray-50">
                  <td className="px-4 py-3 whitespace-nowrap">
                    <div className="flex h-8 w-8 items-center justify-center text-sm font-bold text-gray-900">
                      {item.rank}
                    </div>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap">
                    <div className="space-y-1">
                      <div className="text-sm font-medium text-gray-900">
                        {item.municipality}
                      </div>
                      {item.additional_data?.unique_companies && (
                        <div className="text-xs text-gray-500">
                          {item.additional_data.unique_companies} virksomheder
                          {item.additional_data.organic_percentage &&
                            ` • ${item.additional_data.organic_percentage.toFixed(1)}% økologisk`}
                        </div>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right whitespace-nowrap">
                    <div className="text-sm font-semibold text-gray-900">
                      {formatValue(item.value, item.metric)}
                    </div>
                    <div className="text-xs text-gray-500">2024</div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

export default function MunicipalityRankingsPage() {
  const [data, setData] = useState<MunicipalityRankingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedYear, setSelectedYear] = useState('2024');
  const [limit, setLimit] = useState(100);
  const [mounted, setMounted] = useState(false);

  // Prevent hydration issues
  useEffect(() => {
    setMounted(true);
  }, []);

  const fetchRankings = useCallback(async () => {
    try {
      setLoading(true);
      const response = await fetch(
        `/api/supabase/functions/kommuner?category=all&year=${selectedYear}&limit=${limit}`
      );

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();
      setData(result);
      setError(null);
    } catch (err) {
      console.error('Error fetching municipality rankings:', err);
      setError(
        err instanceof Error ? err.message : 'Ukendt fejl ved hentning af data'
      );
    } finally {
      setLoading(false);
    }
  }, [selectedYear, limit]);

  useEffect(() => {
    if (mounted) {
      fetchRankings();
    }
  }, [fetchRankings, mounted]);

  // Loading state
  if (!mounted || loading) {
    return (
      <div className="container mx-auto py-8">
        <div className="flex min-h-[400px] items-center justify-center">
          <div className="text-center">
            <Loader2 className="mx-auto h-12 w-12 animate-spin text-green-600" />
            <p className="mt-4 text-gray-600">Indlæser kommuneranglister...</p>
          </div>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="container mx-auto py-8">
        <div className="flex min-h-[400px] items-center justify-center">
          <div className="text-center">
            <p className="font-medium text-red-600">
              Fejl ved indlæsning af data
            </p>
            <p className="mt-2 text-gray-600">{error}</p>
            <Button onClick={fetchRankings} className="mt-4">
              Prøv igen
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto space-y-6 py-8">
      {/* Header */}
      <div className="space-y-4 text-center">
        <h1 className="text-4xl font-bold text-gray-900">Kommune Ranglister</h1>
        <p className="mx-auto max-w-3xl text-xl text-gray-600">
          Sammenlign danske kommuner på tværs af landbrug, miljø, produktion og
          dyrevelfærd
        </p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center justify-between gap-4 rounded-lg border bg-white p-4">
        <div className="flex items-center gap-4">
          <Select value={selectedYear} onValueChange={setSelectedYear}>
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Vælg år" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="2024">2024</SelectItem>
              <SelectItem value="2025">2025</SelectItem>
            </SelectContent>
          </Select>

          <Select
            value={limit.toString()}
            onValueChange={(value) => setLimit(parseInt(value))}
          >
            <SelectTrigger className="w-40">
              <SelectValue placeholder="Antal kommuner" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="10">Top 10</SelectItem>
              <SelectItem value="20">Top 20</SelectItem>
              <SelectItem value="50">Top 50</SelectItem>
              <SelectItem value="100">Alle kommuner</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <Button onClick={fetchRankings} disabled={loading}>
          <TrendingUp className="mr-2 h-4 w-4" />
          Opdater data
        </Button>
      </div>

      {/* Rankings Grid - Like old front page */}
      {data && (
        <div className="grid grid-cols-1 gap-6">
          {/* Land Use Rankings */}
          {data.rankings.land_use && (
            <SimpleRankingTable
              title="Størst landbrugsareal"
              description="Kommuner med det største samlede landbrugsareal i 2024"
              items={data.rankings.land_use}
              showTop={20}
            />
          )}

          {/* Production Rankings */}
          {data.rankings.production && (
            <SimpleRankingTable
              title="Højest produktionskapacitet"
              description="Kommuner med den højeste produktionskapacitet i 2024"
              items={data.rankings.production}
              showTop={20}
            />
          )}

          {/* Environmental Rankings */}
          {data.rankings.environmental && (
            <SimpleRankingTable
              title="Højest økologisk andel"
              description="Kommuner med den højeste andel økologisk landbrug i 2024"
              items={data.rankings.environmental}
              showTop={20}
            />
          )}

          {/* Animal Health Rankings */}
          {data.rankings.animal_health && (
            <SimpleRankingTable
              title="Lavest antibiotikaforbrug"
              description="Kommuner med det laveste antibiotikaforbrug per dyr i 2024"
              items={data.rankings.animal_health}
              showTop={20}
            />
          )}

          {/* Worker Safety Rankings */}
          {data.rankings.worker_safety && (
            <SimpleRankingTable
              title="Bedst arbejdssikkerhed"
              description="Kommuner med den bedste arbejdssikkerhed i 2024"
              items={data.rankings.worker_safety}
              showTop={20}
            />
          )}
        </div>
      )}

      {/* Footer like old front page */}
      <div className="border-t border-gray-200 pt-8 text-center">
        <p className="text-xs text-gray-500">
          Data opdateret:{' '}
          {data?.metadata.generated_at
            ? new Date(data.metadata.generated_at).toLocaleDateString('da-DK')
            : ''}{' '}
          • {data?.metadata.categories_included.length || 0} ranglister baseret
          på officielle data fra 2024
        </p>
      </div>
    </div>
  );
}
