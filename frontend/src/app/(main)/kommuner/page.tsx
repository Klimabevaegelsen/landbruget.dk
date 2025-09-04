'use client';

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { MapPin, Leaf, Factory, Heart, Shield, TrendingUp } from 'lucide-react';

interface MunicipalityRanking {
  municipality: string;
  rank: number;
  value: number;
  metric: string;
  additional_data?: Record<string, unknown>;
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

const categoryConfig = {
  land_use: {
    title: 'Landbrugsarealer',
    icon: MapPin,
    description: 'Ranking baseret på landbrugsareal og markfordeling',
    color: 'bg-green-100 text-green-800',
  },
  environmental: {
    title: 'Miljøpræstation',
    icon: Leaf,
    description: 'Ranking baseret på økologisk landbrug og miljøindikatorer',
    color: 'bg-emerald-100 text-emerald-800',
  },
  production: {
    title: 'Produktionskapacitet',
    icon: Factory,
    description: 'Ranking baseret på produktionsanlæg og kapacitet',
    color: 'bg-blue-100 text-blue-800',
  },
  animal_health: {
    title: 'Dyresundhed',
    icon: Heart,
    description: 'Ranking baseret på antibiotikaforbrug (lavere er bedre)',
    color: 'bg-red-100 text-red-800',
  },
  worker_safety: {
    title: 'Arbejdssikkerhed',
    icon: Shield,
    description: 'Ranking baseret på arbejdsulykker og sikkerhed',
    color: 'bg-purple-100 text-purple-800',
  },
};

export default function MunicipalityRankingsPage() {
  const [data, setData] = useState<MunicipalityRankingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedCategory, setSelectedCategory] = useState('land_use');
  const [selectedYear, setSelectedYear] = useState('2024');
  const [limit, setLimit] = useState(20);

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
    fetchRankings();
  }, [fetchRankings]);

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

  const getRankingColor = (rank: number): string => {
    if (rank <= 3) return 'bg-yellow-100 text-yellow-800';
    if (rank <= 10) return 'bg-gray-100 text-gray-800';
    return 'bg-white text-gray-600';
  };

  if (loading) {
    return (
      <div className="container mx-auto py-8">
        <div className="flex min-h-[400px] items-center justify-center">
          <div className="text-center">
            <div className="mx-auto h-12 w-12 animate-spin rounded-full border-b-2 border-green-600"></div>
            <p className="mt-4 text-gray-600">Indlæser kommuneranglister...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto py-8">
        <Card className="border-red-200">
          <CardHeader>
            <CardTitle className="text-red-800">Fejl ved indlæsning</CardTitle>
            <CardDescription className="text-red-600">
              {error.includes('404')
                ? 'Kommune ranglister data er ikke tilgængelige endnu. Systemet er ved at blive opdateret.'
                : error}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <Button onClick={fetchRankings} className="mt-4">
                Prøv igen
              </Button>
              {error.includes('404') && (
                <p className="text-sm text-gray-600">
                  💡 Tip: Siden er lige blevet oprettet og data indlæses. Prøv
                  igen om et par minutter.
                </p>
              )}
            </div>
          </CardContent>
        </Card>
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

        {data && (
          <div className="flex items-center justify-center space-x-4 text-sm text-gray-500">
            <span>📊 {data.metadata.total_municipalities} kommuner</span>
            <span>📅 Data fra {data.metadata.year}</span>
            <span>
              🕒 Opdateret{' '}
              {new Date(data.metadata.generated_at).toLocaleDateString(
                'da-DK',
                {
                  year: 'numeric',
                  month: 'long',
                  day: 'numeric',
                }
              )}
            </span>
          </div>
        )}
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
              <SelectItem value="100">Alle</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <Button onClick={fetchRankings} disabled={loading}>
          <TrendingUp className="mr-2 h-4 w-4" />
          Opdater data
        </Button>
      </div>

      {/* Rankings Tabs */}
      <Tabs
        value={selectedCategory}
        onValueChange={setSelectedCategory}
        className="space-y-6"
      >
        <TabsList className="grid w-full grid-cols-5">
          {Object.entries(categoryConfig).map(([key, config]) => {
            const Icon = config.icon;
            return (
              <TabsTrigger
                key={key}
                value={key}
                className="flex items-center gap-2"
              >
                <Icon className="h-4 w-4" />
                <span className="hidden sm:inline">{config.title}</span>
              </TabsTrigger>
            );
          })}
        </TabsList>

        {Object.entries(categoryConfig).map(([categoryKey, config]) => {
          const rankings =
            data?.rankings[categoryKey as keyof typeof data.rankings];
          const Icon = config.icon;

          return (
            <TabsContent
              key={categoryKey}
              value={categoryKey}
              className="space-y-4"
            >
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-3">
                    <Icon className="h-6 w-6" />
                    {config.title}
                  </CardTitle>
                  <CardDescription>{config.description}</CardDescription>
                </CardHeader>
                <CardContent>
                  {rankings && rankings.length > 0 ? (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-16">Rang</TableHead>
                          <TableHead>Kommune</TableHead>
                          <TableHead className="text-right">Værdi</TableHead>
                          <TableHead className="hidden md:table-cell">
                            Detaljer
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {rankings.map((ranking) => (
                          <TableRow
                            key={ranking.municipality}
                            className="hover:bg-gray-50"
                          >
                            <TableCell>
                              <Badge className={getRankingColor(ranking.rank)}>
                                #{ranking.rank}
                              </Badge>
                            </TableCell>
                            <TableCell className="font-medium">
                              {ranking.municipality}
                            </TableCell>
                            <TableCell className="text-right font-mono">
                              {formatValue(ranking.value, ranking.metric)}
                            </TableCell>
                            <TableCell className="hidden text-sm text-gray-600 md:table-cell">
                              {categoryKey === 'land_use' &&
                                ranking.additional_data && (
                                  <div className="space-y-1">
                                    <div>
                                      {ranking.additional_data.total_fields?.toLocaleString()}{' '}
                                      marker
                                    </div>
                                    <div>
                                      {String(
                                        ranking.additional_data.unique_companies
                                      )}{' '}
                                      virksomheder
                                    </div>
                                    <div>
                                      {typeof ranking.additional_data
                                        .organic_percentage === 'number'
                                        ? ranking.additional_data.organic_percentage.toFixed(
                                            1
                                          )
                                        : String(
                                            ranking.additional_data
                                              .organic_percentage
                                          )}
                                      % økologisk
                                    </div>
                                  </div>
                                )}
                              {categoryKey === 'production' &&
                                ranking.additional_data && (
                                  <div className="space-y-1">
                                    <div>
                                      {String(
                                        ranking.additional_data.total_sites
                                      )}{' '}
                                      anlæg
                                    </div>
                                    <div>
                                      {String(
                                        ranking.additional_data.unique_companies
                                      )}{' '}
                                      virksomheder
                                    </div>
                                  </div>
                                )}
                              {categoryKey === 'environmental' &&
                                ranking.additional_data && (
                                  <div className="space-y-1">
                                    <div>
                                      {ranking.additional_data.total_fields?.toLocaleString()}{' '}
                                      marker
                                    </div>
                                    <div>
                                      {ranking.additional_data.organic_fields?.toLocaleString()}{' '}
                                      økologiske
                                    </div>
                                  </div>
                                )}
                              {categoryKey === 'animal_health' &&
                                ranking.additional_data && (
                                  <div className="space-y-1">
                                    <div>
                                      {String(
                                        ranking.additional_data
                                          .companies_with_antibiotics
                                      )}{' '}
                                      virksomheder
                                    </div>
                                    <div>
                                      {String(
                                        ranking.additional_data
                                          .sites_with_antibiotics
                                      )}{' '}
                                      steder
                                    </div>
                                  </div>
                                )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  ) : (
                    <div className="py-8 text-center text-gray-500">
                      <Icon className="mx-auto mb-4 h-12 w-12 opacity-50" />
                      <p>Ingen data tilgængelig for denne kategori</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          );
        })}
      </Tabs>
    </div>
  );
}
