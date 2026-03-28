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
import { TrendingUp, Loader2, MousePointer } from 'lucide-react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { MunicipalityDetailsModal } from '@/components/kommuner/MunicipalityDetailsModal';

interface MunicipalityRanking {
  municipality: string;
  rank: number;
  value: number;
  metric: string;
  additional_data?: {
    unique_companies?: number;
    organic_percentage?: number;
    total_applications?: number;
    total_treated_area_ha?: number;
    health_burden?: number;
    total_production_sites?: number;
    sites_with_antibiotics?: number;
    total_fields?: number;
    total_area_ha?: number;
    companies_with_incidents?: number;
    incident_types_count?: number;
    organic_area_ha?: number;
    [key: string]: unknown;
  };
}

interface MunicipalityRankingResponse {
  rankings: {
    land_use?: MunicipalityRanking[];
    production?: MunicipalityRanking[];
    pesticide_burden?: MunicipalityRanking[];
    pesticide_pfas?: MunicipalityRanking[];
    pesticide_glyphosate?: MunicipalityRanking[];
    antibiotic_usage?: MunicipalityRanking[];
    environmental?: MunicipalityRanking[];
    worker_safety?: MunicipalityRanking[];
    incidents?: MunicipalityRanking[];
    organic_farming?: MunicipalityRanking[];
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
    case 'total_animal_capacity':
      return `${value.toLocaleString('da-DK')} dyr`;
    case 'organic_farming_percentage':
      return `${value.toFixed(1)}%`;
    case 'total_pesticide_burden':
    case 'pfas_pesticide_burden':
    case 'glyphosate_pesticide_burden':
      return value.toFixed(1);
    case 'total_antibiotic_ddd_usage':
      return `${value.toLocaleString('da-DK')} DDD`;
    case 'avg_nitrogen_leaching_kg':
      return `${value.toFixed(1)} kg/ha`;
    case 'total_workplace_incidents':
    case 'total_incidents':
      return `${value.toLocaleString('da-DK')} tilfælde`;
    default:
      return value.toLocaleString('da-DK');
  }
};

// Simple ranking table component with click functionality
function SimpleRankingTable({
  title,
  description,
  items,
  showTop = 20,
  category,
  onMunicipalityClick,
}: {
  title: string;
  description: string;
  items: MunicipalityRanking[];
  showTop?: number;
  category: string;
  onMunicipalityClick: (municipality: string, category: string) => void;
}) {
  const displayItems = items.slice(0, showTop);

  return (
    <Card className="w-full">
      <CardHeader className="pb-3">
        <CardTitle className="text-lg font-semibold">{title}</CardTitle>
        <CardDescription className="text-muted-foreground text-sm">
          {description}
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0">
        <div className="overflow-hidden">
          <table className="w-full">
            <thead>
              <tr className="border-border bg-muted/30 border-b">
                <th className="text-muted-foreground w-16 px-4 py-3 text-left text-xs font-medium tracking-wider uppercase">
                  Rang
                </th>
                <th className="text-muted-foreground px-4 py-3 text-left text-xs font-medium tracking-wider uppercase">
                  Kommune
                </th>
                <th className="text-muted-foreground px-4 py-3 text-right text-xs font-medium tracking-wider uppercase">
                  Værdi
                </th>
              </tr>
            </thead>
            <tbody className="divide-border divide-y">
              {displayItems.map((item) => (
                <tr
                  key={item.municipality}
                  className="hover:bg-muted/50 group cursor-pointer transition-colors"
                  onClick={() =>
                    onMunicipalityClick(item.municipality, category)
                  }
                  title={`Klik for at se virksomheder i ${item.municipality}`}
                >
                  <td className="px-4 py-3 whitespace-nowrap">
                    <div className="text-foreground flex h-8 w-8 items-center justify-center text-sm font-bold">
                      {item.rank}
                    </div>
                  </td>
                  <td className="px-4 py-3 whitespace-nowrap">
                    <div className="space-y-1">
                      <div className="text-foreground flex items-center gap-2 text-sm font-medium">
                        {item.municipality}
                        <MousePointer className="h-3 w-3 opacity-0 transition-opacity group-hover:opacity-50" />
                      </div>
                      {item.additional_data && (
                        <div className="text-muted-foreground space-y-1 text-xs">
                          {/* Common data */}
                          {item.additional_data.unique_companies && (
                            <div>
                              {item.additional_data.unique_companies}{' '}
                              virksomheder
                            </div>
                          )}

                          {/* Land use specific */}
                          {item.additional_data.total_fields && (
                            <div>
                              {item.additional_data.total_fields.toLocaleString(
                                'da-DK'
                              )}{' '}
                              marker
                            </div>
                          )}
                          {item.additional_data.organic_percentage && (
                            <div>
                              {item.additional_data.organic_percentage.toFixed(
                                1
                              )}
                              % økologisk
                            </div>
                          )}

                          {/* Pesticide specific */}
                          {item.additional_data.total_applications && (
                            <div>
                              {item.additional_data.total_applications.toLocaleString(
                                'da-DK'
                              )}{' '}
                              sprøjtninger
                            </div>
                          )}
                          {item.additional_data.total_treated_area_ha && (
                            <div>
                              {item.additional_data.total_treated_area_ha.toLocaleString(
                                'da-DK'
                              )}{' '}
                              ha behandlet
                            </div>
                          )}

                          {/* Production specific */}
                          {item.additional_data.total_production_sites && (
                            <div>
                              {item.additional_data.total_production_sites}{' '}
                              produktionssteder
                            </div>
                          )}
                          {item.additional_data.sites_with_antibiotics && (
                            <div>
                              {item.additional_data.sites_with_antibiotics}{' '}
                              steder med antibiotika
                            </div>
                          )}

                          {/* Environmental specific */}
                          {item.additional_data.total_area_ha &&
                            item.metric === 'avg_nitrogen_leaching_kg' && (
                              <div>
                                {item.additional_data.total_area_ha.toLocaleString(
                                  'da-DK'
                                )}{' '}
                                ha total
                              </div>
                            )}

                          {/* Incident specific */}
                          {item.additional_data.companies_with_incidents && (
                            <div>
                              {item.additional_data.companies_with_incidents}{' '}
                              virksomheder påvirket
                            </div>
                          )}
                          {item.additional_data.incident_types_count && (
                            <div>
                              {item.additional_data.incident_types_count}{' '}
                              forskellige typer
                            </div>
                          )}

                          {/* Organic farming specific */}
                          {item.additional_data.organic_area_ha &&
                            item.metric === 'organic_farming_percentage' && (
                              <div>
                                {item.additional_data.organic_area_ha.toLocaleString(
                                  'da-DK'
                                )}{' '}
                                ha økologisk
                              </div>
                            )}
                        </div>
                      )}
                    </div>
                  </td>
                  <td className="px-4 py-3 text-right whitespace-nowrap">
                    <div className="text-foreground text-sm font-semibold">
                      {formatValue(item.value, item.metric)}
                    </div>
                    <div className="text-muted-foreground text-xs">2024</div>
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
  // Modal state
  const [selectedMunicipality, setSelectedMunicipality] = useState<
    string | null
  >(null);
  const [selectedCategory, setSelectedCategory] = useState<string>('land_use');

  // Handle municipality click
  const handleMunicipalityClick = (municipality: string, category: string) => {
    setSelectedMunicipality(municipality);
    setSelectedCategory(category);
  };

  const handleCloseModal = () => {
    setSelectedMunicipality(null);
  };

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
            <Loader2 className="text-primary mx-auto h-12 w-12 animate-spin" />
            <p className="text-muted-foreground mt-4">
              Indlæser kommuneranglister...
            </p>
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
            <p className="text-destructive font-medium">
              Fejl ved indlæsning af data
            </p>
            <p className="text-muted-foreground mt-2">{error}</p>
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
        <h1 className="text-foreground text-4xl font-bold">
          Kommune Ranglister
        </h1>
        <p className="text-muted-foreground mx-auto max-w-3xl text-xl">
          Omfattende ranglister for danske kommuner inden for landbrug, miljø,
          dyrevelfærd og arbejdssikkerhed
        </p>
        <p className="text-muted-foreground mx-auto max-w-2xl text-sm">
          Data baseret på markernes faktiske placering og produktionsstedernes
          lokation for præcis geografisk tilknytning
        </p>
        <p className="text-muted-foreground mx-auto mt-2 flex max-w-2xl items-center justify-center gap-1 text-xs">
          <MousePointer className="h-3 w-3" />
          Klik på en kommune for at se hvilke virksomheder der bidrager til
          rangeringen
        </p>
      </div>

      {/* Controls */}
      <div className="bg-background flex flex-wrap items-center justify-between gap-4 rounded-lg border p-4">
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

      {/* Rankings Grid - Comprehensive like main page */}
      {data && (
        <div className="grid grid-cols-1 gap-6">
          {/* Land Use Rankings */}
          {data.rankings.land_use && (
            <SimpleRankingTable
              title="Størst landbrugsareal"
              description="Kommuner med det største samlede landbrugsareal i 2024"
              items={data.rankings.land_use}
              showTop={20}
              category="land_use"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Organic Farming Rankings */}
          {data.rankings.organic_farming && (
            <SimpleRankingTable
              title="Højest økologisk andel"
              description="Kommuner med den højeste andel økologisk landbrug i 2024"
              items={data.rankings.organic_farming}
              showTop={20}
              category="organic_farming"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Animal Production Rankings */}
          {data.rankings.production && (
            <SimpleRankingTable
              title="Størst dyreproduktion"
              description="Kommuner med den største samlede dyreproduktionskapacitet i 2024"
              items={data.rankings.production}
              showTop={20}
              category="production"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Pesticide Burden Rankings */}
          {data.rankings.pesticide_burden && (
            <SimpleRankingTable
              title="Højest pesticidbelastning"
              description="Kommuner med den største samlede pesticidbelastning i 2023"
              items={data.rankings.pesticide_burden}
              showTop={20}
              category="pesticide_burden"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* PFAS Pesticide Rankings */}
          {data.rankings.pesticide_pfas && (
            <SimpleRankingTable
              title="Højest PFAS-pesticid belastning"
              description="Kommuner med den største PFAS-pesticid belastning i 2023"
              items={data.rankings.pesticide_pfas}
              showTop={20}
              category="pesticide_pfas"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Glyphosate Pesticide Rankings */}
          {data.rankings.pesticide_glyphosate && (
            <SimpleRankingTable
              title="Højest glyfosat belastning"
              description="Kommuner med den største glyfosat belastning i 2023"
              items={data.rankings.pesticide_glyphosate}
              showTop={20}
              category="pesticide_glyphosate"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Antibiotic Usage Rankings */}
          {data.rankings.antibiotic_usage && (
            <SimpleRankingTable
              title="Højest antibiotikaforbrug"
              description="Kommuner med det største antibiotikaforbrug på produktionssteder i 2024"
              items={data.rankings.antibiotic_usage}
              showTop={20}
              category="antibiotic_usage"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Environmental Rankings (Nitrogen Leaching) */}
          {data.rankings.environmental && (
            <SimpleRankingTable
              title="Højest kvælstofudledning"
              description="Kommuner med den højeste gennemsnitlige kvælstofudledning pr. hektar"
              items={data.rankings.environmental}
              showTop={20}
              category="environmental"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Worker Safety Rankings */}
          {data.rankings.worker_safety && (
            <SimpleRankingTable
              title="Flest arbejdsulykker"
              description="Kommuner med flest rapporterede arbejdsulykker i landbruget"
              items={data.rankings.worker_safety}
              showTop={20}
              category="worker_safety"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}

          {/* Incidents Rankings */}
          {data.rankings.incidents && (
            <SimpleRankingTable
              title="Flest hændelser"
              description="Kommuner med flest rapporterede hændelser og ulykker i landbruget"
              items={data.rankings.incidents}
              showTop={20}
              category="incidents"
              onMunicipalityClick={handleMunicipalityClick}
            />
          )}
        </div>
      )}

      {/* Footer like old front page */}
      <div className="border-border border-t pt-8 text-center">
        <p className="text-muted-foreground text-xs">
          Data opdateret:{' '}
          {data?.metadata.generated_at
            ? new Date(data.metadata.generated_at).toLocaleDateString('da-DK')
            : ''}{' '}
          • {data?.metadata.categories_included.length || 0} ranglister baseret
          på officielle data fra 2024
        </p>
      </div>

      {/* Municipality Details Modal */}
      {selectedMunicipality && (
        <MunicipalityDetailsModal
          municipality={selectedMunicipality}
          category={selectedCategory}
          year={parseInt(selectedYear)}
          onClose={handleCloseModal}
        />
      )}
    </div>
  );
}
