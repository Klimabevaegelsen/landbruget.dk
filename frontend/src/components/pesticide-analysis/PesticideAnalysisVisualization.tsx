'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Loader2, Search } from 'lucide-react';
import { usePesticideAnalysis } from '@/hooks/usePesticideAnalysis';
import { AnalysisKpiRow } from './AnalysisKpiRow';
import { CompanyFilterPanel } from './CompanyFilterPanel';
import { CompanyListView } from './CompanyListView';
import { CompanyDetailsPanel } from './CompanyDetailsPanel';
import type { PesticideAnalysisFilters, CompanySummary } from './types';

export function PesticideAnalysisVisualization() {
  const [filters, setFilters] = useState<PesticideAnalysisFilters>({
    geography: 'country',
    years: [],
    type: 'total',
    cvr: '',
    page: 1,
    limit: 50,
    sortBy: 'belastning',
    sortOrder: 'desc',
  });

  const [selectedCompany, setSelectedCompany] = useState<CompanySummary | null>(
    null
  );

  const { data, loading, error, refetch } = usePesticideAnalysis(filters);

  const updateFilters = (newFilters: Partial<PesticideAnalysisFilters>) => {
    setFilters((prev) => ({ ...prev, ...newFilters, page: 1 }));
  };

  return (
    <div className="space-y-6">
      {data && <AnalysisKpiRow data={data} />}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
        <div className="lg:col-span-3">
          <div className="sticky top-4 space-y-6">
            <h2 className="text-foreground text-sm font-semibold">Filtre</h2>
            <CompanyFilterPanel
              filters={filters}
              availableYears={data?.filters?.available_years || []}
              availableMunicipalities={
                data?.filters?.available_municipalities || []
              }
              onFiltersChange={updateFilters}
            />
          </div>
        </div>

        <div className="lg:col-span-6">
          <Card className="ring-border ring-1">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                Virksomheder
                {loading && <Loader2 className="h-4 w-4 animate-spin" />}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {error && (
                <div className="border-destructive/20 bg-destructive/10 text-destructive mb-4 rounded border px-4 py-3">
                  {error}
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={refetch}
                    className="ml-2"
                  >
                    Prøv igen
                  </Button>
                </div>
              )}

              {data && !loading && (
                <CompanyListView
                  companies={data.companies}
                  totalCount={data.total_count}
                  currentPage={data.page}
                  limit={data.limit}
                  onCompanySelect={setSelectedCompany}
                  onPageChange={(p) => setFilters((f) => ({ ...f, page: p }))}
                  selectedCompany={selectedCompany}
                  sortBy={filters.sortBy}
                  sortOrder={filters.sortOrder}
                  onSortChange={(sortBy, sortOrder) =>
                    updateFilters({ sortBy, sortOrder })
                  }
                />
              )}

              {loading && !data && (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-8 w-8 animate-spin" />
                  <span className="ml-2">Indlæser data...</span>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="lg:col-span-3">
          <div className="bg-card sticky top-4 rounded-xl border p-5">
            <h2 className="text-foreground mb-4 text-sm font-semibold">
              Virksomhedsdetaljer
            </h2>
            {selectedCompany ? (
              <CompanyDetailsPanel company={selectedCompany} />
            ) : (
              <div className="text-muted-foreground py-8 text-center">
                <Search className="mx-auto mb-3 h-8 w-8 opacity-30" />
                <p className="text-sm">Vælg en virksomhed fra listen</p>
                <p className="mt-1 text-xs">
                  Klik på en virksomhed for at se detaljeret data
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
