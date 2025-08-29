'use client';

import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Loader2, Search, Filter, BarChart3 } from 'lucide-react';
import { useToast } from '@/components/ui/toast';
import CompanyFilterPanel from './CompanyFilterPanel';
import CompanyListView from './CompanyListView';
import CompanyDetailsPanel from './CompanyDetailsPanel';
import {
  PesticideAnalysisFilters,
  CompanySummary,
  PesticideAnalysisResponse,
} from './types';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;

export default function PesticideAnalysisVisualization() {
  const [filters, setFilters] = useState<PesticideAnalysisFilters>({
    geography: 'country',
    year: 'all',
    type: 'total',
    cvr: '',
    page: 1,
    limit: 50,
    sortBy: 'belastning',
    sortOrder: 'desc',
  });

  const [data, setData] = useState<PesticideAnalysisResponse | null>(null);
  const [selectedCompany, setSelectedCompany] = useState<CompanySummary | null>(
    null
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentLoadingToastId, setCurrentLoadingToastId] = useState<
    string | null
  >(null);

  const { addToast, removeToast } = useToast();

  // Fetch data from API
  const fetchData = useCallback(async () => {
    if (!SUPABASE_URL) {
      setError('Supabase URL not configured');
      return;
    }

    // Remove any existing loading toast
    if (currentLoadingToastId) {
      removeToast(currentLoadingToastId);
    }

    // Show loading toast for data fetch
    const toastId = addToast({
      title: 'Indlæser pesticiddata',
      description: 'Henter analysedata...',
      variant: 'loading',
    });
    setCurrentLoadingToastId(toastId);

    setLoading(true);
    setError(null);

    try {
      const params = new URLSearchParams();
      Object.entries(filters).forEach(([key, value]) => {
        if (value !== null && value !== undefined && value !== '') {
          params.append(key, value.toString());
        }
      });

      const response = await fetch(
        `/api/supabase/functions/pesticide-analysis?${params}`
      );

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
      // Remove loading toast when data fetch completes
      if (currentLoadingToastId) {
        removeToast(currentLoadingToastId);
        setCurrentLoadingToastId(null);
      }
    }
  }, [filters, currentLoadingToastId, addToast, removeToast]);

  // Fetch data when filters change
  useEffect(() => {
    fetchData();
  }, [filters, fetchData]);

  // Update filters
  const updateFilters = (newFilters: Partial<PesticideAnalysisFilters>) => {
    // Remove any existing loading toast when filters change (new search)
    if (currentLoadingToastId) {
      removeToast(currentLoadingToastId);
      setCurrentLoadingToastId(null);
    }
    setFilters((prev) => ({ ...prev, ...newFilters, page: 1 })); // Reset to page 1 when filters change
  };

  // Handle company selection
  const handleCompanySelect = (company: CompanySummary) => {
    setSelectedCompany(company);
  };

  // Handle pagination
  const handlePageChange = (newPage: number) => {
    setFilters((prev) => ({ ...prev, page: newPage }));
  };

  return (
    <div className="space-y-6">
      {/* Summary Stats */}
      {data && (
        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Virksomheder
              </CardTitle>
              <BarChart3 className="text-muted-foreground h-4 w-4" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {data.total_count.toLocaleString()}
              </div>
              <p className="text-muted-foreground text-xs">
                Viser {data.companies.length} af {data.total_count}
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Pesticidbelastning
              </CardTitle>
              <BarChart3 className="text-muted-foreground h-4 w-4" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {data.companies
                  .reduce((sum, c) => sum + c.total_belastning, 0)
                  .toFixed(1)}
              </div>
              <p className="text-muted-foreground text-xs">Total belastning</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                PFAS Anvendelse
              </CardTitle>
              <BarChart3 className="text-muted-foreground h-4 w-4" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {data.companies.filter((c) => c.pfas_belastning > 0).length}
              </div>
              <p className="text-muted-foreground text-xs">
                Virksomheder med PFAS
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Glyphosat</CardTitle>
              <BarChart3 className="text-muted-foreground h-4 w-4" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {
                  data.companies.filter((c) => c.glyphosate_belastning > 0)
                    .length
                }
              </div>
              <p className="text-muted-foreground text-xs">
                Virksomheder med glyphosat
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Main Content */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
        {/* Filter Panel */}
        <div className="lg:col-span-3">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Filter className="h-5 w-5" />
                Filtre
              </CardTitle>
            </CardHeader>
            <CardContent>
              <CompanyFilterPanel
                filters={filters}
                availableYears={data?.filters.available_years || []}
                availableMunicipalities={
                  data?.filters.available_municipalities || []
                }
                onFiltersChange={updateFilters}
              />
            </CardContent>
          </Card>
        </div>

        {/* Company List */}
        <div className="lg:col-span-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Search className="h-5 w-5" />
                Virksomheder
                {loading && <Loader2 className="h-4 w-4 animate-spin" />}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {error && (
                <div className="mb-4 rounded border border-red-200 bg-red-50 px-4 py-3 text-red-700">
                  {error}
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={fetchData}
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
                  onCompanySelect={handleCompanySelect}
                  onPageChange={handlePageChange}
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

        {/* Company Details Panel */}
        <div className="lg:col-span-3">
          <Card>
            <CardHeader>
              <CardTitle>Virksomhedsdetaljer</CardTitle>
            </CardHeader>
            <CardContent>
              {selectedCompany ? (
                <CompanyDetailsPanel company={selectedCompany} />
              ) : (
                <div className="text-muted-foreground py-8 text-center">
                  <Search className="mx-auto mb-4 h-12 w-12 opacity-50" />
                  <p>Vælg en virksomhed for at se detaljer</p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
