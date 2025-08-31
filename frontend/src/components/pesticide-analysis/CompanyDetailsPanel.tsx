'use client';

import { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import {
  Building2,
  MapPin,
  Calendar,
  Beaker,
  TrendingUp,
  Loader2,
} from 'lucide-react';
import { useToast } from '@/components/ui/toast';
import {
  CompanySummary,
  CompanyDetailsResponse,
  PesticideProduct,
} from './types';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

interface CompanyDetailsPanelProps {
  company: CompanySummary;
}

export default function CompanyDetailsPanel({
  company,
}: CompanyDetailsPanelProps) {
  const [details, setDetails] = useState<CompanyDetailsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { addToast, removeToast } = useToast();

  // Fetch detailed company data
  useEffect(() => {
    const fetchDetails = async () => {
      if (!SUPABASE_URL || !company.cvr_number) return;

      setLoading(true);
      setError(null);

      // Show loading toast for company details
      const toastId = addToast({
        title: 'Indlæser virksomhedsdetaljer',
        description: `Henter detaljer for ${company.company_name}...`,
        variant: 'loading',
      });

      try {
        const response = await fetch(
          `${SUPABASE_URL}/functions/v1/pesticide-company-details?cvr=${company.cvr_number}`,
          {
            headers: {
              Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
              'Content-Type': 'application/json',
            },
          }
        );

        if (!response.ok) {
          throw new Error(
            `Failed to fetch company details: ${response.status}`
          );
        }

        const result: CompanyDetailsResponse = await response.json();
        setDetails(result);
      } catch (err) {
        setError(
          err instanceof Error ? err.message : 'Failed to fetch details'
        );
        console.error('Company details error:', err);
      } finally {
        setLoading(false);
        // Remove loading toast when details fetch completes
        removeToast(toastId);
      }
    };

    fetchDetails();
  }, [company.cvr_number]); // Only depend on cvr_number, not toast functions

  const formatBelastning = (value: number) => {
    return value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  };

  const getTopProducts = (yearData: {
    applications_by_product: PesticideProduct[];
  }) => {
    return yearData.applications_by_product
      .slice(0, 5)
      .map((product: PesticideProduct) => (
        <div
          key={product.registration_number}
          className="flex items-center justify-between py-1"
        >
          <div className="flex-1">
            <div className="truncate text-xs font-medium">
              {product.product_name || 'Ukendt produkt'}
            </div>
            <div className="flex gap-1 text-xs text-gray-500">
              {product.contains_pfas && (
                <Badge variant="destructive" className="px-1 py-0 text-xs">
                  PFAS
                </Badge>
              )}
              {product.contains_diquat && (
                <Badge variant="destructive" className="px-1 py-0 text-xs">
                  Diquat
                </Badge>
              )}
              {product.contains_glyphosate && (
                <Badge variant="secondary" className="px-1 py-0 text-xs">
                  Glyph
                </Badge>
              )}
            </div>
          </div>
          <div className="ml-2 text-right">
            <div className="text-xs font-medium">
              {formatBelastning(product.total_belastning)}
            </div>
            <div className="text-xs text-gray-500">
              {product.applications} anvendelser
            </div>
          </div>
        </div>
      ));
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span className="ml-2 text-sm">Indlæser detaljer...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="py-4 text-center">
        <div className="mb-2 text-sm text-red-600">{error}</div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => window.location.reload()}
        >
          Prøv igen
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Company Header */}
      <div>
        <div className="mb-2 flex items-center gap-2">
          <Building2 className="h-4 w-4 text-gray-500" />
          <h3 className="text-sm font-semibold">
            {company.company_name || `Virksomhed ${company.cvr_number}`}
          </h3>
        </div>
        <div className="space-y-1 text-xs text-gray-500">
          <div>CVR: {company.cvr_number}</div>
          <div className="flex items-center gap-1">
            <MapPin className="h-3 w-3" />
            {company.municipality !== 'Municipality TBD'
              ? company.municipality
              : 'Ukendt kommune'}
          </div>
        </div>
      </div>

      <Separator />

      {/* Key Metrics */}
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="rounded bg-blue-50 p-2">
          <div className="font-medium text-blue-900">Total Belastning</div>
          <div className="text-lg font-bold text-blue-600">
            {formatBelastning(company.total_belastning)}
          </div>
        </div>
        <div className="rounded bg-gray-50 p-2">
          <div className="font-medium text-gray-700">Anvendelser</div>
          <div className="text-lg font-bold text-gray-900">
            {company.total_applications.toLocaleString()}
          </div>
        </div>
        <div className="rounded bg-green-50 p-2">
          <div className="font-medium text-green-900">Behandlet Areal</div>
          <div className="text-lg font-bold text-green-600">
            {company.total_treated_area_ha.toLocaleString('da-DK', {
              maximumFractionDigits: 0,
            })}{' '}
            ha
          </div>
        </div>
        <div className="rounded bg-purple-50 p-2">
          <div className="font-medium text-purple-900">Produkter</div>
          <div className="text-lg font-bold text-purple-600">
            {company.unique_products}
          </div>
        </div>
      </div>

      {/* Chemical Breakdown - only show if any chemicals have values > 0 */}
      {(company.pfas_belastning > 0 ||
        company.diquat_belastning > 0 ||
        company.glyphosate_belastning > 0) && (
        <div>
          <h4 className="mb-2 flex items-center gap-1 text-sm font-medium">
            <Beaker className="h-4 w-4" />
            Kemikalier
          </h4>
          <div className="space-y-2 text-xs">
            {company.pfas_belastning > 0 && (
              <div className="flex items-center justify-between">
                <Badge variant="destructive" className="text-xs">
                  PFAS
                </Badge>
                <span className="font-medium">
                  {formatBelastning(company.pfas_belastning)}
                </span>
              </div>
            )}
            {company.diquat_belastning > 0 && (
              <div className="flex items-center justify-between">
                <Badge variant="destructive" className="text-xs">
                  Diquat
                </Badge>
                <span className="font-medium">
                  {formatBelastning(company.diquat_belastning)}
                </span>
              </div>
            )}
            {company.glyphosate_belastning > 0 && (
              <div className="flex items-center justify-between">
                <Badge variant="secondary" className="text-xs">
                  Glyphosat
                </Badge>
                <span className="font-medium">
                  {formatBelastning(company.glyphosate_belastning)}
                </span>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Years Active */}
      {company.years_active.length > 0 && (
        <div>
          <h4 className="mb-2 flex items-center gap-1 text-sm font-medium">
            <Calendar className="h-4 w-4" />
            Aktive År
          </h4>
          <div className="flex flex-wrap gap-1">
            {company.years_active
              .sort((a, b) => b - a)
              .map((year) => (
                <Badge key={year} variant="outline" className="text-xs">
                  {year}
                </Badge>
              ))}
          </div>
        </div>
      )}

      {/* Top Products (if details loaded) */}
      {details && details.yearly_breakdown.length > 0 && (
        <div>
          <h4 className="mb-2 flex items-center gap-1 text-sm font-medium">
            <TrendingUp className="h-4 w-4" />
            Top Produkter ({details.yearly_breakdown[0].year})
          </h4>
          <div className="space-y-1">
            {getTopProducts(details.yearly_breakdown[0])}
          </div>
        </div>
      )}

      {/* Municipality Ranking (if details loaded) */}
      {details && (
        <div>
          <h4 className="mb-2 text-sm font-medium">
            Rankering i{' '}
            {company.municipality !== 'Municipality TBD'
              ? company.municipality
              : 'kommunen'}
          </h4>
          <div className="rounded bg-gray-50 p-2 text-xs">
            <div>
              Rang: #{details.municipality_ranking.rank} af{' '}
              {details.municipality_ranking.total_companies_in_municipality}
            </div>
            <div>Percentil: {details.municipality_ranking.percentile}%</div>
          </div>
        </div>
      )}

      <Separator />

      {/* Actions */}
      <div className="space-y-2">
        {details && details.yearly_breakdown.length > 0 && (
          <div className="text-center text-xs text-gray-500">
            Detaljeret data tilgængelig for {details.yearly_breakdown.length} år
          </div>
        )}
      </div>
    </div>
  );
}
