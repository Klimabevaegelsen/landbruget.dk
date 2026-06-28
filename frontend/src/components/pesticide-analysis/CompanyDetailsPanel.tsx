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
import { toast } from 'sonner';
import {
  CompanySummary,
  CompanyDetailsResponse,
  PesticideProduct,
} from './types';

interface CompanyDetailsPanelProps {
  company: CompanySummary;
}

export function CompanyDetailsPanel({ company }: CompanyDetailsPanelProps) {
  const [details, setDetails] = useState<CompanyDetailsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [noDetailsAvailable, setNoDetailsAvailable] = useState(false);

  // Fetch detailed company data
  useEffect(() => {
    const fetchDetails = async () => {
      if (!company.cvr_number) return;

      setLoading(true);
      setError(null);
      setNoDetailsAvailable(false);

      // Show loading toast for company details
      const toastId = toast.loading('Indlæser virksomhedsdetaljer', {
        description: `Henter detaljer for ${company.company_name}...`,
      });

      try {
        const response = await fetch(
          `/api/data/pesticide-company-details?cvr=${company.cvr_number}`
        );

        if (!response.ok) {
          if (response.status === 404) {
            setDetails(null);
            setNoDetailsAvailable(true);
            return;
          }
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
        toast.dismiss(toastId);
      }
    };

    fetchDetails();
  }, [company.cvr_number, company.company_name]);

  const formatBelastning = (value: number) => {
    return value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
  };

  const getTopProducts = (yearData: {
    applications_by_product: PesticideProduct[];
    use_allocations_by_product?: PesticideProduct[];
  }) => {
    return (
      yearData.use_allocations_by_product ?? yearData.applications_by_product
    )
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
            <div className="text-muted-foreground flex gap-1 text-xs">
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
            <div className="text-muted-foreground text-xs">
              {product.use_allocations ?? product.applications} allokeringer
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
        <div className="text-destructive mb-2 text-sm">{error}</div>
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

  if (noDetailsAvailable) {
    return (
      <div className="space-y-4">
        <div>
          <div className="mb-2 flex items-center gap-2">
            <Building2 className="text-muted-foreground h-4 w-4" />
            <h3 className="text-foreground text-sm font-semibold">
              {company.company_name || `Virksomhed ${company.cvr_number}`}
            </h3>
          </div>
          <div className="text-muted-foreground space-y-1 text-xs">
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

        <div className="grid grid-cols-2 gap-3 text-xs">
          <div className="bg-primary/10 rounded p-2">
            <div className="text-primary font-medium">Total Belastning</div>
            <div className="text-primary text-lg font-bold">
              {formatBelastning(company.total_belastning)}
            </div>
          </div>
          <div className="bg-muted rounded p-2">
            <div className="text-muted-foreground font-medium">
              Allokeringer
            </div>
            <div className="text-foreground text-lg font-bold">
              {(
                company.total_use_allocations ?? company.total_applications
              ).toLocaleString()}
            </div>
          </div>
          <div className="bg-accent/10 rounded p-2">
            <div className="text-accent-foreground font-medium">
              Behandlet Areal
            </div>
            <div className="text-accent-foreground text-lg font-bold">
              {company.total_treated_area_ha.toLocaleString('da-DK', {
                maximumFractionDigits: 0,
              })}{' '}
              ha
            </div>
          </div>
          <div className="bg-secondary/10 rounded p-2">
            <div className="text-secondary-foreground font-medium">
              Produkter
            </div>
            <div className="text-secondary-foreground text-lg font-bold">
              {company.unique_products}
            </div>
          </div>
        </div>

        <div className="bg-muted text-muted-foreground rounded p-3 text-sm">
          Ingen ekstra pesticiddetaljer er tilgængelige for denne virksomhed i
          det aktuelle eksporterede datasæt.
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Company Header */}
      <div>
        <div className="mb-2 flex items-center gap-2">
          <Building2 className="text-muted-foreground h-4 w-4" />
          <h3 className="text-foreground text-sm font-semibold">
            {company.company_name || `Virksomhed ${company.cvr_number}`}
          </h3>
        </div>
        <div className="text-muted-foreground space-y-1 text-xs">
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
        <div className="bg-primary/10 rounded p-2">
          <div className="text-primary font-medium">Total Belastning</div>
          <div className="text-primary text-lg font-bold">
            {formatBelastning(company.total_belastning)}
          </div>
        </div>
        <div className="bg-muted rounded p-2">
          <div className="text-muted-foreground font-medium">Allokeringer</div>
          <div className="text-foreground text-lg font-bold">
            {(
              company.total_use_allocations ?? company.total_applications
            ).toLocaleString()}
          </div>
        </div>
        <div className="bg-accent/10 rounded p-2">
          <div className="text-accent-foreground font-medium">
            Behandlet Areal
          </div>
          <div className="text-accent-foreground text-lg font-bold">
            {company.total_treated_area_ha.toLocaleString('da-DK', {
              maximumFractionDigits: 0,
            })}{' '}
            ha
          </div>
        </div>
        <div className="bg-secondary/10 rounded p-2">
          <div className="text-secondary-foreground font-medium">Produkter</div>
          <div className="text-secondary-foreground text-lg font-bold">
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
          <div className="bg-muted rounded p-2 text-xs">
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
          <div className="text-muted-foreground text-center text-xs">
            Detaljeret data tilgængelig for {details.yearly_breakdown.length} år
          </div>
        )}
      </div>
    </div>
  );
}
