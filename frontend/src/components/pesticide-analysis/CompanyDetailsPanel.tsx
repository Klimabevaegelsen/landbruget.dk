'use client';

import { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Building2, MapPin, Calendar, Beaker, TrendingUp, Eye, Loader2 } from 'lucide-react';
import { CompanySummary, CompanyDetailsResponse, PesticideProduct } from './types';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

interface CompanyDetailsPanelProps {
  company: CompanySummary;
  onViewFields: () => void;
}

export default function CompanyDetailsPanel({ company, onViewFields }: CompanyDetailsPanelProps) {
  const [details, setDetails] = useState<CompanyDetailsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch detailed company data
  useEffect(() => {
    const fetchDetails = async () => {
      if (!SUPABASE_URL || !company.cvr_number) return;

      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `${SUPABASE_URL}/functions/v1/pesticide-company-details?cvr=${company.cvr_number}`,
          {
            headers: {
              'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
              'Content-Type': 'application/json',
            },
          }
        );

        if (!response.ok) {
          throw new Error(`Failed to fetch company details: ${response.status}`);
        }

        const result: CompanyDetailsResponse = await response.json();
        setDetails(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch details');
        console.error('Company details error:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchDetails();
  }, [company.cvr_number]);

  const formatBelastning = (value: number) => {
    return value.toLocaleString('da-DK', {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1
    });
  };

  const getTopProducts = (yearData: { applications_by_product: PesticideProduct[] }) => {
    return yearData.applications_by_product
      .slice(0, 5)
      .map((product: PesticideProduct) => (
        <div key={product.registration_number} className="flex justify-between items-center py-1">
          <div className="flex-1">
            <div className="text-xs font-medium truncate">
              {product.product_name || 'Ukendt produkt'}
            </div>
            <div className="text-xs text-gray-500 flex gap-1">
              {product.contains_pfas && <Badge variant="destructive" className="text-xs px-1 py-0">PFAS</Badge>}
              {product.contains_diquat && <Badge variant="destructive" className="text-xs px-1 py-0">Diquat</Badge>}
              {product.contains_glyphosate && <Badge variant="secondary" className="text-xs px-1 py-0">Glyph</Badge>}
            </div>
          </div>
          <div className="text-right ml-2">
            <div className="text-xs font-medium">{formatBelastning(product.total_belastning)}</div>
            <div className="text-xs text-gray-500">{product.applications} anvendelser</div>
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
      <div className="text-center py-4">
        <div className="text-red-600 text-sm mb-2">{error}</div>
        <Button variant="outline" size="sm" onClick={() => window.location.reload()}>
          Prøv igen
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Company Header */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <Building2 className="h-4 w-4 text-gray-500" />
          <h3 className="font-semibold text-sm">
            {company.company_name || `Virksomhed ${company.cvr_number}`}
          </h3>
        </div>
        <div className="text-xs text-gray-500 space-y-1">
          <div>CVR: {company.cvr_number}</div>
          <div className="flex items-center gap-1">
            <MapPin className="h-3 w-3" />
            {company.municipality !== 'Municipality TBD' ? company.municipality : 'Ukendt kommune'}
          </div>
        </div>
      </div>

      <Separator />

      {/* Key Metrics */}
      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="bg-blue-50 p-2 rounded">
          <div className="font-medium text-blue-900">Total Belastning</div>
          <div className="text-lg font-bold text-blue-600">
            {formatBelastning(company.total_belastning)}
          </div>
        </div>
        <div className="bg-gray-50 p-2 rounded">
          <div className="font-medium text-gray-700">Anvendelser</div>
          <div className="text-lg font-bold text-gray-900">
            {company.total_applications.toLocaleString()}
          </div>
        </div>
        <div className="bg-green-50 p-2 rounded">
          <div className="font-medium text-green-900">Behandlet Areal</div>
          <div className="text-lg font-bold text-green-600">
            {company.total_treated_area_ha.toLocaleString('da-DK', { maximumFractionDigits: 0 })} ha
          </div>
        </div>
        <div className="bg-purple-50 p-2 rounded">
          <div className="font-medium text-purple-900">Produkter</div>
          <div className="text-lg font-bold text-purple-600">
            {company.unique_products}
          </div>
        </div>
      </div>

      {/* Chemical Breakdown */}
      <div>
        <h4 className="font-medium text-sm mb-2 flex items-center gap-1">
          <Beaker className="h-4 w-4" />
          Kemikalier
        </h4>
        <div className="space-y-2 text-xs">
          {company.pfas_belastning > 0 && (
            <div className="flex justify-between items-center">
              <Badge variant="destructive" className="text-xs">PFAS</Badge>
              <span className="font-medium">{formatBelastning(company.pfas_belastning)}</span>
            </div>
          )}
          {company.diquat_belastning > 0 && (
            <div className="flex justify-between items-center">
              <Badge variant="destructive" className="text-xs">Diquat</Badge>
              <span className="font-medium">{formatBelastning(company.diquat_belastning)}</span>
            </div>
          )}
          {company.glyphosate_belastning > 0 && (
            <div className="flex justify-between items-center">
              <Badge variant="secondary" className="text-xs">Glyphosat</Badge>
              <span className="font-medium">{formatBelastning(company.glyphosate_belastning)}</span>
            </div>
          )}
          {company.pfas_belastning === 0 && company.diquat_belastning === 0 && company.glyphosate_belastning === 0 && (
            <div className="text-gray-500 text-xs">Ingen særlige kemikalier registreret</div>
          )}
        </div>
      </div>

      {/* Years Active */}
      {company.years_active.length > 0 && (
        <div>
          <h4 className="font-medium text-sm mb-2 flex items-center gap-1">
            <Calendar className="h-4 w-4" />
            Aktive År
          </h4>
          <div className="flex flex-wrap gap-1">
            {company.years_active.sort((a, b) => b - a).map(year => (
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
          <h4 className="font-medium text-sm mb-2 flex items-center gap-1">
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
          <h4 className="font-medium text-sm mb-2">Kommunal Placering</h4>
          <div className="bg-gray-50 p-2 rounded text-xs">
            <div>Rang: #{details.municipality_ranking.rank} af {details.municipality_ranking.total_companies_in_municipality}</div>
            <div>Percentil: {details.municipality_ranking.percentile}%</div>
          </div>
        </div>
      )}

      <Separator />

      {/* Actions */}
      <div className="space-y-2">
        <Button
          variant="outline"
          size="sm"
          onClick={onViewFields}
          className="w-full flex items-center gap-2"
          disabled
        >
          <Eye className="h-4 w-4" />
          Se Marker (kommer snart)
        </Button>

        {details && details.yearly_breakdown.length > 0 && (
          <div className="text-xs text-gray-500 text-center">
            Detaljeret data tilgængelig for {details.yearly_breakdown.length} år
          </div>
        )}
      </div>
    </div>
  );
}
