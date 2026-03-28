'use client';

import React, { useState, useEffect } from 'react';
import { X, Building2, TrendingUp } from 'lucide-react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';

interface CompanyContribution {
  company_id: string;
  company_name: string;
  cvr_number: string;
  value: number;
  percentage_of_municipality: number;
  rank_in_municipality: number;
  additional_data?: {
    field_count?: number;
    organic_area?: number;
    organic_percentage?: number;
    application_count?: number;
    treated_area_ha?: number;
    site_name?: string;
    chr_number?: string;
    total_area_ha?: number;
    [key: string]: unknown;
  };
}

interface MunicipalityDetailsData {
  municipality: string;
  category: string;
  year: number;
  total_municipality_value: number;
  municipality_rank: number | null;
  companies: CompanyContribution[];
  metadata: {
    total_companies: number;
    generated_at: string;
  };
}

interface MunicipalityDetailsModalProps {
  municipality: string | null;
  category: string;
  year: number;
  onClose: () => void;
}

const formatValue = (value: number, category: string): string => {
  switch (category) {
    case 'land_use':
    case 'organic_farming':
      return `${value.toLocaleString('da-DK')} ha`;
    case 'production':
      return `${value.toLocaleString('da-DK')} dyr`;
    case 'pesticide_burden':
    case 'pesticide_pfas':
    case 'pesticide_glyphosate':
      return value.toFixed(1);
    case 'antibiotic_usage':
      return `${value.toLocaleString('da-DK')} DDD`;
    case 'environmental':
      return `${value.toFixed(1)} kg/ha`;
    default:
      return value.toLocaleString('da-DK');
  }
};

const getCategoryTitle = (category: string): string => {
  const titles: Record<string, string> = {
    land_use: 'Landbrugsareal',
    production: 'Dyreproduktion',
    pesticide_burden: 'Pesticidbelastning',
    pesticide_pfas: 'PFAS-pesticider',
    pesticide_glyphosate: 'Glyfosat',
    antibiotic_usage: 'Antibiotikaforbrug',
    environmental: 'Kvælstofudledning',
    organic_farming: 'Økologisk landbrug',
  };
  return titles[category] || category;
};

export function MunicipalityDetailsModal({
  municipality,
  category,
  year,
  onClose,
}: MunicipalityDetailsModalProps) {
  const [data, setData] = useState<MunicipalityDetailsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!municipality) return;

    const fetchDetails = async () => {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch(
          `/api/supabase/functions/municipality-details?municipality=${encodeURIComponent(municipality)}&category=${category}&year=${year}&limit=20`
        );

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const result = await response.json();
        setData(result);
      } catch (err) {
        console.error('Error fetching municipality details:', err);
        setError(
          err instanceof Error ? err.message : 'Fejl ved hentning af data'
        );
      } finally {
        setLoading(false);
      }
    };

    fetchDetails();
  }, [municipality, category, year]);

  if (!municipality) return null;

  return (
    <div className="bg-foreground/50 fixed inset-0 z-50 flex items-center justify-center">
      <div className="bg-background relative max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-lg border shadow-lg">
        {/* Header */}
        <div className="border-border flex items-center justify-between border-b p-6">
          <div>
            <h2 className="text-foreground text-2xl font-bold">
              {municipality}
            </h2>
            <p className="text-muted-foreground mt-1">
              {getCategoryTitle(category)} - virksomheder der bidrager til
              rangeringen
            </p>
          </div>
          <button
            onClick={onClose}
            data-testid="close-municipality-modal-button"
            className="text-muted-foreground hover:text-foreground rounded-full p-2 transition-colors"
          >
            <X className="h-6 w-6" />
          </button>
        </div>

        {/* Content */}
        <div className="max-h-[calc(90vh-120px)] overflow-y-auto p-6">
          {loading && (
            <div className="flex items-center justify-center py-12">
              <div className="text-center">
                <div className="border-primary mx-auto h-8 w-8 animate-spin rounded-full border-2 border-t-transparent"></div>
                <p className="text-muted-foreground mt-4">Indlæser data...</p>
              </div>
            </div>
          )}

          {error && (
            <div className="py-12 text-center">
              <p className="text-destructive">Fejl: {error}</p>
            </div>
          )}

          {data && !loading && (
            <div className="space-y-6">
              {/* Summary */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="h-5 w-5" />
                    Oversigt
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                    <div>
                      <div className="text-primary text-2xl font-bold">
                        {data.metadata.total_companies}
                      </div>
                      <div className="text-muted-foreground text-sm">
                        Virksomheder
                      </div>
                    </div>
                    <div>
                      <div className="text-2xl font-bold">
                        {formatValue(data.total_municipality_value, category)}
                      </div>
                      <div className="text-muted-foreground text-sm">
                        Total værdi
                      </div>
                    </div>
                    <div>
                      <div className="text-2xl font-bold">{year}</div>
                      <div className="text-muted-foreground text-sm">
                        Dataår
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Company List */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Building2 className="h-5 w-5" />
                    Virksomheder
                  </CardTitle>
                  <CardDescription>
                    Virksomheder rangeret efter deres bidrag til {municipality}s
                    position
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
                            Virksomhed
                          </th>
                          <th className="text-muted-foreground px-4 py-3 text-right text-xs font-medium tracking-wider uppercase">
                            Værdi
                          </th>
                          <th className="text-muted-foreground px-4 py-3 text-right text-xs font-medium tracking-wider uppercase">
                            Andel
                          </th>
                        </tr>
                      </thead>
                      <tbody className="divide-border divide-y">
                        {data.companies.map((company) => (
                          <tr
                            key={company.company_id}
                            className="hover:bg-muted/50 transition-colors"
                          >
                            <td className="px-4 py-3 whitespace-nowrap">
                              <div className="text-foreground flex h-8 w-8 items-center justify-center text-sm font-bold">
                                {company.rank_in_municipality}
                              </div>
                            </td>
                            <td className="px-4 py-3 whitespace-nowrap">
                              <div className="space-y-1">
                                <div className="text-foreground text-sm font-medium">
                                  {company.company_name}
                                </div>
                                <div className="text-muted-foreground text-xs">
                                  CVR: {company.cvr_number}
                                  {company.additional_data?.field_count && (
                                    <span className="ml-2">
                                      {company.additional_data.field_count}{' '}
                                      marker
                                    </span>
                                  )}
                                  {company.additional_data?.site_name && (
                                    <span className="ml-2">
                                      {company.additional_data.site_name}
                                    </span>
                                  )}
                                  {company.additional_data
                                    ?.application_count && (
                                    <span className="ml-2">
                                      {
                                        company.additional_data
                                          .application_count
                                      }{' '}
                                      sprøjtninger
                                    </span>
                                  )}
                                </div>
                              </div>
                            </td>
                            <td className="px-4 py-3 text-right whitespace-nowrap">
                              <div className="text-foreground text-sm font-semibold">
                                {formatValue(company.value, category)}
                              </div>
                            </td>
                            <td className="px-4 py-3 text-right whitespace-nowrap">
                              <div className="text-foreground text-sm">
                                {company.percentage_of_municipality.toFixed(1)}%
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
