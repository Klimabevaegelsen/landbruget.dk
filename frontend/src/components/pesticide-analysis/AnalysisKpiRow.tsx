'use client';

import { Badge } from '@/components/ui/badge';
import { Building2, Activity } from 'lucide-react';
import type { PesticideAnalysisResponse } from './types';

interface AnalysisKpiRowProps {
  data: PesticideAnalysisResponse;
}

function formatNum(n: number): string {
  return n.toLocaleString('da-DK');
}

export function AnalysisKpiRow({ data }: AnalysisKpiRowProps) {
  const hasCompanyRows = data.companies.length > 0;
  const totalBelastning = hasCompanyRows
    ? data.companies.reduce((sum, c) => sum + c.total_belastning, 0).toFixed(1)
    : '0.0';
  const applicationCount = data.summary?.total_applications ?? 0;
  const uniquePesticides = data.summary?.unique_pesticides ?? 0;

  return (
    <div
      data-testid="analysis-kpi-row"
      className="grid grid-cols-1 gap-4 md:grid-cols-3"
    >
      <div className="bg-card rounded-xl border p-5">
        <div className="text-muted-foreground mb-1 flex items-center gap-2 text-sm">
          <Building2 className="h-4 w-4" />
          Virksomheder
        </div>
        <p className="text-foreground text-2xl font-bold tabular-nums">
          {formatNum(data.filters.total_companies)}
        </p>
        <p className="text-muted-foreground mt-1 text-xs">
          Viser {data.companies.length} af {formatNum(data.total_count)}
        </p>
      </div>

      <div className="bg-card rounded-xl border p-5">
        <div className="text-muted-foreground mb-1 flex items-center gap-2 text-sm">
          <Activity className="h-4 w-4" />
          {hasCompanyRows ? 'Pesticidbelastning' : 'Pesticidanvendelser'}
        </div>
        <p className="font-display text-foreground text-2xl font-bold tabular-nums">
          {hasCompanyRows ? totalBelastning : formatNum(applicationCount)}
        </p>
        <p className="text-muted-foreground mt-1 text-xs">
          {hasCompanyRows
            ? 'Total belastning (viste virksomheder)'
            : 'Anvendelser i det aktuelle eksportudtræk'}
        </p>
      </div>

      <div className="bg-card rounded-xl border p-5">
        <div className="text-muted-foreground mb-1 text-sm">
          {hasCompanyRows ? 'Kemikalier' : 'Pesticider'}
        </div>
        {hasCompanyRows ? (
          <div className="mt-2 flex flex-wrap gap-2">
            <Badge variant="destructive" className="text-xs">
              PFAS: {formatNum(data.filters.companies_with_pfas)}
            </Badge>
            <Badge className="border-warning/30 bg-warning/10 text-warning text-xs">
              Diquat: {formatNum(data.filters.companies_with_diquat)}
            </Badge>
            <Badge variant="secondary" className="text-xs">
              Glyphosat: {formatNum(data.filters.companies_with_glyphosate)}
            </Badge>
          </div>
        ) : (
          <p className="text-foreground text-2xl font-bold tabular-nums">
            {formatNum(uniquePesticides)}
          </p>
        )}
        <p className="text-muted-foreground mt-2 text-xs">
          {hasCompanyRows
            ? 'Virksomheder med flaggede stoffer'
            : 'Unikke pesticidnavne i eksporten'}
        </p>
      </div>
    </div>
  );
}
