'use client';

import React from 'react';
import { AlertTriangle, Home } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';
import { getPesticideRiskLevel } from './pesticide-utils';

interface RiskSummaryCardProps {
  field: FieldAnalysisData;
}

export function RiskSummaryCard({ field }: RiskSummaryCardProps) {
  const riskLevel = getPesticideRiskLevel(field.total_pesticide_belastning);

  const proximityWarnings = [
    field.residential_buildings_proximity,
    field.educational_facilities_proximity,
    field.water_distance_proximity,
  ].filter(Boolean).length;

  const hasPfas = (field.pfas_applications ?? 0) > 0;
  const hasBnbo = (field.bnbo_area_hectares ?? 0) > 0.001;

  return (
    <Card
      className={`border-l-4 p-4 lg:p-5 ${
        riskLevel.variant === 'destructive'
          ? 'border-l-destructive'
          : riskLevel.variant === 'default'
            ? 'border-l-conventional'
            : 'border-l-primary'
      }`}
      data-testid="risk-summary-card"
    >
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-foreground text-base font-semibold lg:text-lg">
          Risikoprofil
        </h3>
        <Badge variant={riskLevel.variant} data-testid="risk-level-badge">
          {riskLevel.level}
        </Badge>
      </div>

      <div className="grid grid-cols-2 gap-3 text-sm">
        <div className="bg-muted rounded-lg p-2.5">
          <div className="text-muted-foreground text-xs">Belastning</div>
          <div className={`text-lg font-bold ${riskLevel.color}`}>
            {formatNumber(field.total_pesticide_belastning) || '0'}
          </div>
        </div>

        <div className="bg-muted rounded-lg p-2.5">
          <div className="text-muted-foreground text-xs">Pesticider</div>
          <div className="text-foreground text-lg font-bold">
            {field.unique_pesticide_products || 0}
          </div>
        </div>

        {hasPfas && (
          <div className="border-warning/30 bg-warning/5 dark:bg-warning/10 rounded-lg border p-2.5">
            <div className="flex items-center gap-1">
              <AlertTriangle className="text-warning h-3.5 w-3.5" />
              <span className="text-warning-foreground text-xs font-medium">
                PFAS ({field.pfas_applications})
              </span>
            </div>
          </div>
        )}

        {hasBnbo && (
          <div className="bg-primary/10 rounded-lg p-2.5">
            <div className="text-primary text-xs font-medium">
              BNBO: {formatNumber(field.bnbo_area_hectares) || '0'} ha
            </div>
          </div>
        )}

        {proximityWarnings > 0 && (
          <div className="border-warning/30 bg-warning/5 dark:bg-warning/10 col-span-2 flex items-center gap-2 rounded-lg border p-2.5">
            <Home className="text-warning h-3.5 w-3.5" />
            <span className="text-warning-foreground text-xs">
              {proximityWarnings} nærhedsadvarsel
              {proximityWarnings > 1 ? 'er' : ''}
            </span>
          </div>
        )}
      </div>
    </Card>
  );
}
