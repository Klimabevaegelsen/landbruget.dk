'use client';

import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';
import { getPesticideRiskLevel } from './pesticide-utils';
import { DosageRow } from './dosage-row';

interface PesticideSummaryCardProps {
  field: FieldAnalysisData;
}

export function PesticideSummaryCard({ field }: PesticideSummaryCardProps) {
  const riskLevel = getPesticideRiskLevel(field.total_pesticide_belastning);

  return (
    <Card className="p-4 lg:p-6" data-testid="pesticide-summary-card">
      <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
        Pesticidforbrug
      </h3>

      <div className="bg-muted mb-3 rounded-lg p-3 lg:p-4">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-sm font-medium lg:text-base">
            Samlet belastning
          </span>
          <span className={`text-lg font-bold lg:text-xl ${riskLevel.color}`}>
            {formatNumber(field.total_pesticide_belastning) || '0'}
          </span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-muted-foreground text-xs lg:text-sm">
            Risikoniveau
          </span>
          <Badge variant={riskLevel.variant} className="text-xs lg:text-sm">
            {riskLevel.level}
          </Badge>
        </div>
      </div>

      {field.unique_pesticide_products &&
        field.unique_pesticide_products > 0 && (
          <div className="bg-primary/10 mb-3 rounded-lg p-3 lg:p-4">
            <div className="mb-1 flex items-center justify-between">
              <span className="text-primary text-sm font-medium lg:text-base">
                Produkter anvendt
              </span>
              <span className="text-primary text-lg font-bold lg:text-xl">
                {field.unique_pesticide_products}
              </span>
            </div>
            {field.total_pesticide_applications &&
              field.total_pesticide_applications > 0 && (
                <div className="flex items-center justify-between">
                  <span className="text-primary/80 text-xs lg:text-sm">
                    Total pesticider
                  </span>
                  <span className="text-primary text-xs font-medium lg:text-sm">
                    {field.total_pesticide_applications}
                  </span>
                </div>
              )}
          </div>
        )}

      {hasDosageData(field) && (
        <div className="space-y-2 lg:space-y-3">
          <DosageRow
            label="Total dosering (kg)"
            value={field.total_dosage_kg}
            unit="kg"
            decimals={2}
          />
          <DosageRow
            label="Total dosering (L)"
            value={field.total_dosage_liters}
            unit="L"
            decimals={1}
          />
          <DosageRow
            label="Total dosering (g)"
            value={field.total_dosage_grams}
            unit="g"
            decimals={0}
          />
          <DosageRow
            label="Total dosering (ml)"
            value={field.total_dosage_ml}
            unit="ml"
            decimals={0}
          />
          <DosageRow
            label="Total dosering"
            value={field.total_dosage_tablets}
            unit="tabletter"
            decimals={0}
            threshold={0}
          />
        </div>
      )}

      {field.is_partial_coverage && (
        <div className="bg-conventional/10 mt-3 flex items-center space-x-2 rounded-lg p-2 lg:p-3">
          <AlertTriangle className="text-conventional h-4 w-4" />
          <span className="text-conventional/80 text-xs lg:text-sm">
            Delvis markdækning
          </span>
        </div>
      )}
    </Card>
  );
}

function hasDosageData(field: FieldAnalysisData): boolean {
  return (
    (field.total_dosage_kg != null && field.total_dosage_kg > 0.001) ||
    (field.total_dosage_liters != null && field.total_dosage_liters > 0.001) ||
    (field.total_dosage_grams != null && field.total_dosage_grams > 0.001) ||
    (field.total_dosage_ml != null && field.total_dosage_ml > 0.001) ||
    (field.total_dosage_tablets != null && field.total_dosage_tablets > 0)
  );
}
