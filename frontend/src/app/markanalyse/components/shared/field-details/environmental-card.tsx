'use client';

import React from 'react';
import { Card } from '@/components/ui/card';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';

interface EnvironmentalCardProps {
  field: FieldAnalysisData;
}

export function EnvironmentalCard({ field }: EnvironmentalCardProps) {
  const hasBnbo = (field.bnbo_area_hectares ?? 0) > 0.001;
  const hasWetland = (field.wetland_area_hectares ?? 0) > 0.001;

  if (!hasBnbo && !hasWetland) return null;

  return (
    <Card className="p-4 lg:p-6">
      <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
        Miljøområder
      </h3>
      <div className="space-y-2 lg:space-y-3">
        {hasBnbo && (
          <div className="bg-primary/10 rounded-lg p-2 lg:p-3">
            <div className="flex items-center justify-between">
              <span className="text-primary text-sm font-medium lg:text-base">
                BNBO
              </span>
              <span className="text-primary text-sm font-bold lg:text-base">
                {formatNumber(field.bnbo_area_hectares) || '0'} ha
              </span>
            </div>
          </div>
        )}
        {hasWetland && (
          <div className="bg-muted rounded-lg p-2 lg:p-3">
            <div className="flex items-center justify-between">
              <span className="text-foreground text-sm font-medium lg:text-base">
                Lavbund
              </span>
              <span className="text-foreground text-sm font-bold lg:text-base">
                {formatNumber(field.wetland_area_hectares) || '0'} ha
              </span>
            </div>
          </div>
        )}
      </div>
    </Card>
  );
}
