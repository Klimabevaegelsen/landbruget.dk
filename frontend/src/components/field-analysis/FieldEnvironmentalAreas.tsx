'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';

interface FieldEnvironmentalAreasProps {
  fieldData: FieldAnalysisData;
}

export function FieldEnvironmentalAreas({
  fieldData,
}: FieldEnvironmentalAreasProps) {
  const hasBnbo = (fieldData.bnbo_area_hectares ?? 0) > 0;
  const hasWetland = (fieldData.wetland_area_hectares ?? 0) > 0;

  return (
    <div className="mb-4">
      <h3 className="text-foreground mb-2 text-base font-semibold">
        Miljøområder
      </h3>
      <div className="space-y-2">
        {hasBnbo && (
          <div className="bg-primary/10 rounded-lg p-2">
            <div className="flex items-center justify-between">
              <span className="text-primary text-sm font-medium">💧 BNBO</span>
              <span className="text-primary text-sm font-bold">
                {formatNumber(fieldData.bnbo_area_hectares)} ha
              </span>
            </div>
          </div>
        )}
        {hasWetland && (
          <div className="bg-muted rounded-lg p-2">
            <div className="flex items-center justify-between">
              <span className="text-foreground text-sm font-medium">
                💨 Lavbund
              </span>
              <span className="text-foreground text-sm font-bold">
                {formatNumber(fieldData.wetland_area_hectares)} ha
              </span>
            </div>
          </div>
        )}
        {!hasBnbo && !hasWetland && (
          <div className="text-muted-foreground p-2 text-xs italic">
            Ingen registrerede miljøområder
          </div>
        )}
      </div>
    </div>
  );
}
