'use client';

import { Home, School } from 'lucide-react';
import type { FieldAnalysisData } from './types';

interface FieldProximityInfoProps {
  fieldData: FieldAnalysisData;
}

export function FieldProximityInfo({ fieldData }: FieldProximityInfoProps) {
  const hasAny =
    fieldData.residential_buildings_proximity ||
    fieldData.educational_facilities_proximity ||
    fieldData.water_distance_proximity;

  return (
    <div className="mb-4">
      <h3 className="text-foreground mb-2 text-base font-semibold">
        Nærhedsanalyse
      </h3>
      <div className="space-y-1 text-sm">
        {fieldData.residential_buildings_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground flex items-center">
              <Home className="mr-1 h-4 w-4" />
              Boliger:
            </span>
            <span className="text-xs font-medium">
              {fieldData.residential_buildings_proximity}
            </span>
          </div>
        )}
        {fieldData.educational_facilities_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground flex items-center">
              <School className="mr-1 h-4 w-4" />
              Skoler:
            </span>
            <span className="text-xs font-medium">
              {fieldData.educational_facilities_proximity}
            </span>
          </div>
        )}
        {fieldData.water_distance_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground">🌊 Vand:</span>
            <span className="text-xs font-medium">
              {fieldData.water_distance_proximity}
            </span>
          </div>
        )}
        {!hasAny && (
          <div className="text-muted-foreground text-xs italic">
            Ingen nærhedsdata tilgængelig
          </div>
        )}
      </div>
    </div>
  );
}
