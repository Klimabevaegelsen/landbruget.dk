'use client';

import React from 'react';
import { Home, School } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { FieldAnalysisData } from '@/components/field-analysis/types';

interface ProximityCardProps {
  field: FieldAnalysisData;
}

export function ProximityCard({ field }: ProximityCardProps) {
  const hasAny =
    field.residential_buildings_proximity ||
    field.educational_facilities_proximity ||
    field.water_distance_proximity;

  return (
    <Card className="p-4 lg:p-6">
      <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
        Nærhedsanalyse
      </h3>
      <div className="space-y-2 text-sm lg:space-y-3 lg:text-base">
        {field.residential_buildings_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground flex items-center">
              <Home className="mr-1 h-4 w-4" />
              Boliger:
            </span>
            <span className="text-xs font-medium lg:text-sm">
              {field.residential_buildings_proximity}
            </span>
          </div>
        )}
        {field.educational_facilities_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground flex items-center">
              <School className="mr-1 h-4 w-4" />
              Skoler:
            </span>
            <span className="text-xs font-medium lg:text-sm">
              {field.educational_facilities_proximity}
            </span>
          </div>
        )}
        {field.water_distance_proximity && (
          <div className="flex justify-between">
            <span className="text-muted-foreground">Vand:</span>
            <span className="text-xs font-medium lg:text-sm">
              {field.water_distance_proximity}
            </span>
          </div>
        )}
        {!hasAny && (
          <div className="text-muted-foreground text-xs italic lg:text-sm">
            Ingen nærhedsdata tilgængelig
          </div>
        )}
      </div>
    </Card>
  );
}
