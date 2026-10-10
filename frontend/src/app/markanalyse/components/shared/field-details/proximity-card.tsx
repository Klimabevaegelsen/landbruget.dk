'use client';

import React from 'react';
import { Card } from '@/components/ui/card';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { ProximityList } from '@/components/field-analysis/ProximityList';
import {
  formatDistanceFromField,
  parseDistanceM,
  parseProximityList,
} from '@/lib/proximity-parser';

interface ProximityCardProps {
  field: FieldAnalysisData;
}

export function ProximityCard({ field }: ProximityCardProps) {
  const hasAny =
    field.residential_buildings_proximity ||
    field.educational_facilities_proximity ||
    field.water_distance_proximity;
  const residential = parseProximityList(field.residential_buildings_proximity);
  const schools = parseProximityList(field.educational_facilities_proximity);
  const waterDistance = parseDistanceM(field.water_distance_proximity);
  const hasBuildingProximity = residential.length > 0 || schools.length > 0;

  return (
    <Card className="p-4 lg:p-6">
      <h3 className="text-foreground mb-3 text-base font-semibold lg:text-lg">
        Nærhedsanalyse
      </h3>
      <div className="space-y-2 text-sm lg:space-y-3 lg:text-base">
        <ProximityList
          heading="Boliger inden for 100 m af marken (naboer – ikke ejer)"
          entries={residential}
        />
        <ProximityList
          heading="Skoler og daginstitutioner inden for 100 m"
          entries={schools}
        />
        {waterDistance !== null && (
          <p className="text-xs font-medium lg:text-sm">
            Vandløb/sø: {formatDistanceFromField(waterDistance)}
          </p>
        )}
        {hasBuildingProximity && (
          <p className="text-muted-foreground text-[11px] lg:text-xs">
            Adresserne er nabobygninger tæt på marken – ikke markens ejer.
          </p>
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
