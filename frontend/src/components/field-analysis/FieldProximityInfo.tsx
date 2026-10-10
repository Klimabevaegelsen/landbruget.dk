'use client';

import type { FieldAnalysisData } from './types';
import { ProximityList } from '@/components/field-analysis/ProximityList';
import {
  formatDistanceFromField,
  parseDistanceM,
  parseProximityList,
} from '@/lib/proximity-parser';

interface FieldProximityInfoProps {
  fieldData: FieldAnalysisData;
}

export function FieldProximityInfo({ fieldData }: FieldProximityInfoProps) {
  const hasAny =
    fieldData.residential_buildings_proximity ||
    fieldData.educational_facilities_proximity ||
    fieldData.water_distance_proximity;
  const residential = parseProximityList(
    fieldData.residential_buildings_proximity
  );
  const schools = parseProximityList(
    fieldData.educational_facilities_proximity
  );
  const waterDistance = parseDistanceM(fieldData.water_distance_proximity);
  const hasBuildingProximity = residential.length > 0 || schools.length > 0;

  return (
    <div className="mb-4">
      <h3 className="text-foreground mb-2 text-base font-semibold">
        Nærhedsanalyse
      </h3>
      <div className="space-y-1 text-sm">
        <ProximityList
          heading="Boliger inden for 100 m af marken (naboer – ikke ejer)"
          entries={residential}
        />
        <ProximityList
          heading="Skoler og daginstitutioner inden for 100 m"
          entries={schools}
        />
        {waterDistance !== null && (
          <p className="text-xs font-medium">
            Vandløb/sø: {formatDistanceFromField(waterDistance)}
          </p>
        )}
        {hasBuildingProximity && (
          <p className="text-muted-foreground text-[11px]">
            Adresserne er nabobygninger tæt på marken – ikke markens ejer.
          </p>
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
