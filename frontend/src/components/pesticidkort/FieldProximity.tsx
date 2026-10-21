import { Droplets, Home, GraduationCap } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import { describeFieldOperator } from '@/lib/field-operator';
import {
  formatDistanceFromField,
  parseDistanceM,
  parseProximityList,
} from '@/lib/proximity-parser';

export function FieldProximity({ field }: { field: NearbyFieldSummary }) {
  const residential = parseProximityList(field.residential_buildings_proximity);
  const schools = parseProximityList(field.educational_facilities_proximity);
  const waterDistance = parseDistanceM(field.water_distance_proximity);
  const nearestHome = residential[0];
  const nearestSchool = schools[0];
  const hasBuildingProximity = Boolean(nearestHome || nearestSchool);

  if (!nearestHome && !nearestSchool && waterDistance === null) {
    return null;
  }

  return (
    <div
      data-testid={`field-proximity-${field.field_uuid}`}
      className="text-muted-foreground mt-2 space-y-1 text-xs"
    >
      {nearestHome && (
        <div className="flex items-start gap-1">
          <Home className="mt-0.5 h-3 w-3 shrink-0" />
          <span className="min-w-0">
            Nærmeste bolig: {nearestHome.address}
            {nearestHome.buildingType
              ? ` (${nearestHome.buildingType.toLowerCase()})`
              : ''}{' '}
            – {formatDistanceFromField(nearestHome.distanceM)}
            {residential.length > 1 && (
              <span> · {residential.length} adresser inden for 100 m</span>
            )}
          </span>
        </div>
      )}
      {nearestSchool && (
        <div className="flex items-start gap-1">
          <GraduationCap className="mt-0.5 h-3 w-3 shrink-0" />
          <span className="min-w-0">
            Nærmeste skole/daginstitution: {nearestSchool.address} –{' '}
            {formatDistanceFromField(nearestSchool.distanceM)}
          </span>
        </div>
      )}
      {waterDistance !== null && (
        <div className="flex items-start gap-1">
          <Droplets className="mt-0.5 h-3 w-3 shrink-0" />
          <span>Vandløb/sø: {formatDistanceFromField(waterDistance)}</span>
        </div>
      )}
      {hasBuildingProximity && (
        <p className="text-[11px]">
          Adresserne er nabobygninger tæt på marken – ikke markens ejer.
        </p>
      )}
    </div>
  );
}

export function FieldOperator({ field }: { field: NearbyFieldSummary }) {
  const info = describeFieldOperator(
    field.cvr_number,
    field.total_pesticide_applications > 0
  );

  if (!info) return null;

  return (
    <div data-testid={`field-operator-${field.field_uuid}`} className="mt-2">
      <a
        href={info.href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-primary text-xs underline-offset-4 hover:underline"
      >
        {info.label}
      </a>
      {info.note && (
        <p className="text-muted-foreground text-[11px]">{info.note}</p>
      )}
    </div>
  );
}
