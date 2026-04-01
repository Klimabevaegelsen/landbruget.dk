import { Droplets, Home, GraduationCap } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';

export function FieldProximity({ field }: { field: NearbyFieldSummary }) {
  if (
    !field.residential_buildings_proximity &&
    !field.educational_facilities_proximity &&
    !field.water_distance_proximity
  )
    return null;

  return (
    <div className="text-muted-foreground mt-2 flex flex-wrap gap-3 text-xs">
      {field.residential_buildings_proximity && (
        <span className="flex items-center gap-1">
          <Home className="h-3 w-3" />
          {field.residential_buildings_proximity.split('\n')[0]}
        </span>
      )}
      {field.educational_facilities_proximity && (
        <span className="flex items-center gap-1">
          <GraduationCap className="h-3 w-3" />
          {field.educational_facilities_proximity.split('\n')[0]}
        </span>
      )}
      {field.water_distance_proximity && (
        <span className="flex items-center gap-1">
          <Droplets className="h-3 w-3" />
          {field.water_distance_proximity}
        </span>
      )}
    </div>
  );
}

export function FieldProducts({ field }: { field: NearbyFieldSummary }) {
  return (
    <div className="text-muted-foreground mt-2 space-y-1 text-xs">
      {field.pfas_products_detail && (
        <p className="text-warning">PFAS: {field.pfas_products_detail}</p>
      )}
      {field.glyphosate_products_detail && (
        <p>Glyphosat: {field.glyphosate_products_detail}</p>
      )}
      {field.diquat_products_detail && (
        <p className="text-destructive">
          Diquat: {field.diquat_products_detail}
        </p>
      )}
      {field.other_products_detail && (
        <p>Øvrige: {field.other_products_detail}</p>
      )}
    </div>
  );
}
