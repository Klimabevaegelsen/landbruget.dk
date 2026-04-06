import { Droplets, Home, GraduationCap } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import {
  parseProductString,
  formatProduct,
} from '@/components/pesticidkort/field-utils';

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

function ProductList({
  raw,
  colorClass,
  label,
}: {
  raw: string;
  colorClass?: string;
  label: string;
}) {
  const products = parseProductString(raw);
  if (products.length === 0) return null;

  return (
    <div className={cn('space-y-0.5', colorClass)}>
      <span className="font-medium">{label}</span>
      <ul className="list-none space-y-0.5 pl-0">
        {products.map((p) => (
          <li key={`${p.name}-${p.dose}`}>{formatProduct(p)}</li>
        ))}
      </ul>
    </div>
  );
}

export function FieldProducts({ field }: { field: NearbyFieldSummary }) {
  const hasAny =
    field.pfas_products_detail ||
    field.glyphosate_products_detail ||
    field.diquat_products_detail ||
    field.other_products_detail;
  if (!hasAny) return null;

  return (
    <div className="text-muted-foreground mt-2 space-y-2 text-xs">
      {field.pfas_products_detail && (
        <ProductList
          raw={field.pfas_products_detail}
          colorClass="text-warning"
          label="PFAS"
        />
      )}
      {field.glyphosate_products_detail && (
        <ProductList raw={field.glyphosate_products_detail} label="Glyphosat" />
      )}
      {field.diquat_products_detail && (
        <ProductList
          raw={field.diquat_products_detail}
          colorClass="text-destructive"
          label="Diquat"
        />
      )}
      {field.other_products_detail && (
        <ProductList raw={field.other_products_detail} label="Øvrige" />
      )}
    </div>
  );
}
