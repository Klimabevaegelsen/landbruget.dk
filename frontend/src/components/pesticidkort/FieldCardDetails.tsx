import { useMemo } from 'react';
import { Droplets, Home, GraduationCap } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import { parsePesticideDetailWithUnit } from '@/components/pesticidkort/field-utils';
import { CategorySection } from '@/components/pesticidkort/CategorySection';

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

const CATEGORY_DEFS = [
  {
    key: 'pfas',
    field: 'pfas_products_detail' as const,
    label: 'PFAS-holdige pesticider',
    desc: 'Nedbrydes ikke i naturen',
    color: 'text-warning',
  },
  {
    key: 'diquat',
    field: 'diquat_products_detail' as const,
    label: 'Diquat-holdige pesticider',
    desc: 'Forbudt i EU siden 2020',
    color: 'text-destructive',
  },
  {
    key: 'glyphosate',
    field: 'glyphosate_products_detail' as const,
    label: 'Glyphosat-holdige pesticider',
    desc: 'Mest anvendte ukrudtsmiddel i DK',
    color: 'text-primary',
  },
  {
    key: 'other',
    field: 'other_products_detail' as const,
    label: 'Øvrige pesticider',
    desc: '',
    color: '',
  },
];

export function FieldProducts({ field }: { field: NearbyFieldSummary }) {
  const categories = useMemo(() => {
    return CATEGORY_DEFS.map((def) => ({
      ...def,
      products: parsePesticideDetailWithUnit(field[def.field]),
    })).filter((c) => c.products.length > 0);
  }, [
    field.pfas_products_detail,
    field.diquat_products_detail,
    field.glyphosate_products_detail,
    field.other_products_detail,
  ]);

  const totalCount = categories.reduce((sum, c) => sum + c.products.length, 0);
  if (totalCount === 0) return null;
  const autoExpand = totalCount <= 6;

  return (
    <div className="mt-3">
      <p className="text-foreground mb-1 text-sm font-medium">
        {totalCount} {totalCount === 1 ? 'pesticid' : 'pesticider'} anvendt
      </p>
      <div>
        {categories.map((cat) => (
          <CategorySection
            key={cat.key}
            id={cat.key}
            label={cat.label}
            description={cat.desc}
            colorClass={cat.color}
            products={cat.products}
            defaultExpanded={autoExpand}
          />
        ))}
      </div>
      <p className="text-muted-foreground mt-2 text-[11px]">
        Kilde: Miljøstyrelsen
      </p>
    </div>
  );
}
