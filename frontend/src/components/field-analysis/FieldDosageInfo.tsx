'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';

const DOSAGE_FIELDS = [
  {
    key: 'total_dosage_kg',
    label: 'Samlet dosering (kg/ha):',
    unit: 'kg/ha',
    decimals: 3,
    threshold: 0.0001,
  },
  {
    key: 'total_dosage_liters',
    label: 'Samlet dosering (L/ha):',
    unit: 'L/ha',
    decimals: 3,
    threshold: 0.0001,
  },
  {
    key: 'total_dosage_grams',
    label: 'Samlet dosering (g/ha):',
    unit: 'g/ha',
    decimals: 1,
    threshold: 0.001,
  },
  {
    key: 'total_dosage_ml',
    label: 'Samlet dosering (ml/ha):',
    unit: 'ml/ha',
    decimals: 1,
    threshold: 0.001,
  },
  {
    key: 'total_dosage_tablets',
    label: 'Samlet dosering (tabletter/ha):',
    unit: 'tabletter/ha',
    decimals: 2,
    threshold: 0,
  },
] as const;

export function FieldDosageInfo({
  fieldData,
}: {
  fieldData: FieldAnalysisData;
}) {
  const visibleRows = DOSAGE_FIELDS.filter((f) => {
    const val = fieldData[f.key as keyof FieldAnalysisData] as
      | number
      | undefined;
    return val != null && val > f.threshold;
  });
  if (visibleRows.length === 0) return null;

  return (
    <div className="space-y-2">
      {visibleRows.map((f) => {
        const val = fieldData[f.key as keyof FieldAnalysisData] as number;
        return (
          <div
            key={f.key}
            className="flex items-center justify-between text-sm"
          >
            <span className="text-muted-foreground">{f.label}</span>
            <span className="font-medium">
              {formatNumber(val, f.decimals)} {f.unit}
            </span>
          </div>
        );
      })}
    </div>
  );
}
