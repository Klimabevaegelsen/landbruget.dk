'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';

interface FieldBasicInfoProps {
  fieldData: FieldAnalysisData;
}

export function FieldBasicInfo({ fieldData }: FieldBasicInfoProps) {
  const rows = [
    { label: 'Kommune:', value: fieldData.kommune },
    { label: 'CVR:', value: fieldData.cvr_number, mono: true },
    { label: 'Areal:', value: `${formatNumber(fieldData.area_hectares)} ha` },
    { label: 'Afgrøde:', value: fieldData.crop_name || 'Ukendt' },
  ];

  return (
    <div className="mb-6">
      <h3 className="text-foreground mb-4 text-lg font-semibold">
        Grundoplysninger
      </h3>
      <div className="space-y-3 text-sm lg:space-y-2 lg:text-sm">
        {rows.map((row) => (
          <div
            key={row.label}
            className="flex items-center justify-between py-1"
          >
            <span className="text-muted-foreground font-medium">
              {row.label}
            </span>
            <span
              className={
                row.mono
                  ? 'font-mono text-sm lg:text-xs'
                  : 'text-base font-semibold lg:text-sm'
              }
            >
              {row.value}
            </span>
          </div>
        ))}
        <div className="flex items-center justify-between py-1">
          <span className="text-muted-foreground font-medium">Økologisk:</span>
          <span
            className={`text-base font-semibold lg:text-sm ${fieldData.is_organic ? 'text-accent-foreground' : 'text-muted-foreground'}`}
          >
            {fieldData.is_organic ? 'Ja' : 'Nej'}
          </span>
        </div>
      </div>
    </div>
  );
}
