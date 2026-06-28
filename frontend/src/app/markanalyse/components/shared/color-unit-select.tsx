'use client';

import React from 'react';
import type {
  ColorUnit,
  VisualizationMode,
} from '@/components/field-analysis/types';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

interface ColorUnitSelectProps {
  value: ColorUnit;
  onChange: (unit: ColorUnit) => void;
  visualizationMode: VisualizationMode;
  className?: string;
  'data-testid'?: string;
}

const MODE_UNITS: Record<VisualizationMode, ColorUnit[]> = {
  total_pesticide_belastning: [
    'belastning',
    'total',
    'per_hectare',
    'applications',
  ],
  pfas_belastning: ['belastning', 'total', 'per_hectare', 'applications'],
  diquat_belastning: ['belastning', 'total', 'per_hectare', 'applications'],
  glyphosate_belastning: ['belastning', 'total', 'per_hectare', 'applications'],
  applications_count: ['applications', 'total'],
  organic_status: ['total'],
  area_size: ['total'],
};

const UNIT_LABELS: Record<ColorUnit, string> = {
  total: 'Total mængde (kg/L)',
  per_hectare: 'Per hektar',
  belastning: 'Belastning (anbefalet)',
  applications: 'Antal allokeringer',
};

export function ColorUnitSelect({
  value,
  onChange,
  visualizationMode,
  className = '',
  'data-testid': testId = 'color-unit-select',
}: ColorUnitSelectProps) {
  const allowedUnits = MODE_UNITS[visualizationMode];

  return (
    <Select value={value} onValueChange={(v) => onChange(v as ColorUnit)}>
      <SelectTrigger className={className} data-testid={testId}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {allowedUnits.map((unit) => (
          <SelectItem key={unit} value={unit}>
            {UNIT_LABELS[unit]}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
