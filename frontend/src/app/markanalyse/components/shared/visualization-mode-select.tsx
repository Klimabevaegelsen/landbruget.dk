'use client';

import React from 'react';
import type { VisualizationMode } from '@/components/field-analysis/types';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

interface VisualizationModeSelectProps {
  value: VisualizationMode;
  onChange: (mode: VisualizationMode) => void;
  className?: string;
  'data-testid'?: string;
}

export function VisualizationModeSelect({
  value,
  onChange,
  className = '',
  'data-testid': testId = 'visualization-mode-select',
}: VisualizationModeSelectProps) {
  return (
    <Select
      value={value}
      onValueChange={(v) => onChange(v as VisualizationMode)}
    >
      <SelectTrigger className={className} data-testid={testId}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectLabel>Anbefalet</SelectLabel>
          <SelectItem value="total_pesticide_belastning">
            Total pesticidbelastning
          </SelectItem>
          <SelectItem value="organic_status">Økologisk status</SelectItem>
          <SelectItem value="applications_count">Antal pesticider</SelectItem>
        </SelectGroup>
        <SelectGroup>
          <SelectLabel>Avanceret</SelectLabel>
          <SelectItem value="pfas_belastning">PFAS belastning</SelectItem>
          <SelectItem value="diquat_belastning">Diquat belastning</SelectItem>
          <SelectItem value="glyphosate_belastning">
            Glyphosate belastning
          </SelectItem>
          <SelectItem value="area_size">Markareal</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  );
}
