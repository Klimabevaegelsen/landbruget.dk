'use client';

import React from 'react';
import { formatNumber } from '@/lib/formatting';

interface DosageRowProps {
  label: string;
  value: number | undefined | null;
  unit: string;
  decimals: number;
  threshold?: number;
}

export function DosageRow({
  label,
  value,
  unit,
  decimals,
  threshold = 0.001,
}: DosageRowProps) {
  if (value == null || value <= threshold) return null;
  const formatted = formatNumber(value, decimals);
  if (!formatted) return null;

  return (
    <div className="flex items-center justify-between text-sm lg:text-base">
      <span className="text-muted-foreground">{label}:</span>
      <span className="font-medium">
        {formatted} {unit}
      </span>
    </div>
  );
}
