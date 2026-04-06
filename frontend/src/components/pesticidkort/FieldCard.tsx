'use client';

import { useState } from 'react';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import {
  getCropEmoji,
  formatBurden,
  parseProductString,
  formatProduct,
} from '@/components/pesticidkort/field-utils';
import {
  BurdenScale,
  type HistogramBin,
} from '@/components/pesticidkort/BurdenScale';
import {
  FieldProximity,
  FieldProducts,
} from '@/components/pesticidkort/FieldCardDetails';

interface FieldCardProps {
  field: NearbyFieldSummary;
  histogram: HistogramBin[];
  onSelect?: () => void;
}

export function FieldCard({ field, histogram, onSelect }: FieldCardProps) {
  const [expanded, setExpanded] = useState(false);
  const burden = field.total_pesticide_belastning;
  const hasPfas = field.pfas_applications > 0;
  const isZeroBurden = burden === 0;
  const hasProducts =
    field.other_products_detail || field.glyphosate_products_detail;

  const pfasSummary =
    hasPfas && field.pfas_products_detail
      ? parseProductString(field.pfas_products_detail)
          .slice(0, 1)
          .map(formatProduct)
          .join('')
      : null;

  return (
    <div
      data-testid={`field-card-${field.field_uuid}`}
      className={cn(
        'bg-card mb-2 rounded-xl px-4 py-3 transition-all duration-200 hover:shadow-sm hover:-translate-y-0.5',
        hasPfas && 'ring-warning/30 ring-1',
        !hasPfas && burden >= 6 && 'ring-destructive/20 ring-1'
      )}
    >
      <button
        onClick={onSelect}
        data-testid={`field-select-${field.field_uuid}-button`}
        className="w-full text-left"
      >
        <div className="flex items-center gap-2 text-sm">
          <span>{getCropEmoji(field.crop_name)}</span>
          <span className="text-foreground font-medium">{field.crop_name}</span>
          {field.is_organic && (
            <span className="bg-success/10 text-success rounded-full px-1.5 py-0.5 text-[10px] leading-none font-medium">
              Øko
            </span>
          )}
          {hasPfas && (
            <span className="bg-warning/10 text-warning rounded-full px-1.5 py-0.5 text-[10px] leading-none font-medium">
              PFAS
            </span>
          )}
        </div>
        <div className="text-muted-foreground mt-1 flex items-center gap-2 text-xs tabular-nums">
          <span>{field.area_hectares.toFixed(1)} ha</span>
          <span>·</span>
          <span>{Math.round(field.distance_m)} m væk</span>
        </div>

        {!isZeroBurden && (
          <div className="mt-2.5 flex items-center gap-2">
            <BurdenScale burden={burden} histogram={histogram} />
            <span className="text-muted-foreground shrink-0 text-[11px] tabular-nums">
              {formatBurden(burden)}
            </span>
          </div>
        )}
      </button>

      {pfasSummary && (
        <p className="text-warning mt-2 text-xs">{pfasSummary}</p>
      )}

      <FieldProximity field={field} />

      {hasProducts && (
        <button
          onClick={() => setExpanded(!expanded)}
          data-testid={`field-expand-${field.field_uuid}-button`}
          aria-expanded={expanded}
          className="text-muted-foreground mt-1 flex min-h-[44px] items-center gap-1 text-xs hover:underline"
        >
          <ChevronDown
            className={cn(
              'h-3 w-3 transition-transform',
              expanded && 'rotate-180'
            )}
          />
          {expanded ? 'Skjul produkter' : 'Se produkter'}
        </button>
      )}

      {hasProducts && (
        <div
          className={cn(
            'grid transition-[grid-template-rows,opacity] duration-300 ease-[cubic-bezier(0.25,1,0.5,1)]',
            expanded
              ? 'grid-rows-[1fr] opacity-100'
              : 'grid-rows-[0fr] opacity-0'
          )}
          aria-hidden={!expanded}
        >
          <div className="min-h-0 overflow-hidden">
            <FieldProducts field={field} />
          </div>
        </div>
      )}
    </div>
  );
}
