'use client';

import { useState, useEffect, useRef, forwardRef } from 'react';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import { formatBurden } from '@/components/pesticidkort/field-utils';
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
  isSelected?: boolean;
  onSelect?: () => void;
}

export const FieldCard = forwardRef<HTMLDivElement, FieldCardProps>(
  function FieldCard({ field, histogram, isSelected, onSelect }, ref) {
    const [expanded, setExpanded] = useState(false);
    const burden = field.total_pesticide_belastning;
    const hasPfas = field.pfas_applications > 0;
    const isZeroBurden = burden === 0;
    const hasProducts =
      field.pfas_products_detail ||
      field.diquat_products_detail ||
      field.glyphosate_products_detail ||
      field.other_products_detail;

    const prevSelectedRef = useRef(isSelected);
    useEffect(() => {
      if (isSelected && !prevSelectedRef.current && hasProducts) {
        setExpanded(true);
      }
      prevSelectedRef.current = isSelected;
    }, [isSelected, hasProducts]);

    return (
      <div
        ref={ref}
        data-testid={`field-card-${field.field_uuid}`}
        className={cn(
          'bg-card mb-2 rounded-xl px-4 py-3 transition-all duration-200 hover:shadow-md hover:-translate-y-1',
          isSelected && 'ring-primary ring-2',
          field.is_organic && !isSelected && 'border-success/40 border-l-2'
        )}
      >
        <button
          onClick={onSelect}
          data-testid={`field-select-${field.field_uuid}-button`}
          className="w-full text-left"
        >
          <div className="flex items-center gap-2 text-sm">
            <span className="text-foreground font-medium">
              {Math.round(field.distance_m)} m fra dig
            </span>
            <span className="text-muted-foreground text-xs">
              · {field.area_hectares.toFixed(1)} ha
            </span>
            {hasPfas && (
              <span className="bg-warning/10 text-warning rounded-full px-1.5 py-0.5 text-[10px] leading-none font-medium">
                PFAS
              </span>
            )}
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
            {expanded ? 'Skjul pesticider' : 'Vis pesticider'}
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
);
FieldCard.displayName = 'FieldCard';
