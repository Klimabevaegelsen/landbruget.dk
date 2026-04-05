'use client';

import { useState } from 'react';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import {
  getCropEmoji,
  getBurdenDecile,
  getBarColor,
  getBurdenLabel,
} from '@/components/pesticidkort/field-utils';
import {
  FieldProximity,
  FieldProducts,
} from '@/components/pesticidkort/FieldCardDetails';

interface FieldCardProps {
  field: NearbyFieldSummary;
  onSelect?: () => void;
}

export function FieldCard({ field, onSelect }: FieldCardProps) {
  const [expanded, setExpanded] = useState(false);
  const decile = getBurdenDecile(field.total_pesticide_belastning);
  const hasPfas = field.pfas_applications > 0;
  const isHighRisk = decile >= 7 || hasPfas;
  const hasProducts =
    field.other_products_detail || field.glyphosate_products_detail;

  return (
    <div
      data-testid={`field-card-${field.field_uuid}`}
      className={cn(
        'border-border border-b py-4 transition-colors',
        field.is_organic && 'bg-success/[0.04]',
        hasPfas && 'border-l-[3px] border-l-warning bg-warning/[0.04]',
        !hasPfas &&
          isHighRisk &&
          'border-l-[3px] border-l-destructive/40 bg-destructive/[0.03] pl-3'
      )}
    >
      <button
        onClick={onSelect}
        data-testid={`field-select-${field.field_uuid}-button`}
        className="hover:bg-muted/30 w-full rounded px-1 text-left transition-colors"
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
          <span className="text-muted-foreground ml-auto text-xs tabular-nums">
            {field.area_hectares.toFixed(1)} ha
            <span className="mx-1.5">·</span>
            {Math.round(field.distance_m)} m
          </span>
        </div>

        <div className="mt-2.5 flex items-center gap-2">
          <div className="bg-muted h-1.5 flex-1 overflow-hidden rounded-full">
            <div
              className="h-full rounded-full bg-[var(--bar-color)]"
              style={
                {
                  width: `${decile * 10}%`,
                  '--bar-color': getBarColor(decile),
                } as React.CSSProperties
              }
            />
          </div>
          <span className="text-muted-foreground text-right text-[11px] tabular-nums">
            {field.total_pesticide_belastning.toFixed(1)} B/ha (
            {getBurdenLabel(decile)})
          </span>
        </div>
      </button>

      {hasPfas && field.pfas_products_detail && (
        <p className="text-warning mt-2 text-xs">
          {field.pfas_products_detail.split(';')[0].trim()}
        </p>
      )}

      <FieldProximity field={field} />

      {hasProducts && (
        <button
          onClick={() => setExpanded(!expanded)}
          data-testid={`field-expand-${field.field_uuid}-button`}
          aria-expanded={expanded}
          className="text-muted-foreground mt-1 -ml-1 flex min-h-[44px] items-center gap-1 px-1 text-xs hover:underline"
        >
          <ChevronDown
            className={cn(
              'h-3 w-3 transition-transform',
              expanded && 'rotate-180'
            )}
          />
          {expanded ? 'Skjul produkter' : 'Se alle produkter'}
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
