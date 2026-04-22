'use client';

import { X } from 'lucide-react';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import {
  getCropEmoji,
  formatBurden,
} from '@/components/pesticidkort/field-utils';
import {
  FieldProximity,
  FieldProducts,
} from '@/components/pesticidkort/FieldCardDetails';

interface ExploreFieldPanelProps {
  field: NearbyFieldSummary;
  onClose: () => void;
}

export function ExploreFieldPanel({ field, onClose }: ExploreFieldPanelProps) {
  const burden = field.total_pesticide_belastning;
  const hasPfas = field.pfas_applications > 0;

  return (
    <div
      data-testid="explore-field-panel"
      className="bg-background/95 absolute inset-x-0 bottom-[var(--pesticidkort-footer-height)] z-30 max-h-[60vh] overflow-y-auto rounded-t-2xl px-4 pt-4 pb-6 shadow-lg backdrop-blur-sm md:inset-x-auto md:top-48 md:right-4 md:bottom-auto md:max-h-[calc(100vh-13rem)] md:w-80 md:rounded-xl"
    >
      <div className="mb-3 flex items-start justify-between">
        <div>
          <p className="text-foreground text-sm font-medium">
            {getCropEmoji(field.crop_name)} {field.crop_name}
          </p>
          <p className="text-muted-foreground text-xs">
            {field.area_hectares.toFixed(1)} ha
            {field.kommune ? ` · ${field.kommune}` : ''}
          </p>
        </div>
        <button
          onClick={onClose}
          data-testid="explore-field-panel-close-button"
          aria-label="Luk"
          className="text-muted-foreground hover:text-foreground -mt-1 -mr-1 p-1"
        >
          <X className="h-4 w-4" />
        </button>
      </div>

      <div className="mb-3 flex flex-wrap items-center gap-2">
        {field.is_organic && (
          <span className="bg-success/10 text-success rounded-full px-1.5 py-0.5 text-[10px] leading-none font-medium">
            Økologisk
          </span>
        )}
        {hasPfas && (
          <span className="bg-warning/10 text-warning rounded-full px-1.5 py-0.5 text-[10px] leading-none font-medium">
            PFAS
          </span>
        )}
        <span className="text-muted-foreground text-xs tabular-nums">
          {burden > 0 ? formatBurden(burden) : 'Ingen pesticider'}
        </span>
      </div>

      <FieldProximity field={field} />
      <FieldProducts field={field} />
    </div>
  );
}
