'use client';

import { useState } from 'react';
import Link from 'next/link';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

const GRADE_ITEMS = [
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-low)]',
    label: 'Under 0,5 B/ha',
  },
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-mid-low)]',
    label: '0,5\u20132,0 B/ha',
  },
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-mid)]',
    label: '2,0\u20134,0 B/ha',
  },
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-mid-high)]',
    label: '4,0\u20138,0 B/ha',
  },
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-high)]',
    label: 'Over 8,0 B/ha',
  },
  {
    swatch: 'bg-[var(--pesticidkort-color-burden-none)]',
    label: 'Brak / ingen pesticider',
  },
] as const;

const CHEMICAL_META: Record<
  Exclude<ChemicalFilter, 'none'>,
  { label: string; swatch: string }
> = {
  pfas: {
    label: 'PFAS-pesticider',
    swatch: 'bg-[var(--pesticidkort-color-pfas)]',
  },
  glyphosate: {
    label: 'Glyphosat',
    swatch: 'bg-[var(--pesticidkort-color-glyphosate)]',
  },
  diquat: { label: 'Diquat', swatch: 'bg-[var(--pesticidkort-color-diquat)]' },
};

interface MapLegendProps {
  activeFilter?: ChemicalFilter;
}

export function MapLegend({ activeFilter = 'none' }: MapLegendProps) {
  const [collapsed, setCollapsed] = useState(false);
  const hasFilter = activeFilter !== 'none';

  return (
    <div
      data-testid="map-legend"
      className="bg-background/90 border-border/70 absolute bottom-[calc(var(--pesticidkort-footer-height)+1rem)] left-4 z-20 max-w-[calc(100vw-2rem)] rounded-lg border px-3 py-2 shadow-md backdrop-blur-sm"
    >
      <button
        onClick={() => setCollapsed(!collapsed)}
        data-testid="map-legend-toggle-button"
        className="flex min-h-[32px] w-full items-center justify-between gap-2 text-xs font-medium"
      >
        <span className="text-foreground min-w-0 text-left">
          {hasFilter ? CHEMICAL_META[activeFilter].label : 'Pesticidbelastning'}
        </span>
        <ChevronDown
          className={cn(
            'text-muted-foreground h-3.5 w-3.5 transition-transform',
            collapsed && '-rotate-90'
          )}
        />
      </button>
      {!collapsed && (
        <>
          <div className="mt-1.5 space-y-1">
            {hasFilter ? (
              <ChemicalLegendItems filter={activeFilter} />
            ) : (
              <GradeLegendItems />
            )}
          </div>
          <Link
            href="/pesticidanalyse/metode"
            data-testid="legend-methodology-link"
            className="text-primary mt-2 block text-xs font-medium underline-offset-4 hover:underline"
          >
            Om metoden
          </Link>
        </>
      )}
    </div>
  );
}

function GradeLegendItems() {
  return (
    <>
      {GRADE_ITEMS.map((item) => (
        <div key={item.label} className="flex items-center gap-2">
          <div className={cn('h-3 w-3 shrink-0 rounded-sm', item.swatch)} />
          <span className="text-muted-foreground min-w-0 text-[11px] leading-tight">
            {item.label}
          </span>
        </div>
      ))}
    </>
  );
}

function ChemicalLegendItems({
  filter,
}: {
  filter: Exclude<ChemicalFilter, 'none'>;
}) {
  const meta = CHEMICAL_META[filter];
  return (
    <>
      <div className="flex items-center gap-2">
        <div className={cn('h-3 w-3 shrink-0 rounded-sm', meta.swatch)} />
        <span className="text-muted-foreground min-w-0 text-[11px] leading-tight">
          Marker med {meta.label}
        </span>
      </div>
      <div className="flex items-center gap-2">
        <div className="h-3 w-3 shrink-0 rounded-sm bg-[var(--pesticidkort-color-burden-none)] opacity-45" />
        <span className="text-muted-foreground min-w-0 text-[11px] leading-tight">
          Andre marker
        </span>
      </div>
    </>
  );
}
