'use client';

import { useState } from 'react';
import Link from 'next/link';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

const GRADE_ITEMS = [
  { bg: 'bg-[#22c55e]', label: 'A \u2014 Under 0,5 B/ha' },
  { bg: 'bg-[#84cc16]', label: 'B \u2014 0,5\u20132,0 B/ha' },
  { bg: 'bg-[#eab308]', label: 'C \u2014 2,0\u20134,0 B/ha' },
  { bg: 'bg-[#f97316]', label: 'D \u2014 4,0\u20138,0 B/ha' },
  { bg: 'bg-[#dc2626]', label: 'E \u2014 Over 8,0 B/ha' },
  { bg: 'bg-[#d1d5db]', label: 'Brak / ingen pesticider' },
] as const;

const CHEMICAL_META: Record<
  Exclude<ChemicalFilter, 'none'>,
  { label: string; bg: string }
> = {
  pfas: { label: 'PFAS-pesticider', bg: 'bg-[#9333ea]' },
  glyphosate: { label: 'Glyphosat', bg: 'bg-[#0891b2]' },
  diquat: { label: 'Diquat', bg: 'bg-[#db2777]' },
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
      className="bg-background/90 absolute bottom-4 left-4 z-20 rounded-lg px-3 py-2 shadow-md backdrop-blur-sm"
    >
      <button
        onClick={() => setCollapsed(!collapsed)}
        data-testid="map-legend-toggle-button"
        className="flex min-h-[32px] w-full items-center justify-between gap-2 text-xs font-medium"
      >
        <span className="text-foreground">
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
          <div className={cn('h-3 w-3 shrink-0 rounded-sm', item.bg)} />
          <span className="text-muted-foreground text-[11px]">
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
        <div className={cn('h-3 w-3 shrink-0 rounded-sm', meta.bg)} />
        <span className="text-muted-foreground text-[11px]">
          Marker med {meta.label}
        </span>
      </div>
      <div className="flex items-center gap-2">
        <div className="h-3 w-3 shrink-0 rounded-sm bg-[#d1d5db] opacity-20" />
        <span className="text-muted-foreground text-[11px]">Andre marker</span>
      </div>
    </>
  );
}
