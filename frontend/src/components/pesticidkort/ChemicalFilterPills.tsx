'use client';

import { cn } from '@/lib/utils';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

const PILL_BG: Record<Exclude<ChemicalFilter, 'none'>, string> = {
  pfas: 'bg-[#9333ea]',
  glyphosate: 'bg-[#0891b2]',
  diquat: 'bg-[#db2777]',
};

const PILLS: { id: Exclude<ChemicalFilter, 'none'>; label: string }[] = [
  { id: 'pfas', label: 'PFAS' },
  { id: 'glyphosate', label: 'Glyphosat' },
  { id: 'diquat', label: 'Diquat' },
];

interface ChemicalFilterPillsProps {
  active: ChemicalFilter;
  onChange: (filter: ChemicalFilter) => void;
  zoomLevel: number;
}

export function ChemicalFilterPills({
  active,
  onChange,
  zoomLevel,
}: ChemicalFilterPillsProps) {
  return (
    <div
      className="bg-background/90 pointer-events-auto inline-flex gap-1 rounded-full p-1 backdrop-blur-sm"
      data-testid="chemical-filter-pills"
      role="radiogroup"
      aria-label="Filtrer efter kemikalie"
    >
      {PILLS.map(({ id, label }) => {
        const isActive = active === id;
        const needsZoom = id !== 'pfas' && zoomLevel < 12;

        return (
          <button
            key={id}
            role="radio"
            aria-checked={isActive}
            disabled={needsZoom}
            onClick={() => onChange(isActive ? 'none' : id)}
            data-testid={`chemical-filter-${id}`}
            title={needsZoom ? 'Zoom ind for at se' : undefined}
            className={cn(
              'flex items-center gap-1.5 rounded-full px-3 py-2.5 text-xs font-medium transition-colors',
              isActive
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground',
              needsZoom && 'pointer-events-none opacity-40'
            )}
          >
            <span
              className={cn(
                'inline-block h-2 w-2 shrink-0 rounded-full',
                PILL_BG[id]
              )}
            />
            {label}
          </button>
        );
      })}
    </div>
  );
}
