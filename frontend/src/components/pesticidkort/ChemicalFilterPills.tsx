'use client';

import { cn } from '@/lib/utils';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

const PILLS: { id: ChemicalFilter; label: string; dot: string }[] = [
  { id: 'none', label: 'Belastning', dot: 'bg-[#22c55e]' },
  { id: 'pfas', label: 'PFAS', dot: 'bg-[#9333ea]' },
  { id: 'glyphosate', label: 'Glyphosat', dot: 'bg-[#0891b2]' },
  { id: 'diquat', label: 'Diquat', dot: 'bg-[#db2777]' },
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
      className="bg-background/90 pointer-events-auto inline-flex gap-0.5 rounded-full p-0.5 shadow-sm backdrop-blur-sm"
      data-testid="chemical-filter-pills"
      role="radiogroup"
      aria-label="Filtrer kort-visning"
    >
      {PILLS.map(({ id, label, dot }) => {
        const isActive = active === id;
        const needsZoom = id !== 'none' && id !== 'pfas' && zoomLevel < 12;

        return (
          <button
            key={id}
            role="radio"
            aria-checked={isActive}
            disabled={needsZoom}
            onClick={() => onChange(id)}
            data-testid={`chemical-filter-${id}`}
            title={needsZoom ? 'Zoom ind for at se' : undefined}
            className={cn(
              'flex items-center gap-1 rounded-full px-2.5 py-2 text-[11px] font-medium transition-colors',
              isActive
                ? 'bg-foreground text-background shadow-sm'
                : 'hover:bg-muted text-muted-foreground',
              needsZoom && 'pointer-events-none opacity-40'
            )}
          >
            <span
              className={cn(
                'inline-block h-2 w-2 shrink-0 rounded-full',
                isActive ? 'ring-background/50 ring-1' : '',
                dot
              )}
            />
            {label}
          </button>
        );
      })}
    </div>
  );
}
