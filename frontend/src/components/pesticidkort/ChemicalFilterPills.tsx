'use client';

import { cn } from '@/lib/utils';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

const PILLS: { id: ChemicalFilter; label: string; dotClass: string }[] = [
  {
    id: 'none',
    label: 'Belastning',
    dotClass: 'bg-[var(--pesticidkort-color-burden-low)]',
  },
  {
    id: 'pfas',
    label: 'PFAS',
    dotClass: 'bg-[var(--pesticidkort-color-pfas)]',
  },
  {
    id: 'glyphosate',
    label: 'Glyphosat',
    dotClass: 'bg-[var(--pesticidkort-color-glyphosate)]',
  },
  {
    id: 'diquat',
    label: 'Diquat',
    dotClass: 'bg-[var(--pesticidkort-color-diquat)]',
  },
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
      className="flex w-full max-w-full [scrollbar-width:none] justify-center overflow-x-auto px-1 [&::-webkit-scrollbar]:hidden"
      data-testid="chemical-filter-pills"
    >
      <div
        className="bg-background/90 pointer-events-auto inline-flex min-w-max gap-0.5 rounded-full p-0.5 shadow-sm backdrop-blur-sm"
        role="radiogroup"
        aria-label="Filtrer kort-visning"
      >
        {PILLS.map(({ id, label, dotClass }) => {
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
                'flex items-center gap-1 rounded-full px-2 py-1.5 text-[11px] font-medium whitespace-nowrap transition-colors sm:px-2.5 sm:py-2',
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
                  dotClass
                )}
              />
              {label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
