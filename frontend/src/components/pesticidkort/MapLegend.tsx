'use client';

import { useState } from 'react';
import Link from 'next/link';
import { cn } from '@/lib/utils';
import { ChevronDown } from 'lucide-react';

const LEGEND_ITEMS = [
  { bg: 'bg-[#6abf69]', label: 'Under gennemsnit' },
  { bg: 'bg-[#d4c54a]', label: 'Omkring gennemsnit' },
  { bg: 'bg-[#d89135]', label: 'Over gennemsnit' },
  { bg: 'bg-[#c4512c]', label: 'Højeste' },
] as const;

export function MapLegend() {
  const [collapsed, setCollapsed] = useState(false);

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
        <span className="text-foreground">Pesticidbelastning</span>
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
            {LEGEND_ITEMS.map((item) => (
              <div key={item.label} className="flex items-center gap-2">
                <div className={cn('h-3 w-3 shrink-0 rounded-sm', item.bg)} />
                <span className="text-muted-foreground text-[11px]">
                  {item.label}
                </span>
              </div>
            ))}
          </div>
          <Link
            href="/pesticidanalyse/metode"
            data-testid="legend-methodology-link"
            className="text-primary/70 hover:text-primary mt-2 block text-[10px] underline-offset-4 hover:underline"
          >
            Om metoden
          </Link>
        </>
      )}
    </div>
  );
}
