'use client';

import { formatBurden } from '@/components/pesticidkort/field-utils';
import type { FieldHoverData } from '@/components/pesticidkort/types';

export function FieldHoverTooltip({
  cropName,
  cvrNumber,
  burden,
  x,
  y,
}: FieldHoverData) {
  const flipX = typeof window !== 'undefined' && x > window.innerWidth - 200;
  const left = flipX ? x - 12 : x + 12;
  const posStyle = {
    left,
    top: y + 12,
    transform: flipX ? 'translateX(-100%)' : undefined,
  };

  return (
    <div
      data-testid="field-hover-tooltip"
      className="bg-popover text-popover-foreground border-border pointer-events-none fixed z-50 rounded-md border px-2.5 py-1.5 shadow-md"
      style={posStyle}
    >
      <p className="text-xs font-medium">{cropName}</p>
      {cvrNumber && (
        <p className="text-muted-foreground text-[10px]">CVR {cvrNumber}</p>
      )}
      <p className="text-muted-foreground text-[10px] tabular-nums">
        {burden > 0 ? formatBurden(burden) : 'Ingen pesticider'}
      </p>
    </div>
  );
}
