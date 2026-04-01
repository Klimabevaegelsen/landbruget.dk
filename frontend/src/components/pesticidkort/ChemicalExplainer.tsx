'use client';

import { useState, useRef, useEffect } from 'react';
import { Info, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import {
  CHEMICAL_EXPLAINERS,
  type ChemicalType,
} from '@/components/pesticidkort/chemical-explainers';

interface ChemicalExplainerProps {
  chemical: ChemicalType;
}

const riskStyles: Record<string, string> = {
  high: 'bg-destructive text-destructive-foreground',
  moderate: 'bg-warning text-warning-foreground',
  low: 'bg-success text-success-foreground',
};

const riskLabels: Record<string, string> = {
  high: 'Høj risiko',
  moderate: 'Moderat risiko',
  low: 'Lav risiko',
};

export function ChemicalExplainer({ chemical }: ChemicalExplainerProps) {
  const [open, setOpen] = useState(false);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleClickOutside(e: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [open]);

  const info = CHEMICAL_EXPLAINERS[chemical];

  return (
    <div className="relative inline-block" ref={panelRef}>
      <button
        type="button"
        data-testid={`chemical-explainer-${chemical}`}
        onClick={() => setOpen((prev) => !prev)}
        className={cn(
          'inline-flex items-center justify-center rounded-full p-1',
          'text-muted-foreground hover:text-foreground',
          'hover:bg-muted transition-colors'
        )}
        aria-label={`Information om ${info.name}`}
      >
        <Info className="h-4 w-4" />
      </button>

      {open && (
        <div
          data-testid={`chemical-explainer-panel-${chemical}`}
          className={cn(
            'absolute z-50 mt-1 w-72 rounded-lg border border-border',
            'bg-popover p-4 text-popover-foreground shadow-md',
            'left-0 top-full'
          )}
          role="dialog"
          aria-label={`${info.name} information`}
        >
          <div className="mb-2 flex items-start justify-between gap-2">
            <div>
              <h3 className="text-foreground text-sm font-semibold">
                {info.name}
              </h3>
              <p className="text-muted-foreground text-xs">{info.category}</p>
            </div>
            <button
              type="button"
              data-testid={`chemical-explainer-close-${chemical}`}
              onClick={() => setOpen(false)}
              className={cn(
                'rounded-full p-0.5 text-muted-foreground',
                'hover:text-foreground hover:bg-muted transition-colors'
              )}
              aria-label="Luk"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>

          <span
            className={cn(
              'mb-2 inline-block rounded-full px-2 py-0.5 text-xs font-medium',
              riskStyles[info.risk]
            )}
          >
            {riskLabels[info.risk]}
          </span>

          <p className="text-foreground mb-1.5 text-sm">{info.summary}</p>
          <p className="text-muted-foreground mb-2 text-xs">{info.details}</p>

          <p className="text-muted-foreground text-xs">
            <span className="font-medium">Kilde:</span> {info.source}
          </p>
        </div>
      )}
    </div>
  );
}
