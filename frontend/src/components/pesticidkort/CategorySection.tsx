'use client';

import { useState } from 'react';
import { ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import { formatNumber } from '@/lib/formatting';
import type { ParsedEnhancedPesticide } from '@/components/pesticidkort/field-utils';
import { getRiskIcon } from '@/components/pesticidkort/field-utils';

/** Maps raw BMD produktgruppe_pesticid values to citizen-friendly Danish labels. */
const GROUP_LABELS: Record<string, string> = {
  Herbicider: 'ukrudtsmiddel',
  Fungicider: 'svampemiddel',
  Insekticider: 'insektmiddel',
  'Vækstreguleringsmidler (inkl. spiringshæmmende midler)': 'vækstregulering',
  Acaricider: 'midemiddel',
  Nematicider: 'nematodmiddel',
  Rodenticider: 'gnavermiddel',
  Molluskicider: 'sneglmiddel',
  Repellenter: 'afskrækningsmiddel',
  Jorddesinfektionsmidler: 'jorddesinfektion',
  'Midler mod alger, mos og lav': 'alge-/mosmiddel',
};

interface CategorySectionProps {
  id: string;
  label: string;
  description: string;
  colorClass: string;
  products: ParsedEnhancedPesticide[];
  defaultExpanded?: boolean;
}

export function CategorySection({
  id,
  label,
  description,
  colorClass,
  products,
  defaultExpanded = false,
}: CategorySectionProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const sorted = [...products].sort(
    (a, b) => (b.burden ?? b.dosage) - (a.burden ?? a.dosage)
  );

  return (
    <div className="border-border/50 border-b last:border-0">
      <button
        onClick={() => setExpanded(!expanded)}
        className="hover:bg-muted/50 -mx-1 flex w-full items-center gap-2 rounded-md px-1 py-2 text-left text-xs transition-colors"
        data-testid={`category-${id}-toggle`}
      >
        <ChevronRight
          className={cn(
            'h-3 w-3 shrink-0 transition-transform duration-200',
            expanded && 'rotate-90'
          )}
        />
        <span className="flex-1">
          <span className={cn('font-medium', colorClass)}>
            {label} ({products.length})
          </span>
          {description && (
            <span className="text-muted-foreground block text-[11px] leading-tight font-normal">
              {description}
            </span>
          )}
        </span>
      </button>
      <div
        className={cn(
          'grid transition-[grid-template-rows,opacity] duration-200 ease-out',
          expanded ? 'grid-rows-[1fr] opacity-100' : 'grid-rows-[0fr] opacity-0'
        )}
      >
        <div className="min-h-0 overflow-hidden">
          <ul
            className={cn(
              'space-y-2 border-l-2 pb-2 pl-4',
              colorClass
                ? `${colorClass.replace('text-', 'border-')}/30`
                : 'border-border/30'
            )}
          >
            {sorted.map((p, i) => {
              const risk = getRiskIcon(p.healthRisk, p.envRisk, p.signalWord);
              const groupLabel = p.productGroup
                ? GROUP_LABELS[p.productGroup] || p.productGroup.toLowerCase()
                : null;
              return (
                <li
                  key={`${p.name}-${i}`}
                  className="hover:bg-muted/40 -mx-1 rounded px-1 text-xs transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-foreground min-w-0 flex-1 truncate font-medium">
                      {p.name}
                    </span>
                    {risk && (
                      <span
                        className={cn(
                          'flex shrink-0 items-center gap-0.5 rounded px-1.5 py-0.5',
                          risk.bgColor
                        )}
                        title={`Klassificeret som ${risk.level.toLowerCase()}`}
                      >
                        <risk.Icon className={cn('h-3 w-3', risk.color)} />
                        <span
                          className={cn('text-[10px] font-medium', risk.color)}
                        >
                          {risk.level}
                        </span>
                      </span>
                    )}
                  </div>
                  <span className="text-muted-foreground">
                    {groupLabel && <>{groupLabel} · </>}
                    {formatNumber(p.dosage, 2)} {p.unit}/ha
                  </span>
                </li>
              );
            })}
          </ul>
        </div>
      </div>
    </div>
  );
}
