'use client';

import { useEffect, useState } from 'react';
import { ChevronRight, ExternalLink } from 'lucide-react';
import { cn } from '@/lib/utils';
import { formatNumber } from '@/lib/formatting';
import type { ParsedEnhancedPesticide } from '@/components/pesticidkort/field-utils';
import { classifyRisk } from '@/app/markanalyse/components/shared/field-details/pesticide-risk';
import { getGHSRiskIcon } from '@/components/field-analysis/ghs-risk-icons';
import {
  getProductUrl,
  loadMapping,
} from '@/components/pesticidkort/middeldatabasen-links';

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
  const [productIds, setProductIds] = useState<Record<string, string> | null>(
    null
  );
  useEffect(() => {
    if (expanded) loadMapping().then(setProductIds);
  }, [expanded]);
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
              const cls = classifyRisk(p.healthRisk, p.envRisk, p.signalWord);
              const risk = cls ? getGHSRiskIcon(cls) : null;
              const groupLabel = p.productGroup
                ? GROUP_LABELS[p.productGroup] || p.productGroup.toLowerCase()
                : null;
              return (
                <li
                  key={`${p.name}-${i}`}
                  className="hover:bg-muted/40 -mx-1 rounded px-1 text-xs transition-colors"
                >
                  <div className="flex items-center gap-2">
                    <a
                      href={getProductUrl(p.name, productIds)}
                      target="_blank"
                      rel="noopener noreferrer"
                      data-testid={`product-link-${i}`}
                      title={`Se "${p.name}" på Middeldatabasen`}
                      className="text-foreground group/link min-w-0 flex-1 truncate font-medium underline decoration-dotted underline-offset-2 hover:decoration-solid"
                    >
                      {p.name}
                      <ExternalLink className="ml-0.5 inline h-2.5 w-2.5 opacity-0 transition-opacity group-hover/link:opacity-60" />
                    </a>
                    {risk && (
                      <span
                        className={cn(
                          'flex shrink-0 items-center gap-0.5 rounded px-1.5 py-0.5',
                          risk.bgColor
                        )}
                        title={`Klassificeret som ${risk.level.toLowerCase()}`}
                      >
                        <risk.Icon className="h-3.5 w-3.5" />
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
                    {formatNumber(p.dosage, 3)} {p.unit}/ha
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
