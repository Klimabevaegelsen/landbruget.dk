'use client';

import Link from 'next/link';
import { InfoIcon } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';

interface EstimatedBadgeProps {
  variant?: 'modelleret' | 'estimeret';
  className?: string;
}

const LABELS: Record<'modelleret' | 'estimeret', string> = {
  modelleret: 'Modelleret',
  estimeret: 'Estimeret',
};

const EXPLANATIONS: Record<'modelleret' | 'estimeret', string> = {
  modelleret: 'Modellerede data – faktisk sprøjtning kan afvige fra det viste.',
  estimeret: 'Estimerede værdier baseret på offentlige data og beregninger.',
};

export function EstimatedBadge({
  variant = 'modelleret',
  className,
}: EstimatedBadgeProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          data-testid="pesticidkort-estimated-badge"
          aria-label={`${LABELS[variant]} – læs om metoden`}
          className="inline-flex cursor-help items-center focus:outline-none"
        >
          <Badge
            variant="outline"
            className={cn(
              'gap-1.5 border-warning/80 bg-warning text-warning-foreground rounded-full px-3 py-1 text-[11px] font-bold tracking-[0.08em] uppercase shadow-sm',
              className
            )}
          >
            <InfoIcon className="size-3.5 shrink-0" />
            {LABELS[variant]}
          </Badge>
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        <p className="mb-1">{EXPLANATIONS[variant]}</p>
        <Link
          href="/pesticidanalyse/metode"
          className="underline underline-offset-2"
          data-testid="pesticidkort-estimated-badge-link"
        >
          Læs om metoden
        </Link>
      </TooltipContent>
    </Tooltip>
  );
}
