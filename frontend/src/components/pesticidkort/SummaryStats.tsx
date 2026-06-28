'use client';

import { cn } from '@/lib/utils';
import { useCountUp } from '@/components/pesticidkort/useCountUp';
import {
  formatBurden,
  getBurdenLevel,
} from '@/components/pesticidkort/field-utils';
import { EstimatedBadge } from '@/components/pesticidkort/EstimatedBadge';
import {
  DEFAULT_EXPOSURE_RADIUS_M,
  formatRadiusLabel,
} from '@/components/pesticidkort/exposure-utils';

interface SummaryStatsProps {
  fieldsCount: number;
  avgBurden: number;
  nearestFieldM: number;
}

export function SummaryStats({
  fieldsCount,
  avgBurden,
  nearestFieldM,
}: SummaryStatsProps) {
  const fieldsCountDisplay = useCountUp(fieldsCount);
  const nearestFieldDisplay = useCountUp(nearestFieldM);
  const burdenLevel = getBurdenLevel(avgBurden);
  const isHighBurden = burdenLevel.key === 'high';
  const isElevatedBurden = burdenLevel.key === 'midHigh';

  return (
    <div data-testid="summary-stats" aria-live="polite" className="space-y-3">
      <div className="flex items-center justify-between gap-2">
        <p className="text-muted-foreground text-xs font-semibold tracking-widest uppercase">
          Nøgletal
        </p>
        <EstimatedBadge variant="estimeret" />
      </div>
      <div className="grid grid-cols-3 gap-4">
        <div>
          <span className="text-foreground text-3xl font-bold tabular-nums">
            {Math.round(fieldsCountDisplay)}
          </span>
          <p className="text-muted-foreground mt-0.5 text-xs">
            sprøjtede marker inden for{' '}
            {formatRadiusLabel(DEFAULT_EXPOSURE_RADIUS_M)}
          </p>
        </div>

        <div
          className={cn(
            'rounded-lg px-3 py-2 -mx-3 -my-2',
            isHighBurden && 'bg-destructive/8',
            isElevatedBurden && 'bg-warning/8'
          )}
        >
          <span
            className={cn(
              'text-3xl font-bold tabular-nums',
              isHighBurden
                ? 'text-destructive'
                : isElevatedBurden
                  ? 'text-warning'
                  : 'text-foreground'
            )}
          >
            {formatBurden(avgBurden)}
          </span>
          <p
            className={cn(
              'mt-0.5 text-xs',
              isHighBurden
                ? 'text-destructive font-semibold'
                : isElevatedBurden
                  ? 'text-warning font-semibold'
                  : 'text-muted-foreground'
            )}
          >
            gns. belastning
          </p>
        </div>

        <div>
          <span className="text-foreground text-3xl font-bold tabular-nums">
            {formatRadiusLabel(nearestFieldDisplay)}
          </span>
          <p className="text-muted-foreground mt-0.5 text-xs">
            til nærmeste mark
          </p>
        </div>
      </div>
    </div>
  );
}
