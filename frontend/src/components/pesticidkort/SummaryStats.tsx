'use client';

import { cn } from '@/lib/utils';
import { useCountUp } from '@/components/pesticidkort/useCountUp';
import { formatBurden } from '@/components/pesticidkort/field-utils';

interface SummaryStatsProps {
  fieldsCount: number;
  avgBurden: number;
  nearestFieldM: number;
}

function formatDistance(meters: number): string {
  if (meters >= 1000) {
    return `${(meters / 1000).toFixed(1).replace('.', ',')} km`;
  }
  return `${Math.round(meters)} m`;
}

export function SummaryStats({
  fieldsCount,
  avgBurden,
  nearestFieldM,
}: SummaryStatsProps) {
  const fieldsCountDisplay = useCountUp(fieldsCount);
  const nearestFieldDisplay = useCountUp(nearestFieldM);

  return (
    <div
      data-testid="summary-stats"
      aria-live="polite"
      className="grid grid-cols-3 gap-4"
    >
      <div>
        <span className="text-foreground text-3xl font-bold tabular-nums">
          {Math.round(fieldsCountDisplay)}
        </span>
        <p className="text-muted-foreground mt-0.5 text-xs">
          sprøjtede marker inden for 1 km
        </p>
      </div>

      <div
        className={cn(
          'rounded-lg px-3 py-2 -mx-3 -my-2',
          avgBurden >= 8 && 'bg-destructive/8',
          avgBurden >= 4 && avgBurden < 8 && 'bg-warning/8'
        )}
      >
        <span
          className={cn(
            'text-3xl font-bold tabular-nums',
            avgBurden >= 8
              ? 'text-destructive'
              : avgBurden >= 4
                ? 'text-warning'
                : 'text-foreground'
          )}
        >
          {formatBurden(avgBurden)}
        </span>
        <p
          className={cn(
            'mt-0.5 text-xs',
            avgBurden >= 8
              ? 'text-destructive/80 font-medium'
              : avgBurden >= 4
                ? 'text-warning/80 font-medium'
                : 'text-muted-foreground'
          )}
        >
          gns. belastning
        </p>
      </div>

      <div>
        <span className="text-foreground text-3xl font-bold tabular-nums">
          {formatDistance(nearestFieldDisplay)}
        </span>
        <p className="text-muted-foreground mt-0.5 text-xs">
          til nærmeste mark
        </p>
      </div>
    </div>
  );
}
