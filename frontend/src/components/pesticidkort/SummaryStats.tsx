'use client';

import { cn } from '@/lib/utils';
import { useCountUp } from '@/components/pesticidkort/useCountUp';

interface SummaryStatsProps {
  fieldsCount: number;
  pfasFieldsCount: number;
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
  pfasFieldsCount,
  nearestFieldM,
}: SummaryStatsProps) {
  const fieldsCountDisplay = useCountUp(fieldsCount);
  const pfasFieldsCountDisplay = useCountUp(pfasFieldsCount);
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
          pfasFieldsCount > 0 && 'bg-warning/8'
        )}
      >
        <span
          className={cn(
            'text-3xl font-bold tabular-nums',
            pfasFieldsCount > 0 ? 'text-warning' : 'text-foreground'
          )}
        >
          {Math.round(pfasFieldsCountDisplay)}
        </span>
        <p
          className={cn(
            'mt-0.5 text-xs',
            pfasFieldsCount > 0
              ? 'text-warning/80 font-medium'
              : 'text-muted-foreground'
          )}
        >
          bruger PFAS-stoffer
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
