'use client';

import { useMemo } from 'react';
import {
  burdenToPercent,
  NATIONAL_AVG_PERCENT,
} from '@/components/pesticidkort/field-utils';

export interface HistogramBin {
  bin_start: number;
  field_count: number;
}

interface BurdenScaleProps {
  burden: number;
  histogram: HistogramBin[];
}

export function BurdenScale({ burden, histogram }: BurdenScaleProps) {
  const dotPercent = burdenToPercent(burden);

  const bars = useMemo(() => {
    if (histogram.length === 0) return [];
    const maxCount = Math.max(...histogram.map((b) => b.field_count));
    if (maxCount === 0) return [];
    return histogram.map((bin) => ({
      left: burdenToPercent(bin.bin_start),
      width:
        burdenToPercent(bin.bin_start + 0.5) - burdenToPercent(bin.bin_start),
      height: (bin.field_count / maxCount) * 100,
    }));
  }, [histogram]);

  return (
    <div className="relative h-5 flex-1" data-testid="burden-scale">
      {bars.map((bar, i) => (
        <div
          key={i}
          className="bg-muted-foreground/8 absolute bottom-0"
          style={{
            left: `${bar.left}%`,
            width: `${Math.max(bar.width, 0.5)}%`,
            height: `${Math.max(bar.height, 4)}%`,
          }}
        />
      ))}
      <div className="bg-muted absolute bottom-[3px] h-[2px] w-full rounded-full" />
      <div
        className="bg-muted-foreground/25 absolute bottom-0 h-full w-px"
        style={{ left: `${NATIONAL_AVG_PERCENT}%` }}
        title="Landsgennemsnit (2,15 B/ha)"
      />
      <div
        className="bg-foreground absolute bottom-[1px] h-[6px] w-[6px] -translate-x-1/2 rounded-full"
        style={{ left: `${dotPercent}%` }}
      />
    </div>
  );
}
