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
  const avgStyle = { left: `${NATIONAL_AVG_PERCENT}%` };
  const dotStyle = { left: `${dotPercent}%` };

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
    <div className="relative h-6 flex-1" data-testid="burden-scale">
      {bars.map((bar, i) => {
        const barStyle = {
          left: `${bar.left}%`,
          width: `${Math.max(bar.width, 0.5)}%`,
          height: `${Math.max(bar.height, 4)}%`,
        };
        return (
          <div
            key={i}
            className="bg-foreground/18 dark:bg-foreground/24 absolute bottom-0 rounded-[1px]"
            style={barStyle}
          />
        );
      })}
      <div className="bg-foreground/20 dark:bg-foreground/30 absolute bottom-[3px] h-[2px] w-full rounded-full" />
      <div
        className="bg-primary/70 dark:bg-primary/80 absolute bottom-0 h-full w-px"
        style={avgStyle}
        title="Landsgennemsnit (2,15 B/ha)"
      />
      <div
        className="bg-primary ring-background absolute bottom-0 h-[7px] w-[7px] -translate-x-1/2 rounded-full ring-1"
        style={dotStyle}
      />
    </div>
  );
}
