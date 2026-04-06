'use client';

import { useEffect, useState } from 'react';
import {
  motion,
  AnimatePresence,
  useMotionValue,
  useTransform,
  animate,
} from 'motion/react';
import { GwTimelineBar } from '@/components/methodology-groundwater/scrollytelling/gw-timeline-bar';

interface GwTimelineOverlayProps {
  variant: 'bentazon' | 'mcpa';
  visible: boolean;
}

/**
 * Real sample data from GEUS Jupiter (data.geus.dk).
 * DGU 155. 1899, intake 1 — only 1 sample ever taken.
 */
const BENTAZON = {
  startYear: 2016,
  endYear: 2019,
  midpoint: null as null | { year: number; label: string },
  startLabel: 'Fighter 480 sprøjtet',
  endContext: 'Fund i boring',
  endLabel: '490 µg/L',
  endSub: 'DGU 155. 1899',
  transport: '~1,5 års transport',
  duration: 2.5,
  endColor: 'text-destructive',
  samples: [{ year: 2019, conc: '490 µg/L', detected: true }],
} as const;

/**
 * Real sample data from GEUS Jupiter (data.geus.dk).
 * DGU 132. 1056, intake 5 — persistent 4-chlor-2-methylphenol since 1998.
 */
const MCPA = {
  startYear: 1995,
  endYear: 2021,
  midpoint: null as null | { year: number; label: string },
  startLabel: 'MCPA anvendt',
  endContext: 'Seneste prøve',
  endLabel: '3,2 µg/L',
  endSub: 'DGU 132. 1056',
  transport: 'Persistent i 23+ år — alle prøver over grænseværdi',
  duration: 3.5,
  endColor: 'text-primary',
  samples: [
    { year: 1998, conc: '5,0 µg/L', detected: true },
    { year: 2012, conc: '3,4 µg/L', detected: true },
    { year: 2015, conc: '3,1 µg/L', detected: true },
    { year: 2018, conc: '3,0 µg/L', detected: true },
    { year: 2021, conc: '3,2 µg/L', detected: true },
  ],
} as const;

const EASE_TRAVEL = [0.25, 0.1, 0.25, 1] as const;

export function GwTimelineOverlay({
  variant,
  visible,
}: GwTimelineOverlayProps) {
  const config = variant === 'bentazon' ? BENTAZON : MCPA;
  const progress = useMotionValue(0);
  const yearValue = useTransform(
    progress,
    [0, 1],
    [config.startYear, config.endYear]
  );
  const [displayYear, setDisplayYear] = useState<number>(config.startYear);
  const [arrived, setArrived] = useState(false);

  useEffect(() => {
    if (!visible) {
      progress.set(0);
      setArrived(false);
      setDisplayYear(config.startYear);
      return;
    }
    const unsub = yearValue.on('change', (v) => setDisplayYear(Math.round(v)));
    const controls = animate(progress, 1, {
      duration: config.duration,
      ease: EASE_TRAVEL,
      delay: 0.6,
      onComplete: () => setArrived(true),
    });
    return () => {
      unsub();
      controls.stop();
    };
  }, [visible, variant, config, progress, yearValue]);

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.4 }}
          className="bg-card/90 rounded-lg border p-3 shadow-sm backdrop-blur-sm"
          data-testid="timeline-overlay"
        >
          <div className="mb-2 flex items-baseline justify-between">
            <span className="text-muted-foreground text-[9px] font-medium tracking-wider uppercase">
              Tidslinje
            </span>
            <span className="text-foreground font-mono text-sm font-semibold tabular-nums">
              {displayYear}
            </span>
          </div>
          <GwTimelineBar
            config={config}
            variant={variant}
            progress={progress}
            arrived={arrived}
            displayYear={displayYear}
          />
        </motion.div>
      )}
    </AnimatePresence>
  );
}
