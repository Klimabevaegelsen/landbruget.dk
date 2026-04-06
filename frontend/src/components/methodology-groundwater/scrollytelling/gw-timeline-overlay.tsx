'use client';

import { useEffect, useState } from 'react';
import {
  motion,
  AnimatePresence,
  useMotionValue,
  useTransform,
  animate,
} from 'motion/react';
import { GwTimelineBar } from './gw-timeline-bar';

interface GwTimelineOverlayProps {
  variant: 'bentazon' | 'mcpa';
  visible: boolean;
}

const BENTAZON = {
  startYear: 2016,
  endYear: 2019,
  midpoint: null as null | { year: number; label: string },
  startLabel: 'Fighter 480 sprøjtet',
  endLabel: '490 µg/L',
  endSub: 'DGU 155. 1899',
  transport: '~1,5 års transport',
  duration: 2.5,
  endColor: 'text-destructive',
} as const;

const MCPA = {
  startYear: 2012,
  endYear: 2021,
  midpoint: { year: 2015, label: '4-chlor-2-methylphenol' },
  startLabel: 'MCPA anvendt',
  endLabel: '3,2 µg/L',
  endSub: 'DGU 132. 1056',
  transport: '~9 års transport',
  duration: 3.5,
  endColor: 'text-primary',
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
          />
        </motion.div>
      )}
    </AnimatePresence>
  );
}
