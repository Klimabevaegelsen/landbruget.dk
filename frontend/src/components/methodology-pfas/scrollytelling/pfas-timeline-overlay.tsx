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

interface PfasTimelineOverlayProps {
  visible: boolean;
}

/**
 * Real sample data from GEUS Jupiter (data.geus.dk).
 * DGU 151. 2426, intake 1, Skrydstrup.
 *
 * 2021-09-22: Standard PFAS-12 panel → all <0,001 µg/L (TFA not included)
 * 2021-09-24: TFA-specific test → 4,88 µg/L (49× limit)
 *
 * Timeline starts ~2015 when fluorinated pesticides (DFF, fluopyram) were
 * applied on surrounding fields, and ends at the 2021 detection.
 */
const PFAS_CONFIG = {
  startYear: 2015,
  endYear: 2021,
  midpoint: { year: 2021, label: 'PFAS-12: <0,001 µg/L' },
  startLabel: 'Fluorpesticider sprøjtet',
  endContext: 'TFA-specifik test',
  endLabel: '4,88 µg/L',
  endSub: 'DGU 151. 2426',
  transport: '~6 års transport · TFA ikke i standardpanel',
  duration: 2.5,
  endColor: 'text-primary',
  samples: [],
} as const;

const EASE_TRAVEL = [0.25, 0.1, 0.25, 1] as const;

export function PfasTimelineOverlay({ visible }: PfasTimelineOverlayProps) {
  const progress = useMotionValue(0);
  const yearValue = useTransform(
    progress,
    [0, 1],
    [PFAS_CONFIG.startYear, PFAS_CONFIG.endYear]
  );
  const [displayYear, setDisplayYear] = useState<number>(PFAS_CONFIG.startYear);
  const [arrived, setArrived] = useState(false);

  useEffect(() => {
    if (!visible) {
      progress.set(0);
      setArrived(false);
      setDisplayYear(PFAS_CONFIG.startYear);
      return;
    }
    const unsub = yearValue.on('change', (v) => setDisplayYear(Math.round(v)));
    const controls = animate(progress, 1, {
      duration: PFAS_CONFIG.duration,
      ease: EASE_TRAVEL,
      delay: 0.6,
      onComplete: () => setArrived(true),
    });
    return () => {
      unsub();
      controls.stop();
    };
  }, [visible, progress, yearValue]);

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.4 }}
          className="bg-card/90 rounded-lg border p-3 shadow-sm backdrop-blur-sm"
          data-testid="pfas-timeline-overlay"
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
            config={PFAS_CONFIG}
            variant="pfas"
            progress={progress}
            arrived={arrived}
            displayYear={displayYear}
          />
        </motion.div>
      )}
    </AnimatePresence>
  );
}
