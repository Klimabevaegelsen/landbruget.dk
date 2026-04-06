'use client';

import { useEffect } from 'react';
import { motion, useMotionValue, useTransform, animate } from 'motion/react';

interface PfasCounterOverlayProps {
  visible: boolean;
}

export function PfasCounterOverlay({ visible }: PfasCounterOverlayProps) {
  const count = useMotionValue(0);
  const rounded = useTransform(count, (v) => Math.round(v));

  useEffect(() => {
    if (visible) {
      count.set(0);
      const controls = animate(count, 100, {
        duration: 2,
        ease: [0.25, 0.1, 0.25, 1],
      });
      return () => controls.stop();
    }
  }, [visible, count]);

  if (!visible) return null;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.8 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0 }}
      className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center"
      data-testid="pfas-counter-overlay"
    >
      <div className="text-center">
        <motion.span className="text-destructive text-6xl font-black tabular-nums">
          {rounded}
        </motion.span>
        <span className="text-destructive text-6xl font-black">%</span>
        <p className="text-muted-foreground mt-2 text-lg font-medium">
          detektionsrate
        </p>
      </div>
    </motion.div>
  );
}
