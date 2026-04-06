'use client';

import { motion } from 'motion/react';

interface PfasHeadlineOverlayProps {
  visible: boolean;
}

export function PfasHeadlineOverlay({ visible }: PfasHeadlineOverlayProps) {
  if (!visible) return null;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.6 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.6, ease: [0.25, 0.1, 0.25, 1] }}
      className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center"
      data-testid="pfas-headline-overlay"
    >
      <div className="text-center">
        <p className="text-destructive text-5xl font-black tracking-tight md:text-7xl">
          64%
        </p>
        <p className="text-destructive/80 mt-1 text-2xl font-bold tracking-widest uppercase md:text-3xl">
          uovervåget
        </p>
      </div>
    </motion.div>
  );
}
