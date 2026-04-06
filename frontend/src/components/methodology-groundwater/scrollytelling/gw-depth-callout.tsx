'use client';

import { motion, AnimatePresence } from 'motion/react';

interface GwDepthCalloutProps {
  visible: boolean;
}

export function GwDepthCallout({ visible }: GwDepthCalloutProps) {
  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, scale: 0.7 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.7 }}
          transition={{ duration: 0.5, delay: 0.3 }}
          className="pointer-events-none absolute top-1/2 left-1/2 z-20 -translate-x-1/2"
          data-testid="depth-callout"
        >
          <div className="bg-destructive/10 border-destructive rounded-lg border-2 border-dashed px-4 py-3 text-center backdrop-blur-sm">
            <p className="text-destructive text-2xl font-bold tabular-nums">
              490&nbsp;&micro;g/L
            </p>
            <p className="text-destructive/80 mt-1 text-xs font-medium">
              4.900&times; over gr&aelig;nsev&aelig;rdien
            </p>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
