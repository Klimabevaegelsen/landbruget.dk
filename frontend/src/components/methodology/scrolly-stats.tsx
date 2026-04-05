'use client';

import { motion, AnimatePresence } from 'motion/react';

export interface StatRow {
  label: string;
  value: string;
}

interface ScrollyStatsProps {
  rows: StatRow[];
  visible: boolean;
  testId?: string;
}

const EASE_OUT_QUART = [0.25, 1, 0.5, 1] as const;

export function ScrollyStats({
  rows,
  visible,
  testId = 'scrolly-stats',
}: ScrollyStatsProps) {
  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.5, ease: EASE_OUT_QUART }}
          className="bg-card/90 rounded-lg border p-3 shadow-sm backdrop-blur-sm"
          data-testid={testId}
        >
          <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
            {rows.map((r) => (
              <div key={r.label} className="contents">
                <dt className="text-muted-foreground">{r.label}</dt>
                <dd className="text-foreground text-right font-mono font-semibold">
                  {r.value}
                </dd>
              </div>
            ))}
          </dl>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
