'use client';

import { motion, AnimatePresence } from 'motion/react';
import { cn } from '@/lib/utils';
import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

const P = EXAMPLE.pesticide;

const COLS = [
  { key: 'row', label: 'Post', value: `#${EXAMPLE.sjiRow}`, highlight: false },
  { key: 'cvr', label: 'CVR', value: EXAMPLE.cvr, highlight: true },
  { key: 'crop', label: 'Afgrøde', value: EXAMPLE.cropName, highlight: true },
  {
    key: 'area',
    label: 'Areal',
    value: `${P.reportedAreaHa} ha`,
    highlight: false,
  },
  { key: 'product', label: 'Produkt', value: P.name, highlight: false },
  {
    key: 'dose',
    label: 'Dosis',
    value: `${P.totalDose} ${P.unit}`,
    highlight: false,
  },
];

interface ScrollyRecordOverlayProps {
  step: DisaggStepId;
  visible: boolean;
}

export function ScrollyRecordOverlay({
  step,
  visible,
}: ScrollyRecordOverlayProps) {
  const highlightKeys = step === 'fields' || step === 'match';

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: -16, scale: 0.97 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, y: -12, scale: 0.97 }}
          transition={{ type: 'spring', stiffness: 260, damping: 28 }}
          className="bg-card/95 overflow-hidden rounded-lg border shadow-lg backdrop-blur-sm"
          data-testid="scrolly-record-overlay"
        >
          <div className="text-muted-foreground flex items-center gap-1.5 px-3 py-1.5 text-[10px] font-medium tracking-widest uppercase">
            <span className="bg-muted inline-block h-2 w-2 rounded-sm border" />
            SJI-indberetning &mdash; landbrugsår {EXAMPLE.year}/
            {EXAMPLE.year + 1}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-[11px]">
              <thead>
                <tr className="bg-muted/50">
                  {COLS.map((c) => (
                    <th
                      key={c.key}
                      className="text-muted-foreground border-r px-2 py-1 text-left font-medium last:border-r-0"
                    >
                      {c.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <tr>
                  {COLS.map((c) => (
                    <motion.td
                      key={c.key}
                      animate={{
                        backgroundColor:
                          highlightKeys && c.highlight
                            ? 'var(--color-primary-15, rgba(99,102,241,0.15))'
                            : 'transparent',
                      }}
                      transition={{ duration: 0.4 }}
                      className={cn(
                        'border-r px-2 py-1.5 font-mono whitespace-nowrap last:border-r-0',
                        highlightKeys && c.highlight
                          ? 'text-foreground font-semibold'
                          : 'text-foreground/70'
                      )}
                    >
                      {c.value}
                    </motion.td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
