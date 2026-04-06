'use client';

import { useRef, useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import { cn } from '@/lib/utils';
import {
  EXAMPLE,
  SJI_JOURNAL_ROWS,
  TARGET_POST,
} from '@/components/methodology/scrolly-example-data';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

interface ScrollyRecordOverlayProps {
  step: DisaggStepId;
  visible: boolean;
}

export function ScrollyRecordOverlay({
  step,
  visible,
}: ScrollyRecordOverlayProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const targetRef = useRef<HTMLTableRowElement>(null);
  const [spotlit, setSpotlit] = useState(false);
  const shouldSpotlight = step === 'regneark';

  useEffect(() => {
    if (!shouldSpotlight) {
      setSpotlit(false);
      return;
    }
    const timer = setTimeout(() => {
      setSpotlit(true);
      targetRef.current?.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
      });
    }, 600);
    return () => clearTimeout(timer);
  }, [shouldSpotlight]);

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
            SJI-indberetning — CVR {EXAMPLE.cvr} — {EXAMPLE.year}/
            {EXAMPLE.year + 1}
          </div>
          <div
            ref={scrollRef}
            className="max-h-[220px] overflow-x-hidden overflow-y-auto"
          >
            <RecordTable spotlit={spotlit} targetRef={targetRef} />
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

interface RecordTableProps {
  spotlit: boolean;
  targetRef: React.RefObject<HTMLTableRowElement | null>;
}

function RecordTable({ spotlit, targetRef }: RecordTableProps) {
  return (
    <table className="w-full text-[11px]">
      <thead className="sticky top-0 z-10">
        <tr className="bg-muted/80 backdrop-blur-sm">
          {['Post', 'Produkt', 'Areal', 'Dosis'].map((h) => (
            <th
              key={h}
              className="text-muted-foreground border-r px-2 py-1 text-left font-medium last:border-r-0"
            >
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {SJI_JOURNAL_ROWS.map((row) => {
          const isTarget = row.post === TARGET_POST;
          const dimmed = spotlit && !isTarget;
          return (
            <motion.tr
              key={row.post}
              ref={isTarget ? targetRef : undefined}
              animate={{
                opacity: dimmed ? 0.35 : 1,
                backgroundColor:
                  spotlit && isTarget
                    ? 'var(--color-primary-15, rgba(42,139,78,0.12))'
                    : 'transparent',
              }}
              transition={{ duration: 0.4 }}
              className={cn(
                'border-border/20 border-b',
                spotlit && isTarget && 'border-l-primary border-l-2'
              )}
            >
              <td className="text-muted-foreground border-r px-2 py-1 font-mono whitespace-nowrap">
                #{row.post}
              </td>
              <td
                className={cn(
                  'border-r px-2 py-1',
                  spotlit && isTarget
                    ? 'text-foreground font-semibold'
                    : 'text-foreground/70'
                )}
              >
                {row.product}
              </td>
              <td className="text-foreground/70 border-r px-2 py-1 text-right font-mono whitespace-nowrap">
                {row.area} ha
              </td>
              <td className="text-foreground/70 px-2 py-1 text-right font-mono whitespace-nowrap">
                {row.dose} {row.unit}
              </td>
            </motion.tr>
          );
        })}
      </tbody>
    </table>
  );
}
