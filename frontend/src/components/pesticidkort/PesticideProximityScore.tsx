'use client';

import { cn } from '@/lib/utils';
import { motion, useReducedMotion } from 'motion/react';
import type { PesticideGrade } from '@/components/pesticidkort/types';
import {
  getGradeColor,
  getGradeBgColor,
  GRADE_ORDER,
} from '@/lib/pesticide-score';

interface PesticideProximityScoreProps {
  grade: PesticideGrade;
  label: string;
  description: string;
}

export function PesticideProximityScore({
  grade,
  label,
  description,
}: PesticideProximityScoreProps) {
  const reducedMotion = useReducedMotion();
  const gradeIndex = GRADE_ORDER.indexOf(grade);

  return (
    <div
      data-testid="pesticide-proximity-score"
      aria-live="polite"
      className="pt-6 pb-8"
    >
      <p className="text-muted-foreground mb-2 text-xs font-semibold tracking-widest uppercase">
        Pesticideksponering
      </p>
      <motion.div
        initial={reducedMotion ? false : { opacity: 0, y: 8 }}
        animate={reducedMotion ? undefined : { opacity: 1, y: 0 }}
        transition={{ duration: 0.4, ease: [0.25, 1, 0.5, 1], delay: 0.1 }}
        className="flex flex-col gap-1"
      >
        <p
          className={cn(
            'font-display text-3xl leading-tight font-semibold tracking-tight sm:text-4xl',
            getGradeColor(grade)
          )}
        >
          {label}
        </p>
        <p className="text-muted-foreground text-sm leading-relaxed">
          {description}
        </p>
      </motion.div>

      <div
        role="meter"
        aria-label="Pesticideksponering i percentiler"
        aria-valuemin={0}
        aria-valuemax={GRADE_ORDER.length - 1}
        aria-valuenow={gradeIndex}
        className="mt-6 flex items-center gap-1.5"
      >
        {GRADE_ORDER.map((g, segmentIdx) => {
          const isFilled = segmentIdx <= gradeIndex;
          return (
            <motion.div
              key={g}
              data-testid={`grade-indicator-${g}`}
              initial={reducedMotion ? false : { scaleX: 0, opacity: 0.6 }}
              animate={
                reducedMotion
                  ? undefined
                  : { scaleX: 1, opacity: isFilled ? 1 : 0.85 }
              }
              transition={{
                duration: 0.35,
                delay: reducedMotion ? 0 : 0.12 + segmentIdx * 0.06,
                ease: [0.25, 1, 0.5, 1],
              }}
              className={cn(
                'h-2.5 flex-1 origin-left rounded-full transition-all',
                isFilled ? getGradeBgColor(grade) : 'bg-muted'
              )}
            />
          );
        })}
      </div>
      <div className="text-muted-foreground mt-1.5 flex justify-between text-[10px]">
        <span>Under gennemsnit</span>
        <span>Top 1% mest eksponeret</span>
      </div>
    </div>
  );
}
