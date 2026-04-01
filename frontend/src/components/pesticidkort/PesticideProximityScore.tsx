'use client';

import { cn } from '@/lib/utils';
import { motion, useReducedMotion } from 'framer-motion';
import type { PesticideGrade } from '@/components/pesticidkort/types';
import { getGradeColor, getGradeBgColor } from '@/lib/pesticide-score';

const ALL_GRADES: PesticideGrade[] = ['A', 'B', 'C', 'D', 'E'];

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
  const gradeIndex = ALL_GRADES.indexOf(grade);

  return (
    <div
      data-testid="pesticide-proximity-score"
      aria-live="polite"
      className="pt-6 pb-8"
    >
      <div className="flex items-end gap-4">
        <motion.span
          aria-label={`Karakter ${grade}`}
          initial={reducedMotion ? false : { opacity: 0, scale: 0.9, y: 8 }}
          animate={reducedMotion ? undefined : { opacity: 1, scale: 1, y: 0 }}
          transition={{ duration: 0.4, ease: [0.25, 1, 0.5, 1], delay: 0.1 }}
          className={cn(
            'font-display text-[7rem] leading-[0.85] font-semibold tracking-tighter sm:text-[8rem]',
            getGradeColor(grade)
          )}
        >
          {grade}
        </motion.span>
        <div className="mb-3">
          <p className="text-foreground text-xl font-semibold sm:text-2xl">
            {label}
          </p>
          <p className="text-muted-foreground mt-1 text-sm leading-relaxed">
            {description}
          </p>
          <p className="text-muted-foreground mt-2 text-xs">
            Karakterskala: A = lav belastning, E = høj belastning.
          </p>
        </div>
      </div>

      <div
        role="meter"
        aria-label="Pesticidbelastning"
        aria-valuemin={0}
        aria-valuemax={4}
        aria-valuenow={ALL_GRADES.indexOf(grade)}
        className="mt-6 flex items-center gap-1.5"
      >
        {ALL_GRADES.map((g) => {
          const segmentIdx = ALL_GRADES.indexOf(g);
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
                delay: reducedMotion ? 0 : 0.12 + segmentIdx * 0.07,
                ease: [0.25, 1, 0.5, 1],
              }}
              className={cn(
                'h-1.5 flex-1 origin-left rounded-full transition-all',
                isFilled ? getGradeBgColor(grade) : 'bg-muted'
              )}
            />
          );
        })}
      </div>
      <div className="text-muted-foreground mt-1.5 flex justify-between text-[10px]">
        <span>A (lav)</span>
        <span>E (høj)</span>
      </div>
    </div>
  );
}
