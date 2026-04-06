'use client';

import { motion } from 'motion/react';

interface TimelineSample {
  year: number;
  conc: string;
  detected: boolean;
}

interface GwTimelineSamplesProps {
  samples: readonly TimelineSample[];
  startYear: number;
  endYear: number;
  variant: 'bentazon' | 'mcpa' | 'pfas';
  arrived: boolean;
  displayYear: number;
}

export function GwTimelineSamples({
  samples,
  startYear,
  endYear,
  variant,
  arrived,
  displayYear,
}: GwTimelineSamplesProps) {
  if (samples.length <= 1) return null;

  const span = endYear - startYear;
  const dotBg = variant === 'bentazon' ? 'bg-destructive' : 'bg-primary';

  return (
    <div className="relative mt-1 h-5 w-full">
      {samples.map((sample, i) => {
        const frac = (sample.year - startYear) / span;
        const visible = arrived || displayYear >= sample.year;
        return (
          <motion.div
            key={`${sample.year}-${i}`}
            initial={{ opacity: 0, scale: 0.5 }}
            animate={{
              opacity: visible ? 1 : 0,
              scale: visible ? 1 : 0.5,
            }}
            transition={{ duration: 0.3, delay: visible ? i * 0.15 : 0 }}
            className="absolute -translate-x-1/2"
            style={{ left: `${frac * 100}%` }}
          >
            <div
              className={`mx-auto h-1.5 w-1.5 rounded-full ${
                sample.detected ? dotBg : 'bg-muted-foreground/40'
              }`}
            />
            <p
              className={`mt-0.5 text-center font-mono text-[8px] whitespace-nowrap ${
                sample.detected ? 'text-foreground' : 'text-muted-foreground'
              }`}
            >
              {sample.conc}
            </p>
          </motion.div>
        );
      })}
    </div>
  );
}
