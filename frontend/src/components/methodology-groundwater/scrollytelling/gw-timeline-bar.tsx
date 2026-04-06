'use client';

import { motion, type MotionValue, useTransform } from 'motion/react';

interface TimelineConfig {
  startYear: number;
  endYear: number;
  midpoint: { year: number; label: string } | null;
  startLabel: string;
  endLabel: string;
  endSub: string;
  transport: string;
  endColor: string;
}

interface GwTimelineBarProps {
  config: TimelineConfig;
  variant: 'bentazon' | 'mcpa';
  progress: MotionValue<number>;
  arrived: boolean;
}

export function GwTimelineBar({
  config,
  variant,
  progress,
  arrived,
}: GwTimelineBarProps) {
  const widthPct = useTransform(progress, [0, 1], ['0%', '100%']);
  const leftPct = useTransform(progress, [0, 1], ['0%', '100%']);

  const midFraction = config.midpoint
    ? (config.midpoint.year - config.startYear) /
      (config.endYear - config.startYear)
    : null;

  const widthStyle = { width: widthPct };
  const leftStyle = { left: leftPct };
  const midStyle =
    midFraction !== null ? { left: `${midFraction * 100}%` } : undefined;

  const barColor =
    variant === 'bentazon'
      ? 'bg-destructive/60 absolute inset-y-0 left-0 rounded-full'
      : 'bg-primary/60 absolute inset-y-0 left-0 rounded-full';

  const dotColor =
    variant === 'bentazon'
      ? 'bg-destructive absolute top-1/2 h-3 w-3 -translate-y-1/2 rounded-full shadow-sm'
      : 'bg-primary absolute top-1/2 h-3 w-3 -translate-y-1/2 rounded-full shadow-sm';

  return (
    <>
      {/* Timeline bar */}
      <div className="relative h-2 w-full overflow-hidden rounded-full">
        <div className="bg-border absolute inset-0 rounded-full" />
        <motion.div className={barColor} style={widthStyle} />
        <motion.div className={dotColor} style={leftStyle} />
        {midFraction !== null && (
          <div
            className="bg-warning absolute top-1/2 h-2 w-2 -translate-x-1/2 -translate-y-1/2 rounded-full"
            style={midStyle}
          />
        )}
      </div>

      {/* Labels */}
      <div className="mt-2 flex justify-between text-[10px]">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3 }}
        >
          <p className="text-foreground font-semibold">{config.startYear}</p>
          <p className="text-muted-foreground">{config.startLabel}</p>
        </motion.div>
        {config.midpoint && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: arrived ? 1 : 0.3 }}
            className="text-center"
          >
            <p className="text-warning-foreground font-semibold">
              {config.midpoint.year}
            </p>
            <p className="text-muted-foreground max-w-20 leading-tight">
              {config.midpoint.label}
            </p>
          </motion.div>
        )}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: arrived ? 1 : 0.3 }}
          className="text-right"
        >
          <p className={`font-semibold ${config.endColor}`}>{config.endYear}</p>
          <p className={`font-mono ${config.endColor}`}>{config.endLabel}</p>
          <p className="text-muted-foreground">{config.endSub}</p>
        </motion.div>
      </div>

      {/* Transport annotation */}
      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: arrived ? 0.7 : 0 }}
        className="text-muted-foreground mt-1 text-center text-[9px]"
      >
        {config.transport}
      </motion.p>
    </>
  );
}
