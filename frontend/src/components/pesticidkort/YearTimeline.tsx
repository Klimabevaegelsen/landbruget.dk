/* oxlint-disable landbruget/max-component-size */
'use client';

import { useCallback, useRef } from 'react';
import { cn } from '@/lib/utils';

const YEARS = [
  2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024,
] as const;
const FIRST = YEARS[0];
const LAST = YEARS[YEARS.length - 1];

interface YearTimelineProps {
  year: number;
  onChange: (year: number) => void;
  compact?: boolean;
}

export function YearTimeline({ year, onChange, compact }: YearTimelineProps) {
  const trackRef = useRef<HTMLDivElement>(null);

  const handleTrackClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (!trackRef.current) return;
      const rect = trackRef.current.getBoundingClientRect();
      const fraction = Math.max(
        0,
        Math.min(1, (e.clientX - rect.left) / rect.width)
      );
      const idx = Math.round(fraction * (YEARS.length - 1));
      onChange(YEARS[idx]);
    },
    [onChange]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      const idx = YEARS.indexOf(year as (typeof YEARS)[number]);
      if (e.key === 'ArrowRight' && idx < YEARS.length - 1) {
        e.preventDefault();
        onChange(YEARS[idx + 1]);
      } else if (e.key === 'ArrowLeft' && idx > 0) {
        e.preventDefault();
        onChange(YEARS[idx - 1]);
      } else if (e.key === 'Home') {
        e.preventDefault();
        onChange(FIRST);
      } else if (e.key === 'End') {
        e.preventDefault();
        onChange(LAST);
      }
    },
    [year, onChange]
  );

  const activeIdx = YEARS.indexOf(year as (typeof YEARS)[number]);
  const progressWidths = [
    'w-0',
    'w-[11.111111%]',
    'w-[22.222222%]',
    'w-[33.333333%]',
    'w-[44.444444%]',
    'w-[55.555556%]',
    'w-[66.666667%]',
    'w-[77.777778%]',
    'w-[88.888889%]',
    'w-full',
  ] as const;

  return (
    <div
      className={cn('select-none', compact ? 'pt-2 pb-5' : 'py-4')}
      data-testid="year-timeline"
    >
      {!compact && (
        <div className="text-muted-foreground mb-3 flex items-baseline justify-between">
          <span className="text-xs font-medium tracking-wider uppercase">
            Sprøjteår
          </span>
          <span className="text-foreground text-sm font-semibold tabular-nums">
            {year}
          </span>
        </div>
      )}

      <div
        ref={trackRef}
        role="slider"
        aria-label="Vælg sprøjteår"
        aria-valuemin={FIRST}
        aria-valuemax={LAST}
        aria-valuenow={year}
        aria-valuetext={`${year}`}
        tabIndex={0}
        onKeyDown={handleKeyDown}
        onClick={handleTrackClick}
        className={cn(
          'relative cursor-pointer',
          compact ? 'h-6' : 'h-8',
          'focus-visible:outline-primary rounded focus-visible:outline-2 focus-visible:outline-offset-4'
        )}
      >
        {/* Track background */}
        <div className="bg-border absolute top-1/2 right-0 left-0 h-px -translate-y-1/2" />

        {/* Active fill */}
        <div
          className={cn(
            'bg-primary/40 absolute top-1/2 left-0 h-px -translate-y-1/2 transition-[width] duration-200',
            progressWidths[activeIdx]
          )}
        />

        {/* Year dots */}
        <div className="absolute inset-0 grid grid-cols-10">
          {YEARS.map((y) => {
            const isActive = y === year;
            return (
              <button
                key={y}
                onClick={(e) => {
                  e.stopPropagation();
                  onChange(y);
                }}
                data-testid={`year-${y}-button`}
                aria-label={`${y}`}
                aria-current={isActive ? 'true' : undefined}
                tabIndex={-1}
                className="group relative flex h-full w-full items-center justify-center"
              >
                <div
                  className={cn(
                    'rounded-full transition-all duration-200',
                    isActive
                      ? 'bg-primary h-3.5 w-3.5 shadow-sm'
                      : 'bg-border hover:bg-muted-foreground/50 h-2 w-2'
                  )}
                />
                {(!compact || isActive) && (
                  <span
                    data-testid={`year-dot-${y}`}
                    className={cn(
                      'absolute left-1/2 -translate-x-1/2 tabular-nums transition-opacity',
                      'top-full mt-1 text-[10px]',
                      isActive
                        ? 'text-foreground font-semibold opacity-100'
                        : 'text-muted-foreground opacity-0 group-hover:opacity-100 sm:opacity-60'
                    )}
                  >
                    {y}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
