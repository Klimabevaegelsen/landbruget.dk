'use client';

import { useCallback, useEffect, useRef } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';

const YEARS = [
  2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024,
] as const;
const FIRST = YEARS[0];
const LAST = YEARS[YEARS.length - 1];

interface YearTimelineProps {
  year: number;
  onChange: (year: number) => void;
}

export function YearTimeline({ year, onChange }: YearTimelineProps) {
  const stripRef = useRef<HTMLDivElement>(null);
  const activeIdx = YEARS.indexOf(year as (typeof YEARS)[number]);

  useEffect(() => {
    const strip = stripRef.current;
    if (!strip) return;
    const activeChip = strip.querySelector<HTMLElement>(
      `[data-year="${year}"]`
    );
    activeChip?.scrollIntoView({
      behavior: 'smooth',
      inline: 'center',
      block: 'nearest',
    });
  }, [year]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'ArrowRight' && activeIdx < YEARS.length - 1) {
        e.preventDefault();
        onChange(YEARS[activeIdx + 1]);
      } else if (e.key === 'ArrowLeft' && activeIdx > 0) {
        e.preventDefault();
        onChange(YEARS[activeIdx - 1]);
      } else if (e.key === 'Home') {
        e.preventDefault();
        onChange(FIRST);
      } else if (e.key === 'End') {
        e.preventDefault();
        onChange(LAST);
      }
    },
    [activeIdx, onChange]
  );

  return (
    <div
      className="flex items-center gap-0.5 select-none"
      data-testid="year-timeline"
      role="slider"
      aria-label="Vælg sprøjteår"
      aria-valuemin={FIRST}
      aria-valuemax={LAST}
      aria-valuenow={year}
      aria-valuetext={`${year}`}
      tabIndex={0}
      onKeyDown={handleKeyDown}
    >
      <button
        type="button"
        onClick={() => activeIdx > 0 && onChange(YEARS[activeIdx - 1])}
        disabled={activeIdx <= 0}
        aria-label="Forrige år"
        data-testid="year-prev-button"
        className="text-muted-foreground hover:text-foreground flex h-9 w-9 shrink-0 items-center justify-center rounded-full transition-colors disabled:opacity-30"
      >
        <ChevronLeft className="h-4 w-4" />
      </button>

      <div
        ref={stripRef}
        className="flex min-w-0 flex-1 snap-x snap-mandatory gap-0.5 overflow-x-auto scroll-smooth [scrollbar-width:none] [&::-webkit-scrollbar]:hidden"
      >
        {YEARS.map((y) => {
          const isActive = y === year;
          return (
            <button
              key={y}
              type="button"
              data-year={y}
              data-testid={`year-${y}-button`}
              aria-current={isActive ? 'true' : undefined}
              onClick={() => onChange(y)}
              className={cn(
                'shrink-0 snap-center rounded-full px-2.5 py-1.5 text-xs font-medium tabular-nums transition-colors',
                isActive
                  ? 'bg-primary text-primary-foreground shadow-sm'
                  : 'text-muted-foreground hover:bg-muted hover:text-foreground'
              )}
            >
              {y}
            </button>
          );
        })}
      </div>

      <button
        type="button"
        onClick={() =>
          activeIdx < YEARS.length - 1 && onChange(YEARS[activeIdx + 1])
        }
        disabled={activeIdx >= YEARS.length - 1}
        aria-label="Næste år"
        data-testid="year-next-button"
        className="text-muted-foreground hover:text-foreground flex h-9 w-9 shrink-0 items-center justify-center rounded-full transition-colors disabled:opacity-30"
      >
        <ChevronRight className="h-4 w-4" />
      </button>
    </div>
  );
}
