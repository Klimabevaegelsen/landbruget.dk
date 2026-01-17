'use client';

import { useEffect } from 'react';
import { useTemporalStore } from '@/stores/temporal-store';
import { usePMTilesStore } from '@/stores/pmtiles-store';
import { Play, Pause, ChevronLeft, ChevronRight } from 'lucide-react';

interface TemporalControlsProps {
  className?: string;
}

export function TemporalControls({ className = '' }: TemporalControlsProps) {
  const {
    currentYear,
    availableYears,
    isAnimating,
    startAnimation,
    stopAnimation,
    goToNextYear,
    goToPreviousYear,
    canGoNext,
    canGoPrevious,
    getYearRange,
  } = useTemporalStore();

  const { metadata } = usePMTilesStore();

  // Update available years from metadata
  useEffect(() => {
    if (metadata?.years) {
      useTemporalStore.getState().setAvailableYears(metadata.years);
    }
  }, [metadata]);

  const yearRange = getYearRange();

  return (
    <div className={`${className}`}>
      {/* Simple Controls */}
      <div className="mb-3 flex items-center justify-center space-x-4">
        {/* Previous */}
        <button
          onClick={goToPreviousYear}
          disabled={!canGoPrevious() || isAnimating}
          className="rounded-full bg-white/10 p-2 transition-all hover:bg-white/20 disabled:cursor-not-allowed disabled:opacity-30"
        >
          <ChevronLeft className="h-4 w-4 text-white" />
        </button>

        {/* Play/Pause */}
        <button
          onClick={isAnimating ? stopAnimation : startAnimation}
          disabled={availableYears.length <= 1}
          className="rounded-full bg-white p-2 text-black transition-all hover:bg-gray-100 disabled:cursor-not-allowed disabled:opacity-30"
        >
          {isAnimating ? (
            <Pause className="h-4 w-4" />
          ) : (
            <Play className="h-4 w-4" />
          )}
        </button>

        {/* Next */}
        <button
          onClick={goToNextYear}
          disabled={!canGoNext() || isAnimating}
          className="rounded-full bg-white/10 p-2 transition-all hover:bg-white/20 disabled:cursor-not-allowed disabled:opacity-30"
        >
          <ChevronRight className="h-4 w-4 text-white" />
        </button>
      </div>

      {/* Year Progress Bar */}
      {yearRange && (
        <div>
          <div className="mb-1 flex justify-between text-xs text-gray-400">
            <span>{yearRange[0]}</span>
            <span>{yearRange[1]}</span>
          </div>
          <div className="h-0.5 w-full rounded-full bg-white/20">
            <div
              className="h-0.5 rounded-full bg-white transition-all duration-300"
              style={{
                width: `${((currentYear - yearRange[0]) / (yearRange[1] - yearRange[0])) * 100}%`,
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
}
