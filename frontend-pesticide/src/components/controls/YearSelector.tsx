'use client';

import {
  useMapStore,
  useAvailableYearOptions,
  useSelectedYear,
} from '@/stores/map-store';
import { ChevronLeft, ChevronRight, Play, Pause } from 'lucide-react';
import { useEffect, useState } from 'react';

interface YearSelectorProps {
  className?: string;
}

export function YearSelector({ className = '' }: YearSelectorProps) {
  const selectedYear = useSelectedYear();
  const availableYearOptions = useAvailableYearOptions();
  const { setSelectedYear } = useMapStore();

  const [isAnimating, setIsAnimating] = useState(false);
  const [animationInterval, setAnimationInterval] =
    useState<NodeJS.Timeout | null>(null);

  // Filter out 'total' for animation (only animate through numeric years)
  const numericYears = availableYearOptions.filter(
    (year): year is number => typeof year === 'number'
  );

  const startAnimation = () => {
    if (numericYears.length <= 1) return;

    setIsAnimating(true);
    const interval = setInterval(() => {
      const currentIndex = numericYears.indexOf(selectedYear as number);
      const nextIndex =
        currentIndex >= 0 ? (currentIndex + 1) % numericYears.length : 0;
      setSelectedYear(numericYears[nextIndex]);
    }, 1500); // Change year every 1.5 seconds

    setAnimationInterval(interval);
  };

  const stopAnimation = () => {
    setIsAnimating(false);
    if (animationInterval) {
      clearInterval(animationInterval);
      setAnimationInterval(null);
    }
  };

  const goToNextYear = () => {
    if (selectedYear === 'total') {
      // If currently on total, go to first numeric year
      if (numericYears.length > 0) {
        setSelectedYear(numericYears[0]);
      }
    } else {
      const currentIndex = numericYears.indexOf(selectedYear as number);
      if (currentIndex < numericYears.length - 1) {
        setSelectedYear(numericYears[currentIndex + 1]);
      } else {
        // Go to total after last year
        setSelectedYear('total');
      }
    }
  };

  const goToPreviousYear = () => {
    if (selectedYear === 'total') {
      // If currently on total, go to last numeric year
      if (numericYears.length > 0) {
        setSelectedYear(numericYears[numericYears.length - 1]);
      }
    } else {
      const currentIndex = numericYears.indexOf(selectedYear as number);
      if (currentIndex > 0) {
        setSelectedYear(numericYears[currentIndex - 1]);
      }
    }
  };

  const canGoNext = () => {
    return availableYearOptions.length > 1;
  };

  const canGoPrevious = () => {
    return availableYearOptions.length > 1;
  };

  // Cleanup animation on unmount
  useEffect(() => {
    return () => {
      if (animationInterval) {
        clearInterval(animationInterval);
      }
    };
  }, [animationInterval]);

  if (availableYearOptions.length === 0) {
    return (
      <div className={`${className} flex items-center justify-center`}>
        <div className="text-sm text-gray-400">Loading years...</div>
      </div>
    );
  }

  return (
    <div className={`${className} space-y-4`}>
      {/* Navigation Controls */}
      <div className="flex items-center justify-center space-x-4">
        <button
          onClick={goToPreviousYear}
          disabled={!canGoPrevious() || isAnimating}
          className="rounded-lg bg-gray-700 p-2 transition-all hover:bg-gray-600 disabled:cursor-not-allowed disabled:opacity-30"
        >
          <ChevronLeft className="h-5 w-5" />
        </button>

        <button
          onClick={isAnimating ? stopAnimation : startAnimation}
          disabled={numericYears.length <= 1}
          className="rounded-lg bg-blue-600 p-2 transition-all hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-30"
        >
          {isAnimating ? (
            <Pause className="h-5 w-5" />
          ) : (
            <Play className="h-5 w-5" />
          )}
        </button>

        <button
          onClick={goToNextYear}
          disabled={!canGoNext() || isAnimating}
          className="rounded-lg bg-gray-700 p-2 transition-all hover:bg-gray-600 disabled:cursor-not-allowed disabled:opacity-30"
        >
          <ChevronRight className="h-5 w-5" />
        </button>
      </div>

      {/* Current Selection Display */}
      <div className="text-center">
        <div className="inline-block rounded-lg bg-gray-700 px-4 py-2">
          <span className="text-lg font-bold">
            {selectedYear === 'total' ? 'Total (All Years)' : selectedYear}
          </span>
        </div>
      </div>

      {/* Year Options Grid */}
      <div className="space-y-3">
        {/* Individual Years */}
        <div>
          <div className="mb-2 text-sm text-gray-400">Individual Years</div>
          <div className="grid grid-cols-3 gap-2">
            {numericYears.map((year) => (
              <button
                key={year}
                onClick={() => setSelectedYear(year)}
                disabled={isAnimating}
                className={`rounded-lg px-3 py-2 text-sm font-medium transition-all ${
                  selectedYear === year
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-600 text-gray-300 hover:bg-gray-500'
                } disabled:cursor-not-allowed disabled:opacity-30`}
              >
                {year}
              </button>
            ))}
          </div>
        </div>

        {/* Total Option */}
        <div>
          <div className="mb-2 text-sm text-gray-400">Cumulative Data</div>
          <button
            onClick={() => setSelectedYear('total')}
            disabled={isAnimating}
            className={`w-full rounded-lg px-4 py-3 text-sm font-medium transition-all ${
              selectedYear === 'total'
                ? 'bg-gradient-to-r from-purple-600 to-blue-600 text-white shadow-lg'
                : 'bg-gray-600 text-gray-300 hover:bg-gray-500'
            } disabled:cursor-not-allowed disabled:opacity-30`}
          >
            <div className="flex items-center justify-center space-x-2">
              <span>Total</span>
              <span className="text-xs opacity-75">
                (
                {numericYears.length > 0
                  ? `${Math.min(...numericYears)}-${Math.max(...numericYears)}`
                  : 'All Years'}
                )
              </span>
            </div>
          </button>
        </div>
      </div>

      {/* Animation Status */}
      {isAnimating && (
        <div className="text-center">
          <div className="animate-pulse text-xs text-blue-400">
            Animating through years...
          </div>
        </div>
      )}
    </div>
  );
}
