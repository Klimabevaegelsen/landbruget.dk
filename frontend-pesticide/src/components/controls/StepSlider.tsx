'use client';

import {
  useMapStore,
  useAvailableYearOptions,
  useSelectedYear,
  type YearSelection,
} from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import { ChevronLeft, ChevronRight, Play, Pause } from 'lucide-react';
import { useEffect, useState } from 'react';

interface StepSliderProps {
  className?: string;
}

export function StepSlider({ className = '' }: StepSliderProps) {
  const selectedYear = useSelectedYear();
  const availableYearOptions = useAvailableYearOptions();
  const { setSelectedYear } = useMapStore();
  const { isMobile } = useUIStore();

  const [isAnimating, setIsAnimating] = useState(false);
  const [animationInterval, setAnimationInterval] =
    useState<NodeJS.Timeout | null>(null);

  // Get numeric years for animation and display
  const numericYears = availableYearOptions
    .filter((year): year is number => typeof year === 'number')
    .sort((a, b) => a - b);
  const hasTotal = availableYearOptions.includes('total');

  // All options in order: years + total
  const allOptions = [
    ...numericYears,
    ...(hasTotal ? ['total'] : []),
  ] as YearSelection[];

  const currentIndex = allOptions.indexOf(selectedYear);

  const startAnimation = () => {
    if (numericYears.length <= 1) return;

    setIsAnimating(true);
    const interval = setInterval(() => {
      const currentIndex = numericYears.indexOf(selectedYear as number);
      const nextIndex =
        currentIndex >= 0 ? (currentIndex + 1) % numericYears.length : 0;
      setSelectedYear(numericYears[nextIndex]);
    }, 1500);

    setAnimationInterval(interval);
  };

  const stopAnimation = () => {
    setIsAnimating(false);
    if (animationInterval) {
      clearInterval(animationInterval);
      setAnimationInterval(null);
    }
  };

  const goToNext = () => {
    const currentIdx = allOptions.indexOf(selectedYear);
    if (currentIdx < allOptions.length - 1) {
      setSelectedYear(allOptions[currentIdx + 1]);
    }
  };

  const goToPrevious = () => {
    const currentIdx = allOptions.indexOf(selectedYear);
    if (currentIdx > 0) {
      setSelectedYear(allOptions[currentIdx - 1]);
    }
  };

  const canGoNext = currentIndex < allOptions.length - 1;
  const canGoPrevious = currentIndex > 0;

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
        <div className="text-sm text-gray-400">Loading...</div>
      </div>
    );
  }

  // Mobile-optimized version
  if (isMobile) {
    return (
      <div className={`${className} space-y-3`}>
        {/* Navigation Controls - Mobile */}
        <div className="flex items-center justify-center space-x-3">
          {/* Previous Button */}
          <button
            onClick={goToPrevious}
            disabled={!canGoPrevious || isAnimating}
            className="touch-manipulation rounded-lg bg-slate-800 p-2 text-slate-300 transition-all hover:bg-slate-700 hover:text-white disabled:cursor-not-allowed disabled:opacity-30"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>

          {/* Play/Pause Button */}
          <button
            onClick={isAnimating ? stopAnimation : startAnimation}
            disabled={numericYears.length <= 1}
            className="touch-manipulation rounded-lg bg-blue-600 p-2 text-white transition-all hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-30"
          >
            {isAnimating ? (
              <Pause className="h-4 w-4" />
            ) : (
              <Play className="h-4 w-4" />
            )}
          </button>

          {/* Next Button */}
          <button
            onClick={goToNext}
            disabled={!canGoNext || isAnimating}
            className="touch-manipulation rounded-lg bg-slate-800 p-2 text-slate-300 transition-all hover:bg-slate-700 hover:text-white disabled:cursor-not-allowed disabled:opacity-30"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>

        {/* Year Buttons - Mobile Grid */}
        <div className="flex flex-wrap justify-center gap-2">
          {/* Year Buttons */}
          {numericYears.map((year) => (
            <button
              key={year}
              onClick={() => setSelectedYear(year)}
              disabled={isAnimating}
              className={`touch-manipulation rounded-md px-3 py-2 text-sm font-medium transition-all duration-200 ${
                selectedYear === year
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
              } disabled:cursor-not-allowed disabled:opacity-30`}
            >
              {year}
            </button>
          ))}

          {/* Total Button */}
          {hasTotal && (
            <button
              onClick={() => setSelectedYear('total')}
              disabled={isAnimating}
              className={`touch-manipulation rounded-md px-3 py-2 text-sm font-medium transition-all duration-200 ${
                selectedYear === 'total'
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
              } disabled:cursor-not-allowed disabled:opacity-30`}
            >
              Total
            </button>
          )}
        </div>
      </div>
    );
  }

  // Desktop version
  return (
    <div className={`${className} flex items-center space-x-2`}>
      {/* Previous Button */}
      <button
        onClick={goToPrevious}
        disabled={!canGoPrevious || isAnimating}
        className="rounded-lg bg-slate-800 p-2 text-slate-300 transition-all hover:bg-slate-700 hover:text-white disabled:cursor-not-allowed disabled:opacity-30"
      >
        <ChevronLeft className="h-4 w-4" />
      </button>

      {/* Play/Pause Button */}
      <button
        onClick={isAnimating ? stopAnimation : startAnimation}
        disabled={numericYears.length <= 1}
        className="rounded-lg bg-blue-600 p-2 text-white transition-all hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-30"
      >
        {isAnimating ? (
          <Pause className="h-4 w-4" />
        ) : (
          <Play className="h-4 w-4" />
        )}
      </button>

      {/* Step Buttons */}
      <div className="flex items-center space-x-1">
        {/* Year Buttons */}
        {numericYears.map((year) => (
          <button
            key={year}
            onClick={() => setSelectedYear(year)}
            disabled={isAnimating}
            className={`min-w-[50px] rounded-md px-3 py-1.5 text-sm font-medium transition-all duration-200 ${
              selectedYear === year
                ? 'bg-blue-600 text-white'
                : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
            } disabled:cursor-not-allowed disabled:opacity-30`}
          >
            {year}
          </button>
        ))}

        {/* Total Button */}
        {hasTotal && (
          <button
            onClick={() => setSelectedYear('total')}
            disabled={isAnimating}
            className={`min-w-[60px] rounded-md px-3 py-1.5 text-sm font-medium transition-all duration-200 ${
              selectedYear === 'total'
                ? 'bg-blue-600 text-white'
                : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
            } disabled:cursor-not-allowed disabled:opacity-30`}
          >
            Total
          </button>
        )}
      </div>

      {/* Next Button */}
      <button
        onClick={goToNext}
        disabled={!canGoNext || isAnimating}
        className="rounded-lg bg-slate-800 p-2 text-slate-300 transition-all hover:bg-slate-700 hover:text-white disabled:cursor-not-allowed disabled:opacity-30"
      >
        <ChevronRight className="h-4 w-4" />
      </button>
    </div>
  );
}
