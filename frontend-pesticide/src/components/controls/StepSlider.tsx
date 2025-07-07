'use client';

import { useMapStore, useAvailableYearOptions, useSelectedYear, type YearSelection } from '@/stores/map-store';
import { ChevronLeft, ChevronRight, Play, Pause } from 'lucide-react';
import { useEffect, useState } from 'react';

interface StepSliderProps {
  className?: string;
}

export function StepSlider({ className = '' }: StepSliderProps) {
  const selectedYear = useSelectedYear();
  const availableYearOptions = useAvailableYearOptions();
  const { setSelectedYear } = useMapStore();
  
  const [isAnimating, setIsAnimating] = useState(false);
  const [animationInterval, setAnimationInterval] = useState<NodeJS.Timeout | null>(null);

  // Get numeric years for animation and display
  const numericYears = availableYearOptions.filter((year): year is number => typeof year === 'number').sort((a, b) => a - b);
  const hasTotal = availableYearOptions.includes('total');
  
  // All options in order: years + total
  const allOptions = [...numericYears, ...(hasTotal ? ['total'] : [])] as YearSelection[];
  
  const currentIndex = allOptions.indexOf(selectedYear);

  const startAnimation = () => {
    if (numericYears.length <= 1) return;
    
    setIsAnimating(true);
    const interval = setInterval(() => {
      const currentIndex = numericYears.indexOf(selectedYear as number);
      const nextIndex = currentIndex >= 0 ? (currentIndex + 1) % numericYears.length : 0;
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
        <div className="text-gray-400 text-sm">Loading...</div>
      </div>
    );
  }

  return (
    <div className={`${className} flex items-center space-x-2`}>
      {/* Previous Button */}
      <button
        onClick={goToPrevious}
        disabled={!canGoPrevious || isAnimating}
        className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-all"
      >
        <ChevronLeft className="w-4 h-4" />
      </button>

      {/* Play/Pause Button */}
      <button
        onClick={isAnimating ? stopAnimation : startAnimation}
        disabled={numericYears.length <= 1}
        className="p-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-30 disabled:cursor-not-allowed transition-all"
      >
        {isAnimating ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
      </button>

      {/* Step Buttons */}
      <div className="flex items-center space-x-1">
        {/* Year Buttons */}
        {numericYears.map((year) => (
          <button
            key={year}
            onClick={() => setSelectedYear(year)}
            disabled={isAnimating}
            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-all duration-200 min-w-[50px] ${
              selectedYear === year
                ? 'bg-blue-600 text-white'
                : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
            } disabled:opacity-30 disabled:cursor-not-allowed`}
          >
            {year}
          </button>
        ))}
        
        {/* Total Button */}
        {hasTotal && (
          <button
            onClick={() => setSelectedYear('total')}
            disabled={isAnimating}
            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-all duration-200 min-w-[60px] ${
              selectedYear === 'total'
                ? 'bg-blue-600 text-white'
                : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
            } disabled:opacity-30 disabled:cursor-not-allowed`}
          >
            Total
          </button>
        )}
      </div>

      {/* Next Button */}
      <button
        onClick={goToNext}
        disabled={!canGoNext || isAnimating}
        className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed transition-all"
      >
        <ChevronRight className="w-4 h-4" />
      </button>
    </div>
  );
} 