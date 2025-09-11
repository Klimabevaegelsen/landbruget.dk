'use client';

import React from 'react';
import { ChevronDownIcon } from '@heroicons/react/20/solid';
import { YearSelection, getYearRangeDisplay } from './types';

interface YearSliderProps {
  yearSelection: YearSelection;
  onYearChange: (year: number) => void;
  isLoading?: boolean;
  className?: string;
}

export function YearSlider({
  yearSelection,
  onYearChange,
  isLoading = false,
  className = '',
}: YearSliderProps) {
  const { selectedYear, availableYears } = yearSelection;

  // Find the index of the selected year
  const selectedIndex = availableYears.indexOf(selectedYear);

  const handleSliderChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const index = parseInt(event.target.value);
    const year = availableYears[index];
    onYearChange(year);
  };

  const handleSelectChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const year = parseInt(event.target.value);
    onYearChange(year);
  };

  return (
    <div
      className={`rounded-lg bg-white/95 p-3 shadow-lg backdrop-blur-sm sm:p-4 ${className}`}
    >
      {/* Mobile: Dropdown */}
      <div className="sm:hidden">
        <div className="flex items-center space-x-3">
          <label className="text-foreground text-sm font-medium whitespace-nowrap">
            År:
          </label>
          <div className="relative flex-1">
            <select
              value={selectedYear}
              onChange={handleSelectChange}
              disabled={isLoading}
              className={`w-full rounded-md border-gray-300 bg-white py-2 pr-10 pl-3 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none ${
                isLoading ? 'cursor-not-allowed opacity-50' : ''
              }`}
            >
              {availableYears.map((year) => (
                <option key={year} value={year}>
                  {getYearRangeDisplay(year)}
                </option>
              ))}
            </select>
            <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
              <ChevronDownIcon
                className="text-muted-foreground h-4 w-4"
                aria-hidden="true"
              />
            </div>
          </div>
          {isLoading && (
            <div className="h-4 w-4 animate-spin rounded-full border-b-2 border-blue-600"></div>
          )}
        </div>
      </div>

      {/* Desktop: Slider */}
      <div className="hidden sm:block">
        <div className="flex items-center space-x-4">
          <div className="flex-shrink-0">
            <label className="text-foreground text-sm font-medium">
              År: {getYearRangeDisplay(selectedYear)}
            </label>
          </div>

          <div className="relative flex-1">
            <input
              type="range"
              min={0}
              max={availableYears.length - 1}
              value={selectedIndex}
              onChange={handleSliderChange}
              disabled={isLoading}
              className={`slider h-2 w-full cursor-pointer appearance-none rounded-lg bg-gray-200 ${
                isLoading ? 'cursor-not-allowed opacity-50' : ''
              }`}
              style={{
                background: `linear-gradient(to right, #3b82f6 0%, #3b82f6 ${
                  (selectedIndex / (availableYears.length - 1)) * 100
                }%, #e5e7eb ${(selectedIndex / (availableYears.length - 1)) * 100}%, #e5e7eb 100%)`,
              }}
            />

            {/* Year markers */}
            <div className="mt-1 flex justify-between px-1">
              {availableYears.map((year, index) => (
                <div
                  key={year}
                  className={`text-xs transition-colors ${
                    year === selectedYear
                      ? 'font-medium text-blue-600'
                      : 'text-muted-foreground'
                  }`}
                  style={{
                    transform: 'translateX(-50%)',
                    marginLeft: index === 0 ? '0' : undefined,
                    marginRight:
                      index === availableYears.length - 1 ? '0' : undefined,
                  }}
                >
                  {year}
                </div>
              ))}
            </div>
          </div>

          {isLoading && (
            <div className="flex-shrink-0">
              <div className="h-4 w-4 animate-spin rounded-full border-b-2 border-blue-600"></div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
