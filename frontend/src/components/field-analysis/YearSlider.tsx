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
      className={`bg-background/95 rounded-lg p-4 shadow-lg backdrop-blur-sm sm:p-6 ${className}`}
    >
      {/* Mobile: Dropdown */}
      <div className="sm:hidden">
        <div className="flex flex-col space-y-3 sm:flex-row sm:items-center sm:space-y-0 sm:space-x-4">
          <label className="text-foreground text-sm font-medium">År:</label>
          <div className="flex items-center space-x-3">
            <div className="relative min-w-0 flex-1">
              <select
                value={selectedYear}
                onChange={handleSelectChange}
                disabled={isLoading}
                className={`border-border bg-background min-h-[44px] w-full rounded-md py-3 pr-10 pl-3 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none ${
                  isLoading ? 'cursor-not-allowed opacity-50' : ''
                }`}
              >
                {availableYears.map((year) => (
                  <option key={year} value={year}>
                    {getYearRangeDisplay(year)}
                  </option>
                ))}
              </select>
              <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3">
                <ChevronDownIcon
                  className="text-muted-foreground h-5 w-5"
                  aria-hidden="true"
                />
              </div>
            </div>
            {isLoading && (
              <div className="flex-shrink-0">
                <div className="h-5 w-5 animate-spin rounded-full border-b-2 border-blue-600"></div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Desktop: Slider */}
      <div className="hidden sm:block">
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <label className="text-foreground text-base font-medium">
              År: {getYearRangeDisplay(selectedYear)}
            </label>
            {isLoading && (
              <div className="h-4 w-4 animate-spin rounded-full border-b-2 border-blue-600"></div>
            )}
          </div>

          <div className="relative px-2">
            <input
              type="range"
              min={0}
              max={availableYears.length - 1}
              value={selectedIndex}
              onChange={handleSliderChange}
              disabled={isLoading}
              className={`slider h-3 w-full cursor-pointer appearance-none rounded-lg bg-gray-200 ${
                isLoading ? 'cursor-not-allowed opacity-50' : ''
              }`}
              style={{
                background: `linear-gradient(to right, #3b82f6 0%, #3b82f6 ${
                  (selectedIndex / (availableYears.length - 1)) * 100
                }%, #e5e7eb ${(selectedIndex / (availableYears.length - 1)) * 100}%, #e5e7eb 100%)`,
              }}
            />

            {/* Year markers */}
            <div className="mt-4 flex justify-between px-1">
              {availableYears.map((year, index) => (
                <div
                  key={year}
                  className={`text-sm font-medium transition-colors ${
                    year === selectedYear
                      ? 'text-blue-600'
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
        </div>
      </div>
    </div>
  );
}
