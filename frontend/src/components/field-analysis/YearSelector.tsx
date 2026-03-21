'use client';

import React from 'react';
import { ChevronDownIcon } from '@heroicons/react/20/solid';
import { YearSelection } from './types';

interface YearSelectorProps {
  yearSelection: YearSelection;
  onYearChange: (year: number) => void;
  isLoading?: boolean;
  className?: string;
}

export function YearSelector({
  yearSelection,
  onYearChange,
  isLoading = false,
  className = '',
}: YearSelectorProps) {
  const { selectedYear, availableYears } = yearSelection;

  return (
    <div className={`relative ${className}`}>
      <label
        htmlFor="year-selector"
        className="text-foreground mb-2 block text-sm font-medium"
      >
        År
      </label>
      <div className="relative">
        <select
          id="year-selector"
          value={selectedYear}
          onChange={(e) => onYearChange(parseInt(e.target.value))}
          disabled={isLoading}
          data-testid="year-selector-select"
          className={`border-border bg-background block min-h-[44px] w-full rounded-md py-3 pr-10 pl-3 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none ${
            isLoading ? 'cursor-not-allowed opacity-50' : ''
          }`}
        >
          {availableYears.map((year) => (
            <option key={year} value={year}>
              {year}
            </option>
          ))}
        </select>
        <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3">
          <ChevronDownIcon
            className="text-muted-foreground h-5 w-5"
            aria-hidden="true"
          />
        </div>
        {isLoading && (
          <div className="absolute inset-y-0 right-8 flex items-center">
            <div className="h-4 w-4 animate-spin rounded-full border-b-2 border-blue-600"></div>
          </div>
        )}
      </div>
    </div>
  );
}
