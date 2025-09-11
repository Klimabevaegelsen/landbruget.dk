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
        className="text-foreground mb-1 block text-sm font-medium"
      >
        År
      </label>
      <div className="relative">
        <select
          id="year-selector"
          value={selectedYear}
          onChange={(e) => onYearChange(parseInt(e.target.value))}
          disabled={isLoading}
          className={`border-border bg-background block w-full rounded-md py-2 pr-10 pl-3 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 focus:outline-none ${
            isLoading ? 'cursor-not-allowed opacity-50' : ''
          }`}
        >
          {availableYears.map((year) => (
            <option key={year} value={year}>
              {year}
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
    </div>
  );
}
