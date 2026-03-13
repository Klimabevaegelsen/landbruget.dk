'use client';

import React from 'react';
import {
  useMapStore,
  useDataState,
  DataMode,
  DATA_MODE_CONFIG,
} from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';

interface DataModeSelectorProps {
  className?: string;
  variant?: 'sidebar' | 'topbar' | 'mobile';
}

const _ColorScaleLegend: React.FC<{ mode: DataMode }> = ({ mode }) => {
  const config = DATA_MODE_CONFIG[mode];

  // Generate color scale for visualization
  const colorStops = [
    { value: 0, color: 'rgba(255, 255, 255, 0.8)' },
    { value: 1, color: 'rgba(255, 200, 200, 0.8)' },
    { value: 5, color: 'rgba(255, 150, 150, 0.8)' },
    { value: 10, color: 'rgba(255, 100, 100, 0.8)' },
    { value: 20, color: 'rgba(255, 50, 50, 0.8)' },
    { value: 50, color: 'rgba(255, 0, 0, 0.8)' },
    { value: 100, color: 'rgba(200, 0, 0, 0.8)' },
    { value: 200, color: 'rgba(150, 0, 0, 0.8)' },
    { value: 500, color: 'rgba(100, 0, 0, 0.8)' },
    { value: 1000, color: 'rgba(50, 0, 0, 0.8)' },
  ];

  return (
    <div className="mt-3 rounded-lg bg-gray-50 p-3">
      <div className="mb-2 text-sm font-medium text-gray-900">
        {config.label} Scale
      </div>

      {/* Color gradient bar */}
      <div
        className="relative mb-2 h-4 rounded"
        style={{
          background: `linear-gradient(to right, ${colorStops.map((stop) => stop.color).join(', ')})`,
        }}
      >
        <div className="absolute inset-0 rounded border border-gray-300"></div>
      </div>

      {/* Scale labels */}
      <div className="flex justify-between text-xs text-gray-600">
        <span>0</span>
        <span>Low</span>
        <span>Medium</span>
        <span>High</span>
        <span>1000+ {config.unit}</span>
      </div>

      <div className="mt-2 text-xs text-gray-500">{config.description}</div>
    </div>
  );
};

// Define modes with mobile-friendly labels
const modes: {
  key: DataMode;
  label: string;
  shortLabel: string;
  mobileLabel: string;
  description: string;
  color: string;
}[] = [
  {
    key: 'pesticide_total',
    label: 'Total Pesticide',
    shortLabel: 'Total',
    mobileLabel: 'Total',
    description: 'All pesticide applications combined',
    color: 'text-gray-700',
  },
  {
    key: 'pfas',
    label: 'PFAS',
    shortLabel: 'PFAS',
    mobileLabel: 'PFAS',
    description: 'PFAS-containing pesticides only',
    color: 'text-red-600',
  },
  {
    key: 'diquat',
    label: 'Diquat',
    shortLabel: 'Diquat',
    mobileLabel: 'Diquat',
    description: 'Diquat-containing pesticides only',
    color: 'text-blue-600',
  },
  {
    key: 'glyphosate',
    label: 'Glyphosate',
    shortLabel: 'Glyphosate',
    mobileLabel: 'Glyphosate',
    description: 'Glyphosate-containing pesticides only',
    color: 'text-green-600',
  },
];

export const DataModeSelector: React.FC<DataModeSelectorProps> = ({
  className = '',
  variant = 'sidebar',
}) => {
  const { selectedDataMode } = useDataState();
  const { setSelectedDataMode } = useMapStore();
  const { isMobile } = useUIStore();

  // Mobile-optimized topbar version
  if (variant === 'topbar' && isMobile) {
    return (
      <div className={`flex flex-wrap gap-2 ${className}`}>
        {modes.map((mode) => (
          <button
            key={mode.key}
            onClick={() => setSelectedDataMode(mode.key)}
            className={`min-w-0 touch-manipulation rounded-md border px-3 py-2 text-sm font-medium transition-all duration-200 ${
              selectedDataMode === mode.key
                ? 'border-white/30 bg-white/20 text-white shadow-sm'
                : 'border-white/10 bg-black/20 text-white/60 hover:border-white/20 hover:bg-white/10 hover:text-white'
            }`}
          >
            {mode.mobileLabel}
          </button>
        ))}
      </div>
    );
  }

  // Desktop topbar version
  if (variant === 'topbar') {
    return (
      <div className={`flex items-center space-x-1 ${className}`}>
        {modes.map((mode) => (
          <button
            key={mode.key}
            onClick={() => setSelectedDataMode(mode.key)}
            className={`rounded-md border px-3 py-1.5 text-sm font-medium transition-all duration-200 ${
              selectedDataMode === mode.key
                ? 'border-white/30 bg-white/20 text-white shadow-sm'
                : 'border-white/10 bg-black/20 text-white/60 hover:border-white/20 hover:bg-white/10 hover:text-white'
            }`}
          >
            {mode.shortLabel}
          </button>
        ))}
      </div>
    );
  }

  // Mobile-specific variant
  if (variant === 'mobile') {
    return (
      <div className={`space-y-2 ${className}`}>
        {modes.map((mode) => (
          <button
            key={mode.key}
            onClick={() => setSelectedDataMode(mode.key)}
            className={`flex w-full touch-manipulation items-center justify-between rounded-lg border p-4 transition-all duration-200 ${
              selectedDataMode === mode.key
                ? 'border-white/30 bg-white/20 text-white shadow-sm'
                : 'border-white/10 bg-black/20 text-white/60 hover:border-white/20 hover:bg-white/10 hover:text-white'
            }`}
          >
            <div className="flex flex-col items-start">
              <span className="font-medium">{mode.label}</span>
              <span className="text-xs text-white/50">{mode.description}</span>
            </div>
            {selectedDataMode === mode.key && (
              <div className="h-2 w-2 rounded-full bg-white"></div>
            )}
          </button>
        ))}
      </div>
    );
  }

  // Default sidebar version
  return (
    <div className={`space-y-2 ${className}`}>
      {modes.map((mode) => (
        <button
          key={mode.key}
          onClick={() => setSelectedDataMode(mode.key)}
          className={`flex w-full items-center justify-between rounded-lg border p-3 transition-all duration-200 ${
            selectedDataMode === mode.key
              ? 'border-white/30 bg-white/20 text-white shadow-sm'
              : 'border-white/10 bg-black/20 text-white/60 hover:border-white/20 hover:bg-white/10 hover:text-white'
          }`}
        >
          <div className="flex flex-col items-start">
            <span className="text-sm font-medium">{mode.label}</span>
            <span className="text-xs text-white/50">{mode.description}</span>
          </div>
          {selectedDataMode === mode.key && (
            <div className="h-2 w-2 rounded-full bg-white"></div>
          )}
        </button>
      ))}
    </div>
  );
};

export default DataModeSelector;
