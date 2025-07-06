'use client';

import React from 'react';
import { useMapStore, useDataState, DataMode, DATA_MODE_CONFIG } from '@/stores/map-store';

interface DataModeSelectorProps {
  className?: string;
  variant?: 'sidebar' | 'topbar';
}

const ColorScaleLegend: React.FC<{ mode: DataMode }> = ({ mode }) => {
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
    <div className="mt-3 p-3 bg-gray-50 rounded-lg">
      <div className="text-sm font-medium text-gray-900 mb-2">
        {config.label} Scale
      </div>
      
      {/* Color gradient bar */}
      <div className="relative h-4 rounded mb-2" style={{
        background: `linear-gradient(to right, ${colorStops.map(stop => stop.color).join(', ')})`
      }}>
        <div className="absolute inset-0 border border-gray-300 rounded"></div>
      </div>
      
      {/* Scale labels */}
      <div className="flex justify-between text-xs text-gray-600">
        <span>0</span>
        <span>Low</span>
        <span>Medium</span>
        <span>High</span>
        <span>1000+ {config.unit}</span>
      </div>
      
      <div className="mt-2 text-xs text-gray-500">
        {config.description}
      </div>
    </div>
  );
};

export const DataModeSelector: React.FC<DataModeSelectorProps> = ({ 
  className = '', 
  variant = 'sidebar' 
}) => {
  const { selectedDataMode } = useDataState();
  const { setSelectedDataMode } = useMapStore();

  const modes: { key: DataMode; label: string; shortLabel: string; description: string; color: string }[] = [
    {
      key: 'pesticide_total',
      label: 'Total Pesticide',
      shortLabel: 'Total',
      description: 'All pesticide applications combined',
      color: 'text-gray-700',
    },
    {
      key: 'pfas',
      label: 'PFAS',
      shortLabel: 'PFAS',
      description: 'PFAS-containing pesticides only',
      color: 'text-red-600',
    },
    {
      key: 'diquat',
      label: 'Diquat',
      shortLabel: 'Diquat',
      description: 'Diquat-containing pesticides only',
      color: 'text-blue-600',
    },
    {
      key: 'glyphosate',
      label: 'Glyphosate',
      shortLabel: 'Glyphosate',
      description: 'Glyphosate-containing pesticides only',
      color: 'text-green-600',
    },
  ];

  // Minimal top bar version inspired by London Underground Live
  if (variant === 'topbar') {
    return (
      <div className={`flex items-center space-x-1 ${className}`}>
        {modes.map((mode) => (
          <button
            key={mode.key}
            onClick={() => setSelectedDataMode(mode.key)}
            className={`px-3 py-1.5 text-sm font-medium rounded-md transition-all duration-200 ${
              selectedDataMode === mode.key
                ? 'bg-blue-600 text-white shadow-sm'
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'
            }`}
          >
            {mode.shortLabel}
          </button>
        ))}
      </div>
    );
  }

  // Original sidebar version
  return (
    <div className={`bg-white rounded-lg shadow-lg border border-gray-200 p-4 ${className}`}>
      <h3 className="text-lg font-semibold text-gray-900 mb-4">Data Mode</h3>
      
      <div className="space-y-2">
        {modes.map((mode) => (
          <button
            key={mode.key}
            onClick={() => setSelectedDataMode(mode.key)}
            className={`w-full text-left p-3 rounded-lg border transition-all duration-200 ${
              selectedDataMode === mode.key
                ? 'border-blue-500 bg-blue-50 shadow-sm'
                : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
            }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex-1">
                <div className={`font-medium ${mode.color} ${
                  selectedDataMode === mode.key ? 'text-blue-700' : ''
                }`}>
                  {mode.label}
                </div>
                <div className="text-sm text-gray-600 mt-1">
                  {mode.description}
                </div>
              </div>
              
              {selectedDataMode === mode.key && (
                <div className="ml-3">
                  <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
                </div>
              )}
            </div>
          </button>
        ))}
      </div>

      {/* Color scale legend for selected mode */}
      <ColorScaleLegend mode={selectedDataMode} />

      {/* Additional info */}
      <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
        <div className="text-sm text-yellow-800">
          <div className="font-medium mb-1">Note:</div>
          <div>
            Data visualization switches automatically between Kommune (low zoom) and H3 cell (high zoom) layers. 
            Zoom in for detailed field-level analysis.
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataModeSelector; 