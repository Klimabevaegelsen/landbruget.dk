'use client';

import React from 'react';
import { FilterState } from './types';

interface ColorLegendProps {
  filterState: FilterState;
  className?: string;
}

export function ColorLegend({ filterState, className = '' }: ColorLegendProps) {
  const getLegendData = () => {
    switch (filterState.visualizationMode) {
      case 'total_pesticide_belastning':
        return {
          title: 'Pesticidbelastning',
          unit:
            filterState.colorUnit === 'belastning'
              ? 'Belastning'
              : filterState.colorUnit === 'per_hectare'
                ? 'kg/ha'
                : 'kg',
          colors: [
            { color: '#ffffff', label: 'Lav', range: '0-20' },
            { color: '#fecaca', label: 'Lav-medium', range: '20-50' },
            { color: '#f87171', label: 'Medium', range: '50-100' },
            { color: '#dc2626', label: 'Medium-høj', range: '100-200' },
            { color: '#991b1b', label: 'Høj', range: '200+' },
          ],
        };
      case 'pfas_belastning':
        return {
          title: 'PFAS Belastning',
          unit: 'Belastning',
          colors: [
            { color: '#fecaca', label: 'Ingen PFAS', range: '0' },
            { color: '#fca5a5', label: 'Lav PFAS', range: '0-10' },
            { color: '#f87171', label: 'Medium PFAS', range: '10-50' },
            { color: '#ef4444', label: 'Høj PFAS', range: '50-100' },
            { color: '#dc2626', label: 'Meget høj PFAS', range: '100+' },
          ],
        };
      case 'organic_status':
        return {
          title: 'Økologisk Status',
          unit: '',
          colors: [
            { color: '#22c55e', label: 'Økologisk', range: '' },
            { color: '#94a3b8', label: 'Konventionel', range: '' },
          ],
        };
      case 'applications_count':
        return {
          title: 'Antal Applikationer',
          unit: 'Applikationer',
          colors: [
            { color: '#e2e8f0', label: '0', range: '0' },
            { color: '#cbd5e1', label: '1-2', range: '1-2' },
            { color: '#94a3b8', label: '3-5', range: '3-5' },
            { color: '#64748b', label: '6-10', range: '6-10' },
            { color: '#475569', label: '10+', range: '10+' },
          ],
        };
      case 'area_size':
        return {
          title: 'Markareal',
          unit: 'Hektar',
          colors: [
            { color: '#fef3c7', label: 'Meget lille', range: '0-1' },
            { color: '#fcd34d', label: 'Lille', range: '1-5' },
            { color: '#f59e0b', label: 'Medium', range: '5-20' },
            { color: '#d97706', label: 'Stor', range: '20-50' },
            { color: '#92400e', label: 'Meget stor', range: '50+' },
          ],
        };
      default:
        return {
          title: 'Pesticidbelastning',
          unit: 'Belastning',
          colors: [
            { color: '#ffffff', label: 'Lav', range: '0-20' },
            { color: '#fecaca', label: 'Lav-medium', range: '20-50' },
            { color: '#f87171', label: 'Medium', range: '50-100' },
            { color: '#dc2626', label: 'Medium-høj', range: '100-200' },
            { color: '#991b1b', label: 'Høj', range: '200+' },
          ],
        };
    }
  };

  const legendData = getLegendData();

  return (
    <div
      className={`bg-background/95 border-border max-w-xs rounded-lg border p-3 shadow-lg backdrop-blur-sm ${className}`}
      data-testid="color-legend"
    >
      <div className="mb-2">
        <h4 className="text-foreground text-sm font-semibold">
          {legendData.title}
        </h4>
        {legendData.unit && (
          <p className="text-muted-foreground text-xs">{legendData.unit}</p>
        )}
      </div>
      <div className="space-y-1.5">
        {legendData.colors.map((item, index) => (
          <div key={index} className="flex items-center space-x-2">
            <div
              className="border-border h-3 w-3 flex-shrink-0 rounded-sm border"
              style={{ backgroundColor: item.color }}
            />
            <div className="flex min-w-0 flex-1 items-center justify-between">
              <span className="text-foreground truncate text-xs font-medium">
                {item.label}
              </span>
              {item.range && (
                <span className="text-muted-foreground ml-2 flex-shrink-0 text-xs">
                  {item.range}
                </span>
              )}
            </div>
          </div>
        ))}
      </div>
      {filterState.useDecileColoring && (
        <div className="mt-2 border-t pt-2">
          <p className="text-muted-foreground text-xs">
            Decile farvning aktiveret
          </p>
        </div>
      )}
    </div>
  );
}
