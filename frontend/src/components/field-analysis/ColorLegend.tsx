'use client';

import React from 'react';
import { FilterState } from './types';
import { COLOR_SCHEMES, getDecileBreakpoints } from './colorUtils';

interface ColorLegendProps {
  filterState: FilterState;
  className?: string;
}

export function ColorLegend({ filterState, className = '' }: ColorLegendProps) {
  const getLegendData = () => {
    // Get breakpoints for current mode and color unit
    const breakpoints = filterState.useDecileColoring
      ? getDecileBreakpoints(filterState.visualizationMode, filterState.colorUnit)
      : null;

    switch (filterState.visualizationMode) {
      case 'total_pesticide_belastning':
        const pesticideColors = COLOR_SCHEMES.pesticide.colors;
        return {
          title: 'Pesticidbelastning',
          unit:
            filterState.colorUnit === 'belastning'
              ? 'Belastning'
              : filterState.colorUnit === 'per_hectare'
                ? 'kg/ha'
                : 'kg',
          colors: filterState.useDecileColoring && breakpoints
            ? [
                { color: pesticideColors[0], label: 'Decil 1', range: `0-${breakpoints[0].toFixed(1)}` },
                { color: pesticideColors[2], label: 'Decil 3', range: `${breakpoints[1].toFixed(1)}-${breakpoints[2].toFixed(1)}` },
                { color: pesticideColors[4], label: 'Decil 5', range: `${breakpoints[3].toFixed(1)}-${breakpoints[4].toFixed(1)}` },
                { color: pesticideColors[6], label: 'Decil 7', range: `${breakpoints[5].toFixed(1)}-${breakpoints[6].toFixed(1)}` },
                { color: pesticideColors[9], label: 'Decil 10', range: `${breakpoints[8].toFixed(1)}+` },
              ]
            : [
                { color: pesticideColors[0], label: 'Meget lav', range: '0-1' },
                { color: pesticideColors[2], label: 'Lav', range: '1-10' },
                { color: pesticideColors[4], label: 'Medium', range: '10-50' },
                { color: pesticideColors[6], label: 'Høj', range: '50-100' },
                { color: pesticideColors[9], label: 'Meget høj', range: '100+' },
              ],
        };
      case 'pfas_belastning':
        const pfasColors = COLOR_SCHEMES.pfas.colors;
        return {
          title: 'PFAS Belastning',
          unit: 'Belastning',
          colors: filterState.useDecileColoring && breakpoints
            ? [
                { color: pfasColors[0], label: 'Decil 1', range: `0-${breakpoints[0].toFixed(1)}` },
                { color: pfasColors[2], label: 'Decil 3', range: `${breakpoints[1].toFixed(1)}-${breakpoints[2].toFixed(1)}` },
                { color: pfasColors[4], label: 'Decil 5', range: `${breakpoints[3].toFixed(1)}-${breakpoints[4].toFixed(1)}` },
                { color: pfasColors[6], label: 'Decil 7', range: `${breakpoints[5].toFixed(1)}-${breakpoints[6].toFixed(1)}` },
                { color: pfasColors[9], label: 'Decil 10', range: `${breakpoints[8].toFixed(1)}+` },
              ]
            : [
                { color: pfasColors[0], label: 'Meget lav', range: '0-1' },
                { color: pfasColors[2], label: 'Lav', range: '1-10' },
                { color: pfasColors[4], label: 'Medium', range: '10-50' },
                { color: pfasColors[6], label: 'Høj', range: '50-100' },
                { color: pfasColors[9], label: 'Meget høj', range: '100+' },
              ],
        };
      case 'organic_status':
        const organicColors = COLOR_SCHEMES.organic.colors;
        return {
          title: 'Økologisk Status',
          unit: '',
          colors: [
            { color: organicColors[1], label: 'Økologisk', range: '' },
            { color: organicColors[0], label: 'Konventionel', range: '' },
          ],
        };
      case 'applications_count':
        const appColors = COLOR_SCHEMES.applications.colors;
        return {
          title: 'Antal Applikationer',
          unit: 'Applikationer',
          colors: filterState.useDecileColoring && breakpoints
            ? [
                { color: appColors[0], label: 'Decil 1', range: `0-${Math.round(breakpoints[0])}` },
                { color: appColors[2], label: 'Decil 3', range: `${Math.round(breakpoints[1])}-${Math.round(breakpoints[2])}` },
                { color: appColors[4], label: 'Decil 5', range: `${Math.round(breakpoints[3])}-${Math.round(breakpoints[4])}` },
                { color: appColors[6], label: 'Decil 7', range: `${Math.round(breakpoints[5])}-${Math.round(breakpoints[6])}` },
                { color: appColors[9], label: 'Decil 10', range: `${Math.round(breakpoints[8])}+` },
              ]
            : [
                { color: appColors[0], label: '0-3', range: '0-3' },
                { color: appColors[2], label: '4-6', range: '4-6' },
                { color: appColors[4], label: '7-9', range: '7-9' },
                { color: appColors[6], label: '10-12', range: '10-12' },
                { color: appColors[9], label: '13+', range: '13+' },
              ],
        };
      case 'area_size':
        const generalColors = COLOR_SCHEMES.general.colors;
        return {
          title: 'Markareal',
          unit: 'Hektar',
          colors: filterState.useDecileColoring && breakpoints
            ? [
                { color: generalColors[0], label: 'Decil 1', range: `0-${breakpoints[0].toFixed(1)}` },
                { color: generalColors[2], label: 'Decil 3', range: `${breakpoints[1].toFixed(1)}-${breakpoints[2].toFixed(1)}` },
                { color: generalColors[4], label: 'Decil 5', range: `${breakpoints[3].toFixed(1)}-${breakpoints[4].toFixed(1)}` },
                { color: generalColors[6], label: 'Decil 7', range: `${breakpoints[5].toFixed(1)}-${breakpoints[6].toFixed(1)}` },
                { color: generalColors[9], label: 'Decil 10', range: `${breakpoints[8].toFixed(1)}+` },
              ]
            : [
                { color: generalColors[0], label: 'Meget lille', range: '0-1' },
                { color: generalColors[2], label: 'Lille', range: '1-5' },
                { color: generalColors[4], label: 'Medium', range: '5-20' },
                { color: generalColors[6], label: 'Stor', range: '20-50' },
                { color: generalColors[9], label: 'Meget stor', range: '50+' },
              ],
        };
      default:
        const defaultColors = COLOR_SCHEMES.pesticide.colors;
        return {
          title: 'Pesticidbelastning',
          unit: 'Belastning',
          colors: [
            { color: defaultColors[0], label: 'Meget lav', range: '0-1' },
            { color: defaultColors[2], label: 'Lav', range: '1-10' },
            { color: defaultColors[4], label: 'Medium', range: '10-50' },
            { color: defaultColors[6], label: 'Høj', range: '50-100' },
            { color: defaultColors[9], label: 'Meget høj', range: '100+' },
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
