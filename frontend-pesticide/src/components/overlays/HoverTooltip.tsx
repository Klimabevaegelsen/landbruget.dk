'use client';

import React, { useState, useEffect, useCallback, useMemo } from 'react';

// Define HoverInfo interface
interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: any;
  coordinate: [number, number];
  pixel: [number, number];
}

// Color classes for different data types
const COLOR_CLASSES = {
  BNBO: {
    protected: 'bg-green-100 text-green-800 border-green-300',
    buffer: 'bg-blue-100 text-blue-800 border-blue-300',
    agricultural: 'bg-yellow-100 text-yellow-800 border-yellow-300',
    transition: 'bg-orange-100 text-orange-800 border-orange-300',
    unprotected: 'bg-gray-100 text-gray-800 border-gray-300',
  },
  BBR: {
    Residential: 'bg-blue-100 text-blue-800 border-blue-300',
    Agricultural: 'bg-green-100 text-green-800 border-green-300',
    Industrial: 'bg-red-100 text-red-800 border-red-300',
    Commercial: 'bg-purple-100 text-purple-800 border-purple-300',
    Public: 'bg-yellow-100 text-yellow-800 border-yellow-300',
    Other: 'bg-gray-100 text-gray-800 border-gray-300',
  }
};

// Color mapping functions
const getBNBOStatusColor = (statusCode: string): string => {
  const colorMap: Record<string, string> = {
    'protected': '#22c55e',
    'buffer': '#3b82f6',
    'agricultural': '#eab308',
    'transition': '#f97316',
    'unprotected': '#6b7280'
  };
  return colorMap[statusCode] || '#6b7280';
};

const getBBRTypeColor = (buildingType: string): string => {
  const colorMap: Record<string, string> = {
    'Residential': '#3b82f6',
    'Agricultural': '#22c55e',
    'Industrial': '#ef4444',
    'Commercial': '#a855f7',
    'Public': '#eab308',
    'Other': '#6b7280'
  };
  return colorMap[buildingType] || '#6b7280';
};

interface HoverTooltipProps {
  hoverInfo?: HoverInfo | null;
  onHoverChange?: (info: HoverInfo | null) => void;
}

export function HoverTooltip({ hoverInfo, onHoverChange }: HoverTooltipProps) {
  const [isVisible, setIsVisible] = useState(false);
  const [position, setPosition] = useState<{ x: number; y: number } | null>(null);

  // Update visibility and position when hoverInfo changes
  useEffect(() => {
    if (hoverInfo) {
      setIsVisible(true);
      // Position tooltip offset from mouse to avoid covering it
      setPosition({ 
        x: hoverInfo.pixel[0], // Use exact cursor position
        y: hoverInfo.pixel[1]  // Use exact cursor position
      });
    } else {
      setIsVisible(false);
      setPosition(null);
    }
  }, [hoverInfo]);

  // Format functions
  const formatNumber = useCallback((value: number | undefined, decimals: number = 2): string => {
    if (value === undefined || value === null) return '0';
    if (value === 0) return '0';
    if (value < 0.01 && value > 0) return '<0.01';
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: decimals,
    });
  }, []);

  const formatScientific = useCallback((value: number | undefined, unit: string): string => {
    if (value === undefined || value === null) return `0 ${unit}`;
    if (value === 0) return `0 ${unit}`;
    if (value < 0.01 && value > 0) return `<0.01 ${unit}`;
    return `${formatNumber(value)} ${unit}`;
  }, [formatNumber]);

  const formatPercentage = useCallback((value: number | undefined): string => {
    if (value === undefined || value === null) return '0%';
    return `${formatNumber(value * 100, 1)}%`;
  }, [formatNumber]);

  // Get building type color class for BBR
  const getBBRTypeClass = useCallback((buildingType: string): string => {
    return COLOR_CLASSES.BBR[buildingType as keyof typeof COLOR_CLASSES.BBR] || COLOR_CLASSES.BBR.Other;
  }, []);

  // Render tooltip content based on layer type
  const renderTooltipContent = useMemo(() => {
    if (!hoverInfo) return null;

    switch (hoverInfo.layer) {
      case 'h3':
        const pfasGrams = hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams || 0;
        const pesticideLoad = hoverInfo.data.pesticide_load || hoverInfo.data.total_pesticide_load || 0;
        const diquatGrams = hoverInfo.data.diquat_grams || 0;
        const glyphosateGrams = hoverInfo.data.glyphosate_grams || 0;
        const area = hoverInfo.data.agricultural_area_ha || hoverInfo.data.h3_cell_area_ha || 0;
        
        // Calculate intensities
        const pfasIntensity = hoverInfo.data.pfas_intensity || (area > 0 ? pfasGrams / area : 0);
        const pesticideIntensity = hoverInfo.data.pesticide_intensity || (area > 0 ? pesticideLoad / area : 0);
        const diquatIntensity = hoverInfo.data.diquat_intensity || (area > 0 ? diquatGrams / area : 0);
        const glyphosateIntensity = hoverInfo.data.glyphosate_intensity || (area > 0 ? glyphosateGrams / area : 0);

        return (
          <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
            <div className="p-4 space-y-3">
              {/* Header - Clean and minimal */}
              <div className="bg-slate-900 rounded-md px-3 py-2">
                <div className="text-white text-sm font-medium">Agricultural Area</div>
                <div className="text-slate-300 text-xs">
                  {area > 0 ? `${formatNumber(area, 1)} hectares` : 'Area data unavailable'}
                </div>
              </div>
              
              {/* Total Pesticide Load - Primary metric */}
              <div className="bg-orange-50 rounded-md px-3 py-2 border-l-4 border-orange-400">
                <div className="flex items-center justify-between mb-1">
                  <div className="text-orange-800 text-sm font-medium">Total Pesticide Load</div>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-orange-900 font-semibold text-base">{formatNumber(pesticideLoad, 2)}</div>
                    <div className="text-orange-600">kg total</div>
                  </div>
                  <div>
                    <div className="text-orange-900 font-semibold text-base">{formatNumber(pesticideIntensity, 2)}</div>
                    <div className="text-orange-600">kg per hectare</div>
                  </div>
                </div>
              </div>

              {/* PFAS - Clean warning design */}
              <div className="bg-red-50 rounded-md px-3 py-2 border-l-4 border-red-400">
                <div className="flex items-center justify-between mb-1">
                  <div className="text-red-800 text-sm font-medium">PFAS Active Ingredients</div>
                  <div className="text-red-600 text-xs font-medium">Persistent</div>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-red-900 font-semibold text-base">{formatNumber(pfasGrams, 2)}</div>
                    <div className="text-red-600">grams total</div>
                  </div>
                  <div>
                    <div className="text-red-900 font-semibold text-base">{formatNumber(pfasIntensity, 2)}</div>
                    <div className="text-red-600">grams per hectare</div>
                  </div>
                </div>
              </div>

              {/* Glyphosate - Clean design */}
              <div className="bg-green-50 rounded-md px-3 py-2 border-l-4 border-green-400">
                <div className="text-green-800 text-sm font-medium mb-1">Glyphosate Active Ingredients</div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-green-900 font-semibold text-base">{formatNumber(glyphosateGrams, 2)}</div>
                    <div className="text-green-600">grams total</div>
                  </div>
                  <div>
                    <div className="text-green-900 font-semibold text-base">{formatNumber(glyphosateIntensity, 2)}</div>
                    <div className="text-green-600">grams per hectare</div>
                  </div>
                </div>
              </div>

              {/* Diquat - Clean design */}
              <div className="bg-amber-50 rounded-md px-3 py-2 border-l-4 border-amber-400">
                <div className="text-amber-800 text-sm font-medium mb-1">Diquat Active Ingredients</div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-amber-900 font-semibold text-base">{formatNumber(diquatGrams, 2)}</div>
                    <div className="text-amber-600">grams total</div>
                  </div>
                  <div>
                    <div className="text-amber-900 font-semibold text-base">{formatNumber(diquatIntensity, 2)}</div>
                    <div className="text-amber-600">grams per hectare</div>
                  </div>
                </div>
              </div>

              {/* Agricultural Activity - Minimal stats */}
              <div className="bg-slate-50 rounded-md px-3 py-2">
                <div className="text-slate-700 text-sm font-medium mb-2">Activity</div>
                <div className="grid grid-cols-3 gap-2 text-xs">
                  <div className="text-center">
                    <div className="font-semibold text-slate-900 text-sm">
                      {hoverInfo.data.applications || hoverInfo.data.pesticide_application_count || 0}
                    </div>
                    <div className="text-slate-600">Applications</div>
                  </div>
                  <div className="text-center">
                    <div className="font-semibold text-slate-900 text-sm">
                      {hoverInfo.data.field_count || hoverInfo.data.unique_field_count || 0}
                    </div>
                    <div className="text-slate-600">Fields</div>
                  </div>
                  <div className="text-center">
                    <div className="font-semibold text-slate-900 text-sm">
                      {formatPercentage(hoverInfo.data.avg_field_coverage || hoverInfo.data.actual_coverage_ratio || 0)}
                    </div>
                    <div className="text-slate-600">Coverage</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bnbo':
        return (
          <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
            <div className="p-4 space-y-3">
              {/* Header - Environmental Protection */}
              <div className="bg-slate-900 rounded-md px-3 py-2">
                <div className="text-white text-sm font-medium">Environmental Protection Zone</div>
                <div className="text-slate-300 text-xs">
                  {hoverInfo.data.area_ha ? `${formatNumber(hoverInfo.data.area_ha, 1)} hectares` : 'Area data unavailable'}
                </div>
              </div>
              
              {/* Protection Status */}
              <div className="bg-slate-50 rounded-md px-3 py-2">
                <div className="flex items-center space-x-2 mb-2">
                  <div 
                    className="w-3 h-3 rounded border"
                    style={{ backgroundColor: getBNBOStatusColor(hoverInfo.data.status_code) }}
                  ></div>
                  <div className="text-slate-700 text-sm font-medium">
                    {hoverInfo.data.status_description || hoverInfo.data.status_code?.toUpperCase() || 'Unknown'}
                  </div>
                </div>
                <div className="text-slate-600 text-xs">
                  {hoverInfo.data.status_code === 'protected' && 'Fully protected environmental area'}
                  {hoverInfo.data.status_code === 'buffer' && 'Buffer zone around protected area'}
                  {hoverInfo.data.status_code === 'agricultural' && 'Agricultural buffer zone'}
                  {hoverInfo.data.status_code === 'transition' && 'Transition zone'}
                  {hoverInfo.data.status_code === 'unprotected' && 'No environmental protection'}
                </div>
              </div>
            </div>
          </div>
        );

      case 'bbr':
        return (
          <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
            <div className="p-4 space-y-3">
              {/* Header - Building */}
              <div className="bg-slate-900 rounded-md px-3 py-2">
                <div className="text-white text-sm font-medium">Building</div>
                <div className="text-slate-300 text-xs">
                  {formatNumber(hoverInfo.data.floor_area, 0)} m² floor area
                </div>
              </div>
              
              {/* Building Details */}
              <div className="bg-slate-50 rounded-md px-3 py-2">
                <div className="flex items-center space-x-2 mb-2">
                  <div 
                    className="w-3 h-3 rounded border"
                    style={{ backgroundColor: getBBRTypeColor(hoverInfo.data.building_type) }}
                  ></div>
                  <div className="text-slate-700 text-sm font-medium">
                    {hoverInfo.data.building_type || 'Unknown Type'}
                  </div>
                </div>
                <div className="text-slate-600 text-xs">
                  Built {hoverInfo.data.construction_year || 'Unknown'}
                </div>
              </div>
            </div>
          </div>
        );

      default:
        return (
          <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
            <div className="p-4">
              <div className="bg-slate-900 rounded-md px-3 py-2">
                <div className="text-white text-sm font-medium">Data Point</div>
                <div className="text-slate-300 text-xs">Unknown data type</div>
              </div>
            </div>
          </div>
        );
    }
  }, [hoverInfo, formatNumber, formatScientific, formatPercentage, getBNBOStatusColor, getBBRTypeClass]);

  if (!isVisible || !position || !hoverInfo) {
    return null;
  }

  // Smart positioning to keep the hovered cell always visible
  const tooltipWidth = 300;
  const tooltipHeight = 350;
  const tooltipDistance = 150; // Even larger distance from cursor to tooltip - creates clear gap
  const padding = 10; // Screen edge padding
  
  // Determine the best position based on available space
  const spaceRight = window.innerWidth - position.x;
  const spaceLeft = position.x;
  const spaceBelow = window.innerHeight - position.y;
  const spaceAbove = position.y;
  
  let adjustedPosition = {
    left: position.x + tooltipDistance,
    top: position.y + tooltipDistance
  };

  // Choose horizontal position - keep tooltip far from cursor
  if (spaceRight >= tooltipWidth + tooltipDistance + padding) {
    // Enough space on the right - position far to the right
    adjustedPosition.left = position.x + tooltipDistance;
  } else if (spaceLeft >= tooltipWidth + tooltipDistance + padding) {
    // Not enough space on right, position far to the left
    adjustedPosition.left = position.x - tooltipWidth - tooltipDistance;
  } else {
    // Not enough horizontal space, use the side with more room but keep distance
    if (spaceRight > spaceLeft) {
      adjustedPosition.left = position.x + tooltipDistance;
    } else {
      adjustedPosition.left = position.x - tooltipWidth - tooltipDistance;
    }
  }

  // Choose vertical position - keep tooltip far from cursor
  if (spaceBelow >= tooltipHeight + tooltipDistance + padding) {
    // Enough space below - position far below
    adjustedPosition.top = position.y + tooltipDistance;
  } else if (spaceAbove >= tooltipHeight + tooltipDistance + padding) {
    // Not enough space below, position far above
    adjustedPosition.top = position.y - tooltipHeight - tooltipDistance;
  } else {
    // Not enough vertical space, use the side with more room but keep distance
    if (spaceBelow > spaceAbove) {
      adjustedPosition.top = position.y + tooltipDistance;
    } else {
      adjustedPosition.top = position.y - tooltipHeight - tooltipDistance;
    }
  }

  // Final bounds checking
  adjustedPosition.left = Math.max(padding, Math.min(
    adjustedPosition.left,
    window.innerWidth - tooltipWidth - padding
  ));
  
  adjustedPosition.top = Math.max(padding, Math.min(
    adjustedPosition.top,
    window.innerHeight - tooltipHeight - padding
  ));

  return (
    <div
      className="fixed z-50 pointer-events-none"
      style={{
        left: adjustedPosition.left,
        top: adjustedPosition.top,
      }}
    >
      {renderTooltipContent}
    </div>
  );
}

// Hook for managing hover state
export function useHoverTooltip() {
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);

  const handleHover = useCallback((info: any) => {
    if (info?.object && info?.coordinate && info?.pixel) {
      // Determine layer type based on data structure
      let layer: 'h3' | 'bnbo' | 'bbr' = 'h3';
      
      if (info.object.bnbo_id) {
        layer = 'bnbo';
      } else if (info.object.bbr_id) {
        layer = 'bbr';
      } else if (info.object.h3_id) {
        layer = 'h3';
      }

      setHoverInfo({
        layer,
        data: info.object,
        coordinate: info.coordinate,
        pixel: info.pixel
      });
    } else {
      setHoverInfo(null);
    }
  }, []);

  const clearHover = useCallback(() => {
    setHoverInfo(null);
  }, []);

  return {
    hoverInfo,
    handleHover,
    clearHover,
    setHoverInfo
  };
} 