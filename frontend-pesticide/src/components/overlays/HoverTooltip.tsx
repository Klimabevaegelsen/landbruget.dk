'use client';

import React, { useState, useEffect, useCallback, useMemo } from 'react';

// Define HoverInfo interface
interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: Record<string, unknown>;
  coordinate: [number, number];
  pixel: [number, number];
}

// Color classes removed as they were unused

// Color mapping functions
const getBNBOStatusColor = (statusCode: string): string => {
  const colorMap: Record<string, string> = {
    protected: '#22c55e',
    buffer: '#3b82f6',
    agricultural: '#eab308',
    transition: '#f97316',
    unprotected: '#6b7280',
  };
  return colorMap[statusCode] || '#6b7280';
};

const getBBRTypeColor = (buildingType: string): string => {
  const colorMap: Record<string, string> = {
    Residential: '#3b82f6',
    Agricultural: '#22c55e',
    Industrial: '#ef4444',
    Commercial: '#a855f7',
    Public: '#eab308',
    Other: '#6b7280',
  };
  return colorMap[buildingType] || '#6b7280';
};

interface HoverTooltipProps {
  hoverInfo?: HoverInfo | null;
}

export function HoverTooltip({ hoverInfo }: HoverTooltipProps) {
  const [isVisible, setIsVisible] = useState(false);
  const [position, setPosition] = useState<{ x: number; y: number } | null>(
    null
  );

  // Update visibility and position when hoverInfo changes
  useEffect(() => {
    if (hoverInfo) {
      setIsVisible(true);
      // Position tooltip offset from mouse to avoid covering it
      setPosition({
        x: hoverInfo.pixel[0], // Use exact cursor position
        y: hoverInfo.pixel[1], // Use exact cursor position
      });
    } else {
      setIsVisible(false);
      setPosition(null);
    }
  }, [hoverInfo]);

  // Format functions
  const formatNumber = useCallback(
    (value: number | unknown, decimals: number = 2): string => {
      const numValue = typeof value === 'number' ? value : Number(value) || 0;
      if (numValue === 0) return '0';
      if (numValue < 0.01 && numValue > 0) return '<0.01';
      return numValue.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: decimals,
      });
    },
    []
  );

  const formatPercentage = useCallback(
    (value: number | unknown): string => {
      const numValue = typeof value === 'number' ? value : Number(value) || 0;
      return `${formatNumber(numValue * 100, 1)}%`;
    },
    [formatNumber]
  );

  // Render tooltip content based on layer type
  const renderTooltipContent = useMemo(() => {
    if (!hoverInfo) return null;

    switch (hoverInfo.layer) {
      case 'h3':
        const pfasGrams = Number(
          hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams || 0
        );
        const pesticideLoad = Number(
          hoverInfo.data.pesticide_load ||
            hoverInfo.data.total_pesticide_load ||
            0
        );
        const diquatGrams = Number(hoverInfo.data.diquat_grams || 0);
        const glyphosateGrams = Number(hoverInfo.data.glyphosate_grams || 0);
        const area = Number(
          hoverInfo.data.agricultural_area_ha ||
            hoverInfo.data.h3_cell_area_ha ||
            0
        );

        // Calculate intensities
        const pfasIntensity =
          Number(hoverInfo.data.pfas_intensity) ||
          (area > 0 ? pfasGrams / area : 0);
        const pesticideIntensity =
          Number(hoverInfo.data.pesticide_intensity) ||
          (area > 0 ? pesticideLoad / area : 0);
        const diquatIntensity =
          Number(hoverInfo.data.diquat_intensity) ||
          (area > 0 ? diquatGrams / area : 0);
        const glyphosateIntensity =
          Number(hoverInfo.data.glyphosate_intensity) ||
          (area > 0 ? glyphosateGrams / area : 0);

        return (
          <div
            className="max-w-xs rounded-lg border-0 bg-white/95 shadow-2xl backdrop-blur-sm"
            style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}
          >
            <div className="space-y-3 p-4">
              {/* Header - Clean and minimal */}
              <div className="rounded-md bg-slate-900 px-3 py-2">
                <div className="text-sm font-medium text-white">
                  Agricultural Area
                </div>
                <div className="text-xs text-slate-300">
                  {area > 0
                    ? `${formatNumber(area, 1)} hectares`
                    : 'Area data unavailable'}
                </div>
              </div>

              {/* Total Pesticide Load - Primary metric */}
              <div className="rounded-md border-l-4 border-orange-400 bg-orange-50 px-3 py-2">
                <div className="mb-1 flex items-center justify-between">
                  <div className="text-sm font-medium text-orange-800">
                    Total Pesticide Load
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-base font-semibold text-orange-900">
                      {formatNumber(pesticideLoad, 2)}
                    </div>
                    <div className="text-orange-600">kg total</div>
                  </div>
                  <div>
                    <div className="text-base font-semibold text-orange-900">
                      {formatNumber(pesticideIntensity, 2)}
                    </div>
                    <div className="text-orange-600">kg per hectare</div>
                  </div>
                </div>
              </div>

              {/* PFAS - only show if there are PFAS values > 0 */}
              {pfasGrams > 0 && (
                <div className="rounded-md border-l-4 border-amber-400 bg-amber-50 px-3 py-2">
                  <div className="mb-1 flex items-center justify-between">
                    <div className="text-sm font-medium text-amber-800">
                      PFAS Active Ingredients
                    </div>
                    <div className="text-xs font-medium text-amber-600">
                      Persistent
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <div className="text-base font-semibold text-amber-900">
                        {formatNumber(pfasGrams, 2)}
                      </div>
                      <div className="text-amber-600">grams total</div>
                    </div>
                    <div>
                      <div className="text-base font-semibold text-amber-900">
                        {formatNumber(pfasIntensity, 2)}
                      </div>
                      <div className="text-amber-600">grams per hectare</div>
                    </div>
                  </div>
                </div>
              )}

              {/* Glyphosate - only show if there are glyphosate values > 0 */}
              {glyphosateGrams > 0 && (
                <div className="rounded-md border-l-4 border-green-400 bg-green-50 px-3 py-2">
                  <div className="mb-1 text-sm font-medium text-green-800">
                    Glyphosate Active Ingredients
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <div className="text-base font-semibold text-green-900">
                        {formatNumber(glyphosateGrams, 2)}
                      </div>
                      <div className="text-green-600">grams total</div>
                    </div>
                    <div>
                      <div className="text-base font-semibold text-green-900">
                        {formatNumber(glyphosateIntensity, 2)}
                      </div>
                      <div className="text-green-600">grams per hectare</div>
                    </div>
                  </div>
                </div>
              )}

              {/* Diquat - only show if there are diquat values > 0 */}
              {diquatGrams > 0 && (
                <div className="rounded-md border-l-4 border-purple-400 bg-purple-50 px-3 py-2">
                  <div className="mb-1 text-sm font-medium text-purple-800">
                    Diquat Active Ingredients
                  </div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>
                      <div className="text-base font-semibold text-purple-900">
                        {formatNumber(diquatGrams, 2)}
                      </div>
                      <div className="text-purple-600">grams total</div>
                    </div>
                    <div>
                      <div className="text-base font-semibold text-purple-900">
                        {formatNumber(diquatIntensity, 2)}
                      </div>
                      <div className="text-purple-600">grams per hectare</div>
                    </div>
                  </div>
                </div>
              )}

              {/* Agricultural Activity - Minimal stats */}
              <div className="rounded-md bg-slate-50 px-3 py-2">
                <div className="mb-2 text-sm font-medium text-slate-700">
                  Activity
                </div>
                <div className="grid grid-cols-3 gap-2 text-xs">
                  <div className="text-center">
                    <div className="text-sm font-semibold text-slate-900">
                      {hoverInfo.data.applications ||
                        hoverInfo.data.pesticide_application_count ||
                        0}
                    </div>
                    <div className="text-slate-600">Applications</div>
                  </div>
                  <div className="text-center">
                    <div className="text-sm font-semibold text-slate-900">
                      {hoverInfo.data.field_count ||
                        hoverInfo.data.unique_field_count ||
                        0}
                    </div>
                    <div className="text-slate-600">Fields</div>
                  </div>
                  <div className="text-center">
                    <div className="text-sm font-semibold text-slate-900">
                      {formatPercentage(
                        hoverInfo.data.avg_field_coverage ||
                          hoverInfo.data.actual_coverage_ratio ||
                          0
                      )}
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
          <div
            className="max-w-xs rounded-lg border-0 bg-white/95 shadow-2xl backdrop-blur-sm"
            style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}
          >
            <div className="space-y-3 p-4">
              {/* Header - Environmental Protection */}
              <div className="rounded-md bg-slate-900 px-3 py-2">
                <div className="text-sm font-medium text-white">
                  Environmental Protection Zone
                </div>
                <div className="text-xs text-slate-300">
                  {hoverInfo.data.area_ha
                    ? `${formatNumber(hoverInfo.data.area_ha, 1)} hectares`
                    : 'Area data unavailable'}
                </div>
              </div>

              {/* Protection Status */}
              <div className="rounded-md bg-slate-50 px-3 py-2">
                <div className="mb-2 flex items-center space-x-2">
                  <div
                    className="h-3 w-3 rounded border"
                    style={{
                      backgroundColor: getBNBOStatusColor(
                        hoverInfo.data.status_code
                      ),
                    }}
                  ></div>
                  <div className="text-sm font-medium text-slate-700">
                    {hoverInfo.data.status_description ||
                      hoverInfo.data.status_code?.toUpperCase() ||
                      'Unknown'}
                  </div>
                </div>
                <div className="text-xs text-slate-600">
                  {hoverInfo.data.status_code === 'protected' &&
                    'Fully protected environmental area'}
                  {hoverInfo.data.status_code === 'buffer' &&
                    'Buffer zone around protected area'}
                  {hoverInfo.data.status_code === 'agricultural' &&
                    'Agricultural buffer zone'}
                  {hoverInfo.data.status_code === 'transition' &&
                    'Transition zone'}
                  {hoverInfo.data.status_code === 'unprotected' &&
                    'No environmental protection'}
                </div>
              </div>
            </div>
          </div>
        );

      case 'bbr':
        return (
          <div
            className="max-w-xs rounded-lg border-0 bg-white/95 shadow-2xl backdrop-blur-sm"
            style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}
          >
            <div className="space-y-3 p-4">
              {/* Header - Building */}
              <div className="rounded-md bg-slate-900 px-3 py-2">
                <div className="text-sm font-medium text-white">Building</div>
                <div className="text-xs text-slate-300">
                  {formatNumber(hoverInfo.data.floor_area, 0)} m² floor area
                </div>
              </div>

              {/* Building Details */}
              <div className="rounded-md bg-slate-50 px-3 py-2">
                <div className="mb-2 flex items-center space-x-2">
                  <div
                    className="h-3 w-3 rounded border"
                    style={{
                      backgroundColor: getBBRTypeColor(
                        hoverInfo.data.building_type
                      ),
                    }}
                  ></div>
                  <div className="text-sm font-medium text-slate-700">
                    {hoverInfo.data.building_type || 'Unknown Type'}
                  </div>
                </div>
                <div className="text-xs text-slate-600">
                  Built {hoverInfo.data.construction_year || 'Unknown'}
                </div>
              </div>
            </div>
          </div>
        );

      default:
        return (
          <div
            className="max-w-xs rounded-lg border-0 bg-white/95 shadow-2xl backdrop-blur-sm"
            style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}
          >
            <div className="p-4">
              <div className="rounded-md bg-slate-900 px-3 py-2">
                <div className="text-sm font-medium text-white">Data Point</div>
                <div className="text-xs text-slate-300">Unknown data type</div>
              </div>
            </div>
          </div>
        );
    }
  }, [hoverInfo, formatNumber, formatPercentage]);

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

  const adjustedPosition = {
    left: position.x + tooltipDistance,
    top: position.y + tooltipDistance,
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
  adjustedPosition.left = Math.max(
    padding,
    Math.min(adjustedPosition.left, window.innerWidth - tooltipWidth - padding)
  );

  adjustedPosition.top = Math.max(
    padding,
    Math.min(adjustedPosition.top, window.innerHeight - tooltipHeight - padding)
  );

  return (
    <div
      className="pointer-events-none fixed z-50"
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

  const handleHover = useCallback((info: Record<string, unknown>) => {
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
        pixel: info.pixel,
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
    setHoverInfo,
  };
}
