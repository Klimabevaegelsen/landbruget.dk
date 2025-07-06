'use client';

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { HoverInfo } from '@/stores/map-store';

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
  const [position, setPosition] = useState<{ x: number; y: number } | null>(null);
  const [isVisible, setIsVisible] = useState(false);

  // Update position when hover info changes
  useEffect(() => {
    if (hoverInfo?.pixel) {
      const [x, y] = hoverInfo.pixel;
      setPosition({ x: x + 10, y: y - 10 });
      setIsVisible(true);
    } else {
      setIsVisible(false);
    }
  }, [hoverInfo]);

  // Format number with appropriate precision
  const formatNumber = useCallback((value: number | null | undefined, decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    
    if (value === 0) return '0';
    
    if (value < 0.01 && value > 0) {
      return value.toExponential(2);
    }
    
    return value.toLocaleString('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: decimals,
    });
  }, []);

  // Format scientific notation with units
  const formatScientific = useCallback((value: number | null | undefined, unit: string): string => {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    if (value === 0) return `0 ${unit}`;
    
    if (value < 0.01 && value > 0) {
      return `${value.toExponential(2)} ${unit}`;
    }
    
    return `${value.toLocaleString('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 2,
    })} ${unit}`;
  }, []);

  // Format percentage
  const formatPercentage = useCallback((value: number | null | undefined): string => {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    return (value * 100).toFixed(1) + '%';
  }, []);

  // Get status color class for BNBO
  const getBNBOStatusClass = useCallback((statusCode: string): string => {
    return COLOR_CLASSES.BNBO[statusCode as keyof typeof COLOR_CLASSES.BNBO] || COLOR_CLASSES.BNBO.unprotected;
  }, []);

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
          <div className="bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm">
            <div className="p-4 space-y-3">
              {/* Header - Scientific and serious */}
              <div className="relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white">
                <div className="absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"></div>
                <div className="relative">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="font-mono text-sm font-semibold tracking-wide">AGRICULTURAL ANALYSIS CELL</h3>
                      <p className="font-mono text-xs text-gray-300">
                        H3 RES-{hoverInfo.data.h3_resolution || hoverInfo.data.resolution || 7} • YEAR {hoverInfo.data.year || 2023}
                      </p>
                    </div>
                    <div className="text-right">
                      <div className="font-mono text-xs text-gray-400">AREA</div>
                      <div className="font-mono text-sm font-bold text-white">
                        {area > 0 ? `${formatNumber(area, 1)} ha` : 'N/A'}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              
              {/* Critical Chemical Data - Serious warning styling */}
              <div className="space-y-2">
                <div className="border border-red-300 bg-red-50 rounded p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className="w-2 h-2 bg-red-600 rounded-full"></div>
                      <span className="font-mono text-xs font-semibold text-red-800 tracking-wide">PFAS CONTAMINATION</span>
                    </div>
                    <div className="text-red-600 font-mono text-xs">⚠ PERSISTENT</div>
                  </div>
                  <div className="grid grid-cols-2 gap-3 text-xs">
                    <div>
                      <div className="font-mono text-red-900 font-bold text-lg">{formatNumber(pfasGrams, 2)}</div>
                      <div className="font-mono text-red-600">g total mass</div>
                    </div>
                    <div>
                      <div className="font-mono text-red-900 font-bold text-lg">{formatNumber(pfasIntensity, 2)}</div>
                      <div className="font-mono text-red-600">g/ha intensity</div>
                    </div>
                  </div>
                </div>

                <div className="border border-orange-300 bg-orange-50 rounded p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <div className="w-2 h-2 bg-orange-600 rounded-full"></div>
                      <span className="font-mono text-xs font-semibold text-orange-800 tracking-wide">PESTICIDE LOAD</span>
                    </div>
                    <div className="text-orange-600 font-mono text-xs">⚠ ACTIVE</div>
                  </div>
                  <div className="grid grid-cols-2 gap-3 text-xs">
                    <div>
                      <div className="font-mono text-orange-900 font-bold text-lg">{formatNumber(pesticideLoad, 2)}</div>
                      <div className="font-mono text-orange-600">kg total load</div>
                    </div>
                    <div>
                      <div className="font-mono text-orange-900 font-bold text-lg">{formatNumber(pesticideIntensity, 2)}</div>
                      <div className="font-mono text-orange-600">kg/ha intensity</div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Specific Chemical Analysis */}
              <div className="border border-gray-300 bg-gray-50 rounded p-3">
                <h4 className="font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide">CHEMICAL COMPOSITION ANALYSIS</h4>
                <div className="grid grid-cols-2 gap-3 text-xs">
                  <div className="border-l-2 border-green-500 pl-2">
                    <div className="font-mono text-gray-600">GLYPHOSATE</div>
                    <div className="font-mono font-bold text-gray-900">{formatScientific(glyphosateGrams, 'g')}</div>
                    <div className="font-mono text-gray-500">{formatScientific(glyphosateIntensity, 'g/ha')}</div>
                  </div>
                  <div className="border-l-2 border-yellow-500 pl-2">
                    <div className="font-mono text-gray-600">DIQUAT</div>
                    <div className="font-mono font-bold text-gray-900">{formatScientific(diquatGrams, 'g')}</div>
                    <div className="font-mono text-gray-500">{formatScientific(diquatIntensity, 'g/ha')}</div>
                  </div>
                </div>
              </div>

              {/* Agricultural Activity */}
              <div className="border border-gray-300 bg-gray-50 rounded p-3">
                <h4 className="font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide">AGRICULTURAL ACTIVITY</h4>
                <div className="grid grid-cols-3 gap-3 text-xs">
                  <div className="text-center">
                    <div className="font-mono font-bold text-gray-900 text-lg">
                      {hoverInfo.data.applications || hoverInfo.data.pesticide_application_count || 0}
                    </div>
                    <div className="font-mono text-gray-600">APPLICATIONS</div>
                  </div>
                  <div className="text-center">
                    <div className="font-mono font-bold text-gray-900 text-lg">
                      {hoverInfo.data.field_count || hoverInfo.data.unique_field_count || 0}
                    </div>
                    <div className="font-mono text-gray-600">FIELD COUNT</div>
                  </div>
                  <div className="text-center">
                    <div className="font-mono font-bold text-gray-900 text-lg">
                      {formatPercentage(hoverInfo.data.avg_field_coverage || hoverInfo.data.actual_coverage_ratio || 0)}
                    </div>
                    <div className="font-mono text-gray-600">COVERAGE</div>
                  </div>
                </div>
              </div>

              {/* Cell ID Footer */}
              <div className="border-t border-gray-300 pt-2">
                <div className="font-mono text-xs text-gray-500">
                  <span className="font-semibold">CELL ID:</span>
                  <span className="ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono">
                    {(hoverInfo.data.h3_id || hoverInfo.data.h3_cell) ? 
                      (hoverInfo.data.h3_id || hoverInfo.data.h3_cell).toString().substring(0, 16) + '...' : 
                      'UNKNOWN'
                    }
                  </span>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bnbo':
        return (
          <div className="bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm">
            <div className="p-4 space-y-3">
              {/* Header - Environmental Protection */}
              <div className="relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white">
                <div className="absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"></div>
                <div className="relative">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="font-mono text-sm font-semibold tracking-wide">ENVIRONMENTAL PROTECTION ZONE</h3>
                      <p className="font-mono text-xs text-gray-300">
                        BNBO SECTOR {hoverInfo.data.bnbo_id ? hoverInfo.data.bnbo_id.substring(0, 8).toUpperCase() : 'UNKNOWN'}
                      </p>
                    </div>
                    <div className="text-right">
                      <div className="font-mono text-xs text-gray-400">AREA</div>
                      <div className="font-mono text-sm font-bold text-white">
                        {hoverInfo.data.area_ha ? `${formatNumber(hoverInfo.data.area_ha, 1)} ha` : 'N/A'}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              
              {/* Protection Status */}
              <div className="border border-gray-300 bg-gray-50 rounded p-3">
                <div className="flex items-center space-x-3 mb-3">
                  <div 
                    className="w-4 h-4 rounded border border-gray-400"
                    style={{ backgroundColor: getBNBOStatusColor(hoverInfo.data.status_code) }}
                  ></div>
                  <div>
                    <div className="font-mono text-sm font-semibold text-gray-900">
                      {hoverInfo.data.status_description || hoverInfo.data.status_code?.toUpperCase() || 'UNKNOWN'}
                    </div>
                    <div className="font-mono text-xs text-gray-600">
                      YEAR {hoverInfo.data.year || 'N/A'}
                    </div>
                  </div>
                </div>

                <div className="font-mono text-xs text-gray-500">
                  {hoverInfo.data.status_code === 'protected' && 'Fully protected environmental area'}
                  {hoverInfo.data.status_code === 'buffer' && 'Buffer zone around protected area'}
                  {hoverInfo.data.status_code === 'agricultural' && 'Agricultural buffer zone'}
                  {hoverInfo.data.status_code === 'transition' && 'Transition zone'}
                  {hoverInfo.data.status_code === 'unprotected' && 'No environmental protection'}
                </div>
              </div>

              {/* Technical Metadata */}
              <div className="border-t border-gray-300 pt-2">
                <div className="font-mono text-xs text-gray-500">
                  <span className="font-semibold">BNBO ID:</span>
                  <span className="ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono">
                    {hoverInfo.data.bnbo_id || 'UNKNOWN'}
                  </span>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bbr':
        return (
          <div className="bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm">
            <div className="p-4 space-y-3">
              {/* Header - Building Analysis */}
              <div className="relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white">
                <div className="absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"></div>
                <div className="relative">
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="font-mono text-sm font-semibold tracking-wide">BUILDING REGISTRY ANALYSIS</h3>
                      <p className="font-mono text-xs text-gray-300">
                        BBR ID {hoverInfo.data.bbr_id ? hoverInfo.data.bbr_id.substring(0, 8).toUpperCase() : 'UNKNOWN'}
                      </p>
                    </div>
                    <div className="text-right">
                      <div className="font-mono text-xs text-gray-400">FLOOR AREA</div>
                      <div className="font-mono text-sm font-bold text-white">
                        {formatNumber(hoverInfo.data.floor_area, 0)} m²
                      </div>
                    </div>
                  </div>
                </div>
              </div>
              
              {/* Building Details */}
              <div className="border border-gray-300 bg-gray-50 rounded p-3">
                <div className="flex items-center space-x-3 mb-3">
                  <div 
                    className="w-4 h-4 rounded border border-gray-400"
                    style={{ backgroundColor: getBBRTypeColor(hoverInfo.data.building_type) }}
                  ></div>
                  <div>
                    <div className="font-mono text-sm font-semibold text-gray-900">
                      {hoverInfo.data.building_type?.toUpperCase() || 'UNKNOWN TYPE'}
                    </div>
                    <div className="font-mono text-xs text-gray-600">
                      CONSTRUCTED {hoverInfo.data.construction_year || 'UNKNOWN'}
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-3 text-xs">
                  <div>
                    <div className="font-mono text-gray-600">BUILDING CODE</div>
                    <div className="font-mono font-bold text-gray-900">{hoverInfo.data.building_code || 'N/A'}</div>
                  </div>
                  <div>
                    <div className="font-mono text-gray-600">FLOOR AREA</div>
                    <div className="font-mono font-bold text-gray-900">{formatNumber(hoverInfo.data.floor_area, 0)} m²</div>
                  </div>
                </div>
              </div>

              {/* Technical Metadata */}
              <div className="border-t border-gray-300 pt-2">
                <div className="font-mono text-xs text-gray-500">
                  <span className="font-semibold">BBR ID:</span>
                  <span className="ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono">
                    {hoverInfo.data.bbr_id || 'UNKNOWN'}
                  </span>
                </div>
              </div>
            </div>
          </div>
        );

      default:
        return (
          <div className="bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm">
            <div className="p-4 space-y-3">
              {/* Header - Unknown Data */}
              <div className="relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white">
                <div className="absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"></div>
                <div className="relative">
                  <h3 className="font-mono text-sm font-semibold tracking-wide">DATA ANALYSIS POINT</h3>
                  <p className="font-mono text-xs text-gray-300">UNKNOWN DATA TYPE</p>
                </div>
              </div>

              {/* Data Fields */}
              <div className="border border-gray-300 bg-gray-50 rounded p-3">
                <h4 className="font-mono text-xs font-semibold text-gray-900 mb-3 tracking-wide">RAW DATA FIELDS</h4>
                <div className="space-y-2 max-h-32 overflow-y-auto">
                  {Object.entries(hoverInfo.data).map(([key, value]) => {
                    if (value === null || value === undefined) return null;
                    
                    const formattedKey = key.replace(/_/g, ' ').toUpperCase();
                    
                    return (
                      <div key={key} className="flex justify-between items-center py-1 border-b border-gray-200 last:border-b-0">
                        <span className="font-mono text-gray-600 text-xs font-medium">{formattedKey}:</span>
                        <span className="font-mono text-gray-900 font-semibold text-xs">
                          {typeof value === 'number' ? formatNumber(value) : String(value)}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        );
    }
  }, [hoverInfo, formatNumber, formatScientific, formatPercentage, getBNBOStatusClass, getBBRTypeClass]);

  if (!isVisible || !position || !hoverInfo) {
    return null;
  }

  // Adjust position to prevent tooltip from going off screen
  const adjustedPosition = {
    left: Math.min(position.x, window.innerWidth - 320),
    top: Math.min(position.y, window.innerHeight - 400),
  };

  if (position.x + 320 > window.innerWidth) {
    adjustedPosition.left = position.x - 320 - 20;
  }

  if (position.y + 400 > window.innerHeight) {
    adjustedPosition.top = position.y - 400 - 20;
  }

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