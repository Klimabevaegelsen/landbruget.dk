'use client';

import { useState, useEffect, useCallback, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getBNBOStatusColor, getBBRTypeColor, COLOR_CLASSES } from '@/lib/color-schemes';

interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: any;
  coordinate: [number, number];
  pixel: [number, number];
}

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
    
    if (Math.abs(value) >= 1000) {
      return (value / 1000).toFixed(1) + 'K';
    }
    
    return value.toFixed(decimals);
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
        return (
          <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200 max-w-sm">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-bold text-lg text-gray-900">H3 Hexagon</h3>
              <div className="w-4 h-4 bg-gradient-to-r from-blue-400 to-blue-600 rounded"></div>
            </div>
            
            <div className="space-y-2 text-sm">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <span className="text-gray-600">H3 ID:</span>
                  <div className="font-mono text-xs text-gray-800 break-all">
                    {hoverInfo.data.h3_id}
                  </div>
                </div>
                <div>
                  <span className="text-gray-600">Year:</span>
                  <div className="font-semibold text-gray-900">
                    {hoverInfo.data.year}
                  </div>
                </div>
              </div>

              <div className="border-t pt-2 space-y-1">
                <div className="flex justify-between">
                  <span className="text-gray-600">Pesticide Load:</span>
                  <span className="font-semibold text-blue-600">
                    {formatNumber(hoverInfo.data.total_pesticide_load)} kg/ha
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">PFAS Mass:</span>
                  <span className="font-semibold text-red-600">
                    {formatNumber(hoverInfo.data.total_pfas_grams)} g
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Agricultural Area:</span>
                  <span className="font-semibold">
                    {formatNumber(hoverInfo.data.agricultural_area_ha)} ha
                  </span>
                </div>
              </div>

              <div className="border-t pt-2 space-y-1">
                <div className="flex justify-between">
                  <span className="text-gray-600">Field Count:</span>
                  <span className="font-semibold">{hoverInfo.data.field_count || 0}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Applications:</span>
                  <span className="font-semibold">{hoverInfo.data.pesticide_application_count || 0}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Coverage:</span>
                  <span className="font-semibold">
                    {formatPercentage(hoverInfo.data.avg_field_coverage)}
                  </span>
                </div>
              </div>

              {hoverInfo.data.h3_resolution && (
                <div className="border-t pt-2">
                  <div className="text-xs text-gray-500">
                    H3 Resolution: {hoverInfo.data.h3_resolution}
                  </div>
                </div>
              )}
            </div>
          </div>
        );

      case 'bnbo':
        return (
          <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200 max-w-sm">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-bold text-lg text-gray-900">BNBO Protected Area</h3>
              <div 
                className="w-4 h-4 rounded"
                style={{ backgroundColor: getBNBOStatusColor(hoverInfo.data.status_code) }}
              ></div>
            </div>
            
            <div className="space-y-2 text-sm">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <span className="text-gray-600">BNBO ID:</span>
                  <div className="font-mono text-xs text-gray-800">
                    {hoverInfo.data.bnbo_id}
                  </div>
                </div>
                <div>
                  <span className="text-gray-600">Year:</span>
                  <div className="font-semibold text-gray-900">
                    {hoverInfo.data.year}
                  </div>
                </div>
              </div>

              <div className="border-t pt-2">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-gray-600">Status:</span>
                  <span className={`px-2 py-1 rounded text-xs font-medium ${getBNBOStatusClass(hoverInfo.data.status_code)}`}>
                    {hoverInfo.data.status_description || hoverInfo.data.status_code}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Area:</span>
                  <span className="font-semibold">
                    {formatNumber(hoverInfo.data.area_ha)} ha
                  </span>
                </div>
              </div>

              <div className="border-t pt-2">
                <div className="text-xs text-gray-500">
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
          <div className="bg-white p-4 rounded-lg shadow-lg border border-gray-200 max-w-sm">
            <div className="flex items-center justify-between mb-3">
              <h3 className="font-bold text-lg text-gray-900">Building</h3>
              <div 
                className="w-4 h-4 rounded-full"
                style={{ backgroundColor: getBBRTypeColor(hoverInfo.data.building_type) }}
              ></div>
            </div>
            
            <div className="space-y-2 text-sm">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <span className="text-gray-600">BBR ID:</span>
                  <div className="font-mono text-xs text-gray-800">
                    {hoverInfo.data.bbr_id}
                  </div>
                </div>
                <div>
                  <span className="text-gray-600">Building Code:</span>
                  <div className="font-mono text-xs text-gray-800">
                    {hoverInfo.data.building_code || 'N/A'}
                  </div>
                </div>
              </div>

              <div className="border-t pt-2">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-gray-600">Type:</span>
                  <span className={`px-2 py-1 rounded text-xs font-medium ${getBBRTypeClass(hoverInfo.data.building_type)}`}>
                    {hoverInfo.data.building_type || 'Unknown'}
                  </span>
                </div>
                
                <div className="space-y-1">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Construction Year:</span>
                    <span className="font-semibold">
                      {hoverInfo.data.construction_year || 'Unknown'}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Floor Area:</span>
                    <span className="font-semibold">
                      {formatNumber(hoverInfo.data.floor_area, 0)} m²
                    </span>
                  </div>
                </div>
              </div>

              {hoverInfo.data.address && (
                <div className="border-t pt-2">
                  <span className="text-gray-600">Address:</span>
                  <div className="text-xs text-gray-800 mt-1">
                    {hoverInfo.data.address}
                  </div>
                </div>
              )}
            </div>
          </div>
        );

      default:
        return null;
    }
  }, [hoverInfo, formatNumber, formatPercentage, getBNBOStatusClass, getBBRTypeClass]);

  if (!isVisible || !position || !hoverInfo) {
    return null;
  }

  return (
    <AnimatePresence>
      <motion.div
        className="absolute pointer-events-none z-50"
        style={{
          left: position.x,
          top: position.y,
          transform: 'translate(-50%, -100%)'
        }}
        initial={{ opacity: 0, scale: 0.9, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.9, y: 10 }}
        transition={{ duration: 0.15, ease: 'easeOut' }}
      >
        {renderTooltipContent}
        
        {/* Tooltip Arrow */}
        <div className="absolute top-full left-1/2 transform -translate-x-1/2">
          <div className="w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-white"></div>
          <div className="absolute top-0 left-1/2 transform -translate-x-1/2 -translate-y-px">
            <div className="w-0 h-0 border-l-3 border-r-3 border-t-3 border-transparent border-t-gray-200"></div>
          </div>
        </div>
      </motion.div>
    </AnimatePresence>
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