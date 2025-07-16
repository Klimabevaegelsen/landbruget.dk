'use client';

import React, { useMemo, useCallback, useState, useEffect } from 'react';
import { X, ChevronUp, ChevronDown } from 'lucide-react';

// Define HoverInfo interface
interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: Record<string, unknown>;
  coordinate: [number, number];
  pixel: [number, number];
}

interface MobileBottomPanelProps {
  hoverInfo?: HoverInfo | null;
  onClose?: () => void;
  isVisible?: boolean;
}

export function MobileBottomPanel({ hoverInfo, onClose, isVisible = false }: MobileBottomPanelProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [dragOffset, setDragOffset] = useState(0);
  const [isDragging, setIsDragging] = useState(false);

  // Reset expansion state when panel visibility changes
  useEffect(() => {
    if (!isVisible) {
      setIsExpanded(false);
      setDragOffset(0);
    }
  }, [isVisible]);

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

  // Handle drag gestures for panel expansion
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    const touch = e.touches[0];
    setIsDragging(true);
    (e.target as any)._startY = touch.clientY;
  }, []);

  const handleTouchMove = useCallback((e: React.TouchEvent) => {
    if (!isDragging) return;
    
    const touch = e.touches[0];
    const startY = (e.target as any)._startY || touch.clientY;
    const deltaY = startY - touch.clientY;
    
    // Only allow upward drag to expand
    if (deltaY > 0) {
      setDragOffset(Math.min(deltaY, 200));
    }
  }, [isDragging]);

  const handleTouchEnd = useCallback((e: React.TouchEvent) => {
    if (!isDragging) return;
    
    setIsDragging(false);
    
    // Expand if dragged up significantly
    if (dragOffset > 50) {
      setIsExpanded(true);
    }
    
    setDragOffset(0);
  }, [isDragging, dragOffset]);

  // Render panel content based on layer type
  const renderPanelContent = useMemo(() => {
    if (!hoverInfo) return null;

    switch (hoverInfo.layer) {
      case 'h3':
        const pfasGrams = Number(hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams || 0);
        const pesticideLoad = Number(hoverInfo.data.pesticide_load || hoverInfo.data.total_pesticide_load || 0);
        const diquatGrams = Number(hoverInfo.data.diquat_grams || 0);
        const glyphosateGrams = Number(hoverInfo.data.glyphosate_grams || 0);
        const area = Number(hoverInfo.data.agricultural_area_ha || hoverInfo.data.h3_cell_area_ha || 0);
        
        // Calculate intensities with proper type casting
        const pfasIntensity = Number(hoverInfo.data.pfas_intensity) || (area > 0 ? pfasGrams / area : 0);
        const pesticideIntensity = Number(hoverInfo.data.pesticide_intensity) || (area > 0 ? pesticideLoad / area : 0);
        const diquatIntensity = Number(hoverInfo.data.diquat_intensity) || (area > 0 ? diquatGrams / area : 0);
        const glyphosateIntensity = Number(hoverInfo.data.glyphosate_intensity) || (area > 0 ? glyphosateGrams / area : 0);

        const applicationCount = Number(hoverInfo.data.application_count || 0);
        const fieldCount = Number(hoverInfo.data.field_count || 0);
        const coveragePercent = Number(hoverInfo.data.coverage_percent || 0);

        return (
          <div className="space-y-3">
            {/* Compact Header */}
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium text-white">Agricultural Area</h3>
                  <div className="text-white/70 text-xs font-mono">
                    {area > 0 ? `${formatNumber(area, 1)} ha` : 'Area unavailable'}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="p-1 rounded-full bg-white/20 hover:bg-white/30 transition-colors touch-manipulation"
                  >
                    <ChevronUp className="w-4 h-4 text-white" />
                  </button>
                )}
              </div>
            </div>

            {/* Key Metrics - Always Visible */}
            <div className="grid grid-cols-2 gap-2">
              {/* Total Pesticide Load */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-orange-400/30">
                <div className="text-center">
                  <div className="text-base font-bold text-orange-300 font-mono">{formatNumber(pesticideLoad, 1)}</div>
                  <div className="text-xs text-orange-400/80 uppercase tracking-wide">Pesticide (kg)</div>
                  <div className="text-xs text-orange-300/70 mt-1 font-mono">{formatNumber(pesticideIntensity, 1)} kg/ha</div>
                </div>
              </div>

              {/* PFAS */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-red-400/30">
                <div className="text-center">
                  <div className="text-base font-bold text-red-300 font-mono">{formatNumber(pfasGrams, 1)}</div>
                  <div className="text-xs text-red-400/80 uppercase tracking-wide">PFAS (g)</div>
                  <div className="text-xs text-red-300/70 mt-1 font-mono">{formatNumber(pfasIntensity, 1)} g/ha</div>
                </div>
              </div>
            </div>

            {/* Expanded Content */}
            {isExpanded && (
              <div className="space-y-3 animate-in slide-in-from-bottom-2 duration-300">
                {/* Additional Metrics */}
                <div className="grid grid-cols-2 gap-2">
                  {/* Glyphosate */}
                  <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-green-400/30">
                    <div className="text-center">
                      <div className="text-base font-bold text-green-300 font-mono">{formatNumber(glyphosateGrams, 1)}</div>
                      <div className="text-xs text-green-400/80 uppercase tracking-wide">Glyphosate (g)</div>
                      <div className="text-xs text-green-300/70 mt-1 font-mono">{formatNumber(glyphosateIntensity, 1)} g/ha</div>
                    </div>
                  </div>

                  {/* Diquat */}
                  <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-amber-400/30">
                    <div className="text-center">
                      <div className="text-base font-bold text-amber-300 font-mono">{formatNumber(diquatGrams, 1)}</div>
                      <div className="text-xs text-amber-400/80 uppercase tracking-wide">Diquat (g)</div>
                      <div className="text-xs text-amber-300/70 mt-1 font-mono">{formatNumber(diquatIntensity, 1)} g/ha</div>
                    </div>
                  </div>
                </div>

                {/* Activity Summary */}
                <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
                  <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Activity Summary</h4>
                  <div className="grid grid-cols-3 gap-3 text-center">
                    <div>
                      <div className="text-base font-bold text-white font-mono">{applicationCount}</div>
                      <div className="text-xs text-white/60 uppercase tracking-wide">Applications</div>
                    </div>
                    <div>
                      <div className="text-base font-bold text-white font-mono">{fieldCount}</div>
                      <div className="text-xs text-white/60 uppercase tracking-wide">Fields</div>
                    </div>
                    <div>
                      <div className="text-base font-bold text-white font-mono">{formatNumber(coveragePercent, 0)}%</div>
                      <div className="text-xs text-white/60 uppercase tracking-wide">Coverage</div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        );

      case 'bnbo':
        return (
          <div className="space-y-3">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium text-white uppercase tracking-wide">BNBO Protected Area</h3>
                  <div className="text-white/70 text-xs font-mono">
                    {String(hoverInfo.data.status || 'Status unknown')}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="p-1 rounded-full bg-white/20 hover:bg-white/30 transition-colors touch-manipulation"
                  >
                    <ChevronUp className="w-4 h-4 text-white" />
                  </button>
                )}
              </div>
            </div>
            
            {isExpanded && (
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-white/20 animate-in slide-in-from-bottom-2 duration-300">
                <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Area Details</h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-white/60 uppercase tracking-wide">Area:</span>
                    <span className="font-mono text-white">{formatNumber(Number(hoverInfo.data.area_ha), 2)} ha</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-white/60 uppercase tracking-wide">Protection Level:</span>
                    <span className="font-mono text-white">{String(hoverInfo.data.protection_level || 'Unknown')}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        );

      case 'bbr':
        return (
          <div className="space-y-3">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium text-white uppercase tracking-wide">Building</h3>
                  <div className="text-white/70 text-xs font-mono">
                    {String(hoverInfo.data.building_type || 'Type unknown')}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="p-1 rounded-full bg-white/20 hover:bg-white/30 transition-colors touch-manipulation"
                  >
                    <ChevronUp className="w-4 h-4 text-white" />
                  </button>
                )}
              </div>
            </div>
            
            {isExpanded && (
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-white/20 animate-in slide-in-from-bottom-2 duration-300">
                <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Building Details</h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-white/60 uppercase tracking-wide">Type:</span>
                    <span className="font-mono text-white">{String(hoverInfo.data.building_type || 'Unknown')}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-white/60 uppercase tracking-wide">Use:</span>
                    <span className="font-mono text-white">{String(hoverInfo.data.building_use || 'Unknown')}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-white/60 uppercase tracking-wide">Year:</span>
                    <span className="font-mono text-white">{String(hoverInfo.data.construction_year || 'Unknown')}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        );

      default:
        return null;
    }
  }, [hoverInfo, formatNumber, isExpanded]);

  if (!isVisible || !hoverInfo) return null;

  return (
    <>
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-black/50 backdrop-blur-sm z-40 transition-opacity duration-300"
        onClick={onClose}
      />
      
      {/* Bottom Panel */}
      <div 
        className={`fixed bottom-0 left-0 right-0 bg-black/95 backdrop-blur-md border-t border-white/20 shadow-2xl z-50 transform transition-all duration-300 ease-out ${
          isExpanded ? 'max-h-[80vh]' : 'max-h-[50vh]'
        }`}
        style={{
          transform: `translateY(${Math.max(0, -dragOffset)}px)`,
        }}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
      >
        {/* Header with drag handle */}
        <div className="bg-black/80 backdrop-blur-sm border-b border-white/10 p-4 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-8 h-1 bg-white/40 rounded-full"></div>
            <h2 className="text-sm font-medium text-white uppercase tracking-wide">Area Details</h2>
          </div>
          <div className="flex items-center space-x-2">
            {isExpanded && (
              <button
                onClick={() => setIsExpanded(false)}
                className="p-1 rounded-full bg-white/20 hover:bg-white/30 transition-colors touch-manipulation"
              >
                <ChevronDown className="w-4 h-4 text-white" />
              </button>
            )}
            <button
              onClick={onClose}
              className="p-1 rounded-full bg-white/20 hover:bg-white/30 transition-colors border border-white/20 touch-manipulation"
            >
              <X className="w-4 h-4 text-white/70 hover:text-white" />
            </button>
          </div>
        </div>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-y-auto p-4 pb-safe">
          {renderPanelContent}
        </div>
      </div>
    </>
  );
} 