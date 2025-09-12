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

export function MobileBottomPanel({
  hoverInfo,
  onClose,
  isVisible = false,
}: MobileBottomPanelProps) {
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
  const formatNumber = useCallback(
    (value: number | undefined, decimals: number = 2): string => {
      if (value === undefined || value === null) return '0';
      if (value === 0) return '0';
      if (value < 0.01 && value > 0) return '<0.01';
      return value.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: decimals,
      });
    },
    []
  );

  // Handle drag gestures for panel expansion
  const handleTouchStart = useCallback((e: React.TouchEvent) => {
    const touch = e.touches[0];
    setIsDragging(true);
    (e.target as any)._startY = touch.clientY;
  }, []);

  const handleTouchMove = useCallback(
    (e: React.TouchEvent) => {
      if (!isDragging) return;

      const touch = e.touches[0];
      const startY = (e.target as any)._startY || touch.clientY;
      const deltaY = startY - touch.clientY;

      // Only allow upward drag to expand
      if (deltaY > 0) {
        setDragOffset(Math.min(deltaY, 200));
      }
    },
    [isDragging]
  );

  const handleTouchEnd = useCallback(
    (_e: React.TouchEvent) => {
      if (!isDragging) return;

      setIsDragging(false);

      // Expand if dragged up significantly
      if (dragOffset > 50) {
        setIsExpanded(true);
      }

      setDragOffset(0);
    },
    [isDragging, dragOffset]
  );

  // Render panel content based on layer type
  const renderPanelContent = useMemo(() => {
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

        // Calculate intensities with proper type casting
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

        const applicationCount = Number(hoverInfo.data.application_count || 0);
        const fieldCount = Number(hoverInfo.data.field_count || 0);
        const coveragePercent = Number(hoverInfo.data.coverage_percent || 0);

        return (
          <div className="space-y-3">
            {/* Compact Header */}
            <div className="rounded-lg border border-white/20 bg-white/10 p-3 backdrop-blur-sm">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium text-white">
                    Agricultural Area
                  </h3>
                  <div className="font-mono text-xs text-white/70">
                    {area > 0
                      ? `${formatNumber(area, 1)} ha`
                      : 'Area unavailable'}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="touch-manipulation rounded-full bg-white/20 p-1 transition-colors hover:bg-white/30"
                  >
                    <ChevronUp className="h-4 w-4 text-white" />
                  </button>
                )}
              </div>
            </div>

            {/* Key Metrics - Always Visible */}
            <div className="grid grid-cols-2 gap-2">
              {/* Total Pesticide Load */}
              <div className="rounded-lg border border-orange-400/30 bg-black/40 p-3 backdrop-blur-sm">
                <div className="text-center">
                  <div className="font-mono text-base font-bold text-orange-300">
                    {formatNumber(pesticideLoad, 1)}
                  </div>
                  <div className="text-xs tracking-wide text-orange-400/80 uppercase">
                    Pesticide (kg)
                  </div>
                  <div className="mt-1 font-mono text-xs text-orange-300/70">
                    {formatNumber(pesticideIntensity, 1)} kg/ha
                  </div>
                </div>
              </div>

              {/* PFAS - only show if there are PFAS values > 0 */}
              {pfasGrams > 0 && (
                <div className="rounded-lg border border-red-400/30 bg-black/40 p-3 backdrop-blur-sm">
                  <div className="text-center">
                    <div className="font-mono text-base font-bold text-red-300">
                      {formatNumber(pfasGrams, 1)}
                    </div>
                    <div className="text-xs tracking-wide text-red-400/80 uppercase">
                      PFAS (g)
                    </div>
                    <div className="mt-1 font-mono text-xs text-red-300/70">
                      {formatNumber(pfasIntensity, 1)} g/ha
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Expanded Content */}
            {isExpanded && (
              <div className="animate-in slide-in-from-bottom-2 space-y-3 duration-300">
                {/* Additional Metrics - only show if there are values > 0 */}
                {(glyphosateGrams > 0 || diquatGrams > 0) && (
                  <div className="grid grid-cols-2 gap-2">
                    {/* Glyphosate - only show if there are glyphosate values > 0 */}
                    {glyphosateGrams > 0 && (
                      <div className="rounded-lg border border-green-400/30 bg-black/40 p-3 backdrop-blur-sm">
                        <div className="text-center">
                          <div className="font-mono text-base font-bold text-green-300">
                            {formatNumber(glyphosateGrams, 1)}
                          </div>
                          <div className="text-xs tracking-wide text-green-400/80 uppercase">
                            Glyphosate (g)
                          </div>
                          <div className="mt-1 font-mono text-xs text-green-300/70">
                            {formatNumber(glyphosateIntensity, 1)} g/ha
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Diquat - only show if there are diquat values > 0 */}
                    {diquatGrams > 0 && (
                      <div className="rounded-lg border border-amber-400/30 bg-black/40 p-3 backdrop-blur-sm">
                        <div className="text-center">
                          <div className="font-mono text-base font-bold text-amber-300">
                            {formatNumber(diquatGrams, 1)}
                          </div>
                          <div className="text-xs tracking-wide text-amber-400/80 uppercase">
                            Diquat (g)
                          </div>
                          <div className="mt-1 font-mono text-xs text-amber-300/70">
                            {formatNumber(diquatIntensity, 1)} g/ha
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {/* Activity Summary */}
                <div className="rounded-lg border border-white/20 bg-white/10 p-3 backdrop-blur-sm">
                  <h4 className="mb-2 text-sm font-medium tracking-wide text-white uppercase">
                    Activity Summary
                  </h4>
                  <div className="grid grid-cols-3 gap-3 text-center">
                    <div>
                      <div className="font-mono text-base font-bold text-white">
                        {applicationCount}
                      </div>
                      <div className="text-xs tracking-wide text-white/60 uppercase">
                        Applications
                      </div>
                    </div>
                    <div>
                      <div className="font-mono text-base font-bold text-white">
                        {fieldCount}
                      </div>
                      <div className="text-xs tracking-wide text-white/60 uppercase">
                        Fields
                      </div>
                    </div>
                    <div>
                      <div className="font-mono text-base font-bold text-white">
                        {formatNumber(coveragePercent, 0)}%
                      </div>
                      <div className="text-xs tracking-wide text-white/60 uppercase">
                        Coverage
                      </div>
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
            <div className="rounded-lg border border-white/20 bg-white/10 p-3 backdrop-blur-sm">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium tracking-wide text-white uppercase">
                    BNBO Protected Area
                  </h3>
                  <div className="font-mono text-xs text-white/70">
                    {String(hoverInfo.data.status || 'Status unknown')}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="touch-manipulation rounded-full bg-white/20 p-1 transition-colors hover:bg-white/30"
                  >
                    <ChevronUp className="h-4 w-4 text-white" />
                  </button>
                )}
              </div>
            </div>

            {isExpanded && (
              <div className="animate-in slide-in-from-bottom-2 rounded-lg border border-white/20 bg-black/40 p-3 backdrop-blur-sm duration-300">
                <h4 className="mb-2 text-sm font-medium tracking-wide text-white uppercase">
                  Area Details
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="tracking-wide text-white/60 uppercase">
                      Area:
                    </span>
                    <span className="font-mono text-white">
                      {formatNumber(Number(hoverInfo.data.area_ha), 2)} ha
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="tracking-wide text-white/60 uppercase">
                      Protection Level:
                    </span>
                    <span className="font-mono text-white">
                      {String(hoverInfo.data.protection_level || 'Unknown')}
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
        );

      case 'bbr':
        return (
          <div className="space-y-3">
            <div className="rounded-lg border border-white/20 bg-white/10 p-3 backdrop-blur-sm">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium tracking-wide text-white uppercase">
                    Building
                  </h3>
                  <div className="font-mono text-xs text-white/70">
                    {String(hoverInfo.data.building_type || 'Type unknown')}
                  </div>
                </div>
                {!isExpanded && (
                  <button
                    onClick={() => setIsExpanded(true)}
                    className="touch-manipulation rounded-full bg-white/20 p-1 transition-colors hover:bg-white/30"
                  >
                    <ChevronUp className="h-4 w-4 text-white" />
                  </button>
                )}
              </div>
            </div>

            {isExpanded && (
              <div className="animate-in slide-in-from-bottom-2 rounded-lg border border-white/20 bg-black/40 p-3 backdrop-blur-sm duration-300">
                <h4 className="mb-2 text-sm font-medium tracking-wide text-white uppercase">
                  Building Details
                </h4>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="tracking-wide text-white/60 uppercase">
                      Type:
                    </span>
                    <span className="font-mono text-white">
                      {String(hoverInfo.data.building_type || 'Unknown')}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="tracking-wide text-white/60 uppercase">
                      Use:
                    </span>
                    <span className="font-mono text-white">
                      {String(hoverInfo.data.building_use || 'Unknown')}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="tracking-wide text-white/60 uppercase">
                      Year:
                    </span>
                    <span className="font-mono text-white">
                      {String(hoverInfo.data.construction_year || 'Unknown')}
                    </span>
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
        className="fixed inset-0 z-40 bg-black/50 backdrop-blur-sm transition-opacity duration-300"
        onClick={onClose}
      />

      {/* Bottom Panel */}
      <div
        className={`fixed right-0 bottom-0 left-0 z-50 transform border-t border-white/20 bg-black/95 shadow-2xl backdrop-blur-md transition-all duration-300 ease-out ${
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
        <div className="flex items-center justify-between border-b border-white/10 bg-black/80 p-4 backdrop-blur-sm">
          <div className="flex items-center space-x-3">
            <div className="h-1 w-8 rounded-full bg-white/40"></div>
            <h2 className="text-sm font-medium tracking-wide text-white uppercase">
              Area Details
            </h2>
          </div>
          <div className="flex items-center space-x-2">
            {isExpanded && (
              <button
                onClick={() => setIsExpanded(false)}
                className="touch-manipulation rounded-full bg-white/20 p-1 transition-colors hover:bg-white/30"
              >
                <ChevronDown className="h-4 w-4 text-white" />
              </button>
            )}
            <button
              onClick={onClose}
              className="touch-manipulation rounded-full border border-white/20 bg-white/20 p-1 transition-colors hover:bg-white/30"
            >
              <X className="h-4 w-4 text-white/70 hover:text-white" />
            </button>
          </div>
        </div>

        {/* Scrollable Content */}
        <div className="pb-safe flex-1 overflow-y-auto p-4">
          {renderPanelContent}
        </div>
      </div>
    </>
  );
}
