'use client';

import React, { useMemo, useCallback } from 'react';
import { X } from 'lucide-react';

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

  // Render panel content based on layer type
  const renderPanelContent = useMemo(() => {
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

        const applicationCount = hoverInfo.data.application_count || 0;
        const fieldCount = hoverInfo.data.field_count || 0;
        const coveragePercent = hoverInfo.data.coverage_percent || 0;

        return (
          <div className="space-y-4">
            {/* Header - London Underground Style */}
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h3 className="text-base font-medium mb-1 text-white">AGRICULTURAL AREA</h3>
              <div className="text-white/70 text-sm font-mono">
                {area > 0 ? `${formatNumber(area, 1)} HECTARES` : 'AREA DATA UNAVAILABLE'}
              </div>
            </div>

            {/* Compact metrics grid - London Underground Style */}
            <div className="grid grid-cols-2 gap-3">
              {/* Total Pesticide Load */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-orange-400/30">
                <div className="text-center">
                  <div className="text-lg font-bold text-orange-300 font-mono">{formatNumber(pesticideLoad, 2)}</div>
                  <div className="text-xs text-orange-400/80 uppercase tracking-wide">Total Pesticide (kg)</div>
                  <div className="text-sm text-orange-300/70 mt-1 font-mono">{formatNumber(pesticideIntensity, 2)} kg/ha</div>
                </div>
              </div>

              {/* PFAS */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-red-400/30">
                <div className="text-center">
                  <div className="text-lg font-bold text-red-300 font-mono">{formatNumber(pfasGrams, 2)}</div>
                  <div className="text-xs text-red-400/80 uppercase tracking-wide">PFAS (grams)</div>
                  <div className="text-sm text-red-300/70 mt-1 font-mono">{formatNumber(pfasIntensity, 2)} g/ha</div>
                </div>
              </div>

              {/* Glyphosate */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-green-400/30">
                <div className="text-center">
                  <div className="text-lg font-bold text-green-300 font-mono">{formatNumber(glyphosateGrams, 2)}</div>
                  <div className="text-xs text-green-400/80 uppercase tracking-wide">Glyphosate (grams)</div>
                  <div className="text-sm text-green-300/70 mt-1 font-mono">{formatNumber(glyphosateIntensity, 2)} g/ha</div>
                </div>
              </div>

              {/* Diquat */}
              <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-amber-400/30">
                <div className="text-center">
                  <div className="text-lg font-bold text-amber-300 font-mono">{formatNumber(diquatGrams, 2)}</div>
                  <div className="text-xs text-amber-400/80 uppercase tracking-wide">Diquat (grams)</div>
                  <div className="text-sm text-amber-300/70 mt-1 font-mono">{formatNumber(diquatIntensity, 2)} g/ha</div>
                </div>
              </div>
            </div>

            {/* Activity Summary - London Underground Style */}
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Activity Summary</h4>
              <div className="grid grid-cols-3 gap-3 text-center">
                <div>
                  <div className="text-lg font-bold text-white font-mono">{applicationCount}</div>
                  <div className="text-xs text-white/60 uppercase tracking-wide">Applications</div>
                </div>
                <div>
                  <div className="text-lg font-bold text-white font-mono">{fieldCount}</div>
                  <div className="text-xs text-white/60 uppercase tracking-wide">Fields</div>
                </div>
                <div>
                  <div className="text-lg font-bold text-white font-mono">{formatNumber(coveragePercent, 0)}%</div>
                  <div className="text-xs text-white/60 uppercase tracking-wide">Coverage</div>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bnbo':
        return (
          <div className="space-y-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h3 className="text-base font-medium mb-1 text-white uppercase tracking-wide">BNBO Protected Area</h3>
              <div className="text-white/70 text-sm font-mono">
                {hoverInfo.data.status || 'STATUS UNKNOWN'}
              </div>
            </div>
            
            <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Area Details</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-white/60 uppercase tracking-wide">Area:</span>
                  <span className="font-mono text-white">{formatNumber(hoverInfo.data.area_ha, 2)} ha</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/60 uppercase tracking-wide">Protection Level:</span>
                  <span className="font-mono text-white">{hoverInfo.data.protection_level || 'Unknown'}</span>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bbr':
        return (
          <div className="space-y-4">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h3 className="text-base font-medium mb-1 text-white uppercase tracking-wide">Building</h3>
              <div className="text-white/70 text-sm font-mono">
                {hoverInfo.data.building_type || 'TYPE UNKNOWN'}
              </div>
            </div>
            
            <div className="bg-black/40 backdrop-blur-sm rounded-lg p-3 border border-white/20">
              <h4 className="text-white font-medium mb-2 text-sm uppercase tracking-wide">Building Details</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-white/60 uppercase tracking-wide">Type:</span>
                  <span className="font-mono text-white">{hoverInfo.data.building_type || 'Unknown'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/60 uppercase tracking-wide">Use:</span>
                  <span className="font-mono text-white">{hoverInfo.data.building_use || 'Unknown'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-white/60 uppercase tracking-wide">Year:</span>
                  <span className="font-mono text-white">{hoverInfo.data.construction_year || 'Unknown'}</span>
                </div>
              </div>
            </div>
          </div>
        );

      default:
        return null;
    }
  }, [hoverInfo, formatNumber]);

  if (!isVisible || !hoverInfo) return null;

  return (
    <>
      {/* Backdrop - London Underground Style */}
      <div 
        className="fixed inset-0 bg-black/70 backdrop-blur-sm z-40 transition-opacity duration-300"
        onClick={onClose}
      />
      
      {/* Bottom Panel - London Underground Style */}
      <div className="fixed bottom-0 left-0 right-0 bg-black/95 backdrop-blur-md border-t border-white/20 shadow-2xl z-50 transform transition-transform duration-300 ease-out max-h-[70vh] flex flex-col">
        {/* Header with drag handle - London Underground Style */}
        <div className="bg-black/80 backdrop-blur-sm border-b border-white/10 p-4 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-8 h-1 bg-white/40 rounded-full"></div>
            <h2 className="text-base font-medium text-white uppercase tracking-wide">Area Details</h2>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-white/10 rounded-full transition-colors border border-white/20"
          >
            <X className="w-5 h-5 text-white/70 hover:text-white" />
          </button>
        </div>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-y-auto p-4">
          {renderPanelContent}
        </div>
      </div>
    </>
  );
} 