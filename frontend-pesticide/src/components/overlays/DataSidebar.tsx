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

interface DataSidebarProps {
  hoverInfo?: HoverInfo | null;
  onClose?: () => void;
  isVisible?: boolean;
}

export function DataSidebar({ hoverInfo, onClose, isVisible = false }: DataSidebarProps) {
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

  // Render sidebar content based on layer type
  const renderSidebarContent = useMemo(() => {
    if (!hoverInfo) return null;

    switch (hoverInfo.layer) {
      case 'h3':
        const pfasGrams = Number(hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams) || 0;
        const pesticideLoad = Number(hoverInfo.data.pesticide_load || hoverInfo.data.total_pesticide_load) || 0;
        const diquatGrams = Number(hoverInfo.data.diquat_grams) || 0;
        const glyphosateGrams = Number(hoverInfo.data.glyphosate_grams) || 0;
        const area = Number(hoverInfo.data.agricultural_area_ha || hoverInfo.data.h3_cell_area_ha) || 0;
        
        // Calculate intensities
        const pfasIntensity = Number(hoverInfo.data.pfas_intensity) || (area > 0 ? pfasGrams / area : 0);
        const pesticideIntensity = Number(hoverInfo.data.pesticide_intensity) || (area > 0 ? pesticideLoad / area : 0);
        const diquatIntensity = Number(hoverInfo.data.diquat_intensity) || (area > 0 ? diquatGrams / area : 0);
        const glyphosateIntensity = Number(hoverInfo.data.glyphosate_intensity) || (area > 0 ? glyphosateGrams / area : 0);

        const applicationCount = Number(hoverInfo.data.application_count) || 0;
        const fieldCount = Number(hoverInfo.data.field_count) || 0;
        const coveragePercent = Number(hoverInfo.data.coverage_percent) || 0;

        return (
          <div className="space-y-3">
            {/* Header */}
            <div className="bg-slate-700 rounded-lg p-3 border border-slate-600">
              <h3 className="text-base font-semibold mb-1 text-white">Agricultural Area</h3>
              <div className="text-slate-300 text-sm">
                {area > 0 ? `${formatNumber(area, 1)} hectares` : 'Area data unavailable'}
              </div>
            </div>

            {/* Total Pesticide Load */}
            <div className="bg-slate-800 rounded-lg p-3 border-l-4 border-orange-400">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-orange-300 font-medium text-sm">Total Pesticide Load</h4>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="text-center">
                  <div className="text-lg font-bold text-orange-200">{formatNumber(pesticideLoad, 2)}</div>
                  <div className="text-xs text-orange-400">kg total</div>
                </div>
                <div className="text-center">
                  <div className="text-lg font-bold text-orange-200">{formatNumber(pesticideIntensity, 2)}</div>
                  <div className="text-xs text-orange-400">kg per hectare</div>
                </div>
              </div>
            </div>

            {/* PFAS */}
            <div className="bg-slate-800 rounded-lg p-3 border-l-4 border-red-400">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-red-300 font-medium text-sm">PFAS Active Ingredients</h4>
                <span className="text-xs bg-red-900/50 text-red-300 px-1.5 py-0.5 rounded-full font-medium border border-red-700">
                  Persistent
                </span>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="text-center">
                  <div className="text-lg font-bold text-red-200">{formatNumber(pfasGrams, 2)}</div>
                  <div className="text-xs text-red-400">grams total</div>
                </div>
                <div className="text-center">
                  <div className="text-lg font-bold text-red-200">{formatNumber(pfasIntensity, 2)}</div>
                  <div className="text-xs text-red-400">grams per hectare</div>
                </div>
              </div>
            </div>

            {/* Glyphosate */}
            <div className="bg-slate-800 rounded-lg p-3 border-l-4 border-green-400">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-green-300 font-medium text-sm">Glyphosate Active Ingredients</h4>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="text-center">
                  <div className="text-lg font-bold text-green-200">{formatNumber(glyphosateGrams, 2)}</div>
                  <div className="text-xs text-green-400">grams total</div>
                </div>
                <div className="text-center">
                  <div className="text-lg font-bold text-green-200">{formatNumber(glyphosateIntensity, 2)}</div>
                  <div className="text-xs text-green-400">grams per hectare</div>
                </div>
              </div>
            </div>

            {/* Diquat */}
            <div className="bg-slate-800 rounded-lg p-3 border-l-4 border-amber-400">
              <div className="flex items-center justify-between mb-2">
                <h4 className="text-amber-300 font-medium text-sm">Diquat Active Ingredients</h4>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div className="text-center">
                  <div className="text-lg font-bold text-amber-200">{formatNumber(diquatGrams, 2)}</div>
                  <div className="text-xs text-amber-400">grams total</div>
                </div>
                <div className="text-center">
                  <div className="text-lg font-bold text-amber-200">{formatNumber(diquatIntensity, 2)}</div>
                  <div className="text-xs text-amber-400">grams per hectare</div>
                </div>
              </div>
            </div>

            {/* Activity Summary */}
            <div className="bg-slate-800 rounded-lg p-3 border border-slate-600">
              <h4 className="text-slate-200 font-medium mb-2 text-sm">Activity</h4>
              <div className="grid grid-cols-3 gap-3 text-center">
                <div>
                  <div className="text-base font-bold text-slate-100">{applicationCount}</div>
                  <div className="text-xs text-slate-400">Applications</div>
                </div>
                <div>
                  <div className="text-base font-bold text-slate-100">{fieldCount}</div>
                  <div className="text-xs text-slate-400">Fields</div>
                </div>
                <div>
                  <div className="text-base font-bold text-slate-100">{formatNumber(coveragePercent, 0)}%</div>
                  <div className="text-xs text-slate-400">Coverage</div>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bnbo':
        return (
          <div className="space-y-4">
            <div className="bg-slate-700 rounded-lg p-4 border border-slate-600">
              <h3 className="text-lg font-semibold mb-2 text-white">BNBO Protected Area</h3>
              <div className="text-slate-300 text-sm">
                {hoverInfo.data.status || 'Status unknown'}
              </div>
            </div>
            
            <div className="bg-slate-800 rounded-lg p-4 border border-slate-600">
              <h4 className="text-slate-200 font-semibold mb-3">Area Details</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-400">Area:</span>
                  <span className="font-medium text-slate-200">{formatNumber(Number(hoverInfo.data.area_ha), 2)} ha</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Protection Level:</span>
                  <span className="font-medium text-slate-200">{hoverInfo.data.protection_level || 'Unknown'}</span>
                </div>
              </div>
            </div>
          </div>
        );

      case 'bbr':
        return (
          <div className="space-y-4">
            <div className="bg-slate-700 rounded-lg p-4 border border-slate-600">
              <h3 className="text-lg font-semibold mb-2 text-white">Building</h3>
              <div className="text-slate-300 text-sm">
                {hoverInfo.data.building_type || 'Type unknown'}
              </div>
            </div>
            
            <div className="bg-slate-800 rounded-lg p-4 border border-slate-600">
              <h4 className="text-slate-200 font-semibold mb-3">Building Details</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-slate-400">Type:</span>
                  <span className="font-medium text-slate-200">{hoverInfo.data.building_type || 'Unknown'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Use:</span>
                  <span className="font-medium text-slate-200">{hoverInfo.data.building_use || 'Unknown'}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-slate-400">Year:</span>
                  <span className="font-medium text-slate-200">{hoverInfo.data.construction_year || 'Unknown'}</span>
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
    <div className="fixed top-[9rem] right-0 h-[calc(100vh-9rem)] w-80 bg-slate-900/95 backdrop-blur-sm border-l border-slate-700 shadow-2xl z-40 transform transition-transform duration-300 ease-in-out">
      {/* Header */}
      <div className="bg-slate-800 border-b border-slate-700 p-3 flex items-center justify-between">
        <h2 className="text-base font-semibold text-white">Area Details</h2>
        <button
          onClick={onClose}
          className="p-1 hover:bg-slate-700 rounded-full transition-colors"
        >
          <X className="w-4 h-4 text-slate-400 hover:text-white" />
        </button>
      </div>

      {/* Content */}
      <div className="p-3 overflow-y-auto h-full pb-16">
        {renderSidebarContent}
      </div>
    </div>
  );
} 