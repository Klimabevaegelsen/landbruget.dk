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

export function DataSidebar({
  hoverInfo,
  onClose,
  isVisible = false,
}: DataSidebarProps) {
  // Format functions
  const formatNumber = (
    value: number | undefined,
    decimals: number = 2
  ): string => {
    if (value === undefined || value === null) return '0';
    if (value === 0) return '0';
    if (value < 0.01 && value > 0) return '<0.01';
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 0,
      maximumFractionDigits: decimals,
    });
  };

  // Render sidebar content based on layer type
  const renderSidebarContent = () => {
    if (!hoverInfo) return null;

    if (hoverInfo.layer === 'h3') {
      const pfasGrams =
        Number(hoverInfo.data.pfas_grams || hoverInfo.data.total_pfas_grams) ||
        0;
      const pesticideLoad =
        Number(
          hoverInfo.data.pesticide_load || hoverInfo.data.total_pesticide_load
        ) || 0;
      const diquatGrams = Number(hoverInfo.data.diquat_grams) || 0;
      const glyphosateGrams = Number(hoverInfo.data.glyphosate_grams) || 0;
      const area =
        Number(
          hoverInfo.data.agricultural_area_ha || hoverInfo.data.h3_cell_area_ha
        ) || 0;

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

      const applicationCount = Number(hoverInfo.data.application_count) || 0;
      const fieldCount = Number(hoverInfo.data.field_count) || 0;
      const coveragePercent = Number(hoverInfo.data.coverage_percent) || 0;

      return (
        <div className="space-y-3">
          {/* Header */}
          <div className="rounded-lg border border-slate-600 bg-slate-700 p-3">
            <h3 className="mb-1 text-base font-semibold text-white">
              Agricultural Area
            </h3>
            <div className="text-sm text-slate-300">
              {area > 0
                ? `${formatNumber(area, 1)} hectares`
                : 'Area data unavailable'}
            </div>
          </div>

          {/* Total Pesticide Load */}
          <div className="rounded-lg border-l-4 border-orange-400 bg-slate-800 p-3">
            <div className="mb-2 flex items-center justify-between">
              <h4 className="text-sm font-medium text-orange-300">
                Total Pesticide Load
              </h4>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="text-center">
                <div className="text-lg font-bold text-orange-200">
                  {String(formatNumber(pesticideLoad, 2))}
                </div>
                <div className="text-xs text-orange-400">kg total</div>
              </div>
              <div className="text-center">
                <div className="text-lg font-bold text-orange-200">
                  {String(formatNumber(pesticideIntensity, 2))}
                </div>
                <div className="text-xs text-orange-400">kg per hectare</div>
              </div>
            </div>
          </div>

          {/* PFAS */}
          <div className="rounded-lg border-l-4 border-red-400 bg-slate-800 p-3">
            <div className="mb-2 flex items-center justify-between">
              <h4 className="text-sm font-medium text-red-300">
                PFAS Active Ingredients
              </h4>
              <span className="rounded-full border border-red-700 bg-red-900/50 px-1.5 py-0.5 text-xs font-medium text-red-300">
                Persistent
              </span>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="text-center">
                <div className="text-lg font-bold text-red-200">
                  {formatNumber(pfasGrams, 2)}
                </div>
                <div className="text-xs text-red-400">grams total</div>
              </div>
              <div className="text-center">
                <div className="text-lg font-bold text-red-200">
                  {formatNumber(pfasIntensity, 2)}
                </div>
                <div className="text-xs text-red-400">grams per hectare</div>
              </div>
            </div>
          </div>

          {/* Glyphosate */}
          <div className="rounded-lg border-l-4 border-green-400 bg-slate-800 p-3">
            <div className="mb-2 flex items-center justify-between">
              <h4 className="text-sm font-medium text-green-300">
                Glyphosate Active Ingredients
              </h4>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="text-center">
                <div className="text-lg font-bold text-green-200">
                  {formatNumber(glyphosateGrams, 2)}
                </div>
                <div className="text-xs text-green-400">grams total</div>
              </div>
              <div className="text-center">
                <div className="text-lg font-bold text-green-200">
                  {formatNumber(glyphosateIntensity, 2)}
                </div>
                <div className="text-xs text-green-400">grams per hectare</div>
              </div>
            </div>
          </div>

          {/* Diquat */}
          <div className="rounded-lg border-l-4 border-amber-400 bg-slate-800 p-3">
            <div className="mb-2 flex items-center justify-between">
              <h4 className="text-sm font-medium text-amber-300">
                Diquat Active Ingredients
              </h4>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="text-center">
                <div className="text-lg font-bold text-amber-200">
                  {formatNumber(diquatGrams, 2)}
                </div>
                <div className="text-xs text-amber-400">grams total</div>
              </div>
              <div className="text-center">
                <div className="text-lg font-bold text-amber-200">
                  {formatNumber(diquatIntensity, 2)}
                </div>
                <div className="text-xs text-amber-400">grams per hectare</div>
              </div>
            </div>
          </div>

          {/* Activity Summary */}
          <div className="rounded-lg border border-slate-600 bg-slate-800 p-3">
            <h4 className="mb-2 text-sm font-medium text-slate-200">
              Activity
            </h4>
            <div className="grid grid-cols-3 gap-3 text-center">
              <div>
                <div className="text-base font-bold text-slate-100">
                  {applicationCount}
                </div>
                <div className="text-xs text-slate-400">Applications</div>
              </div>
              <div>
                <div className="text-base font-bold text-slate-100">
                  {fieldCount}
                </div>
                <div className="text-xs text-slate-400">Fields</div>
              </div>
              <div>
                <div className="text-base font-bold text-slate-100">
                  {formatNumber(coveragePercent, 0)}%
                </div>
                <div className="text-xs text-slate-400">Coverage</div>
              </div>
            </div>
          </div>
        </div>
      );
    }

    if (hoverInfo.layer === 'bnbo') {
      return (
        <div className="space-y-4">
          <div className="rounded-lg border border-slate-600 bg-slate-700 p-4">
            <h3 className="mb-2 text-lg font-semibold text-white">
              BNBO Protected Area
            </h3>
            <div className="text-sm text-slate-300">
              {hoverInfo.data.status || 'Status unknown'}
            </div>
          </div>

          <div className="rounded-lg border border-slate-600 bg-slate-800 p-4">
            <h4 className="mb-3 font-semibold text-slate-200">Area Details</h4>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-slate-400">Area:</span>
                <span className="font-medium text-slate-200">
                  {formatNumber(Number(hoverInfo.data.area_ha), 2)} ha
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-400">Protection Level:</span>
                <span className="font-medium text-slate-200">
                  {hoverInfo.data.protection_level || 'Unknown'}
                </span>
              </div>
            </div>
          </div>
        </div>
      );
    }

    if (hoverInfo.layer === 'bbr') {
      return (
        <div className="space-y-4">
          <div className="rounded-lg border border-slate-600 bg-slate-700 p-4">
            <h3 className="mb-2 text-lg font-semibold text-white">Building</h3>
            <div className="text-sm text-slate-300">
              {hoverInfo.data.building_type || 'Type unknown'}
            </div>
          </div>

          <div className="rounded-lg border border-slate-600 bg-slate-800 p-4">
            <h4 className="mb-3 font-semibold text-slate-200">
              Building Details
            </h4>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-slate-400">Type:</span>
                <span className="font-medium text-slate-200">
                  {hoverInfo.data.building_type || 'Unknown'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-400">Use:</span>
                <span className="font-medium text-slate-200">
                  {hoverInfo.data.building_use || 'Unknown'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-slate-400">Year:</span>
                <span className="font-medium text-slate-200">
                  {hoverInfo.data.construction_year || 'Unknown'}
                </span>
              </div>
            </div>
          </div>
        </div>
      );
    }

    return null;
  };

  return (
    <div
      className={`fixed top-0 right-0 z-40 h-full w-80 transform border-l border-slate-700 bg-slate-900/95 shadow-2xl backdrop-blur-sm transition-transform duration-300 ease-in-out ${
        isVisible ? 'translate-x-0' : 'translate-x-full'
      }`}
    >
      {/* Spacer to account for top bar */}
      <div className="h-[9rem]"></div>

      {/* Header */}
      <div className="flex items-center justify-between border-b border-slate-700 bg-slate-800 p-3">
        <h2 className="text-base font-semibold text-white">Area Details</h2>
        <button
          onClick={onClose}
          className="rounded-full p-1 transition-colors hover:bg-slate-700"
        >
          <X className="h-4 w-4 text-slate-400 hover:text-white" />
        </button>
      </div>

      {/* Content */}
      <div className="h-[calc(100vh-9rem-3.5rem)] overflow-y-auto p-3 pb-16">
        {hoverInfo ? (
          renderSidebarContent()
        ) : (
          <div className="flex h-full items-center justify-center text-slate-400">
            <p className="text-center">Hover over an area to see details</p>
          </div>
        )}
      </div>
    </div>
  );
}
