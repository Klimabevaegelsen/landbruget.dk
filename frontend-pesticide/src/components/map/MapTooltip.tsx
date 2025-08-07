'use client';

import React from 'react';
import { useTooltipState, BNBO_STATUS_CONFIG } from '@/stores/map-store';

interface TooltipData {
  // H3 data fields - both old and new field names
  h3_id?: string;
  h3_cell?: string;
  h3_resolution?: number;
  center_lat?: number;
  center_lon?: number;
  h3_cell_area_ha?: number;
  total_intersection_area_ha?: number;
  actual_coverage_ratio?: number;
  unique_field_count?: number;
  field_count?: number;
  
  // Pesticide data - both old and new field names
  pfas_grams?: number;
  pesticide_load?: number;
  diquat_grams?: number;
  glyphosate_grams?: number;
  applications?: number;
  pfas_applications?: number;
  diquat_applications?: number;
  glyphosate_applications?: number;
  pfas_intensity?: number;
  pesticide_intensity?: number;
  diquat_intensity?: number;
  glyphosate_intensity?: number;
  
  // Original field names for backward compatibility
  total_pfas_containing_active_ingredient_grams?: number;
  total_diquat_containing_active_ingredient_grams?: number;
  total_glyphosate_containing_active_ingredient_grams?: number;
  total_pesticide_belastning?: number;
  total_pfas_pesticide_belastning?: number;
  total_diquat_pesticide_belastning?: number;
  total_glyphosate_pesticide_belastning?: number;
  total_pesticide_applications?: number;
  pfas_containing_applications?: number;
  diquat_containing_applications?: number;
  glyphosate_containing_applications?: number;
  crop_types?: string;
  crop_diversity?: number;
  pfas_containing_active_ingredient_intensity_grams_per_ha?: number;
  diquat_containing_active_ingredient_intensity_grams_per_ha?: number;
  glyphosate_containing_active_ingredient_intensity_grams_per_ha?: number;
  pesticide_belastning_per_ha?: number;
  avg_field_coverage?: number;

  // Kommune data fields
  kommune_code?: number;
  kommune_name?: string;
  region_code?: number;
  kommune_area_ha?: number;
  kommune_centroid_x?: number;
  kommune_centroid_y?: number;
  total_agricultural_area_ha?: number;
  agricultural_area_ha?: number;
  unique_company_count?: number;
  company_count?: number;
  avg_field_coverage_ratio?: number;
  max_field_coverage_ratio?: number;
  min_field_coverage_ratio?: number;
  unique_pfas_products?: number;
  unique_diquat_products?: number;
  unique_glyphosate_products?: number;
  unique_pesticide_products?: number;
  pfas_pesticide_belastning_per_ha?: number;
  diquat_pesticide_belastning_per_ha?: number;
  glyphosate_pesticide_belastning_per_ha?: number;
  agricultural_coverage_pct?: number;

  // BNBO data fields
  bnbo_id?: string;
  status?: string;
  description?: string;
  area_ha?: number;

  // General fields
  year?: number;
  resolution?: number;
}

const formatNumber = (value: number | undefined, decimals: number = 2): string => {
  if (value === undefined || value === null) return 'N/A';
  if (value === 0) return '0';
  
  if (value < 0.01 && value > 0) {
    return value.toExponential(2);
  }
  
  return value.toLocaleString('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
};



const formatPercentage = (value: number | undefined): string => {
  if (value === undefined || value === null) return 'N/A';
  return `${formatNumber(value * 100, 1)}%`;
};

function getTooltipType(data: TooltipData): 'h3' | 'kommune' | 'bnbo' {
  if (data.bnbo_id || data.status) return 'bnbo';
  if (data.kommune_code || data.kommune_name) return 'kommune';
  return 'h3';
}

const H3Tooltip: React.FC<{ data: TooltipData }> = ({ data }) => {
  
  const pfasGrams = data.pfas_grams || data.total_pfas_containing_active_ingredient_grams || 0;
  const pesticideLoad = data.pesticide_load || data.total_pesticide_belastning || 0;
  const diquatGrams = data.diquat_grams || data.total_diquat_containing_active_ingredient_grams || 0;
  const glyphosateGrams = data.glyphosate_grams || data.total_glyphosate_containing_active_ingredient_grams || 0;
  const applications = data.applications || data.total_pesticide_applications || 0;
  const fieldCount = data.unique_field_count || data.field_count || 0;
  const area = data.h3_cell_area_ha || data.agricultural_area_ha || 0;
  const coverage = data.actual_coverage_ratio || data.avg_field_coverage || 0;

  // Calculate intensities
  const pfasIntensity = data.pfas_intensity || data.pfas_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? pfasGrams / area : 0);
  const pesticideIntensity = data.pesticide_intensity || data.pesticide_belastning_per_ha || (area > 0 ? pesticideLoad / area : 0);
  const diquatIntensity = data.diquat_intensity || data.diquat_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? diquatGrams / area : 0);
  const glyphosateIntensity = data.glyphosate_intensity || data.glyphosate_containing_active_ingredient_intensity_grams_per_ha || (area > 0 ? glyphosateGrams / area : 0);

  return (
    <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs space-y-3" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
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
              <div className="font-semibold text-slate-900 text-sm">{applications}</div>
              <div className="text-slate-600">Applications</div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-slate-900 text-sm">{fieldCount}</div>
              <div className="text-slate-600">Fields</div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-slate-900 text-sm">{formatPercentage(coverage)}</div>
              <div className="text-slate-600">Coverage</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const KommuneTooltip: React.FC<{ data: TooltipData }> = ({ data }) => {
  
  const pfasGrams = data.pfas_grams || data.total_pfas_containing_active_ingredient_grams || 0;
  const pesticideLoad = data.pesticide_load || data.total_pesticide_belastning || 0;
  const diquatGrams = data.diquat_grams || data.total_diquat_containing_active_ingredient_grams || 0;
  const glyphosateGrams = data.glyphosate_grams || data.total_glyphosate_containing_active_ingredient_grams || 0;
  const applications = data.applications || data.total_pesticide_applications || 0;
  const fieldCount = data.field_count || data.unique_field_count || 0;
  const area = data.agricultural_area_ha || data.total_agricultural_area_ha || 0;
  const coverage = data.agricultural_coverage_pct ? data.agricultural_coverage_pct / 100 : 0;

  // Calculate intensities
  const pfasIntensity = data.pfas_pesticide_belastning_per_ha || (area > 0 ? pfasGrams / area : 0);
  const pesticideIntensity = data.pesticide_belastning_per_ha || (area > 0 ? pesticideLoad / area : 0);
  const diquatIntensity = data.diquat_pesticide_belastning_per_ha || (area > 0 ? diquatGrams / area : 0);
  const glyphosateIntensity = data.glyphosate_pesticide_belastning_per_ha || (area > 0 ? glyphosateGrams / area : 0);

  return (
    <div className="bg-white/95 backdrop-blur-sm border-0 rounded-lg shadow-2xl max-w-xs space-y-3" style={{ fontFamily: 'system-ui, -apple-system, sans-serif' }}>
      <div className="p-4 space-y-3">
        {/* Header - Clean and minimal */}
        <div className="bg-slate-900 rounded-md px-3 py-2">
          <div className="text-white text-sm font-medium">Municipality: {data.kommune_name || 'Unknown'}</div>
          <div className="text-slate-300 text-xs">
            {area > 0 ? `${formatNumber(area, 1)} hectares agricultural area` : 'Area data unavailable'}
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
              <div className="font-semibold text-slate-900 text-sm">{applications}</div>
              <div className="text-slate-600">Applications</div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-slate-900 text-sm">{fieldCount}</div>
              <div className="text-slate-600">Fields</div>
            </div>
            <div className="text-center">
              <div className="font-semibold text-slate-900 text-sm">{formatPercentage(coverage)}</div>
              <div className="text-slate-600">Coverage</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

const BNBOTooltip: React.FC<{ data: TooltipData }> = ({ data }) => {
  const statusConfig = data.status ? BNBO_STATUS_CONFIG[data.status as keyof typeof BNBO_STATUS_CONFIG] : null;

  return (
    <div className="space-y-3">
      {/* Header - Environmental Protection */}
      <div className="relative overflow-hidden rounded border border-gray-300 bg-gray-900 px-4 py-3 text-white">
        <div className="absolute inset-0 bg-gradient-to-r from-gray-800 to-gray-900"></div>
        <div className="relative">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="font-mono text-sm font-semibold tracking-wide">ENVIRONMENTAL PROTECTION ZONE</h3>
              <p className="font-mono text-xs text-gray-300">
                BNBO SECTOR {data.bnbo_id ? data.bnbo_id.substring(0, 8).toUpperCase() : 'UNKNOWN'}
              </p>
            </div>
            <div className="text-right">
              <div className="font-mono text-xs text-gray-400">AREA</div>
              <div className="font-mono text-sm font-bold text-white">
                {data.area_ha ? `${formatNumber(data.area_ha, 1)} ha` : 'N/A'}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Protection Status */}
      <div className="border border-gray-300 bg-gray-50 rounded p-3">
        {statusConfig && (
          <div className="flex items-center space-x-3 mb-3">
            <div 
              className="w-4 h-4 rounded border border-gray-400" 
              style={{ backgroundColor: statusConfig.color }}
            />
            <div>
              <div className="font-mono text-sm font-semibold text-gray-900">{statusConfig.label}</div>
              <div className="font-mono text-xs text-gray-600">{statusConfig.description}</div>
            </div>
          </div>
        )}

        {data.description && (
          <div className="mt-3">
            <div className="font-mono text-xs font-semibold text-gray-900 mb-1">DESCRIPTION:</div>
            <p className="font-mono text-xs text-gray-600 break-words">{data.description}</p>
          </div>
        )}
      </div>

      {/* Technical Metadata */}
      <div className="border-t border-gray-300 pt-2">
        <div className="font-mono text-xs text-gray-500">
          <span className="font-semibold">BNBO ID:</span>
          <span className="ml-1 bg-gray-200 px-1 py-0.5 rounded font-mono">
            {data.bnbo_id || 'UNKNOWN'}
          </span>
        </div>
      </div>
    </div>
  );
};

export const MapTooltip: React.FC = () => {
  const { showTooltip, tooltipData, tooltipPosition } = useTooltipState();

  if (!showTooltip || !tooltipData) {
    return null;
  }

  const tooltipType = getTooltipType(tooltipData);

  // Position tooltip far from cursor to avoid covering the hovered area
  const tooltipDistance = 150; // Large distance from cursor
  const tooltipWidth = 320;
  const tooltipHeight = 400;
  const padding = 10;
  
  // Determine available space in each direction
  const spaceRight = window.innerWidth - tooltipPosition.x;
  const spaceLeft = tooltipPosition.x;
  const spaceBelow = window.innerHeight - tooltipPosition.y;
  const spaceAbove = tooltipPosition.y;
  
  const adjustedPosition = {
    left: tooltipPosition.x + tooltipDistance,
    top: tooltipPosition.y + tooltipDistance
  };

  // Choose horizontal position - keep tooltip far from cursor
  if (spaceRight >= tooltipWidth + tooltipDistance + padding) {
    // Enough space on the right - position far to the right
    adjustedPosition.left = tooltipPosition.x + tooltipDistance;
  } else if (spaceLeft >= tooltipWidth + tooltipDistance + padding) {
    // Not enough space on right, position far to the left
    adjustedPosition.left = tooltipPosition.x - tooltipWidth - tooltipDistance;
  } else {
    // Not enough horizontal space, use the side with more room but keep distance
    if (spaceRight > spaceLeft) {
      adjustedPosition.left = tooltipPosition.x + tooltipDistance;
    } else {
      adjustedPosition.left = tooltipPosition.x - tooltipWidth - tooltipDistance;
    }
  }

  // Choose vertical position - keep tooltip far from cursor
  if (spaceBelow >= tooltipHeight + tooltipDistance + padding) {
    // Enough space below - position far below
    adjustedPosition.top = tooltipPosition.y + tooltipDistance;
  } else if (spaceAbove >= tooltipHeight + tooltipDistance + padding) {
    // Not enough space below, position far above
    adjustedPosition.top = tooltipPosition.y - tooltipHeight - tooltipDistance;
  } else {
    // Not enough vertical space, use the side with more room but keep distance
    if (spaceBelow > spaceAbove) {
      adjustedPosition.top = tooltipPosition.y + tooltipDistance;
    } else {
      adjustedPosition.top = tooltipPosition.y - tooltipHeight - tooltipDistance;
    }
  }

  // Final bounds checking to ensure tooltip stays on screen
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
      <div className="bg-white border border-gray-400 rounded-lg shadow-xl max-w-sm">
        <div className="p-4">
          {tooltipType === 'h3' && <H3Tooltip data={tooltipData} />}
          {tooltipType === 'kommune' && <KommuneTooltip data={tooltipData} />}
          {tooltipType === 'bnbo' && <BNBOTooltip data={tooltipData} />}

          {/* Raw data debug section - more scientific */}
          {process.env.NODE_ENV === 'development' && (
            <details className="mt-3">
              <summary className="font-mono text-xs text-gray-500 cursor-pointer hover:text-gray-700">
                RAW DATA DEBUG
              </summary>
              <pre className="font-mono text-xs mt-2 p-2 bg-gray-100 rounded overflow-auto max-h-32 text-gray-700">
                {JSON.stringify(tooltipData, null, 2)}
              </pre>
            </details>
          )}
        </div>
      </div>
    </div>
  );
};

export default MapTooltip; 