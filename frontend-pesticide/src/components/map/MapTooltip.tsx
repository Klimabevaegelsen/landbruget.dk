'use client';

import React from 'react';
import { useTooltipState, DATA_MODE_CONFIG, BNBO_STATUS_CONFIG } from '@/stores/map-store';

interface TooltipData {
  // H3 data fields
  h3_cell?: string;
  center_lat?: number;
  center_lon?: number;
  h3_cell_area_ha?: number;
  total_intersection_area_ha?: number;
  actual_coverage_ratio?: number;
  unique_field_count?: number;
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

  // Kommune data fields
  kommune_code?: number;
  kommune_name?: string;
  region_code?: number;
  kommune_area_ha?: number;
  kommune_centroid_x?: number;
  kommune_centroid_y?: number;
  total_agricultural_area_ha?: number;
  unique_company_count?: number;
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

const getTooltipType = (data: TooltipData): 'h3' | 'kommune' | 'bnbo' | 'unknown' => {
  if (data.h3_cell) return 'h3';
  if (data.kommune_code || data.kommune_name) return 'kommune';
  if (data.bnbo_id || data.status) return 'bnbo';
  return 'unknown';
};

const H3Tooltip: React.FC<{ data: TooltipData }> = ({ data }) => (
  <div className="space-y-3">
    <div className="border-b border-gray-200 pb-2">
      <h3 className="font-semibold text-gray-900">H3 Cell</h3>
      <p className="text-sm text-gray-600 font-mono">{data.h3_cell}</p>
    </div>

    <div className="grid grid-cols-2 gap-3 text-sm">
      <div>
        <span className="text-gray-600">Area:</span>
        <div className="font-medium">{formatNumber(data.h3_cell_area_ha)} ha</div>
      </div>
      <div>
        <span className="text-gray-600">Fields:</span>
        <div className="font-medium">{formatNumber(data.unique_field_count, 0)}</div>
      </div>
      <div>
        <span className="text-gray-600">Coverage:</span>
        <div className="font-medium">{formatPercentage(data.actual_coverage_ratio)}</div>
      </div>
      <div>
        <span className="text-gray-600">Crop Types:</span>
        <div className="font-medium">{formatNumber(data.crop_diversity, 0)}</div>
      </div>
    </div>

    <div className="border-t border-gray-200 pt-2">
      <h4 className="font-medium text-gray-900 mb-2">Pesticide Intensity (per ha)</h4>
      <div className="space-y-1 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-600">Total:</span>
          <span className="font-medium">{formatNumber(data.pesticide_belastning_per_ha)} kg/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-red-600">PFAS:</span>
          <span className="font-medium">{formatNumber(data.pfas_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-blue-600">Diquat:</span>
          <span className="font-medium">{formatNumber(data.diquat_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-green-600">Glyphosate:</span>
          <span className="font-medium">{formatNumber(data.glyphosate_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
      </div>
    </div>

    <div className="border-t border-gray-200 pt-2">
      <h4 className="font-medium text-gray-900 mb-2">Total Applications</h4>
      <div className="space-y-1 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-600">All:</span>
          <span className="font-medium">{formatNumber(data.total_pesticide_applications, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-red-600">PFAS:</span>
          <span className="font-medium">{formatNumber(data.pfas_containing_applications, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-blue-600">Diquat:</span>
          <span className="font-medium">{formatNumber(data.diquat_containing_applications, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-green-600">Glyphosate:</span>
          <span className="font-medium">{formatNumber(data.glyphosate_containing_applications, 0)}</span>
        </div>
      </div>
    </div>

    {data.crop_types && (
      <div className="border-t border-gray-200 pt-2">
        <h4 className="font-medium text-gray-900 mb-1">Crop Types</h4>
        <p className="text-xs text-gray-600 max-w-xs break-words">{data.crop_types}</p>
      </div>
    )}
  </div>
);

const KommuneTooltip: React.FC<{ data: TooltipData }> = ({ data }) => (
  <div className="space-y-3">
    <div className="border-b border-gray-200 pb-2">
      <h3 className="font-semibold text-gray-900">{data.kommune_name}</h3>
      <p className="text-sm text-gray-600">Kommune Code: {data.kommune_code}</p>
    </div>

    <div className="grid grid-cols-2 gap-3 text-sm">
      <div>
        <span className="text-gray-600">Total Area:</span>
        <div className="font-medium">{formatNumber(data.kommune_area_ha)} ha</div>
      </div>
      <div>
        <span className="text-gray-600">Agricultural:</span>
        <div className="font-medium">{formatNumber(data.total_agricultural_area_ha)} ha</div>
      </div>
      <div>
        <span className="text-gray-600">Fields:</span>
        <div className="font-medium">{formatNumber(data.unique_field_count, 0)}</div>
      </div>
      <div>
        <span className="text-gray-600">Companies:</span>
        <div className="font-medium">{formatNumber(data.unique_company_count, 0)}</div>
      </div>
    </div>

    <div className="border-t border-gray-200 pt-2">
      <h4 className="font-medium text-gray-900 mb-2">Coverage Statistics</h4>
      <div className="space-y-1 text-sm">
                 <div className="flex justify-between">
           <span className="text-gray-600">Agricultural %:</span>
           <span className="font-medium">{formatPercentage((data.agricultural_coverage_pct || 0) / 100)}</span>
         </div>
        <div className="flex justify-between">
          <span className="text-gray-600">Avg Coverage:</span>
          <span className="font-medium">{formatPercentage(data.avg_field_coverage_ratio)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-gray-600">Max Coverage:</span>
          <span className="font-medium">{formatPercentage(data.max_field_coverage_ratio)}</span>
        </div>
      </div>
    </div>

    <div className="border-t border-gray-200 pt-2">
      <h4 className="font-medium text-gray-900 mb-2">Pesticide Intensity (per ha)</h4>
      <div className="space-y-1 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-600">Total:</span>
          <span className="font-medium">{formatNumber(data.pesticide_belastning_per_ha)} kg/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-red-600">PFAS:</span>
          <span className="font-medium">{formatNumber(data.pfas_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-blue-600">Diquat:</span>
          <span className="font-medium">{formatNumber(data.diquat_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
        <div className="flex justify-between">
          <span className="text-green-600">Glyphosate:</span>
          <span className="font-medium">{formatNumber(data.glyphosate_containing_active_ingredient_intensity_grams_per_ha)} g/ha</span>
        </div>
      </div>
    </div>

    <div className="border-t border-gray-200 pt-2">
      <h4 className="font-medium text-gray-900 mb-2">Product Diversity</h4>
      <div className="grid grid-cols-2 gap-2 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-600">Total:</span>
          <span className="font-medium">{formatNumber(data.unique_pesticide_products, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-red-600">PFAS:</span>
          <span className="font-medium">{formatNumber(data.unique_pfas_products, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-blue-600">Diquat:</span>
          <span className="font-medium">{formatNumber(data.unique_diquat_products, 0)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-green-600">Glyphosate:</span>
          <span className="font-medium">{formatNumber(data.unique_glyphosate_products, 0)}</span>
        </div>
      </div>
    </div>

    {data.crop_types && (
      <div className="border-t border-gray-200 pt-2">
        <h4 className="font-medium text-gray-900 mb-1">Crop Types</h4>
        <p className="text-xs text-gray-600 max-w-xs break-words">{data.crop_types}</p>
      </div>
    )}
  </div>
);

const BNBOTooltip: React.FC<{ data: TooltipData }> = ({ data }) => {
  const statusConfig = data.status ? BNBO_STATUS_CONFIG[data.status as keyof typeof BNBO_STATUS_CONFIG] : null;

  return (
    <div className="space-y-3">
      <div className="border-b border-gray-200 pb-2">
        <h3 className="font-semibold text-gray-900">BNBO Area</h3>
        {data.bnbo_id && (
          <p className="text-sm text-gray-600 font-mono">{data.bnbo_id}</p>
        )}
      </div>

      <div className="space-y-2">
        {statusConfig && (
          <div className="flex items-center space-x-2">
            <div 
              className="w-3 h-3 rounded-full" 
              style={{ backgroundColor: statusConfig.color }}
            />
            <div>
              <div className="font-medium text-sm">{statusConfig.label}</div>
              <div className="text-xs text-gray-600">{statusConfig.description}</div>
            </div>
          </div>
        )}

        {data.area_ha && (
          <div className="text-sm">
            <span className="text-gray-600">Area:</span>
            <span className="font-medium ml-2">{formatNumber(data.area_ha)} ha</span>
          </div>
        )}

        {data.description && (
          <div className="text-sm">
            <span className="text-gray-600">Description:</span>
            <p className="text-xs text-gray-600 mt-1 max-w-xs break-words">{data.description}</p>
          </div>
        )}
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

  // Position tooltip to avoid going off screen
  const adjustedPosition = {
    left: Math.min(tooltipPosition.x + 10, window.innerWidth - 320),
    top: Math.min(tooltipPosition.y + 10, window.innerHeight - 400),
  };

  // If tooltip would go off the right edge, position it to the left of cursor
  if (tooltipPosition.x + 320 > window.innerWidth) {
    adjustedPosition.left = tooltipPosition.x - 320 - 10;
  }

  // If tooltip would go off the bottom edge, position it above cursor
  if (tooltipPosition.y + 400 > window.innerHeight) {
    adjustedPosition.top = tooltipPosition.y - 400 - 10;
  }

  return (
    <div
      className="absolute z-50 pointer-events-none"
      style={{
        left: adjustedPosition.left,
        top: adjustedPosition.top,
      }}
    >
      <div className="bg-white rounded-lg shadow-lg border border-gray-200 p-4 max-w-sm">
        {tooltipType === 'h3' && <H3Tooltip data={tooltipData} />}
        {tooltipType === 'kommune' && <KommuneTooltip data={tooltipData} />}
        {tooltipType === 'bnbo' && <BNBOTooltip data={tooltipData} />}
        {tooltipType === 'unknown' && (
          <div className="text-sm text-gray-600">
            <p>Unknown data type</p>
            <pre className="text-xs mt-2 overflow-hidden">
              {JSON.stringify(tooltipData, null, 2).slice(0, 200)}...
            </pre>
          </div>
        )}
      </div>
    </div>
  );
};

export default MapTooltip; 