'use client';

import React from 'react';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Building2, MapPin, Droplets, Beaker, Recycle, Factory } from 'lucide-react';
import { FertilizerTooltipData } from '../livestock-analysis/types';

interface FertilizerMapTooltipProps {
  data: FertilizerTooltipData;
  position: { x: number; y: number };
  visualizationMode: string;
}

export function FertilizerMapTooltip({
  data,
  position,
  visualizationMode
}: FertilizerMapTooltipProps) {
  const formatNumber = (value: number | undefined | null): string => {
    if (value == null || isNaN(value)) return 'N/A';
    return new Intl.NumberFormat('da-DK').format(Math.round(value));
  };

  const formatDecimal = (value: number | undefined | null, decimals = 1): string => {
    if (value == null || isNaN(value)) return 'N/A';
    return new Intl.NumberFormat('da-DK', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    }).format(value);
  };

  // Position tooltip to avoid screen edges
  const tooltipStyle: React.CSSProperties = {
    position: 'absolute',
    left: position.x > window.innerWidth - 320 ? position.x - 340 : position.x + 10,
    top: position.y > window.innerHeight - 220 ? position.y - 230 : position.y + 10,
    zIndex: 1000,
    pointerEvents: 'none',
  };

  const getPrimaryMetric = () => {
    switch (visualizationMode) {
      case 'nitrogen_production':
        return {
          label: 'Kvælstofproduktion',
          value: formatNumber(data.total_nitrogen_production),
          icon: Droplets,
          unit: 'kg N/år',
          color: 'text-blue-600'
        };
      case 'phosphorus_production':
        return {
          label: 'Fosforproduktion',
          value: formatNumber(data.total_phosphorus_production),
          icon: Beaker,
          unit: 'kg P/år',
          color: 'text-orange-600'
        };
      case 'commercial_fertilizer':
        return {
          label: 'Handelsgødning',
          value: formatNumber(data.commercial_fertilizer_usage),
          icon: Factory,
          unit: 'kg N/år',
          color: 'text-green-600'
        };
      case 'biogas':
        return {
          label: 'Biogasproduktion',
          value: formatNumber(data.biogas_production),
          icon: Recycle,
          unit: 'kg biomasse/år',
          color: 'text-purple-600'
        };
      case 'manure_types':
        return {
          label: 'Gødningsblanding',
          value: data.dominant_fertilizer_type || 'Blandet',
          icon: Recycle,
          unit: '',
          color: 'text-amber-600'
        };
      case 'nutrient_balance':
        return {
          label: 'Næringsstofbalance',
          value: formatNumber(data.total_nitrogen_production),
          icon: Droplets,
          unit: 'balance',
          color: 'text-emerald-600'
        };
      default:
        return {
          label: 'Kvælstofproduktion',
          value: formatNumber(data.total_nitrogen_production),
          icon: Droplets,
          unit: 'kg N/år',
          color: 'text-blue-600'
        };
    }
  };

  const primaryMetric = getPrimaryMetric();
  const IconComponent = primaryMetric.icon;

  return (
    <div style={tooltipStyle}>
      <Card className="w-80 border shadow-lg">
        <CardContent className="p-4">
          {/* Header */}
          <div className="mb-3">
            <div className="flex items-start justify-between">
              <div className="flex-1 min-w-0">
                <h3 className="text-sm font-semibold text-foreground truncate">
                  {data.company_name}
                </h3>
                <div className="flex items-center gap-1 mt-1">
                  <Building2 className="h-3 w-3 text-muted-foreground" />
                  <span className="text-xs text-muted-foreground">
                    CVR: {data.cvr_number}
                  </span>
                </div>
              </div>
              <Badge variant="outline" className="text-xs">
                {data.dominant_fertilizer_type || 'Gødning'}
              </Badge>
            </div>
            
            <div className="flex items-center gap-1 mt-1">
              <MapPin className="h-3 w-3 text-muted-foreground" />
              <span className="text-xs text-muted-foreground">
                {data.municipality}
              </span>
            </div>
          </div>

          {/* Primary Metric */}
          <div className="mb-3 p-3 bg-muted/30 rounded-lg">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <IconComponent className={`h-4 w-4 ${primaryMetric.color}`} />
                <span className="text-sm font-medium">{primaryMetric.label}</span>
              </div>
              <div className="text-right">
                <div className={`text-lg font-bold ${primaryMetric.color}`}>
                  {primaryMetric.value}
                </div>
                {primaryMetric.unit && (
                  <div className="text-xs text-muted-foreground">
                    {primaryMetric.unit}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Additional Metrics Grid */}
          <div className="grid grid-cols-2 gap-2 text-xs">
            {visualizationMode !== 'nitrogen_production' && (
              <div className="text-center p-2 bg-muted/20 rounded">
                <div className="font-medium text-blue-600">
                  {formatNumber(data.total_nitrogen_production)}
                </div>
                <div className="text-muted-foreground">kg N</div>
              </div>
            )}
            
            {visualizationMode !== 'phosphorus_production' && (
              <div className="text-center p-2 bg-muted/20 rounded">
                <div className="font-medium text-orange-600">
                  {formatNumber(data.total_phosphorus_production)}
                </div>
                <div className="text-muted-foreground">kg P</div>
              </div>
            )}
            
            {visualizationMode !== 'commercial_fertilizer' && (
              <div className="text-center p-2 bg-muted/20 rounded">
                <div className="font-medium text-green-600">
                  {formatNumber(data.commercial_fertilizer_usage)}
                </div>
                <div className="text-muted-foreground">Handels N</div>
              </div>
            )}

            {visualizationMode !== 'biogas' && (
              <div className="text-center p-2 bg-muted/20 rounded">
                <div className="font-medium text-purple-600">
                  {formatNumber(data.biogas_production)}
                </div>
                <div className="text-muted-foreground">Biogas</div>
              </div>
            )}
          </div>

          {/* Fertilizer Type Breakdown */}
          <div className="mt-3 pt-2 border-t">
            <div className="text-xs font-medium text-muted-foreground mb-1">
              Gødningstyper:
            </div>
            <div className="flex flex-wrap gap-1">
              {data.total_nitrogen_production && data.total_nitrogen_production > 0 && (
                <Badge variant="secondary" className="text-xs">Organisk</Badge>
              )}
              {data.commercial_fertilizer_usage && data.commercial_fertilizer_usage > 0 && (
                <Badge variant="secondary" className="text-xs">Kommerciel</Badge>
              )}
              {data.biogas_production && data.biogas_production > 0 && (
                <Badge variant="secondary" className="text-xs">Biogas</Badge>
              )}
            </div>
          </div>

          {/* Coordinates */}
          <div className="mt-2 pt-2 border-t text-xs text-muted-foreground">
            <div className="flex justify-between">
              <span>Koordinater:</span>
              <span>
                {formatDecimal(data.coordinate[1], 4)}°, {formatDecimal(data.coordinate[0], 4)}°
              </span>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
