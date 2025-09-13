'use client';

import React from 'react';
import { Card, CardContent } from '@/components/ui/card';

interface ColorScheme {
  name: string;
  colors: string[];
  property: string;
}

interface FertilizerColorLegendProps {
  scheme: ColorScheme;
  range?: [number, number];
}

export function FertilizerColorLegend({ scheme, range }: FertilizerColorLegendProps) {
  const formatValue = (value: number): string => {
    if (value >= 1000000) {
      return `${(value / 1000000).toFixed(1)}M`;
    } else if (value >= 1000) {
      return `${(value / 1000).toFixed(1)}K`;
    } else if (value >= 1) {
      return Math.round(value).toString();
    } else {
      return value.toFixed(2);
    }
  };

  if (!range || range[0] === range[1]) {
    return null;
  }

  const [min, max] = range;
  const steps = scheme.colors.length;
  
  // Generate intermediate values
  const values = Array.from({ length: steps }, (_, i) => {
    const ratio = i / (steps - 1);
    return min + (max - min) * ratio;
  });

  const getSchemeDescription = (property: string): string => {
    switch (property) {
      case 'total_nitrogen_production':
        return 'Samlet kvælstofproduktion fra husdyrgødning og organiske kilder (kg N/år)';
      case 'total_phosphorus_production':
        return 'Samlet fosforproduktion fra husdyrgødning og organiske kilder (kg P/år)';
      case 'commercial_fertilizer_usage':
        return 'Forbrug af indkøbt handelsgødning (kg N/år)';
      case 'biogas_production':
        return 'Estimeret biogasproduktion fra afgasset biomasse (kg/år)';
      case 'manure_diversity':
        return 'Mangfoldighed og volumen af forskellige gødningstyper';
      case 'nutrient_balance':
        return 'Næringsstofbalance: kvote minus forbrug (positiv = overskud)';
      default:
        return 'Fertilizer og næringsstof produktion/forbrug';
    }
  };

  return (
    <Card className="w-72 border shadow-sm">
      <CardContent className="p-3">
        <div className="text-sm font-medium mb-2">{scheme.name}</div>
        
        {/* Color bar */}
        <div className="flex h-4 rounded overflow-hidden mb-2">
          {scheme.colors.map((color, index) => (
            <div
              key={index}
              className="flex-1"
              style={{ backgroundColor: color }}
            />
          ))}
        </div>
        
        {/* Value labels */}
        <div className="flex justify-between text-xs text-muted-foreground mb-2">
          <span>{formatValue(min)}</span>
          <span>{formatValue((min + max) / 2)}</span>
          <span>{formatValue(max)}</span>
        </div>
        
        {/* Description */}
        <div className="text-xs text-muted-foreground leading-relaxed">
          {getSchemeDescription(scheme.property)}
        </div>

        {/* Additional context for specific schemes */}
        {scheme.property === 'nutrient_balance' && (
          <div className="mt-2 text-xs">
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 rounded" style={{ backgroundColor: scheme.colors[0] }}></div>
              <span className="text-red-600">Underskud</span>
            </div>
            <div className="flex items-center gap-2 mt-1">
              <div className="w-3 h-3 rounded" style={{ backgroundColor: scheme.colors[scheme.colors.length - 1] }}></div>
              <span className="text-green-600">Overskud</span>
            </div>
          </div>
        )}

        {scheme.property === 'manure_diversity' && (
          <div className="mt-2 text-xs text-muted-foreground">
            Højere værdi = flere gødningstyper og større volumen
          </div>
        )}
      </CardContent>
    </Card>
  );
}
