'use client';

import React from 'react';
import { Layers, Filter, Calendar, Eye, EyeOff } from 'lucide-react';
import {
  MobileMenu,
  MobileMenuSection,
  MobileMenuItem,
} from '@/components/ui/mobile-menu';
import {
  LayerVisibility,
  FilterState,
  YearSelection,
  getYearRangeDisplay,
} from '@/components/field-analysis/types';

interface MobileFieldMenuProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  yearSelection: YearSelection;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
  onYearChange: (year: number) => void;
}

export function MobileFieldMenu({
  layerVisibility,
  filterState,
  yearSelection,
  onLayerToggle,
  onFilterChange,
  onYearChange,
}: MobileFieldMenuProps) {
  const layerLabels: Record<keyof LayerVisibility, string> = {
    fields: 'Marker',
    bnbo: 'BNBO områder',
    wetlands: 'Lavbundsjorder',
    water_projects: 'Vandprojekter',
    buildings: 'Bygninger',
  };

  return (
    <MobileMenu
      title="Markanalyse"
      description={`Data for ${getYearRangeDisplay(yearSelection.selectedYear)}`}
    >
      {/* Layer Controls */}
      <MobileMenuSection title="Kortlag">
        {Object.entries(layerVisibility).map(([key, visible]) => (
          <MobileMenuItem
            key={key}
            icon={
              visible ? (
                <Eye className="h-4 w-4" />
              ) : (
                <EyeOff className="h-4 w-4" />
              )
            }
            label={layerLabels[key as keyof LayerVisibility]}
            active={visible}
            onClick={() => onLayerToggle(key as keyof LayerVisibility)}
          />
        ))}
      </MobileMenuSection>

      {/* Filter Controls */}
      <MobileMenuSection title="Filtre">
        <MobileMenuItem
          icon={<Filter className="h-4 w-4" />}
          label="Kun økologiske marker"
          active={filterState.organicOnly}
          onClick={() =>
            onFilterChange({ organicOnly: !filterState.organicOnly })
          }
        />

        <div className="space-y-2 px-4 py-2">
          <label className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
            Visualisering
          </label>
          <select
            value={filterState.visualizationMode}
            onChange={(e) =>
              onFilterChange({
                visualizationMode: e.target
                  .value as FilterState['visualizationMode'],
              })
            }
            className="touch-target border-border bg-background w-full rounded-lg border px-3 py-3 text-sm"
          >
            <option value="total_pesticide_belastning">
              Total pesticide belastning
            </option>
            <option value="organic_status">Økologi status</option>
            <option value="risk_level">Risiko niveau</option>
          </select>
        </div>

        <MobileMenuItem
          icon={<Layers className="h-4 w-4" />}
          label="Decile farvning"
          active={filterState.useDecileColoring}
          onClick={() =>
            onFilterChange({
              useDecileColoring: !filterState.useDecileColoring,
            })
          }
        />
      </MobileMenuSection>

      {/* Year Selection */}
      <MobileMenuSection title="Dataår">
        {yearSelection.availableYears.map((year) => (
          <MobileMenuItem
            key={year}
            icon={<Calendar className="h-4 w-4" />}
            label={`${year} data`}
            active={yearSelection.selectedYear === year}
            onClick={() => onYearChange(year)}
          />
        ))}
      </MobileMenuSection>

      {/* Quick Stats */}
      <MobileMenuSection title="Statistik">
        <div className="bg-muted space-y-2 rounded-lg px-4 py-3">
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">Marker</span>
            <span className="text-sm font-medium">617.774</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">BNBO områder</span>
            <span className="text-sm font-medium">2.761</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">
              Lavbundsjorder
            </span>
            <span className="text-sm font-medium">768.646</span>
          </div>
        </div>
      </MobileMenuSection>
    </MobileMenu>
  );
}
