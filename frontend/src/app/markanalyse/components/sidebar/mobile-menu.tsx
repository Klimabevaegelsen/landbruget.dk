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
  const layerConfigs = [
    {
      key: 'fields' as const,
      name: 'Landbrugsmarker',
      description: '617.774 marker med pesticidforbrug og miljødata',
    },
    {
      key: 'bnbo' as const,
      name: 'BNBO Områder',
      description: '2.761 boringsnære beskyttelsesområder',
    },
    {
      key: 'wetlands' as const,
      name: 'Lavbundsområder',
      description: '768.646 lavbundsjorder med tørvindhold',
    },
    {
      key: 'water_projects' as const,
      name: 'Vandprojekter',
      description: '2.138 vandprojekter til miljøgenopretning',
    },
    {
      key: 'buildings' as const,
      name: 'Bygninger',
      description: '268.260 bygninger inden for 100m af pesticidmarker',
    },
  ];

  return (
    <MobileMenu
      title="Markanalyse"
      description={`Data for ${getYearRangeDisplay(yearSelection.selectedYear)}`}
    >
      {/* Layer Controls */}
      <MobileMenuSection title="Kortlag">
        {layerConfigs.map((layer) => (
          <div key={layer.key} className="space-y-1">
            <MobileMenuItem
              icon={
                layerVisibility[layer.key] ? (
                  <Eye className="h-4 w-4" />
                ) : (
                  <EyeOff className="h-4 w-4" />
                )
              }
              label={layer.name}
              active={layerVisibility[layer.key]}
              onClick={() => onLayerToggle(layer.key)}
            />
            <p className="text-xs text-muted-foreground px-4 pb-2">
              {layer.description}
            </p>
          </div>
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
            Visualiseringsmodus
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
              Total pesticidbelastning
            </option>
            <option value="pfas_belastning">PFAS belastning</option>
            <option value="diquat_belastning">Diquat belastning</option>
            <option value="glyphosate_belastning">
              Glyphosate belastning
            </option>
            <option value="applications_count">Antal applikationer</option>
            <option value="organic_status">Økologisk status</option>
            <option value="area_size">Markareal</option>
          </select>
        </div>

        <div className="space-y-2 px-4 py-2">
          <label className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
            Farveskala enhed
          </label>
          <select
            value={filterState.colorUnit}
            onChange={(e) =>
              onFilterChange({
                colorUnit: e.target.value as FilterState['colorUnit'],
              })
            }
            className="touch-target border-border bg-background w-full rounded-lg border px-3 py-3 text-sm"
          >
            <option value="total">Total mængde (kg/L)</option>
            <option value="per_hectare">Per hektar</option>
            <option value="belastning">Belastning (anbefalet)</option>
            <option value="applications">Antal applikationer</option>
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

        <div className="px-4 py-2">
          <button
            onClick={() =>
              onFilterChange({
                organicOnly: false,
                visualizationMode: 'total_pesticide_belastning',
                colorUnit: 'belastning',
                useDecileColoring: true,
              })
            }
            className="w-full rounded-lg bg-muted px-3 py-3 text-sm text-muted-foreground transition-colors hover:bg-muted/80"
          >
            Nulstil filtre
          </button>
        </div>
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
      <MobileMenuSection title="Datastatistik">
        <div className="bg-muted space-y-2 rounded-lg px-4 py-3">
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">Landbrugsmarker</span>
            <span className="text-sm font-medium">617.774</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">BNBO områder</span>
            <span className="text-sm font-medium">2.761</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">Lavbundsjorder</span>
            <span className="text-sm font-medium">768.646</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">Vandprojekter</span>
            <span className="text-sm font-medium">2.138</span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-muted-foreground text-sm">Bygninger</span>
            <span className="text-sm font-medium">268.260</span>
          </div>
        </div>

        {/* Chemical-specific stats based on visualization mode */}
        {filterState.visualizationMode === 'pfas_belastning' && (
          <div className="bg-red-50 border border-red-200 space-y-1 rounded-lg px-4 py-3">
            <div className="text-red-800 text-sm font-medium">PFAS Pesticider</div>
            <div className="text-red-700 text-xs">156.025 marker med PFAS</div>
            <div className="text-red-700 text-xs">Potentielt sundhedsskadelige</div>
          </div>
        )}

        {filterState.visualizationMode === 'diquat_belastning' && (
          <div className="bg-blue-50 border border-blue-200 space-y-1 rounded-lg px-4 py-3">
            <div className="text-blue-800 text-sm font-medium">Diquat Pesticider</div>
            <div className="text-blue-700 text-xs">471 marker med diquat</div>
            <div className="text-blue-700 text-xs">Kontakt herbicid</div>
          </div>
        )}

        {filterState.visualizationMode === 'glyphosate_belastning' && (
          <div className="bg-green-50 border border-green-200 space-y-1 rounded-lg px-4 py-3">
            <div className="text-green-800 text-sm font-medium">Glyphosate Pesticider</div>
            <div className="text-green-700 text-xs">105.511 marker med glyphosate</div>
            <div className="text-green-700 text-xs">Mest anvendte ukrudtsmiddel</div>
          </div>
        )}
      </MobileMenuSection>
    </MobileMenu>
  );
}
