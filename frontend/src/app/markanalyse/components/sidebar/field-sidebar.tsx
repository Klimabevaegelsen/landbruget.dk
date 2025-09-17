'use client';

import React, { useState } from 'react';
import { Layers, Filter, Calendar, Settings } from 'lucide-react';
import {
  Sidebar,
  SidebarHeader,
  SidebarHeaderContent,
  SidebarContent,
  SidebarGroup,
  SidebarItem,
} from '@/components/ui/sidebar';
import {
  LayerVisibility,
  FilterState,
  YearSelection,
} from '@/components/field-analysis/types';
import { SettingsPanel } from '@/components/field-analysis/SettingsPanel';

interface FieldSidebarProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  yearSelection: YearSelection;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
  onYearChange: (year: number) => void;
  onExpandedChange?: (expanded: boolean) => void;
}

export function FieldSidebar({
  layerVisibility,
  filterState,
  yearSelection,
  onLayerToggle,
  onFilterChange,
  onYearChange,
  onExpandedChange,
}: FieldSidebarProps) {
  const [activeSection, setActiveSection] = useState<
    'layers' | 'filters' | 'years' | 'settings'
  >('layers');

  return (
    <Sidebar
      defaultExpanded={false}
      collapsible
      onExpandedChange={onExpandedChange}
    >
      <SidebarHeader className="flex items-center justify-center">
        <div className="bg-primary flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-xl shadow-sm">
          <Layers className="text-primary-foreground h-5 w-5" />
        </div>
        <SidebarHeaderContent className="ml-3 min-w-0 flex-1">
          <h2 className="text-foreground truncate text-base font-bold">
            Markanalyse
          </h2>
          <p className="text-muted-foreground truncate text-sm font-medium">
            {yearSelection.selectedYear} data
          </p>
        </SidebarHeaderContent>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarItem
            icon={<Layers className="h-4 w-4" />}
            label="Kortlag"
            active={activeSection === 'layers'}
            onClick={() => setActiveSection('layers')}
          />
          <SidebarItem
            icon={<Filter className="h-4 w-4" />}
            label="Filtre"
            active={activeSection === 'filters'}
            onClick={() => setActiveSection('filters')}
          />
          <SidebarItem
            icon={<Calendar className="h-4 w-4" />}
            label="År"
            active={activeSection === 'years'}
            onClick={() => setActiveSection('years')}
          />
          <SidebarItem
            icon={<Settings className="h-4 w-4" />}
            label="Indstillinger"
            active={activeSection === 'settings'}
            onClick={() => setActiveSection('settings')}
          />
        </SidebarGroup>

        {/* Layer Controls */}
        {activeSection === 'layers' && (
          <SidebarGroup label="Kortlag">
            <div className="space-y-3 px-3 py-2">
              {Object.entries(layerVisibility).map(([key, visible]) => {
                const layerNames: Record<string, string> = {
                  fields: 'Landbrugsmarker',
                  bnbo: 'BNBO Områder',
                  wetlands: 'Lavbundsområder',
                  water_projects: 'Vandprojekter',
                  buildings: 'Bygninger',
                };
                return (
                  <label
                    key={key}
                    className="hover:bg-accent/50 flex cursor-pointer items-center gap-3 rounded p-2 text-sm transition-colors"
                  >
                    <input
                      type="checkbox"
                      checked={visible}
                      onChange={() =>
                        onLayerToggle(key as keyof LayerVisibility)
                      }
                      className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                    />
                    <span className="font-medium">
                      {layerNames[key] || key}
                    </span>
                  </label>
                );
              })}
            </div>
          </SidebarGroup>
        )}

        {/* Filter Controls */}
        {activeSection === 'filters' && (
          <SidebarGroup label="Filtre">
            <div className="space-y-4 px-3 py-2">
              <label className="hover:bg-accent/50 flex cursor-pointer items-center gap-3 rounded p-2 text-sm transition-colors">
                <input
                  type="checkbox"
                  checked={filterState.organicOnly}
                  onChange={(e) =>
                    onFilterChange({ organicOnly: e.target.checked })
                  }
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span className="font-medium">Kun økologiske marker</span>
              </label>

              <div className="space-y-2">
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
                  className="border-border bg-background hover:bg-accent/20 focus:ring-primary w-full rounded-lg border px-3 py-2.5 text-sm transition-colors focus:border-transparent focus:ring-2"
                >
                  <option value="total_pesticide_belastning">
                    Total pesticidbelastning
                  </option>
                  <option value="pfas_belastning">PFAS belastning</option>
                  <option value="diquat_belastning">Diquat belastning</option>
                  <option value="glyphosate_belastning">
                    Glyphosate belastning
                  </option>
                  <option value="applications_count">
                    Antal applikationer
                  </option>
                  <option value="organic_status">Økologisk status</option>
                  <option value="area_size">Markareal</option>
                </select>
              </div>

              <label className="hover:bg-accent/50 flex cursor-pointer items-center gap-3 rounded p-2 text-sm transition-colors">
                <input
                  type="checkbox"
                  checked={filterState.useDecileColoring}
                  onChange={(e) =>
                    onFilterChange({ useDecileColoring: e.target.checked })
                  }
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span className="font-medium">Decile farvning</span>
              </label>
            </div>
          </SidebarGroup>
        )}

        {/* Year Selection */}
        {activeSection === 'years' && (
          <SidebarGroup label="År">
            <div className="space-y-3 px-3 py-2">
              {yearSelection.availableYears.map((year) => (
                <label
                  key={year}
                  className="hover:bg-accent/50 border-border/10 flex cursor-pointer items-center gap-4 rounded border-b p-3 text-sm transition-colors"
                >
                  <input
                    type="radio"
                    name="year"
                    value={year}
                    checked={yearSelection.selectedYear === year}
                    onChange={() => onYearChange(year)}
                    className="text-primary border-border focus:ring-primary mr-2 h-4 w-4 focus:ring-2"
                  />
                  <span className="inline-block min-w-[3rem] font-medium">
                    {year}
                  </span>
                </label>
              ))}
            </div>
          </SidebarGroup>
        )}

        {/* Settings Panel */}
        {activeSection === 'settings' && (
          <SidebarGroup>
            <SettingsPanel />
          </SidebarGroup>
        )}
      </SidebarContent>
    </Sidebar>
  );
}
