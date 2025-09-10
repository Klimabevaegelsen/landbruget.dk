'use client';

import React, { useState } from 'react';
import { Layers, Filter, Calendar, Settings } from 'lucide-react';
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarGroup,
  SidebarItem,
} from '@/components/ui/sidebar';
import {
  LayerVisibility,
  FilterState,
  YearSelection,
} from '@/components/field-analysis/types';

interface FieldSidebarProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  yearSelection: YearSelection;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
  onYearChange: (year: number) => void;
}

export function FieldSidebar({
  layerVisibility,
  filterState,
  yearSelection,
  onLayerToggle,
  onFilterChange,
  onYearChange,
}: FieldSidebarProps) {
  const [activeSection, setActiveSection] = useState<
    'layers' | 'filters' | 'years' | 'settings'
  >('layers');

  return (
    <Sidebar defaultExpanded={false} collapsible>
      <SidebarHeader>
        <div className="flex items-center gap-3">
          <div className="bg-primary flex h-8 w-8 items-center justify-center rounded-lg">
            <Layers className="text-primary-foreground h-4 w-4" />
          </div>
          <div className="min-w-0 flex-1">
            <h2 className="text-foreground truncate text-sm font-semibold">
              Markanalyse
            </h2>
            <p className="text-muted-foreground truncate text-xs">
              {yearSelection.selectedYear} data
            </p>
          </div>
        </div>
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
            <div className="space-y-2 px-3">
              {Object.entries(layerVisibility).map(([key, visible]) => (
                <label
                  key={key}
                  className="flex cursor-pointer items-center gap-2 text-sm"
                >
                  <input
                    type="checkbox"
                    checked={visible}
                    onChange={() => onLayerToggle(key as keyof LayerVisibility)}
                    className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                  />
                  <span className="capitalize">{key.replace('_', ' ')}</span>
                </label>
              ))}
            </div>
          </SidebarGroup>
        )}

        {/* Filter Controls */}
        {activeSection === 'filters' && (
          <SidebarGroup label="Filtre">
            <div className="space-y-4 px-3">
              <label className="flex cursor-pointer items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={filterState.organicOnly}
                  onChange={(e) =>
                    onFilterChange({ organicOnly: e.target.checked })
                  }
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span>Kun økologiske marker</span>
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
                  className="border-border bg-background w-full rounded-md border px-3 py-2 text-sm"
                >
                  <option value="total_pesticide_belastning">
                    Total belastning
                  </option>
                  <option value="organic_status">Økologi status</option>
                  <option value="risk_level">Risiko niveau</option>
                </select>
              </div>

              <label className="flex cursor-pointer items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={filterState.useDecileColoring}
                  onChange={(e) =>
                    onFilterChange({ useDecileColoring: e.target.checked })
                  }
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span>Decile farvning</span>
              </label>
            </div>
          </SidebarGroup>
        )}

        {/* Year Selection */}
        {activeSection === 'years' && (
          <SidebarGroup label="År">
            <div className="space-y-2 px-3">
              {yearSelection.availableYears.map((year) => (
                <label
                  key={year}
                  className="flex cursor-pointer items-center gap-2 text-sm"
                >
                  <input
                    type="radio"
                    name="year"
                    value={year}
                    checked={yearSelection.selectedYear === year}
                    onChange={() => onYearChange(year)}
                    className="text-primary border-border focus:ring-primary h-4 w-4 focus:ring-2"
                  />
                  <span>{year}</span>
                </label>
              ))}
            </div>
          </SidebarGroup>
        )}
      </SidebarContent>
    </Sidebar>
  );
}
