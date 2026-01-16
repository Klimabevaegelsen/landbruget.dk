'use client';

import React, { useState } from 'react';
import { Layers, Filter, Calendar, Settings, Download } from 'lucide-react';
import {
  Sidebar,
  SidebarHeader,
  SidebarHeaderContent,
  SidebarContent,
  SidebarGroup,
  SidebarItem,
  useSidebar,
} from '@/components/ui/sidebar';
import { Button } from '@/components/ui/button';
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
  onDownloadCSV?: () => void;
  currentZoom?: number;
}

export function FieldSidebar({
  layerVisibility,
  filterState,
  yearSelection,
  onLayerToggle,
  onFilterChange,
  onYearChange,
  onExpandedChange,
  onDownloadCSV,
  currentZoom = 0,
}: FieldSidebarProps) {
  const [activeSection, setActiveSection] = useState<
    'layers' | 'filters' | 'years' | 'settings' | 'export'
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
          <SidebarItem
            icon={<Download className="h-4 w-4" />}
            label="Eksporter"
            active={activeSection === 'export'}
            onClick={() => setActiveSection('export')}
          />
        </SidebarGroup>

        <FieldSidebarExpandedContent
          activeSection={activeSection}
          layerVisibility={layerVisibility}
          filterState={filterState}
          yearSelection={yearSelection}
          onLayerToggle={onLayerToggle}
          onFilterChange={onFilterChange}
          onYearChange={onYearChange}
          onDownloadCSV={onDownloadCSV}
          currentZoom={currentZoom}
        />
      </SidebarContent>
    </Sidebar>
  );
}

// Separate component to handle the expanded content with direct access to sidebar context
function FieldSidebarExpandedContent({
  activeSection,
  layerVisibility,
  filterState,
  yearSelection,
  onLayerToggle,
  onFilterChange,
  onYearChange,
  onDownloadCSV,
  currentZoom = 0,
}: {
  activeSection: 'layers' | 'filters' | 'years' | 'settings' | 'export';
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  yearSelection: YearSelection;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
  onYearChange: (year: number) => void;
  onDownloadCSV?: () => void;
  currentZoom?: number;
}) {
  const { isExpanded } = useSidebar();

  // Don't render any detailed content when collapsed
  if (!isExpanded) {
    return null;
  }

  return (
    <>
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
                    onChange={() => onLayerToggle(key as keyof LayerVisibility)}
                    className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                  />
                  <span className="font-medium">{layerNames[key] || key}</span>
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
                <option value="applications_count">Antal pesticider</option>
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

      {/* Export Section */}
      {activeSection === 'export' && (
        <SidebarGroup label="Eksporter Data">
          <div className="space-y-3 px-3 py-2">
            <p className="text-muted-foreground text-sm">
              Download synlige marker som CSV-fil. Kræver zoom niveau 10+ for at
              aktivere.
            </p>

            <Button
              onClick={onDownloadCSV}
              disabled={!onDownloadCSV || currentZoom < 10}
              size="sm"
              className="w-full"
            >
              <Download className="mr-2 h-4 w-4" />
              Download CSV
            </Button>

            <div className="text-muted-foreground text-xs">
              Zoom niveau: {currentZoom.toFixed(1)}
              {currentZoom < 10 && ' (zoom ind for at aktivere)'}
            </div>

            <div className="border-t pt-2">
              <p className="text-muted-foreground text-xs">
                CSV-filen indeholder alle synlige marker med deres pesticiddata,
                miljødata og geografiske information.
              </p>
            </div>
          </div>
        </SidebarGroup>
      )}

      {/* Settings Panel */}
      {activeSection === 'settings' && (
        <SidebarGroup>
          <SettingsPanel />
        </SidebarGroup>
      )}
    </>
  );
}
