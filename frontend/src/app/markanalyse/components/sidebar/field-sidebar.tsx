'use client';

import React, { useState } from 'react';
import {
  Layers,
  Filter,
  Calendar,
  Settings,
  Download,
  Wheat,
  Droplets,
  Wind,
  Waves,
  Home,
} from 'lucide-react';
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
import { VisualizationModeSelect } from '@/app/markanalyse/components/shared/visualization-mode-select';

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
      defaultExpanded={true}
      collapsible
      onExpandedChange={onExpandedChange}
      data-testid="layer-control-panel"
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

  const layerConfigs = [
    {
      key: 'fields' as const,
      name: 'Landbrugsmarker',
      description: '617.774 marker med pesticidforbrug og miljodata',
      icon: Wheat,
    },
    {
      key: 'bnbo' as const,
      name: 'BNBO Omrader',
      description: '2.761 boringsnaere beskyttelsesomrader',
      icon: Droplets,
    },
    {
      key: 'wetlands' as const,
      name: 'Lavbundsomrader',
      description: '768.646 lavbundsjorder med torvindhold',
      icon: Wind,
    },
    {
      key: 'water_projects' as const,
      name: 'Vandprojekter',
      description: '2.138 vandprojekter til miljogenopretning',
      icon: Waves,
    },
    {
      key: 'buildings' as const,
      name: 'Bygninger',
      description: '268.260 bygninger taet pa pesticidmarker',
      icon: Home,
    },
  ];

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
            <div className="space-y-2" data-testid="filter-control">
              <label className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
                Visualisering
              </label>
              <VisualizationModeSelect
                value={filterState.visualizationMode}
                onChange={(mode) => onFilterChange({ visualizationMode: mode })}
                className="w-full"
              />
            </div>

            {layerConfigs.map((layer) => {
              const visible = layerVisibility[layer.key];
              const Icon = layer.icon;

              return (
                <div
                  key={layer.key}
                  className="hover:bg-accent/50 rounded p-2 transition-colors"
                  data-testid="layer-item"
                >
                  <label className="flex cursor-pointer items-start gap-3 text-sm">
                    <input
                      type="checkbox"
                      checked={visible}
                      onChange={() => onLayerToggle(layer.key)}
                      data-testid="layer-toggle"
                      aria-label={layer.name}
                      className="text-primary border-border focus:ring-primary mt-0.5 h-4 w-4 rounded focus:ring-2"
                    />
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <Icon className="h-4 w-4 flex-shrink-0" />
                        <span className="font-medium" data-testid="layer-name">
                          {layer.name}
                        </span>
                      </div>
                      <p
                        className="text-muted-foreground mt-1 text-xs"
                        data-testid="layer-description"
                      >
                        {layer.description}
                      </p>
                    </div>
                  </label>
                </div>
              );
            })}
          </div>
        </SidebarGroup>
      )}

      {/* Filter Controls */}
      {activeSection === 'filters' && (
        <SidebarGroup label="Filtre">
          <div className="space-y-4 px-3 py-2">
            <div data-testid="filter-control">
              <label className="hover:bg-accent/50 flex cursor-pointer items-center gap-3 rounded p-2 text-sm transition-colors">
                <input
                  type="checkbox"
                  checked={filterState.organicOnly}
                  onChange={(e) =>
                    onFilterChange({ organicOnly: e.target.checked })
                  }
                  data-testid="organic-only-checkbox"
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span className="font-medium">Kun økologiske marker</span>
              </label>
            </div>

            <div className="space-y-2" data-testid="filter-control">
              <label className="text-muted-foreground text-xs font-medium tracking-wider uppercase">
                Visualisering
              </label>
              <VisualizationModeSelect
                value={filterState.visualizationMode}
                onChange={(mode) => onFilterChange({ visualizationMode: mode })}
                className="w-full"
              />
            </div>

            <div data-testid="filter-control">
              <label className="hover:bg-accent/50 flex cursor-pointer items-center gap-3 rounded p-2 text-sm transition-colors">
                <input
                  type="checkbox"
                  checked={filterState.useDecileColoring}
                  onChange={(e) =>
                    onFilterChange({ useDecileColoring: e.target.checked })
                  }
                  data-testid="decile-coloring-checkbox"
                  className="text-primary border-border focus:ring-primary h-4 w-4 rounded focus:ring-2"
                />
                <span className="font-medium">Decile farvning</span>
              </label>
            </div>
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
                  data-testid={`year-${year}-radio`}
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
