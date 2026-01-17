'use client';

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group';
import {
  LayerVisibility,
  FilterState,
  VisualizationMode,
  ColorUnit,
} from './types';
import {
  Wheat,
  Droplets,
  Wind,
  Waves,
  Home,
  TestTube,
  Leaf,
  Eye,
  Palette,
  Calendar,
  Download,
} from 'lucide-react';

interface LayerControlPanelEnhancedProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
  onDownloadCSV?: () => void;
  isDownloadEnabled?: boolean;
  currentZoom?: number;
}

export function LayerControlPanelEnhanced({
  layerVisibility,
  filterState,
  onLayerToggle,
  onFilterChange,
  onDownloadCSV,
  isDownloadEnabled = false,
  currentZoom = 0,
}: LayerControlPanelEnhancedProps) {
  const layerConfigs = [
    {
      key: 'fields' as const,
      name: 'Landbrugsmarker',
      description: '617.774 marker med pesticidforbrug og miljødata',
      icon: Wheat,
      count: '617k',
      color: 'bg-organic/10 text-organic border-organic/20',
    },
    {
      key: 'bnbo' as const,
      name: 'BNBO Områder',
      description: '2.761 boringsnære beskyttelsesområder',
      icon: Droplets,
      count: '2.8k',
      color: 'bg-bnbo/10 text-bnbo border-bnbo/20',
    },
    {
      key: 'wetlands' as const,
      name: 'Lavbundsområder',
      description: '768.646 lavbundsjorder med tørvindhold',
      icon: Wind,
      count: '769k',
      color: 'bg-wetland/10 text-wetland border-wetland/20',
    },
    {
      key: 'water_projects' as const,
      name: 'Vandprojekter',
      description: '2.138 vandprojekter til miljøgenopretning',
      icon: Waves,
      count: '2.1k',
      color: 'bg-primary/10 text-primary border-primary/20',
    },
    {
      key: 'buildings' as const,
      name: 'Bygninger',
      description: '268.260 bygninger inden for 100m af pesticidmarker',
      icon: Home,
      count: '268k',
      color: 'bg-muted text-muted-foreground border-border',
    },
  ];

  const visualizationModes = [
    {
      value: 'total_pesticide_belastning',
      label: 'Total Pesticidbelastning',
      icon: TestTube,
    },
    { value: 'pfas_belastning', label: 'PFAS Belastning', icon: TestTube },
    {
      value: 'glyphosate_belastning',
      label: 'Glyphosate Belastning',
      icon: TestTube,
    },
    { value: 'diquat_belastning', label: 'Diquat Belastning', icon: TestTube },
  ];

  return (
    <div className="space-y-4 p-4" data-testid="layer-control-panel">
      {/* Layer Visibility Controls */}
      <Card data-testid="layer-visibility-card">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Eye className="h-4 w-4" />
            Lag Synlighed
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4" data-testid="layer-visibility-controls">
          {layerConfigs.map((layer) => {
            const Icon = layer.icon;
            const isVisible = layerVisibility[layer.key];

            return (
              <div key={layer.key} className="flex items-start space-x-3" data-testid={`layer-toggle-${layer.key}`}>
                <Switch
                  id={layer.key}
                  checked={isVisible}
                  onCheckedChange={() => onLayerToggle(layer.key)}
                  data-testid={`layer-switch-${layer.key}`}
                />
                <div className="flex-1 space-y-1">
                  <div className="flex items-center gap-2">
                    <Label
                      htmlFor={layer.key}
                      className="flex cursor-pointer items-center gap-2 font-medium"
                    >
                      <Icon className="h-4 w-4" />
                      {layer.name}
                    </Label>
                    <Badge
                      variant="outline"
                      className={`text-xs ${layer.color}`}
                      data-testid={`layer-badge-${layer.key}`}
                    >
                      {layer.count}
                    </Badge>
                  </div>
                  <p className="text-muted-foreground text-xs">
                    {layer.description}
                  </p>
                </div>
              </div>
            );
          })}
        </CardContent>
      </Card>

      {/* Visualization Controls */}
      <Card data-testid="visualization-card">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Palette className="h-4 w-4" />
            Visualisering
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4" data-testid="visualization-controls">
          {/* Visualization Mode */}
          <div className="space-y-2" data-testid="visualization-mode">
            <Label className="text-sm font-medium">Visualiseringsmode</Label>
            <Select
              value={filterState.visualizationMode}
              onValueChange={(value) =>
                onFilterChange({
                  visualizationMode: value as VisualizationMode,
                })
              }
            >
              <SelectTrigger data-testid="visualization-mode-select">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {visualizationModes.map((mode) => {
                  const Icon = mode.icon;
                  return (
                    <SelectItem key={mode.value} value={mode.value}>
                      <div className="flex items-center gap-2">
                        <Icon className="h-4 w-4" />
                        {mode.label}
                      </div>
                    </SelectItem>
                  );
                })}
              </SelectContent>
            </Select>
          </div>

          {/* Color Unit Toggle */}
          <div className="space-y-2" data-testid="color-unit-toggle">
            <Label className="text-sm font-medium">Farveenhed</Label>
            <ToggleGroup
              type="single"
              value={filterState.colorUnit}
              onValueChange={(value) =>
                value && onFilterChange({ colorUnit: value as ColorUnit })
              }
              className="justify-start"
              data-testid="color-unit-group"
            >
              <ToggleGroupItem value="belastning" aria-label="Belastning" data-testid="color-unit-belastning">
                Belastning
              </ToggleGroupItem>
              <ToggleGroupItem value="dosage" aria-label="Dosering" data-testid="color-unit-dosage">
                Dosering
              </ToggleGroupItem>
            </ToggleGroup>
          </div>

          {/* Organic Filter */}
          <div className="flex items-center space-x-3" data-testid="organic-filter">
            <Switch
              id="organic-only"
              checked={filterState.organicOnly}
              onCheckedChange={(checked) =>
                onFilterChange({ organicOnly: checked })
              }
              data-testid="organic-only-switch"
            />
            <Label
              htmlFor="organic-only"
              className="flex cursor-pointer items-center gap-2"
            >
              <Leaf className="h-4 w-4 text-green-600" />
              Kun økologiske marker
            </Label>
          </div>

          {/* Decile Coloring */}
          <div className="flex items-center space-x-3" data-testid="decile-coloring-filter">
            <Switch
              id="decile-coloring"
              checked={filterState.useDecileColoring}
              onCheckedChange={(checked) =>
                onFilterChange({ useDecileColoring: checked })
              }
              data-testid="decile-coloring-switch"
            />
            <Label htmlFor="decile-coloring" className="cursor-pointer">
              Brug decil-farvning
            </Label>
          </div>
        </CardContent>
      </Card>

      {/* Quick Stats */}
      <Card data-testid="stats-card">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Calendar className="h-4 w-4" />
            Datastatistik
          </CardTitle>
        </CardHeader>
        <CardContent data-testid="stats-content">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="space-y-1">
              <div className="text-muted-foreground">Synlige lag:</div>
              <div className="font-medium">
                {Object.values(layerVisibility).filter(Boolean).length} /{' '}
                {Object.keys(layerVisibility).length}
              </div>
            </div>
            <div className="space-y-1">
              <div className="text-muted-foreground">Aktive filtre:</div>
              <div className="font-medium">
                {(filterState.organicOnly ? 1 : 0) +
                  (filterState.useDecileColoring ? 0 : 1)}
              </div>
            </div>
          </div>

          {filterState.organicOnly && (
            <div className="mt-3 rounded-md bg-green-50 p-2 dark:bg-green-950/20">
              <div className="flex items-center gap-2 text-sm text-green-700 dark:text-green-300">
                <Leaf className="h-4 w-4" />
                Kun økologiske marker vises
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* CSV Download */}
      <Card data-testid="download-card">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Download className="h-4 w-4" />
            Eksporter Data
          </CardTitle>
        </CardHeader>
        <CardContent data-testid="download-controls">
          <div className="space-y-3">
            <p className="text-muted-foreground text-xs">
              Download synlige marker som CSV-fil. Kræver zoom niveau 10+ for at
              aktivere.
            </p>

            <div className="flex items-center gap-2">
              <Button
                onClick={onDownloadCSV}
                disabled={!isDownloadEnabled}
                size="sm"
                className="flex-1"
                data-testid="download-csv-button"
              >
                <Download className="mr-2 h-4 w-4" />
                Download CSV
              </Button>
            </div>

            <div className="text-muted-foreground text-xs" data-testid="zoom-level-info">
              Zoom niveau: {currentZoom.toFixed(1)}
              {currentZoom < 10 && ' (zoom ind for at aktivere)'}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
