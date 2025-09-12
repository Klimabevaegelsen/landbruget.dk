'use client';

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
import { 
  ToggleGroup, 
  ToggleGroupItem 
} from '@/components/ui/toggle-group';
import { LayerVisibility, FilterState } from './types';
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
} from 'lucide-react';

interface LayerControlPanelEnhancedProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
}

export function LayerControlPanelEnhanced({
  layerVisibility,
  filterState,
  onLayerToggle,
  onFilterChange,
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
    { value: 'total_pesticide_belastning', label: 'Total Pesticidbelastning', icon: TestTube },
    { value: 'pfas_belastning', label: 'PFAS Belastning', icon: TestTube },
    { value: 'glyphosate_belastning', label: 'Glyphosate Belastning', icon: TestTube },
    { value: 'diquat_belastning', label: 'Diquat Belastning', icon: TestTube },
  ];

  return (
    <div className="space-y-4 p-4">
      {/* Layer Visibility Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Eye className="h-4 w-4" />
            Lag Synlighed
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {layerConfigs.map((layer) => {
            const Icon = layer.icon;
            const isVisible = layerVisibility[layer.key];
            
            return (
              <div key={layer.key} className="flex items-start space-x-3">
                <Switch
                  id={layer.key}
                  checked={isVisible}
                  onCheckedChange={() => onLayerToggle(layer.key)}
                />
                <div className="flex-1 space-y-1">
                  <div className="flex items-center gap-2">
                    <Label 
                      htmlFor={layer.key}
                      className="flex items-center gap-2 font-medium cursor-pointer"
                    >
                      <Icon className="h-4 w-4" />
                      {layer.name}
                    </Label>
                    <Badge variant="outline" className={`text-xs ${layer.color}`}>
                      {layer.count}
                    </Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    {layer.description}
                  </p>
                </div>
              </div>
            );
          })}
        </CardContent>
      </Card>

      {/* Visualization Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Palette className="h-4 w-4" />
            Visualisering
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Visualization Mode */}
          <div className="space-y-2">
            <Label className="text-sm font-medium">Visualiseringsmode</Label>
            <Select
              value={filterState.visualizationMode}
              onValueChange={(value) => onFilterChange({ visualizationMode: value })}
            >
              <SelectTrigger>
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
          <div className="space-y-2">
            <Label className="text-sm font-medium">Farveenhed</Label>
            <ToggleGroup
              type="single"
              value={filterState.colorUnit}
              onValueChange={(value) => value && onFilterChange({ colorUnit: value })}
              className="justify-start"
            >
              <ToggleGroupItem value="belastning" aria-label="Belastning">
                Belastning
              </ToggleGroupItem>
              <ToggleGroupItem value="dosage" aria-label="Dosering">
                Dosering
              </ToggleGroupItem>
            </ToggleGroup>
          </div>

          {/* Organic Filter */}
          <div className="flex items-center space-x-3">
            <Switch
              id="organic-only"
              checked={filterState.organicOnly}
              onCheckedChange={(checked) => onFilterChange({ organicOnly: checked })}
            />
            <Label htmlFor="organic-only" className="flex items-center gap-2 cursor-pointer">
              <Leaf className="h-4 w-4 text-green-600" />
              Kun økologiske marker
            </Label>
          </div>

          {/* Decile Coloring */}
          <div className="flex items-center space-x-3">
            <Switch
              id="decile-coloring"
              checked={filterState.useDecileColoring}
              onCheckedChange={(checked) => onFilterChange({ useDecileColoring: checked })}
            />
            <Label htmlFor="decile-coloring" className="cursor-pointer">
              Brug decil-farvning
            </Label>
          </div>
        </CardContent>
      </Card>

      {/* Quick Stats */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            <Calendar className="h-4 w-4" />
            Datastatistik
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="space-y-1">
              <div className="text-muted-foreground">Synlige lag:</div>
              <div className="font-medium">
                {Object.values(layerVisibility).filter(Boolean).length} / {Object.keys(layerVisibility).length}
              </div>
            </div>
            <div className="space-y-1">
              <div className="text-muted-foreground">Aktive filtre:</div>
              <div className="font-medium">
                {(filterState.organicOnly ? 1 : 0) + (filterState.useDecileColoring ? 0 : 1)}
              </div>
            </div>
          </div>
          
          {filterState.organicOnly && (
            <div className="mt-3 p-2 bg-green-50 dark:bg-green-950/20 rounded-md">
              <div className="flex items-center gap-2 text-sm text-green-700 dark:text-green-300">
                <Leaf className="h-4 w-4" />
                Kun økologiske marker vises
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
