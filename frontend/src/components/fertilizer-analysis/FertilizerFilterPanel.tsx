'use client';

import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Badge } from '@/components/ui/badge';
import { Slider } from '@/components/ui/slider';
import { Search, Filter, X, Droplets, Beaker, Factory, Recycle } from 'lucide-react';
import { FertilizerMapFilters } from '../livestock-analysis/types';

interface FertilizerFilterPanelProps {
  filters: FertilizerMapFilters;
  onFiltersChange: (filters: Partial<FertilizerMapFilters>) => void;
  availableFertilizerTypes?: string[];
  availableMunicipalities?: string[];
  availableYears?: number[];
  dataRanges?: {
    nitrogen: [number, number];
    phosphorus: [number, number];
    commercial: [number, number];
    biogas: [number, number];
  };
  className?: string;
}

export function FertilizerFilterPanel({
  filters,
  onFiltersChange,
  availableFertilizerTypes = [],
  availableMunicipalities = [],
  availableYears = [2023, 2022, 2021],
  dataRanges,
  className = '',
}: FertilizerFilterPanelProps) {
  const [cvrInput, setCvrInput] = useState('');

  const handleCvrSearch = () => {
    console.log('CVR search:', cvrInput);
    // This would trigger a search in the parent component
  };

  const handleCvrClear = () => {
    setCvrInput('');
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleCvrSearch();
    }
  };

  const clearFilters = () => {
    onFiltersChange({
      fertilizerType: undefined,
      municipality: undefined,
      minNitrogen: undefined,
      maxNitrogen: undefined,
      minPhosphorus: undefined,
      maxPhosphorus: undefined,
      minBiogas: undefined,
      maxBiogas: undefined,
      minCommercialFertilizer: undefined,
      maxCommercialFertilizer: undefined,
    });
    setCvrInput('');
  };

  const visualizationModes = [
    { value: 'nitrogen_production', label: 'Kvælstofproduktion', icon: Droplets, color: 'text-blue-600' },
    { value: 'phosphorus_production', label: 'Fosforproduktion', icon: Beaker, color: 'text-orange-600' },
    { value: 'commercial_fertilizer', label: 'Handelsgødning', icon: Factory, color: 'text-green-600' },
    { value: 'biogas', label: 'Biogasproduktion', icon: Recycle, color: 'text-purple-600' },
    { value: 'manure_types', label: 'Gødningstyper', icon: Recycle, color: 'text-amber-600' },
    { value: 'nutrient_balance', label: 'Næringsstofbalance', icon: Droplets, color: 'text-emerald-600' },
  ];

  return (
    <Card className={className}>
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            <Filter className="h-4 w-4" />
            Filtrer Gødningsdata
          </CardTitle>
          <Button
            variant="ghost"
            size="sm"
            onClick={clearFilters}
            className="text-xs"
          >
            <X className="h-3 w-3 mr-1" />
            Ryd
          </Button>
        </div>
      </CardHeader>

      <CardContent className="space-y-6">
        {/* Visualization Mode */}
        <div>
          <Label className="text-sm font-medium mb-3 block">Visualisering</Label>
          <RadioGroup
            value={filters.visualizationMode}
            onValueChange={(value) => onFiltersChange({ visualizationMode: value as any })}
            className="space-y-2"
          >
            {visualizationModes.map((mode) => {
              const IconComponent = mode.icon;
              return (
                <div key={mode.value} className="flex items-center space-x-2">
                  <RadioGroupItem value={mode.value} id={mode.value} />
                  <Label htmlFor={mode.value} className="flex items-center gap-2 text-sm cursor-pointer">
                    <IconComponent className={`h-4 w-4 ${mode.color}`} />
                    {mode.label}
                  </Label>
                </div>
              );
            })}
          </RadioGroup>
        </div>

        {/* Year Selection */}
        <div>
          <Label className="text-sm font-medium mb-2 block">År</Label>
          <Select
            value={filters.year?.toString()}
            onValueChange={(value) => onFiltersChange({ year: parseInt(value) })}
          >
            <SelectTrigger>
              <SelectValue placeholder="Vælg år" />
            </SelectTrigger>
            <SelectContent>
              {availableYears.map(year => (
                <SelectItem key={year} value={year.toString()}>
                  {year}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* CVR Search */}
        <div>
          <Label className="text-sm font-medium mb-2 block">CVR Nummer</Label>
          <div className="flex gap-2">
            <Input
              placeholder="Søg efter CVR..."
              value={cvrInput}
              onChange={(e) => setCvrInput(e.target.value)}
              onKeyPress={handleKeyPress}
              className="flex-1"
            />
            <Button
              variant="outline"
              size="sm"
              onClick={handleCvrSearch}
              disabled={!cvrInput.trim()}
            >
              <Search className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* Fertilizer Type Filter */}
        {availableFertilizerTypes.length > 0 && (
          <div>
            <Label className="text-sm font-medium mb-2 block">Gødningstype</Label>
            <Select
              value={filters.fertilizerType}
              onValueChange={(value) => onFiltersChange({ 
                fertilizerType: value === 'all' ? undefined : value 
              })}
            >
              <SelectTrigger>
                <SelectValue placeholder="Alle gødningstyper" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Alle gødningstyper</SelectItem>
                {availableFertilizerTypes.map(type => (
                  <SelectItem key={type} value={type}>
                    {type}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        {/* Municipality Filter */}
        {availableMunicipalities.length > 0 && (
          <div>
            <Label className="text-sm font-medium mb-2 block">Kommune</Label>
            <Select
              value={filters.municipality}
              onValueChange={(value) => onFiltersChange({ 
                municipality: value === 'all' ? undefined : value 
              })}
            >
              <SelectTrigger>
                <SelectValue placeholder="Alle kommuner" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Alle kommuner</SelectItem>
                {availableMunicipalities.map(municipality => (
                  <SelectItem key={municipality} value={municipality}>
                    {municipality}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        {/* Nitrogen Range Filter */}
        {dataRanges?.nitrogen && (
          <div>
            <Label className="text-sm font-medium mb-2 block">
              <Droplets className="inline h-3 w-3 mr-1 text-blue-600" />
              Kvælstofproduktion (kg N)
              {(filters.minNitrogen || filters.maxNitrogen) && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {filters.minNitrogen || 0} - {filters.maxNitrogen || dataRanges.nitrogen[1]}
                </Badge>
              )}
            </Label>
            <div className="space-y-2">
              <Slider
                value={[
                  filters.minNitrogen || dataRanges.nitrogen[0],
                  filters.maxNitrogen || dataRanges.nitrogen[1]
                ]}
                onValueChange={([min, max]) => onFiltersChange({ minNitrogen: min, maxNitrogen: max })}
                min={dataRanges.nitrogen[0]}
                max={dataRanges.nitrogen[1]}
                step={Math.max(1, Math.floor((dataRanges.nitrogen[1] - dataRanges.nitrogen[0]) / 100))}
                className="w-full"
              />
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{dataRanges.nitrogen[0]}</span>
                <span>{dataRanges.nitrogen[1]}</span>
              </div>
            </div>
          </div>
        )}

        {/* Phosphorus Range Filter */}
        {dataRanges?.phosphorus && (
          <div>
            <Label className="text-sm font-medium mb-2 block">
              <Beaker className="inline h-3 w-3 mr-1 text-orange-600" />
              Fosforproduktion (kg P)
              {(filters.minPhosphorus || filters.maxPhosphorus) && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {filters.minPhosphorus || 0} - {filters.maxPhosphorus || dataRanges.phosphorus[1]}
                </Badge>
              )}
            </Label>
            <div className="space-y-2">
              <Slider
                value={[
                  filters.minPhosphorus || dataRanges.phosphorus[0],
                  filters.maxPhosphorus || dataRanges.phosphorus[1]
                ]}
                onValueChange={([min, max]) => onFiltersChange({ minPhosphorus: min, maxPhosphorus: max })}
                min={dataRanges.phosphorus[0]}
                max={dataRanges.phosphorus[1]}
                step={Math.max(1, Math.floor((dataRanges.phosphorus[1] - dataRanges.phosphorus[0]) / 100))}
                className="w-full"
              />
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{dataRanges.phosphorus[0]}</span>
                <span>{dataRanges.phosphorus[1]}</span>
              </div>
            </div>
          </div>
        )}

        {/* Commercial Fertilizer Range Filter */}
        {dataRanges?.commercial && (
          <div>
            <Label className="text-sm font-medium mb-2 block">
              <Factory className="inline h-3 w-3 mr-1 text-green-600" />
              Handelsgødning (kg N)
              {(filters.minCommercialFertilizer || filters.maxCommercialFertilizer) && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {filters.minCommercialFertilizer || 0} - {filters.maxCommercialFertilizer || dataRanges.commercial[1]}
                </Badge>
              )}
            </Label>
            <div className="space-y-2">
              <Slider
                value={[
                  filters.minCommercialFertilizer || dataRanges.commercial[0],
                  filters.maxCommercialFertilizer || dataRanges.commercial[1]
                ]}
                onValueChange={([min, max]) => onFiltersChange({ 
                  minCommercialFertilizer: min, 
                  maxCommercialFertilizer: max 
                })}
                min={dataRanges.commercial[0]}
                max={dataRanges.commercial[1]}
                step={Math.max(1, Math.floor((dataRanges.commercial[1] - dataRanges.commercial[0]) / 100))}
                className="w-full"
              />
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{dataRanges.commercial[0]}</span>
                <span>{dataRanges.commercial[1]}</span>
              </div>
            </div>
          </div>
        )}

        {/* Biogas Range Filter */}
        {dataRanges?.biogas && (
          <div>
            <Label className="text-sm font-medium mb-2 block">
              <Recycle className="inline h-3 w-3 mr-1 text-purple-600" />
              Biogasproduktion (kg)
              {(filters.minBiogas || filters.maxBiogas) && (
                <Badge variant="secondary" className="ml-2 text-xs">
                  {filters.minBiogas || 0} - {filters.maxBiogas || dataRanges.biogas[1]}
                </Badge>
              )}
            </Label>
            <div className="space-y-2">
              <Slider
                value={[
                  filters.minBiogas || dataRanges.biogas[0],
                  filters.maxBiogas || dataRanges.biogas[1]
                ]}
                onValueChange={([min, max]) => onFiltersChange({ minBiogas: min, maxBiogas: max })}
                min={dataRanges.biogas[0]}
                max={dataRanges.biogas[1]}
                step={Math.max(1, Math.floor((dataRanges.biogas[1] - dataRanges.biogas[0]) / 100))}
                className="w-full"
              />
              <div className="flex justify-between text-xs text-muted-foreground">
                <span>{dataRanges.biogas[0]}</span>
                <span>{dataRanges.biogas[1]}</span>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
