'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Search, X } from 'lucide-react';
import { PesticideAnalysisFilters } from './types';

interface CompanyFilterPanelProps {
  filters: PesticideAnalysisFilters;
  availableYears: number[];
  availableMunicipalities: string[];
  onFiltersChange: (filters: Partial<PesticideAnalysisFilters>) => void;
}

export default function CompanyFilterPanel({
  filters,
  availableYears,
  availableMunicipalities,
  onFiltersChange,
}: CompanyFilterPanelProps) {
  const [cvrInput, setCvrInput] = useState(filters.cvr);

  const handleCvrSearch = () => {
    onFiltersChange({ cvr: cvrInput });
  };

  const handleCvrClear = () => {
    setCvrInput('');
    onFiltersChange({ cvr: '' });
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleCvrSearch();
    }
  };

  return (
    <div className="space-y-6">
      {/* Geography Filter */}
      <div>
        <Label className="mb-3 block text-sm font-medium">Geografi</Label>
        <RadioGroup
          value={filters.geography}
          onValueChange={(value) => onFiltersChange({ geography: value })}
        >
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="country" id="country" />
            <Label htmlFor="country" className="text-sm">
              Hele landet
            </Label>
          </div>
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="municipality" id="municipality" />
            <Label htmlFor="municipality" className="text-sm">
              Specifik kommune (markområde)
            </Label>
          </div>
        </RadioGroup>

        {filters.geography !== 'country' && (
          <div className="mt-3">
            <Select
              value={filters.geography === 'country' ? '' : filters.geography}
              onValueChange={(value) => onFiltersChange({ geography: value })}
            >
              <SelectTrigger>
                <SelectValue placeholder="Vælg kommune" />
              </SelectTrigger>
              <SelectContent>
                {availableMunicipalities
                  .filter((m) => m && m !== 'Municipality TBD')
                  .sort()
                  .map((municipality) => (
                    <SelectItem key={municipality} value={municipality}>
                      {municipality}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </div>
        )}
      </div>

      {/* Time Period Filter */}
      <div>
        <Label className="mb-3 block text-sm font-medium">Tidsperiode</Label>
        <div className="space-y-2">
          <div className="flex items-center space-x-2">
            <input
              type="checkbox"
              id="all-years"
              checked={filters.years.length === 0}
              onChange={(e) => {
                if (e.target.checked) {
                  onFiltersChange({ years: [] });
                }
              }}
              className="h-4 w-4 rounded border-gray-300"
            />
            <Label htmlFor="all-years" className="text-sm">
              Alle år
            </Label>
          </div>
          {availableYears
            .sort((a, b) => b - a)
            .map((year) => (
              <div key={year} className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id={`year-${year}`}
                  checked={filters.years.includes(year)}
                  onChange={(e) => {
                    if (e.target.checked) {
                      onFiltersChange({
                        years: [...filters.years, year].sort((a, b) => b - a),
                      });
                    } else {
                      onFiltersChange({
                        years: filters.years.filter((y) => y !== year),
                      });
                    }
                  }}
                  className="h-4 w-4 rounded border-gray-300"
                />
                <Label htmlFor={`year-${year}`} className="text-sm">
                  {year}
                </Label>
              </div>
            ))}
        </div>
      </div>

      {/* Pesticide Type Filter */}
      <div>
        <Label className="mb-3 block text-sm font-medium">Pesticidtype</Label>
        <RadioGroup
          value={filters.type}
          onValueChange={(value) =>
            onFiltersChange({
              type: value as 'total' | 'pfas' | 'diquat' | 'glyphosate',
            })
          }
        >
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="total" id="total" />
            <Label htmlFor="total" className="text-sm">
              Total belastning
            </Label>
          </div>
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="pfas" id="pfas" />
            <Label htmlFor="pfas" className="text-sm">
              PFAS
            </Label>
          </div>
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="diquat" id="diquat" />
            <Label htmlFor="diquat" className="text-sm">
              Diquat
            </Label>
          </div>
          <div className="flex items-center space-x-2">
            <RadioGroupItem value="glyphosate" id="glyphosate" />
            <Label htmlFor="glyphosate" className="text-sm">
              Glyphosat
            </Label>
          </div>
        </RadioGroup>
      </div>

      {/* CVR Search */}
      <div>
        <Label htmlFor="cvr-search" className="mb-3 block text-sm font-medium">
          CVR-nummer
        </Label>
        <div className="flex gap-2">
          <Input
            id="cvr-search"
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
            className="px-3"
          >
            <Search className="h-4 w-4" />
          </Button>
          {filters.cvr && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleCvrClear}
              className="px-3"
            >
              <X className="h-4 w-4" />
            </Button>
          )}
        </div>
        {filters.cvr && (
          <p className="text-muted-foreground mt-1 text-xs">
            Filtreret på CVR: {filters.cvr}
          </p>
        )}
      </div>

      {/* Results per page */}
      <div>
        <Label className="mb-3 block text-sm font-medium">
          Resultater per side
        </Label>
        <Select
          value={filters.limit.toString()}
          onValueChange={(value) => onFiltersChange({ limit: parseInt(value) })}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="25">25</SelectItem>
            <SelectItem value="50">50</SelectItem>
            <SelectItem value="100">100</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
