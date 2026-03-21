'use client';

import { useState, useEffect, useRef } from 'react';
import dynamic from 'next/dynamic';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  MapPin,
  Calendar,
  Eye,
  EyeOff,
  Maximize2,
  Minimize2,
  Loader2,
} from 'lucide-react';
import { CompanySummary } from './types';
import {
  LayerVisibility,
  FilterState,
} from '@/components/field-analysis/types';

// Dynamically import the map to avoid SSR issues
const FieldAnalysisMap = dynamic(
  () => import('@/components/field-analysis/FieldAnalysisMap'),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-64 items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span className="text-muted-foreground ml-2 text-sm">
          Indlæser kort...
        </span>
      </div>
    ),
  }
);

interface CompanyFieldsMapProps {
  company: CompanySummary;
  selectedYear?: number;
  className?: string;
}

export function CompanyFieldsMap({
  company,
  selectedYear = 2023,
  className = '',
}: CompanyFieldsMapProps) {
  const [isClient, setIsClient] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Map state
  const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
    fields: true,
    bnbo: true,
    wetlands: false,
    water_projects: false,
    buildings: false,
  });

  const [filterState] = useState<FilterState>({
    organicOnly: false,
    visualizationMode: 'total_pesticide_belastning',
    colorUnit: 'belastning',
    useDecileColoring: true,
    companyFilter: company.cvr_number, // Filter by this company's CVR
  });

  // Generate PMTiles URLs for the selected year
  const pmtilesUrls = {
    fields: `https://data.pesticidkortet.dk/pmtiles/field_analysis_${selectedYear}.pmtiles`,
    bnbo: 'https://data.pesticidkortet.dk/pmtiles/bnbo_areas.pmtiles',
    wetlands:
      'https://data.pesticidkortet.dk/pmtiles/wetlands_all_2024.pmtiles',
    water_projects:
      'https://data.pesticidkortet.dk/pmtiles/water_projects_2024.pmtiles',
    buildings:
      'https://data.pesticidkortet.dk/pmtiles/buildings_proximity.pmtiles',
  };

  // Ensure client-side only rendering
  useEffect(() => {
    setIsClient(true);
  }, []);

  // Handle loading state
  useEffect(() => {
    setIsLoading(true);

    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    loadingTimeoutRef.current = setTimeout(() => {
      setIsLoading(false);
      loadingTimeoutRef.current = null;
    }, 3000);

    return () => {
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
    };
  }, [selectedYear, company.cvr_number]);

  const handleMapReady = () => {
    setIsLoading(false);
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
      loadingTimeoutRef.current = null;
    }
  };

  const toggleLayer = (layer: keyof LayerVisibility) => {
    setLayerVisibility((prev) => ({
      ...prev,
      [layer]: !prev[layer],
    }));
  };

  const handleFieldSelect = (_field: unknown) => {
    // Handle field selection - could show field details in a popup
  };

  const handleLocationSelect = (_location: { lat: number; lng: number }) => {
    // Handle location selection
  };

  const handleMapClick = (_coordinates: { lat: number; lng: number }) => {
    // Handle map click
  };

  if (!isClient) {
    return (
      <Card className={className}>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <MapPin className="h-4 w-4" />
            Virksomhedens marker
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex h-64 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin" />
            <span className="text-muted-foreground ml-2 text-sm">
              Forbereder kort...
            </span>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={className}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            <MapPin className="h-4 w-4" />
            Virksomhedens marker ({selectedYear})
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">
              CVR: {company.cvr_number}
            </Badge>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsExpanded(!isExpanded)}
            >
              {isExpanded ? (
                <Minimize2 className="h-4 w-4" />
              ) : (
                <Maximize2 className="h-4 w-4" />
              )}
            </Button>
          </div>
        </div>

        {/* Layer Controls */}
        <div className="mt-2 flex flex-wrap gap-2">
          <Button
            variant={layerVisibility.fields ? 'default' : 'outline'}
            size="sm"
            onClick={() => toggleLayer('fields')}
            className="h-7 text-xs"
          >
            {layerVisibility.fields ? (
              <Eye className="mr-1 h-3 w-3" />
            ) : (
              <EyeOff className="mr-1 h-3 w-3" />
            )}
            Marker
          </Button>
          <Button
            variant={layerVisibility.bnbo ? 'default' : 'outline'}
            size="sm"
            onClick={() => toggleLayer('bnbo')}
            className="h-7 text-xs"
          >
            {layerVisibility.bnbo ? (
              <Eye className="mr-1 h-3 w-3" />
            ) : (
              <EyeOff className="mr-1 h-3 w-3" />
            )}
            BNBO
          </Button>
          <Button
            variant={layerVisibility.wetlands ? 'default' : 'outline'}
            size="sm"
            onClick={() => toggleLayer('wetlands')}
            className="h-7 text-xs"
          >
            {layerVisibility.wetlands ? (
              <Eye className="mr-1 h-3 w-3" />
            ) : (
              <EyeOff className="mr-1 h-3 w-3" />
            )}
            Vådområder
          </Button>
          <Button
            variant={layerVisibility.buildings ? 'default' : 'outline'}
            size="sm"
            onClick={() => toggleLayer('buildings')}
            className="h-7 text-xs"
          >
            {layerVisibility.buildings ? (
              <Eye className="mr-1 h-3 w-3" />
            ) : (
              <EyeOff className="mr-1 h-3 w-3" />
            )}
            Bygninger
          </Button>
        </div>
      </CardHeader>

      <CardContent className="p-0">
        <div
          className={`relative ${isExpanded ? 'h-96' : 'h-64'} transition-all duration-300`}
        >
          {isLoading && (
            <div className="bg-background/80 absolute inset-0 z-10 flex items-center justify-center">
              <div className="flex items-center">
                <Loader2 className="h-5 w-5 animate-spin" />
                <span className="text-muted-foreground ml-2 text-sm">
                  Indlæser kortdata...
                </span>
              </div>
            </div>
          )}

          <FieldAnalysisMap
            pmtilesUrls={pmtilesUrls}
            layerVisibility={layerVisibility}
            filterState={filterState}
            onFieldSelect={handleFieldSelect}
            onLocationSelect={handleLocationSelect}
            onMapClick={handleMapClick}
            onMapReady={handleMapReady}
          />
        </div>

        {/* Map Info */}
        <div className="bg-muted/30 border-t p-3">
          <div className="text-muted-foreground flex items-center justify-between text-xs">
            <div className="flex items-center gap-4">
              <span>Filtreret på CVR: {company.cvr_number}</span>
              <span>År: {selectedYear}</span>
            </div>
            <div className="flex items-center gap-2">
              <Calendar className="h-3 w-3" />
              <span>Pesticidata fra {selectedYear}</span>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
