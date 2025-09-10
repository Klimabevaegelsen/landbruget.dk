'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { useMobileDetection } from '@/hooks/use-mobile-detection';
import { FieldSidebar } from './sidebar/field-sidebar';
import { MobileFieldMenu } from './sidebar/mobile-menu';
import { FieldDetailsSheet } from './sheets/field-details-sheet';
import { LoadingState } from '@/components/field-analysis/LoadingState';
import { YearSlider } from '@/components/field-analysis/YearSlider';
import {
  FieldAnalysisData,
  LayerVisibility,
  FilterState,
  YearSelection,
} from '@/components/field-analysis/types';

// Dynamically import the map component to avoid SSR issues
const FieldAnalysisMap = dynamic(
  () => import('@/components/field-analysis/FieldAnalysisMap'),
  {
    ssr: false,
    loading: () => <LoadingState message="Indlæser kort..." />,
  }
);

export default function FieldAnalysisMain() {
  const [isClient, setIsClient] = useState(false);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const { isMobile } = useMobileDetection();

  // State management
  const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
    fields: true,
    bnbo: true,
    wetlands: false,
    water_projects: false,
    buildings: true,
  });

  const [filterState, setFilterState] = useState<FilterState>({
    organicOnly: false,
    visualizationMode: 'total_pesticide_belastning',
    colorUnit: 'belastning',
    useDecileColoring: true,
  });

  const [yearSelection, setYearSelection] = useState<YearSelection>({
    selectedYear: 2023, // Default to most recent year (2023 pesticides + 2024 fields)
    availableYears: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023], // Available years from PMTiles generation
  });

  const [selectedField, setSelectedField] = useState<FieldAnalysisData | null>(
    null
  );
  const [clickedCoordinates, setClickedCoordinates] = useState<{
    lat: number;
    lng: number;
  } | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // Generate PMTiles URLs dynamically based on selected year
  const pmtilesUrls = {
    fields: `https://data.pesticidkortet.dk/pmtiles/field_analysis_${yearSelection.selectedYear}.pmtiles`,
    bnbo:
      process.env.NEXT_PUBLIC_BNBO_PMTILES_URL ||
      'https://data.pesticidkortet.dk/pmtiles/bnbo_areas.pmtiles',
    wetlands:
      process.env.NEXT_PUBLIC_WETLANDS_PMTILES_URL ||
      'https://data.pesticidkortet.dk/pmtiles/wetlands_all_2024.pmtiles',
    water_projects:
      process.env.NEXT_PUBLIC_WATER_PROJECTS_PMTILES_URL ||
      'https://data.pesticidkortet.dk/pmtiles/water_projects_2024.pmtiles',
    buildings:
      process.env.NEXT_PUBLIC_BUILDINGS_PMTILES_URL ||
      'https://data.pesticidkortet.dk/pmtiles/buildings_proximity_2024.pmtiles',
  };

  // Ensure client-side only rendering
  useEffect(() => {
    setIsClient(true);
  }, []);

  // Handle loading state when PMTiles URLs change
  useEffect(() => {
    setIsLoading(true);

    // Clear any existing timeout
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    // Fallback timeout to prevent getting stuck in loading state
    loadingTimeoutRef.current = setTimeout(() => {
      console.warn('Map loading timeout - forcing loading state to false');
      setIsLoading(false);
      loadingTimeoutRef.current = null;
    }, 5000); // 5 second fallback timeout

    return () => {
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
    };
  }, [pmtilesUrls.fields]);

  // Handle layer visibility changes
  const handleLayerToggle = useCallback((layerName: keyof LayerVisibility) => {
    setLayerVisibility((prev) => ({
      ...prev,
      [layerName]: !prev[layerName],
    }));
  }, []);

  // Handle filter changes
  const handleFilterChange = useCallback((newFilters: Partial<FilterState>) => {
    setFilterState((prev) => ({ ...prev, ...newFilters }));
  }, []);

  // Handle field selection
  const handleFieldSelect = useCallback((fieldData: FieldAnalysisData) => {
    setSelectedField(fieldData);
  }, []);

  // Handle map clicks (for coordinates only)
  const handleMapClick = useCallback(
    (coordinates: { lat: number; lng: number }) => {
      setClickedCoordinates(coordinates);

      // If a field is currently selected, update its click coordinates
      if (selectedField) {
        setSelectedField((prev) =>
          prev
            ? {
                ...prev,
                click_coordinates: coordinates,
              }
            : null
        );
      }
    },
    [selectedField]
  );

  // Handle year selection changes
  const handleYearChange = useCallback((year: number) => {
    setYearSelection((prev) => ({ ...prev, selectedYear: year }));
    // Reset selected field and coordinates when year changes
    setSelectedField(null);
    setClickedCoordinates(null);
    // Set loading state while new PMTiles load
    setIsLoading(true);
  }, []);

  // Handle map ready callback
  const handleMapReady = useCallback(() => {
    console.log('Map ready - clearing loading state');

    // Clear the fallback timeout since map loaded successfully
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
      loadingTimeoutRef.current = null;
    }

    setIsLoading(false);
  }, []);

  // Handle escape key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (selectedField) {
          setSelectedField(null);
        } else if (clickedCoordinates) {
          setClickedCoordinates(null);
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [selectedField, clickedCoordinates]);

  // Prevent hydration mismatch by not rendering until client-side
  if (!isClient) {
    return <LoadingState message="Indlæser..." />;
  }

  return (
    <div className="bg-background flex h-screen">
      {/* Desktop Sidebar */}
      {!isMobile && (
        <FieldSidebar
          layerVisibility={layerVisibility}
          filterState={filterState}
          yearSelection={yearSelection}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
          onYearChange={handleYearChange}
        />
      )}

      {/* Mobile Menu */}
      {isMobile && (
        <MobileFieldMenu
          layerVisibility={layerVisibility}
          filterState={filterState}
          yearSelection={yearSelection}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
          onYearChange={handleYearChange}
        />
      )}

      {/* Main Map Area */}
      <div className="relative flex-1">
        {/* Year Slider - positioned for both mobile and desktop */}
        <div className="pointer-events-auto absolute top-4 right-4 left-4 z-30 md:right-4 md:left-auto">
          <YearSlider
            yearSelection={yearSelection}
            onYearChange={handleYearChange}
            isLoading={false} // Allow year changes even during loading
          />
        </div>

        {isLoading ? (
          <LoadingState message="Indlæser kortdata..." />
        ) : (
          <FieldAnalysisMap
            pmtilesUrls={pmtilesUrls}
            layerVisibility={layerVisibility}
            filterState={filterState}
            onFieldSelect={handleFieldSelect}
            onMapClick={handleMapClick}
            onMapReady={handleMapReady}
          />
        )}
      </div>

      {/* Field Details Sheet - Mobile First */}
      <FieldDetailsSheet
        field={selectedField}
        isOpen={!!selectedField}
        onClose={() => setSelectedField(null)}
      />

      {/* Coordinate Panel - Only show when coordinates are clicked but no field is selected */}
      {!selectedField && clickedCoordinates && !isMobile && (
        <div className="bg-card w-80 border-l shadow-lg">
          <div className="p-4">
            <h3 className="mb-3 text-lg font-semibold">GPS Koordinater</h3>
            <div className="space-y-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">Latitude:</span>
                <span className="font-mono">
                  {clickedCoordinates.lat.toFixed(5)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">Longitude:</span>
                <span className="font-mono">
                  {clickedCoordinates.lng.toFixed(5)}
                </span>
              </div>
            </div>
            <button
              onClick={() => setClickedCoordinates(null)}
              className="bg-primary text-primary-foreground hover:bg-primary/90 mt-4 w-full rounded-lg px-4 py-2 transition-colors"
            >
              Luk
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
