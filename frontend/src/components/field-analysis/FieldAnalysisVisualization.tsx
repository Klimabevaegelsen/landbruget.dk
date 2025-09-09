'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { LayerControlPanel } from './LayerControlPanel';
import { FieldDetailsPanel } from './FieldDetailsPanel';
import { CoordinatePanel } from './CoordinatePanel';
import { LoadingState } from './LoadingState';
import { YearSlider } from './YearSlider';
import {
  FieldAnalysisData,
  LayerVisibility,
  FilterState,
  YearSelection,
  getYearRangeDisplay,
} from './types';

// Dynamically import the map component to avoid SSR issues
const FieldAnalysisMap = dynamic(() => import('./FieldAnalysisMap'), {
  ssr: false,
  loading: () => <LoadingState message="Indlæser kort..." />,
});

export default function FieldAnalysisVisualization() {
  const [isClient, setIsClient] = useState(false);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

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
  const [mobileControlsOpen, setMobileControlsOpen] = useState(false);

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
      console.warn('⚠️ Map loading timeout - forcing loading state to false');
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

  // Handle orientation changes on mobile
  useEffect(() => {
    const handleOrientationChange = () => {
      // Close mobile panels on orientation change for better UX
      setMobileControlsOpen(false);
    };

    window.addEventListener('orientationchange', handleOrientationChange);
    return () =>
      window.removeEventListener('orientationchange', handleOrientationChange);
  }, []);

  // Initialize visualization - loading is handled by map component
  useEffect(() => {
    if (!isClient) return;
    // Map component will handle loading states via onMapReady callback
  }, [isClient]);

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
    // Auto-close mobile controls when field is selected for better UX
    setMobileControlsOpen(false);
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
    console.log('✅ Map ready - clearing loading state');

    // Clear the fallback timeout since map loaded successfully
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
      loadingTimeoutRef.current = null;
    }

    setIsLoading(false);
  }, []);

  // Handle escape key and prevent body scroll
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (selectedField) {
          setSelectedField(null);
        } else if (mobileControlsOpen) {
          setMobileControlsOpen(false);
        } else if (clickedCoordinates) {
          setClickedCoordinates(null);
        }
      }
    };

    // Prevent body scroll when modals are open
    // On mobile: prevent scroll for all panels (they're overlays)
    // On desktop: only prevent scroll for mobile controls (panels are sidebars)
    const isMobile = window.innerWidth < 1024; // lg breakpoint
    if (
      mobileControlsOpen ||
      (isMobile && (selectedField || clickedCoordinates))
    ) {
      document.body.style.overflow = 'hidden';
      document.addEventListener('keydown', handleKeyDown);
    } else {
      document.body.style.overflow = 'unset';
    }

    // Add keydown listener for desktop panels without preventing body scroll
    if (!isMobile && (selectedField || clickedCoordinates)) {
      document.addEventListener('keydown', handleKeyDown);
    }

    return () => {
      document.body.style.overflow = 'unset';
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [mobileControlsOpen, selectedField, clickedCoordinates]);

  // Prevent hydration mismatch by not rendering until client-side
  if (!isClient) {
    return <LoadingState message="Indlæser..." />;
  }

  return (
    <div className="relative flex h-screen flex-col overflow-hidden lg:flex-row">
      {/* Mobile Control Panel Toggle */}
      <div
        className="absolute top-4 left-4 z-30 lg:hidden"
        style={{ top: 'max(1rem, env(safe-area-inset-top))' }}
      >
        <button
          onClick={() => setMobileControlsOpen(!mobileControlsOpen)}
          className="flex min-h-[44px] min-w-[44px] items-center justify-center rounded-lg bg-white p-3 shadow-lg transition-colors hover:bg-gray-50 active:bg-gray-100"
          aria-label="Toggle controls"
        >
          <svg
            className="h-6 w-6"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 6h16M4 12h16M4 18h16"
            />
          </svg>
        </button>
      </div>

      {/* Left Control Panel - Desktop: sidebar, Mobile: overlay */}
      <div
        className={` ${mobileControlsOpen ? 'block' : 'hidden'} fixed inset-0 z-30 h-full w-full overflow-y-auto bg-white shadow-lg lg:relative lg:inset-auto lg:z-10 lg:block lg:h-full lg:w-80 lg:shadow-lg`}
        style={{
          paddingTop: mobileControlsOpen
            ? 'env(safe-area-inset-top)'
            : undefined,
          paddingBottom: mobileControlsOpen
            ? 'env(safe-area-inset-bottom)'
            : undefined,
        }}
      >
        {/* Mobile close button */}
        <div className="sticky top-0 z-10 flex items-center justify-between border-b bg-white p-4 lg:hidden">
          <div>
            <h2 className="text-lg font-semibold">Kortlag og filtre</h2>
            <p className="text-sm text-gray-600">
              Data for {getYearRangeDisplay(yearSelection.selectedYear)}
            </p>
          </div>
          <button
            onClick={() => setMobileControlsOpen(false)}
            className="flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 hover:bg-gray-100 active:bg-gray-200"
            aria-label="Luk kontrolpanel"
          >
            <svg
              className="h-5 w-5"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>

        {/* Desktop header */}
        <div className="hidden border-b bg-white p-4 lg:block">
          <h2 className="text-lg font-semibold">Kortlag og filtre</h2>
          <p className="text-sm text-gray-600">
            Data for {getYearRangeDisplay(yearSelection.selectedYear)}
          </p>
        </div>

        <LayerControlPanel
          layerVisibility={layerVisibility}
          filterState={filterState}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
        />
      </div>

      {/* Mobile Controls Backdrop */}
      {mobileControlsOpen && (
        <div
          className="bg-opacity-50 fixed inset-0 z-20 bg-black lg:hidden"
          onClick={() => setMobileControlsOpen(false)}
        />
      )}

      {/* Main Map Area */}
      <div className="relative flex-1">
        {/* Year Slider - positioned below search bar */}
        <div className="absolute top-20 right-4 left-4 z-10 lg:top-4 lg:right-4 lg:left-[22rem]">
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

      {/* Right Details Panel - Desktop: sidebar, Mobile: overlay */}
      {selectedField && (
        <>
          <div
            className={`fixed inset-0 z-30 h-full w-full overflow-y-auto bg-white shadow-lg lg:relative lg:inset-auto lg:z-10 lg:h-full lg:w-80 lg:shadow-lg`}
            style={{
              paddingTop: 'env(safe-area-inset-top)',
              paddingBottom: 'env(safe-area-inset-bottom)',
            }}
          >
            <FieldDetailsPanel
              fieldData={selectedField}
              onClose={() => setSelectedField(null)}
            />
          </div>

          {/* Mobile Details Backdrop */}
          <div
            className="bg-opacity-50 fixed inset-0 z-20 bg-black lg:hidden"
            onClick={() => setSelectedField(null)}
          />
        </>
      )}

      {/* Coordinate Panel - Only show when coordinates are clicked but no field is selected */}
      {!selectedField && clickedCoordinates && (
        <>
          <div
            className={`fixed inset-0 z-30 h-full w-full overflow-y-auto bg-white shadow-lg lg:relative lg:inset-auto lg:z-10 lg:h-full lg:w-80 lg:shadow-lg`}
            style={{
              paddingTop: 'env(safe-area-inset-top)',
              paddingBottom: 'env(safe-area-inset-bottom)',
            }}
          >
            <CoordinatePanel
              coordinates={clickedCoordinates}
              onClose={() => setClickedCoordinates(null)}
            />
          </div>

          {/* Mobile Coordinate Panel Backdrop */}
          <div
            className="bg-opacity-50 fixed inset-0 z-20 bg-black lg:hidden"
            onClick={() => setClickedCoordinates(null)}
          />
        </>
      )}
    </div>
  );
}
