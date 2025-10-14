'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { ViewState } from '@vis.gl/react-maplibre';
import { LayerControlPanelEnhanced as LayerControlPanel } from './LayerControlPanel';
import { FieldDetailsPanel } from './FieldDetailsPanel';
import { LoadingState } from './LoadingState';
import { YearSlider } from './YearSlider';
import { pmtilesCacheService } from '@/services/pmtiles-cache-service';
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

  // Map viewport state - preserve zoom and center when changing years
  const [mapViewState, setMapViewState] = useState<Partial<ViewState>>({
    longitude: 9.501785,
    latitude: 56.26392,
    zoom: 7,
  });

  // Generate PMTiles URLs with caching optimization
  const [pmtilesUrls, setPmtilesUrls] = useState<{
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string;
  }>({
    fields: '',
    bnbo: '',
    wetlands: '',
    water_projects: '',
    buildings: '',
  });

  // Ensure client-side only rendering
  useEffect(() => {
    setIsClient(true);
  }, []);

  // Load PMTiles URLs with caching
  useEffect(() => {
    const loadPMTilesUrls = async () => {
      console.log(
        `🗺️ Loading PMTiles URLs for year ${yearSelection.selectedYear}`
      );

      try {
        const urls = await pmtilesCacheService.getFieldAnalysisUrls(
          yearSelection.selectedYear
        );
        setPmtilesUrls(urls);

        // Preload adjacent years for smooth year switching
        await pmtilesCacheService.preloadAdjacentYears(
          yearSelection.selectedYear,
          yearSelection.availableYears
        );

        console.log('✅ PMTiles URLs loaded and adjacent years preloaded');
      } catch (error) {
        console.error('❌ Failed to load PMTiles URLs:', error);
      }
    };

    if (isClient) {
      loadPMTilesUrls();
    }
  }, [yearSelection.selectedYear, yearSelection.availableYears, isClient]);

  // Handle loading state when PMTiles URLs change
  useEffect(() => {
    if (!pmtilesUrls.fields) return; // Skip if URLs not loaded yet

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

      // Clear field selection when clicking on empty areas
      setSelectedField(null);
    },
    []
  );

  // Handle map view state changes
  const handleMapViewStateChange = useCallback((viewState: ViewState) => {
    setMapViewState({
      longitude: viewState.longitude,
      latitude: viewState.latitude,
      zoom: viewState.zoom,
      pitch: viewState.pitch,
      bearing: viewState.bearing,
    });
  }, []);

  // Handle year selection changes - now preserves viewport and selected field
  const handleYearChange = useCallback((year: number) => {
    setYearSelection((prev) => ({ ...prev, selectedYear: year }));
    // Keep selected field and coordinates when year changes to maintain context
    // Only set loading state while new PMTiles load
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
    <div className="relative flex h-screen touch-pan-x touch-pan-y flex-col overflow-hidden lg:flex-row">
      {/* Mobile Control Panel Toggle */}
      <div
        className="pointer-events-auto absolute top-4 left-4 z-40 lg:hidden"
        style={{ top: 'max(1rem, env(safe-area-inset-top))' }}
      >
        <button
          onClick={() => setMobileControlsOpen(!mobileControlsOpen)}
          className="bg-background hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] touch-manipulation items-center justify-center rounded-lg p-3 shadow-lg transition-colors"
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
        className={` ${mobileControlsOpen ? 'block' : 'hidden'} bg-background fixed inset-0 z-50 h-full w-full overflow-y-auto shadow-lg lg:relative lg:inset-auto lg:z-10 lg:block lg:h-full lg:w-80 lg:shadow-lg`}
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
        <div className="bg-background sticky top-0 z-10 flex items-center justify-between border-b p-4 lg:hidden">
          <div>
            <h2 className="text-lg font-semibold">Kortlag og filtre</h2>
            <p className="text-muted-foreground text-sm">
              Data for {getYearRangeDisplay(yearSelection.selectedYear)}
            </p>
          </div>
          <button
            onClick={() => setMobileControlsOpen(false)}
            className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2"
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
        <div className="bg-background hidden border-b p-4 lg:block">
          <h2 className="text-lg font-semibold">Kortlag og filtre</h2>
          <p className="text-muted-foreground text-sm">
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
          className="bg-opacity-50 bg-background fixed inset-0 z-40 lg:hidden"
          onClick={() => setMobileControlsOpen(false)}
        />
      )}

      {/* Main Map Area */}
      <div className="relative flex-1 touch-pan-x touch-pan-y">
        {/* Year Slider - positioned below search bar */}
        <div className="pointer-events-none absolute top-20 right-4 left-4 z-30 lg:top-4 lg:right-4 lg:left-[22rem]">
          <div className="pointer-events-auto">
            <YearSlider
              yearSelection={yearSelection}
              onYearChange={handleYearChange}
              isLoading={false} // Allow year changes even during loading
            />
          </div>
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
            viewState={mapViewState}
            onViewStateChange={handleMapViewStateChange}
          />
        )}
      </div>

      {/* Right Details Panel - Desktop: sidebar, Mobile: bottom sheet */}
      {selectedField && (
        <>
          {/* Desktop: Fixed sidebar */}
          <div className="bg-background relative z-10 hidden h-full w-80 shadow-lg lg:block">
            <FieldDetailsPanel
              fieldData={selectedField}
              onClose={() => setSelectedField(null)}
            />
          </div>

          {/* Mobile: Bottom sheet */}
          <div className="lg:hidden">
            <div
              className="fixed inset-0 z-40 bg-black/50"
              onClick={() => setSelectedField(null)}
            />
            <div
              className="bg-background fixed inset-x-0 bottom-0 z-[70] max-h-[85vh] overflow-y-auto rounded-t-xl shadow-2xl"
              style={{
                paddingBottom: 'max(1rem, env(safe-area-inset-bottom))',
              }}
            >
              <FieldDetailsPanel
                fieldData={selectedField}
                onClose={() => setSelectedField(null)}
              />
            </div>
          </div>
        </>
      )}

      {/* Coordinate Panel - Desktop: sidebar, Mobile: bottom sheet */}
      {!selectedField && clickedCoordinates && (
        <>
          {/* Desktop: Fixed sidebar */}
          <div className="bg-background relative z-10 hidden h-full w-80 shadow-lg lg:block">
            <FieldDetailsPanel
              coordinates={clickedCoordinates}
              onClose={() => setClickedCoordinates(null)}
            />
          </div>

          {/* Mobile: Bottom sheet */}
          <div className="lg:hidden">
            <div
              className="fixed inset-0 z-40 bg-black/50"
              onClick={() => setClickedCoordinates(null)}
            />
            <div
              className="bg-background fixed inset-x-0 bottom-0 z-[70] max-h-[85vh] overflow-y-auto rounded-t-xl shadow-2xl"
              style={{
                paddingBottom: 'max(1rem, env(safe-area-inset-bottom))',
              }}
            >
              <FieldDetailsPanel
                coordinates={clickedCoordinates}
                onClose={() => setClickedCoordinates(null)}
              />
            </div>
          </div>
        </>
      )}
    </div>
  );
}
