'use client';

import React, {
  useState,
  useEffect,
  useCallback,
  useRef,
  useMemo,
} from 'react';
import dynamic from 'next/dynamic';
import { ViewState } from 'react-map-gl/maplibre';
import { useMobileDetection } from '@/hooks/use-mobile-detection';
import { FieldSidebar } from './sidebar/field-sidebar';
import { MobileFieldMenu } from './sidebar/mobile-menu';
import { FieldDetailsSheet } from './sheets/field-details-sheet';
import { FieldDetailsContent } from './shared/field-details-content';
import { LoadingState } from '@/components/field-analysis/LoadingState';
import { YearSlider } from '@/components/field-analysis/YearSlider';
import { pmtilesCacheService } from '@/services/pmtiles-cache-service';
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
  const [isPanelCollapsed, setIsPanelCollapsed] = useState(false);

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

  // Memoize availableYears to prevent unnecessary re-renders
  const availableYears = useMemo(
    () => yearSelection.availableYears,
    [yearSelection.availableYears]
  );

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
          availableYears
        );

        console.log('✅ PMTiles URLs loaded and adjacent years preloaded');
      } catch (error) {
        console.error('❌ Failed to load PMTiles URLs:', error);
      }
    };

    if (isClient) {
      loadPMTilesUrls();
    }
  }, [yearSelection.selectedYear, availableYears, isClient]);

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
    setIsPanelCollapsed(false); // Expand panel when new field is selected
  }, []);

  // Handle map clicks (for coordinates only)
  const handleMapClick = useCallback(
    (coordinates: { lat: number; lng: number }) => {
      setClickedCoordinates(coordinates);
      setIsPanelCollapsed(false); // Expand panel when coordinates are clicked

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
    <div className="bg-background h-[calc(100vh-120px)] min-h-[calc(100vh-120px)]">
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

      {/* Main Content Area - Account for fixed sidebar */}
      <div
        className={`relative h-full transition-all duration-200 ${
          !isMobile ? 'ml-[70px] md:ml-[280px]' : ''
        }`}
      >
        {/* Year Slider - positioned to avoid sidebar and panel collision */}
        <div
          className={`pointer-events-auto absolute top-4 z-30 transition-all duration-300 md:top-6 md:max-w-sm lg:top-20 ${
            !isMobile &&
            (selectedField || clickedCoordinates) &&
            !isPanelCollapsed
              ? 'right-[25rem] xl:right-[29rem]'
              : 'right-4 md:right-6'
          }`}
        >
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
            viewState={mapViewState}
            onViewStateChange={handleMapViewStateChange}
            hasRightPanel={
              !isMobile &&
              (!!selectedField || !!clickedCoordinates) &&
              !isPanelCollapsed
            }
          />
        )}
      </div>

      {/* Desktop Field Details Panel */}
      {!isMobile && selectedField && (
        <div
          className={`bg-background fixed top-[120px] right-0 z-40 flex h-[calc(100vh-120px)] flex-col border-l shadow-xl transition-all duration-300 ${
            isPanelCollapsed ? 'w-16' : 'w-96 xl:w-[28rem]'
          }`}
        >
          <div className="flex-1 overflow-hidden">
            {isPanelCollapsed ? (
              /* Collapsed State - Show only toggle button */
              <div className="flex h-full flex-col items-center justify-start p-4">
                <button
                  onClick={() => setIsPanelCollapsed(false)}
                  className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                  aria-label="Vis panel"
                  title="Vis markdetaljer"
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
                      d="M15 19l-7-7 7-7"
                    />
                  </svg>
                </button>
              </div>
            ) : (
              /* Expanded State - Show full content */
              <div className="h-full overflow-y-auto">
                {/* Header */}
                <div className="bg-background/95 sticky top-0 z-10 border-b p-6 pb-4 backdrop-blur-sm">
                  <div className="flex items-center justify-between">
                    <div className="min-w-0 flex-1">
                      <h2 className="text-foreground text-xl font-bold">
                        Markdetaljer
                      </h2>
                      <p className="text-muted-foreground mt-1 truncate text-sm">
                        {selectedField.crop_name || 'Ukendt afgrøde'} •{' '}
                        {selectedField.kommune}
                      </p>
                    </div>
                    <div className="flex items-center gap-2">
                      {/* Collapse Button */}
                      <button
                        onClick={() => setIsPanelCollapsed(true)}
                        className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                        aria-label="Skjul panel"
                        title="Skjul panel for bedre kortoversigt"
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
                            d="M9 5l7 7-7 7"
                          />
                        </svg>
                      </button>
                      {/* Close Button */}
                      <button
                        onClick={() => setSelectedField(null)}
                        className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                        aria-label="Luk panel"
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
                  </div>
                </div>

                {/* Field Details Content */}
                <div className="p-6 pt-4">
                  <FieldDetailsContent field={selectedField} />
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Desktop Coordinate Panel - Only show when coordinates are clicked but no field is selected */}
      {!isMobile && !selectedField && clickedCoordinates && (
        <div
          className={`bg-background fixed top-[120px] right-0 z-40 flex h-[calc(100vh-120px)] flex-col border-l shadow-xl transition-all duration-300 ${
            isPanelCollapsed ? 'w-16' : 'w-80 xl:w-96'
          }`}
        >
          {isPanelCollapsed ? (
            /* Collapsed State - Show only toggle button */
            <div className="flex h-full flex-col items-center justify-start p-4">
              <button
                onClick={() => setIsPanelCollapsed(false)}
                className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                aria-label="Vis koordinater"
                title="Vis GPS koordinater"
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
                    d="M15 19l-7-7 7-7"
                  />
                </svg>
              </button>
            </div>
          ) : (
            /* Expanded State - Show full content */
            <div className="p-6">
              <div className="mb-6 flex items-center justify-between">
                <div className="min-w-0 flex-1">
                  <h3 className="text-lg font-semibold">GPS Koordinater</h3>
                  <p className="text-muted-foreground mt-1 truncate text-sm">
                    Klik på kortet for at få koordinater
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  {/* Collapse Button */}
                  <button
                    onClick={() => setIsPanelCollapsed(true)}
                    className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                    aria-label="Skjul panel"
                    title="Skjul panel for bedre kortoversigt"
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
                        d="M9 5l7 7-7 7"
                      />
                    </svg>
                  </button>
                  {/* Close Button */}
                  <button
                    onClick={() => setClickedCoordinates(null)}
                    className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
                    aria-label="Luk panel"
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
              </div>
              <div className="bg-primary/10 rounded-lg p-4">
                <div className="space-y-3 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Latitude:</span>
                    <span className="font-mono font-medium">
                      {clickedCoordinates.lat.toFixed(5)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Longitude:</span>
                    <span className="font-mono font-medium">
                      {clickedCoordinates.lng.toFixed(5)}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Mobile Field Details Sheet */}
      {isMobile && (
        <FieldDetailsSheet
          field={selectedField}
          isOpen={!!selectedField}
          onClose={() => setSelectedField(null)}
        />
      )}
    </div>
  );
}
