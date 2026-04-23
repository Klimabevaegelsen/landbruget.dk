'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { ViewState } from '@vis.gl/react-maplibre';
import { LayerControlPanelEnhanced as LayerControlPanel } from './LayerControlPanel';
import { FieldDetailsPanel } from './FieldDetailsPanel';
import { LoadingState } from './LoadingState';
import { YearSlider } from './YearSlider';
import { MobileControlsPanel } from './MobileControlsPanel';
import { ResponsiveDetailPanel } from './ResponsiveDetailPanel';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import {
  FieldAnalysisData,
  LayerVisibility,
  FilterState,
  YearSelection,
  getYearRangeDisplay,
} from './types';

const FieldAnalysisMap = dynamic(() => import('./FieldAnalysisMap'), {
  ssr: false,
  loading: () => <LoadingState message="Indlæser kort..." />,
});

export function FieldAnalysisVisualization() {
  const [isClient, setIsClient] = useState(false);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);

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
    selectedYear: 2023,
    availableYears: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
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

  const [mapViewState, setMapViewState] = useState<Partial<ViewState>>({
    longitude: 9.501785,
    latitude: 56.26392,
    zoom: 7,
  });

  const [pmtilesUrls, setPmtilesUrls] = useState<{
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string | null;
  }>({
    fields: '',
    bnbo: '',
    wetlands: '',
    water_projects: '',
    buildings: null,
  });

  useEffect(() => {
    setIsClient(true);
  }, []);

  // Load PMTiles URLs with caching
  useEffect(() => {
    const loadPMTilesUrls = async () => {
      try {
        const urls = await pmtilesCacheService.getFieldAnalysisUrls(
          yearSelection.selectedYear
        );
        setPmtilesUrls(urls);

        pmtilesCacheService
          .preloadAdjacentYears(
            yearSelection.selectedYear,
            yearSelection.availableYears
          )
          .catch(() => {});
      } catch {
        // PMTiles URL loading failure handled silently
      }
    };

    if (isClient) {
      loadPMTilesUrls();
    }
  }, [yearSelection.selectedYear, yearSelection.availableYears, isClient]);

  // Loading state with fallback timeout
  useEffect(() => {
    if (!pmtilesUrls.fields) return;

    setIsLoading(true);

    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    loadingTimeoutRef.current = setTimeout(() => {
      setIsLoading(false);
      loadingTimeoutRef.current = null;
    }, 5000);

    return () => {
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
    };
  }, [pmtilesUrls.fields]);

  // Close mobile panels on orientation change
  useEffect(() => {
    const handleOrientationChange = () => {
      setMobileControlsOpen(false);
    };

    window.addEventListener('orientationchange', handleOrientationChange);
    return () =>
      window.removeEventListener('orientationchange', handleOrientationChange);
  }, []);

  const handleLayerToggle = useCallback((layerName: keyof LayerVisibility) => {
    setLayerVisibility((prev) => ({
      ...prev,
      [layerName]: !prev[layerName],
    }));
  }, []);

  const handleFilterChange = useCallback((newFilters: Partial<FilterState>) => {
    setFilterState((prev) => ({ ...prev, ...newFilters }));
  }, []);

  const handleFieldSelect = useCallback((fieldData: FieldAnalysisData) => {
    setSelectedField(fieldData);
    setMobileControlsOpen(false);
  }, []);

  const handleMapClick = useCallback(
    (coordinates: { lat: number; lng: number }) => {
      setClickedCoordinates(coordinates);
      setSelectedField(null);
    },
    []
  );

  const handleMapViewStateChange = useCallback((viewState: ViewState) => {
    setMapViewState({
      longitude: viewState.longitude,
      latitude: viewState.latitude,
      zoom: viewState.zoom,
      pitch: viewState.pitch,
      bearing: viewState.bearing,
    });
  }, []);

  const handleYearChange = useCallback((year: number) => {
    setYearSelection((prev) => ({ ...prev, selectedYear: year }));
    setIsLoading(true);
  }, []);

  const handleMapReady = useCallback(() => {
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
      loadingTimeoutRef.current = null;
    }
    setIsLoading(false);
  }, []);

  // Escape key and body scroll prevention
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

    const isMobile = window.innerWidth < 1024;
    if (
      mobileControlsOpen ||
      (isMobile && (selectedField || clickedCoordinates))
    ) {
      document.body.style.overflow = 'hidden';
      document.addEventListener('keydown', handleKeyDown);
    } else {
      document.body.style.overflow = 'unset';
    }

    if (!isMobile && (selectedField || clickedCoordinates)) {
      document.addEventListener('keydown', handleKeyDown);
    }

    return () => {
      document.body.style.overflow = 'unset';
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [mobileControlsOpen, selectedField, clickedCoordinates]);

  if (!isClient) {
    return <LoadingState message="Indlæser..." />;
  }

  const yearSubtitle = `Data for ${getYearRangeDisplay(yearSelection.selectedYear)}`;

  return (
    <div className="relative flex h-screen touch-pan-x touch-pan-y flex-col overflow-hidden lg:flex-row">
      <MobileControlsPanel
        isOpen={mobileControlsOpen}
        onToggle={() => setMobileControlsOpen(!mobileControlsOpen)}
        onClose={() => setMobileControlsOpen(false)}
        headerTitle="Kortlag og filtre"
        headerSubtitle={yearSubtitle}
      >
        <LayerControlPanel
          layerVisibility={layerVisibility}
          filterState={filterState}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
        />
      </MobileControlsPanel>

      {/* Main Map Area */}
      <div className="relative flex-1 touch-pan-x touch-pan-y">
        <div className="pointer-events-none absolute top-20 right-4 left-4 z-30 lg:top-4 lg:right-4 lg:left-[22rem]">
          <div className="pointer-events-auto">
            <YearSlider
              yearSelection={yearSelection}
              onYearChange={handleYearChange}
              isLoading={false}
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

      {/* Right Details Panel */}
      {selectedField && (
        <ResponsiveDetailPanel onClose={() => setSelectedField(null)}>
          <FieldDetailsPanel
            fieldData={selectedField}
            onClose={() => setSelectedField(null)}
          />
        </ResponsiveDetailPanel>
      )}

      {!selectedField && clickedCoordinates && (
        <ResponsiveDetailPanel onClose={() => setClickedCoordinates(null)}>
          <FieldDetailsPanel
            coordinates={clickedCoordinates}
            onClose={() => setClickedCoordinates(null)}
          />
        </ResponsiveDetailPanel>
      )}
    </div>
  );
}
