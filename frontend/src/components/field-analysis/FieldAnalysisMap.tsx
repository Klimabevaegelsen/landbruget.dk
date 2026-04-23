'use client';

import React, {
  useEffect,
  useRef,
  useState,
  useCallback,
  useMemo,
  memo,
} from 'react';
import { AlertTriangle } from 'lucide-react';
import Map, {
  MapLayerMouseEvent,
  NavigationControl,
  ViewState,
  MapRef,
} from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { LayerVisibility, FilterState, FieldAnalysisData } from './types';
import { SearchBar } from './SearchBar';
import { ColorLegend } from './ColorLegend';
import { computeCentroid } from '@/utils/geo';
import {
  MapInstance,
  FALLBACK_MAP_STYLE,
  INTERACTIVE_LAYER_IDS,
  getLayerDisplayName,
  TooltipInfo,
} from './map-constants';
import { MapTooltip } from './MapTooltip';
import { buildFieldsPaintProps } from './map-paint';
import {
  removeFieldsLayers,
  addFieldsLayers,
  addBNBOLayers,
  addWetlandsLayers,
  addWaterProjectsLayers,
  addBuildingsLayers,
} from './map-layers';

interface FieldAnalysisMapProps {
  pmtilesUrls: {
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string | null;
  };
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  onFieldSelect: (fieldData: FieldAnalysisData) => void;
  onLocationSelect?: (location: {
    lat: number;
    lng: number;
    address: string;
  }) => void;
  onMapClick?: (coordinates: { lat: number; lng: number }) => void;
  onMapReady?: () => void;
  viewState?: Partial<ViewState>;
  onViewStateChange?: (viewState: ViewState) => void;
  hasRightPanel?: boolean;
  queryVisibleFieldsRef?: React.MutableRefObject<
    (() => FieldAnalysisData[]) | null
  >;
}

const FieldAnalysisMap = memo(function FieldAnalysisMap({
  pmtilesUrls,
  layerVisibility,
  filterState,
  onFieldSelect,
  onLocationSelect,
  onMapClick,
  onMapReady,
  viewState: externalViewState,
  onViewStateChange,
  hasRightPanel,
  queryVisibleFieldsRef,
}: FieldAnalysisMapProps) {
  const { mapStyle } = useMapTheme();

  const mapRef = useRef<MapRef | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hoverInfo, setHoverInfo] = useState<TooltipInfo | null>(null);
  const [isSearchActive, setIsSearchActive] = useState(false);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const loadedSourcesRef = useRef<Set<string>>(new Set());
  const [styleLoadFailed, setStyleLoadFailed] = useState(false);
  const [currentMapStyle, setCurrentMapStyle] = useState(mapStyle);
  const prevPaintPropsRef = useRef<string | null>(null);

  // Internal view state for controlled map
  const [internalViewState, setInternalViewState] = useState<
    ViewState & { width: number; height: number }
  >({
    longitude: 9.501785,
    latitude: 56.26392,
    zoom: 7,
    pitch: 0,
    bearing: 0,
    padding: { top: 0, bottom: 0, left: 0, right: 0 },
    width: 800,
    height: 600,
  });

  // Handle map style changes and fallback
  useEffect(() => {
    if (!styleLoadFailed) {
      setCurrentMapStyle(mapStyle);
    }
  }, [mapStyle, styleLoadFailed]);

  const currentViewState: ViewState & { width: number; height: number } =
    externalViewState
      ? { ...internalViewState, ...externalViewState }
      : internalViewState;

  // Track map interactions to prevent clicks during dragging
  const isMapDraggingRef = useRef(false);
  const lastMapMoveTimeRef = useRef<number>(0);

  // RAF throttling for smooth performance
  const rafIdRef = useRef<number | null>(null);
  const lastViewState = useRef<ViewState | null>(null);
  const finalNotifyTimeout = useRef<NodeJS.Timeout | null>(null);

  const handleViewStateChange = useCallback(
    (evt: { viewState: ViewState }) => {
      const newViewState = evt.viewState;
      const currentVS = externalViewState || internalViewState;

      const hasSignificantChange =
        !currentVS ||
        Math.abs(newViewState.longitude - (currentVS.longitude ?? 0)) >
          0.0001 ||
        Math.abs(newViewState.latitude - (currentVS.latitude ?? 0)) > 0.0001 ||
        Math.abs(newViewState.zoom - (currentVS.zoom ?? 0)) > 0.01;

      if (hasSignificantChange) {
        lastMapMoveTimeRef.current = Date.now();
        isMapDraggingRef.current = true;
      }

      if (!externalViewState) {
        setInternalViewState({
          ...newViewState,
          width: internalViewState.width,
          height: internalViewState.height,
        });
      }

      lastViewState.current = newViewState;

      if (onViewStateChange && !rafIdRef.current) {
        rafIdRef.current = requestAnimationFrame(() => {
          rafIdRef.current = null;
          if (lastViewState.current) {
            onViewStateChange(lastViewState.current);
          }
        });
      }

      if (finalNotifyTimeout.current) {
        clearTimeout(finalNotifyTimeout.current);
      }
      finalNotifyTimeout.current = setTimeout(() => {
        if (onViewStateChange && lastViewState.current) {
          onViewStateChange(lastViewState.current);
        }
        isMapDraggingRef.current = false;
      }, 150);
    },
    [onViewStateChange, externalViewState, internalViewState]
  );

  const handleLocationSelect = useCallback(
    (location: { lat: number; lng: number; address: string }) => {
      if (!mapRef.current) return;

      const map = mapRef.current.getMap() as unknown as {
        flyTo: (options: {
          center: [number, number];
          zoom: number;
          duration?: number;
        }) => void;
      };

      map.flyTo({
        center: [location.lng, location.lat],
        zoom: 14,
        duration: 1500,
      });

      onLocationSelect?.(location);
    },
    [onLocationSelect]
  );

  // Stable callback refs to prevent excessive re-creation
  const onMapReadyRef = useRef(onMapReady);
  const pmtilesUrlsRef = useRef(pmtilesUrls);

  useEffect(() => {
    onMapReadyRef.current = onMapReady;
    pmtilesUrlsRef.current = pmtilesUrls;
  });

  const startLoadingTimeout = useCallback(() => {
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    loadingTimeoutRef.current = setTimeout(() => {
      console.warn('Map loading timeout - forcing loading state to false');
      setIsLoading(false);
      onMapReadyRef.current?.();
    }, 3000);
  }, []);

  const checkAllSourcesLoaded = useCallback(() => {
    const requiredSources = Object.keys(pmtilesUrlsRef.current).filter(
      (key) =>
        pmtilesUrlsRef.current[key as keyof typeof pmtilesUrlsRef.current]
    );
    const allLoaded = requiredSources.every((source) =>
      loadedSourcesRef.current.has(source)
    );

    if (allLoaded && isLoading) {
      console.log('All PMTiles sources loaded successfully');
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
      setIsLoading(false);
      onMapReadyRef.current?.();
    }
  }, [isLoading]);

  const handleSourceData = useCallback(
    (e: { sourceId: string; isSourceLoaded: boolean; dataType?: string }) => {
      if (
        e.isSourceLoaded &&
        Object.keys(pmtilesUrlsRef.current).includes(e.sourceId)
      ) {
        loadedSourcesRef.current.add(e.sourceId);
        checkAllSourcesLoaded();
      }
    },
    [checkAllSourcesLoaded]
  );

  // Initialize PMTiles protocol with retry mechanism
  useEffect(() => {
    let retryCount = 0;
    const maxRetries = 3;

    const initializePMTiles = async () => {
      try {
        startLoadingTimeout();

        const [maplibregl, { Protocol }] = await Promise.all([
          import('maplibre-gl'),
          import('pmtiles'),
        ]);

        if (
          !(window as unknown as { __pmtiles_protocol_registered?: boolean })
            .__pmtiles_protocol_registered
        ) {
          const protocol = new Protocol();
          maplibregl.default.addProtocol('pmtiles', protocol.tile);
          (
            window as unknown as { __pmtiles_protocol_registered?: boolean }
          ).__pmtiles_protocol_registered = true;
        }
      } catch (err) {
        console.error(
          `Failed to initialize PMTiles (attempt ${retryCount + 1}):`,
          err
        );
        if (retryCount < maxRetries) {
          retryCount++;
          setTimeout(initializePMTiles, 1000 * retryCount);
          return;
        }

        if (loadingTimeoutRef.current) {
          clearTimeout(loadingTimeoutRef.current);
          loadingTimeoutRef.current = null;
        }
        setError(
          'Kunne ikke indlæse kortdata efter flere forsøg. Prøv at genindlæse siden.'
        );
        setIsLoading(false);
      }
    };

    initializePMTiles();

    return () => {
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
    };
  }, [startLoadingTimeout]);

  // Cleanup event listeners and RAF on unmount
  useEffect(() => {
    const currentMapRef = mapRef.current;
    return () => {
      if (rafIdRef.current) {
        cancelAnimationFrame(rafIdRef.current);
        rafIdRef.current = null;
      }
      if (finalNotifyTimeout.current) {
        clearTimeout(finalNotifyTimeout.current);
        finalNotifyTimeout.current = null;
      }
      if (currentMapRef) {
        const map = currentMapRef.getMap() as MapInstance & {
          off: (
            event: string,
            handler: (e: { sourceId: string; isSourceLoaded: boolean }) => void
          ) => void;
        };
        map.off('sourcedata', handleSourceData);
      }
    };
  }, [handleSourceData]);

  // Memoize paint properties
  const fieldsPaintProps = useMemo(
    () => buildFieldsPaintProps(filterState),
    [
      filterState.visualizationMode,
      filterState.colorUnit,
      filterState.useDecileColoring,
    ]
  );

  // Handle map load and add sources
  const onMapLoad = useCallback(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap() as MapInstance;
    if (!map) return;

    try {
      loadedSourcesRef.current.clear();

      const mapWithEvents = map as MapInstance & {
        on: (
          event: string,
          handler: (e: { sourceId: string; isSourceLoaded: boolean }) => void
        ) => void;
        once: (event: string, handler: () => void) => void;
      };
      mapWithEvents.on('sourcedata', handleSourceData);

      const sourceErrors: string[] = [];
      let sourcesAdded = 0;

      Object.entries(pmtilesUrls).forEach(([layerName, url]) => {
        if (url && !map.getSource(layerName)) {
          try {
            map.addSource(layerName, {
              type: 'vector',
              url: `pmtiles://${url}`,
            });
            sourcesAdded++;
          } catch (error) {
            const errorMessage = `Failed to add ${layerName} source: ${error}`;
            console.warn(errorMessage);
            sourceErrors.push(errorMessage);
          }
        }
      });

      if (sourcesAdded === 0) {
        if (loadingTimeoutRef.current) {
          clearTimeout(loadingTimeoutRef.current);
          loadingTimeoutRef.current = null;
        }
        setIsLoading(false);
        onMapReady?.();
        return;
      }

      if (sourceErrors.length > 0) {
        console.warn('Some map sources failed to load:', sourceErrors);
      }

      // Add all layers using extracted functions
      addFieldsLayers(map, {
        paintProps: fieldsPaintProps,
        visible: layerVisibility.fields,
        companyFilter: filterState.companyFilter,
      });
      addBNBOLayers(map, layerVisibility);
      addWetlandsLayers(map, layerVisibility);
      addWaterProjectsLayers(map, layerVisibility);
      addBuildingsLayers(map, layerVisibility);

      mapWithEvents.once('idle', () => {
        setTimeout(() => {
          if (loadingTimeoutRef.current) {
            clearTimeout(loadingTimeoutRef.current);
            loadingTimeoutRef.current = null;
          }
          setIsLoading(false);
          onMapReadyRef.current?.();
        }, 100);
      });

      console.log(`Waiting for ${sourcesAdded} PMTiles sources to load...`);
    } catch (err) {
      console.error('Error adding map sources/layers:', err);
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
      setError('Failed to load map data');
      setIsLoading(false);
    }
  }, [
    pmtilesUrls,
    fieldsPaintProps,
    layerVisibility,
    filterState.companyFilter,
    onMapReady,
    handleSourceData,
  ]);

  // Ensure sources are added when map and URLs are both ready
  useEffect(() => {
    if (!mapRef.current || !pmtilesUrls.fields) return;

    const map = mapRef.current.getMap();
    if (!map || !map.loaded()) return;

    const fieldsSource = map.getSource('fields');

    if (!fieldsSource) {
      onMapLoad();
      return;
    }
  }, [pmtilesUrls.fields, onMapLoad]);

  // Handle PMTiles URL changes (year selection)
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    const fieldsSource = map.getSource('fields');
    if (fieldsSource && pmtilesUrls.fields) {
      startLoadingTimeout();
      setIsLoading(true);

      try {
        const newSource = {
          type: 'vector' as const,
          url: `pmtiles://${pmtilesUrls.fields}`,
        };

        loadedSourcesRef.current.delete('fields');

        removeFieldsLayers(map as unknown as MapInstance);
        map.removeSource('fields');
        map.addSource('fields', newSource);

        addFieldsLayers(map as unknown as MapInstance, {
          paintProps: fieldsPaintProps,
          visible: layerVisibility.fields,
          companyFilter: filterState.companyFilter,
        });
      } catch (error) {
        console.error('Error updating PMTiles for year:', error);
        if (loadingTimeoutRef.current) {
          clearTimeout(loadingTimeoutRef.current);
          loadingTimeoutRef.current = null;
        }
        setError('Failed to load data for selected year');
        setIsLoading(false);
      }
    }
  }, [
    pmtilesUrls.fields,
    fieldsPaintProps,
    layerVisibility.fields,
    filterState.companyFilter,
    onMapReady,
    startLoadingTimeout,
  ]);

  // Update layer visibility when props change
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    if (map.getLayer('fields-fill')) {
      map.setLayoutProperty(
        'fields-fill',
        'visibility',
        layerVisibility.fields ? 'visible' : 'none'
      );
      map.setLayoutProperty(
        'fields-outline',
        'visibility',
        layerVisibility.fields ? 'visible' : 'none'
      );

      if (map.getLayer('organic-symbols')) {
        map.setLayoutProperty(
          'organic-symbols',
          'visibility',
          layerVisibility.fields &&
            filterState.visualizationMode === 'organic_status'
            ? 'visible'
            : 'none'
        );
      }
    }

    if (map.getLayer('bnbo-fill')) {
      map.setLayoutProperty(
        'bnbo-fill',
        'visibility',
        layerVisibility.bnbo ? 'visible' : 'none'
      );
      map.setLayoutProperty(
        'bnbo-outline',
        'visibility',
        layerVisibility.bnbo ? 'visible' : 'none'
      );
    }

    if (map.getLayer('wetlands-fill')) {
      map.setLayoutProperty(
        'wetlands-fill',
        'visibility',
        layerVisibility.wetlands ? 'visible' : 'none'
      );
      map.setLayoutProperty(
        'wetlands-outline',
        'visibility',
        layerVisibility.wetlands ? 'visible' : 'none'
      );
    }

    if (map.getLayer('water-projects-fill')) {
      map.setLayoutProperty(
        'water-projects-fill',
        'visibility',
        layerVisibility.water_projects ? 'visible' : 'none'
      );
      map.setLayoutProperty(
        'water-projects-outline',
        'visibility',
        layerVisibility.water_projects ? 'visible' : 'none'
      );
    }

    if (map.getLayer('buildings-fill')) {
      map.setLayoutProperty(
        'buildings-fill',
        'visibility',
        layerVisibility.buildings ? 'visible' : 'none'
      );
      map.setLayoutProperty(
        'buildings-outline',
        'visibility',
        layerVisibility.buildings ? 'visible' : 'none'
      );
    }
  }, [layerVisibility, filterState.visualizationMode]);

  // Update filters when company filter changes
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    const companyFilter: unknown = filterState.companyFilter
      ? ['==', ['get', 'cvr_number'], parseInt(filterState.companyFilter)]
      : null;

    if (map.getLayer('fields-fill')) {
      map.setFilter(
        'fields-fill',
        companyFilter ? (companyFilter as never) : null
      );
    }
    if (map.getLayer('fields-outline')) {
      map.setFilter(
        'fields-outline',
        companyFilter ? (companyFilter as never) : null
      );
    }
    if (map.getLayer('fields-partial-coverage-base')) {
      const partialFilter = companyFilter
        ? ['all', companyFilter, ['==', ['get', 'is_partial_coverage'], true]]
        : ['==', ['get', 'is_partial_coverage'], true];
      map.setFilter('fields-partial-coverage-base', partialFilter as never);
      map.setFilter('fields-partial-coverage-pattern', partialFilter as never);
    }
    if (map.getLayer('organic-borders')) {
      let organicFilter: unknown = ['==', ['get', 'is_organic'], true];
      if (companyFilter) {
        organicFilter = ['all', companyFilter, organicFilter];
      }
      map.setFilter('organic-borders', organicFilter as never);
    }
  }, [filterState.companyFilter]);

  // Update field visualization when filterState changes
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    const propsString = JSON.stringify(fieldsPaintProps);
    if (prevPaintPropsRef.current === propsString) {
      return;
    }
    prevPaintPropsRef.current = propsString;

    requestAnimationFrame(() => {
      if (map.getLayer('fields-fill')) {
        const paintProps = fieldsPaintProps;

        map.setPaintProperty(
          'fields-fill',
          'fill-color',
          paintProps['fill-color']
        );

        if (map.getLayer('fields-partial-coverage-base')) {
          map.setPaintProperty(
            'fields-partial-coverage-base',
            'fill-color',
            paintProps['fill-color']
          );
        }

        if (map.getLayer('organic-borders')) {
          map.setLayoutProperty(
            'organic-borders',
            'visibility',
            layerVisibility.fields ? 'visible' : 'none'
          );
        }

        if (map.getLayer('fields-partial-coverage-base')) {
          map.setLayoutProperty(
            'fields-partial-coverage-base',
            'visibility',
            layerVisibility.fields ? 'visible' : 'none'
          );
          map.setLayoutProperty(
            'fields-partial-coverage-pattern',
            'visibility',
            layerVisibility.fields ? 'visible' : 'none'
          );
        }
      }
    });
  }, [fieldsPaintProps, layerVisibility.fields]);

  // Query for field data at a specific coordinate
  const queryFieldDataAtCoordinate = useCallback(
    async (lng: number, lat: number): Promise<FieldAnalysisData | null> => {
      if (!mapRef.current) return null;

      const map = mapRef.current.getMap();
      const point = map.project([lng, lat]);

      const fieldFeatures = map.queryRenderedFeatures(point, {
        layers: ['fields-fill'],
      });

      if (fieldFeatures.length > 0) {
        const fieldData = fieldFeatures[0].properties as FieldAnalysisData;
        fieldData.click_coordinates = { lat, lng };
        return fieldData;
      }

      return null;
    },
    []
  );

  // Query all visible field features in the current map view
  const queryVisibleFields = useCallback((): FieldAnalysisData[] => {
    if (!mapRef.current) return [];

    const map = mapRef.current.getMap();

    if (
      !map.getLayer('fields-fill') ||
      map.getLayoutProperty('fields-fill', 'visibility') === 'none'
    ) {
      return [];
    }

    const fieldFeatures = map.queryRenderedFeatures(undefined, {
      layers: ['fields-fill'],
    });

    const uniqueFields: Record<string, FieldAnalysisData> = {};

    fieldFeatures.forEach((feature) => {
      const fieldData = { ...feature.properties } as FieldAnalysisData;
      if (fieldData.field_uuid && !uniqueFields[fieldData.field_uuid]) {
        if (feature.geometry && !fieldData.click_coordinates) {
          const centroid = computeCentroid(feature.geometry);
          if (centroid) {
            fieldData.click_coordinates = centroid;
          }
        }
        uniqueFields[fieldData.field_uuid] = fieldData;
      }
    });

    return Object.values(uniqueFields);
  }, []);

  // Expose queryVisibleFields function via ref
  useEffect(() => {
    if (queryVisibleFieldsRef) {
      queryVisibleFieldsRef.current = queryVisibleFields;
    }
  }, [queryVisibleFields, queryVisibleFieldsRef]);

  // Check if mouse is over UI elements
  const isOverUIElement = useCallback(
    (x: number, y: number): boolean => {
      const isMobile = window.innerWidth < 768;
      const screenWidth = window.innerWidth;

      if (isMobile) {
        const searchTop = 64;
        const searchBottom = 144;
        const legendTop = isSearchActive ? 400 : 128;
        const legendBottom = legendTop + 300;

        if (
          y >= searchTop &&
          y <= searchBottom &&
          x >= 16 &&
          x <= screenWidth - 16
        ) {
          return true;
        }

        if (
          y >= legendTop &&
          y <= legendBottom &&
          x >= 16 &&
          x <= screenWidth - 16
        ) {
          return true;
        }
      } else {
        const searchLeft = 90;
        const searchTop = 16;
        const searchWidth =
          window.innerWidth >= 1280
            ? 448
            : window.innerWidth >= 1024
              ? 384
              : 320;
        const searchBottom = 80;

        const legendLeft = 90;
        const legendTop = isSearchActive ? 352 : 80;
        const legendWidth = 384;
        const legendBottom = legendTop + 400;

        if (
          x >= searchLeft &&
          x <= searchLeft + searchWidth &&
          y >= searchTop &&
          y <= searchBottom
        ) {
          return true;
        }

        if (
          x >= legendLeft &&
          x <= legendLeft + legendWidth &&
          y >= legendTop &&
          y <= legendBottom
        ) {
          return true;
        }
      }

      return false;
    },
    [isSearchActive]
  );

  // Handle hover events
  const onHover = useCallback(
    async (event: MapLayerMouseEvent) => {
      if (isOverUIElement(event.point.x, event.point.y)) {
        setHoverInfo(null);
        return;
      }

      const feature = event.features && event.features[0];
      if (feature) {
        const layerName = getLayerDisplayName(feature.layer.id);
        let properties = feature.properties || {};

        if (
          feature.layer.id.startsWith('bnbo-') ||
          feature.layer.id.startsWith('wetlands-') ||
          feature.layer.id.startsWith('water-projects-')
        ) {
          const underlyingFieldData = await queryFieldDataAtCoordinate(
            event.lngLat.lng,
            event.lngLat.lat
          );

          if (underlyingFieldData) {
            properties = {
              ...properties,
              ...underlyingFieldData,
            };
          }
        }

        setHoverInfo({
          x: event.point.x,
          y: event.point.y,
          properties,
          layerName,
          visualizationMode: filterState.visualizationMode,
          colorUnit: filterState.colorUnit,
        });
      } else {
        setHoverInfo(null);
      }
    },
    [
      filterState.visualizationMode,
      filterState.colorUnit,
      isOverUIElement,
      queryFieldDataAtCoordinate,
    ]
  );

  // Handle click events
  const onClick = useCallback(
    async (event: MapLayerMouseEvent) => {
      const now = Date.now();

      if (now - lastMapMoveTimeRef.current < 50) {
        return;
      }

      const coordinates = {
        lat: event.lngLat.lat,
        lng: event.lngLat.lng,
      };

      const feature = event.features && event.features[0];
      if (feature && feature.layer.id.startsWith('fields-')) {
        const fieldData = feature.properties as FieldAnalysisData;
        fieldData.click_coordinates = coordinates;
        onFieldSelect(fieldData);
      } else if (
        feature &&
        (feature.layer.id.startsWith('bnbo-') ||
          feature.layer.id.startsWith('wetlands-') ||
          feature.layer.id.startsWith('water-projects-'))
      ) {
        const underlyingFieldData = await queryFieldDataAtCoordinate(
          coordinates.lng,
          coordinates.lat
        );

        if (underlyingFieldData) {
          onFieldSelect(underlyingFieldData);
        } else {
          onMapClick?.(coordinates);
        }
      } else if (feature) {
        onMapClick?.(coordinates);
      } else {
        onMapClick?.(coordinates);
      }
    },
    [onFieldSelect, onMapClick, queryFieldDataAtCoordinate]
  );

  if (error) {
    return (
      <div className="bg-destructive/10 flex h-full items-center justify-center">
        <div className="text-center">
          <div className="text-destructive mb-2 flex items-center justify-center text-xl">
            <AlertTriangle className="mr-2 h-6 w-6" />
            Fejl
          </div>
          <div className="text-foreground">{error}</div>
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="bg-muted flex h-full items-center justify-center">
        <div className="text-center">
          <div className="text-foreground mb-2 text-lg font-medium">
            Indlæser kortdata...
          </div>
          <div className="border-primary mx-auto h-8 w-8 animate-spin rounded-full border-4 border-t-transparent"></div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="map-container relative h-full w-full"
      style={{
        touchAction: 'pan-x pan-y',
        cursor: 'grab',
        userSelect: 'none',
        overflow: 'hidden',
      }}
      data-testid="field-analysis-map"
    >
      {/* Search Bar */}
      <div
        className={`pointer-events-auto absolute z-30 transition-all duration-200 ${'top-[5rem] right-4 left-4 md:top-4 md:right-auto md:left-[90px] md:w-80 lg:w-96 xl:w-[28rem]'} ${
          hasRightPanel ? 'md:right-[21rem] xl:right-[29rem]' : 'md:right-4'
        }`}
        style={{
          top: 'max(4rem, calc(env(safe-area-inset-top) + 3rem))',
        }}
      >
        <SearchBar
          onLocationSelect={handleLocationSelect}
          placeholder="Søg efter adresser, byer, regioner..."
          className="w-full"
          onSearchStateChange={setIsSearchActive}
        />
      </div>

      <Map
        ref={mapRef}
        initialViewState={currentViewState}
        onMove={handleViewStateChange}
        style={{ width: '100%', height: '100%' }}
        mapStyle={styleLoadFailed ? FALLBACK_MAP_STYLE : currentMapStyle}
        interactiveLayerIds={INTERACTIVE_LAYER_IDS}
        onLoad={onMapLoad}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
        onClick={onClick}
        cursor="grab"
        dragPan={true}
        scrollZoom={true}
        doubleClickZoom={true}
        keyboard={true}
        touchPitch={false}
        onError={(error: unknown) => {
          console.error('Map error:', error);
          const errorMessage =
            (error as { error?: { message?: string } })?.error?.message || '';
          if (
            errorMessage.includes('style') ||
            errorMessage.includes('fetch')
          ) {
            setStyleLoadFailed(true);
          }
        }}
        onStyleData={() => {}}
        onSourceData={() => {}}
        reuseMaps={false}
      >
        <NavigationControl position="top-right" />
      </Map>

      {/* Color Legend */}
      <div
        className={`pointer-events-auto absolute right-4 left-4 z-30 max-h-[40vh] max-w-[calc(100vw-2rem)] overflow-auto transition-all duration-200 md:right-auto md:left-[90px] md:max-h-[calc(100vh-12rem)] md:max-w-xs ${
          isSearchActive ? 'md:top-[22rem]' : 'md:top-20'
        }`}
        style={{
          top: isSearchActive
            ? 'max(25rem, calc(env(safe-area-inset-top) + 24rem))'
            : 'max(8rem, calc(env(safe-area-inset-top) + 7rem))',
        }}
        data-testid="color-legend-container"
      >
        <ColorLegend filterState={filterState} />
      </div>

      {hoverInfo && <MapTooltip {...hoverInfo} />}
    </div>
  );
});

export default FieldAnalysisMap;
