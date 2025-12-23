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
import { getDecileBreakpoints, getColorScheme } from './colorUtils';
import { SearchBar } from './SearchBar';
import { ColorLegend } from './ColorLegend';

// Type for MapLibre map instance - updated for @vis.gl/react-maplibre
interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown, beforeId?: string) => unknown;
  removeLayer: (id: string) => void;
  removeSource: (id: string) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  setPaintProperty: (id: string, prop: string, value: unknown) => void;
  addSource: (id: string, source: unknown) => void;
  addImage: (
    id: string,
    image: HTMLCanvasElement | ImageBitmap | ImageData
  ) => void;
  hasImage: (id: string) => boolean;
  setFilter: (id: string, filter: unknown) => void;
}

// Type for MapLibre layer with optional filter property
interface MapLibreLayer {
  id: string;
  source: string;
  'source-layer': string;
  type: string;
  paint: Record<string, unknown>;
  layout: Record<string, unknown>;
  filter?: unknown;
  minzoom?: number;
  maxzoom?: number;
}

interface FieldAnalysisMapProps {
  pmtilesUrls: {
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string;
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
  hasRightPanel?: boolean; // New prop to indicate if right panel is open
  queryVisibleFieldsRef?: React.MutableRefObject<
    (() => FieldAnalysisData[]) | null
  >;
}

interface TooltipInfo {
  x: number;
  y: number;
  properties: Record<string, unknown>;
  layerName: string;
  visualizationMode: FilterState['visualizationMode'];
  colorUnit: FilterState['colorUnit'];
}

function MapTooltip({
  x,
  y,
  properties,
  layerName,
  visualizationMode,
  colorUnit,
}: TooltipInfo) {
  const formatValue = (value: unknown, unit?: string): string => {
    // Handle null, undefined, or non-numeric values
    if (value == null || (typeof value === 'number' && isNaN(value))) {
      return '0';
    }
    if (typeof value === 'number') {
      // Special formatting for different types of values
      let formatted: string;

      // For animal counts (dyr), use whole numbers with Danish thousand separators
      if (
        unit === 'dyr' ||
        (typeof unit === 'string' && unit.includes('dyr'))
      ) {
        formatted = Math.round(value).toLocaleString('da-DK');
      }
      // For areas, show appropriate precision
      else if (unit === 'ha' || unit === 'hektar') {
        formatted = value.toLocaleString('da-DK', {
          minimumFractionDigits: value < 1 ? 2 : 1,
          maximumFractionDigits: value < 1 ? 2 : 1,
        });
      }
      // For percentages, show 1 decimal place
      else if (
        unit === '%' ||
        (typeof unit === 'string' && unit.includes('%'))
      ) {
        formatted = value.toLocaleString('da-DK', {
          minimumFractionDigits: 1,
          maximumFractionDigits: 1,
        });
      }
      // For large numbers (likely counts), use whole numbers
      else if (value >= 1000) {
        formatted = Math.round(value).toLocaleString('da-DK');
      }
      // For small numbers, use appropriate precision
      else {
        formatted = value.toLocaleString('da-DK', {
          maximumFractionDigits: value < 1 ? 2 : 1,
        });
      }

      return unit ? `${formatted} ${unit}` : formatted;
    }
    return String(value ?? '');
  };

  const getRelevantData = () => {
    const data: Array<{ label: string; value: unknown; unit?: string }> = [];

    // Show layer-specific data first
    if (layerName === 'BNBO Område') {
      // Show BNBO status if available
      if (properties.status_category) {
        const statusLabel =
          properties.status_category === 'Action Required'
            ? 'BNBO handling påkrævet'
            : properties.status_category === 'Completed'
              ? 'BNBO gennemført'
              : 'BNBO status';
        const statusValue =
          properties.status_category === 'Action Required'
            ? 'Handling påkrævet'
            : properties.status_category === 'Completed'
              ? 'Gennemført'
              : properties.status_category;
        data.push({ label: statusLabel, value: statusValue });
      }
    } else if (layerName === 'Lavbundsområde') {
      // Show wetland-specific data
      if (properties.wetland_id) {
        data.push({ label: 'Vådomr. ID', value: properties.wetland_id });
      }
      if (properties.toerv_pct) {
        data.push({ label: 'Tørv indhold', value: properties.toerv_pct });
      }
      if (properties.toerv_description) {
        data.push({
          label: 'Tørv beskrivelse',
          value: properties.toerv_description,
        });
      }
    } else if (layerName === 'Vandprojekt') {
      // Show water project-specific data
      if (properties.project_id) {
        data.push({ label: 'Projekt ID', value: properties.project_id });
      }
      if (properties.feature_count) {
        data.push({ label: 'Antal features', value: properties.feature_count });
      }
      if (properties.dissolved_at) {
        data.push({ label: 'Opløst dato', value: properties.dissolved_at });
      }
    }

    // Always show basic field info if available (for underlying field data)
    if (properties.crop_name) {
      data.push({ label: 'Afgrøde', value: properties.crop_name });
    }

    if (properties.area_hectares) {
      data.push({
        label: 'Markareal',
        value: properties.area_hectares,
        unit: 'ha',
      });
    }

    if (properties.is_organic !== undefined) {
      data.push({
        label: 'Økologisk',
        value: properties.is_organic ? 'Ja' : 'Nej',
      });
    }

    if (properties.kommune) {
      data.push({ label: 'Kommune', value: properties.kommune });
    }

    // Show building-specific data if available
    if (layerName === 'Bygning') {
      if (properties.address) {
        data.push({ label: 'Adresse', value: properties.address });
      }

      // Show building usage category with Danish labels
      if (properties.building_usage_category) {
        const categoryLabels: Record<string, string> = {
          residential: 'Bolig',
          agricultural: 'Landbrug',
          publicServices: 'Skole og daginstitutioner',
        };
        const categoryLabel =
          categoryLabels[properties.building_usage_category as string] ||
          properties.building_usage_category;
        data.push({ label: 'Kategori', value: categoryLabel });
      }

      // Show BBR usage code with official Danish labels
      if (properties.bbr_usage_code) {
        const bbrUsageLabels: Record<string, string> = {
          // Boliger (100-199)
          '110': 'Stuehus til landbrugsejendom',
          '120': 'Fritliggende enfamiliehus',
          '130': 'Række-, kæde- eller dobbelthus',
          '140': 'Etageboligbebyggelse',
          '150': 'Kollegium',
          '160': 'Døgninstitution',
          '190': 'Anden boligbenyttelse',

          // Erhverv (200-299)
          '210': 'Kontor og lign.',
          '211': 'Pengeinstitut, forsikring og lign.',
          '212': 'Offentlig administration',
          '213': 'Liberalt erhverv',
          '214': 'Anden kontorvirksomhed',
          '215': 'Konsulentvirksomhed og lign.',
          '216': 'Virksomhed og kontor i samme bygning',
          '217': 'Blandet erhverv og kontor',
          '218': 'IT og kommunikation',
          '219': 'Anden erhvervsvirksomhed',
          '220': 'Butik og lign.',
          '230': 'Hotel og restaurant',
          '240': 'Finansiel tjeneste',
          '250': 'Håndværk og industri i bymæssig bebyggelse',
          '290': 'Anden erhvervsbebyggelse',

          // Produktions- og lagerbygninger (300-399)
          '310': 'Industri',
          '320': 'Værksted og lign.',
          '330': 'Lager',
          '340': 'Energiproduktion og -forsyning',
          '390': 'Anden produktions- og lagerbygning',

          // Transport (400-499)
          '410': 'Garageanlæg',
          '420': 'Bygning til kollektiv transport',
          '421': 'Jernbanestation og lign.',
          '422': 'Bustation og lign.',
          '429': 'Anden transportbygning',
          '441': 'Lufthavn',
          '490': 'Anden transportbebyggelse',

          // Institutioner (500-599)
          '510': 'Undervisning og forskning',
          '520': 'Hospital og sygehus',
          '530': 'Sundhed og sociale formål',
          '540': 'Institution',
          '550': 'Forsamling og sport',
          '560': 'Kultur og kirke',
          '590': 'Anden institutionsbebyggelse',

          // Fritidsbebyggelse (600-699)
          '610': 'Sommerhus',
          '620': 'Anden fritidsbebyggelse',
          '690': 'Anden fritidsbebyggelse',

          // Landbrugs- og skovbrugsbygninger (900-999)
          '910': 'Stuehus til landbrugsejendom',
          '920': 'Driftsbygning til landbrugsejendom',
          '930': 'Anden bygning til landbrugsformål',
          '940': 'Bygning til gartneri, planteskole og lign.',
          '950': 'Bygning til pelsdyravl',
          '960': 'Bygning til fiskeopdræt',
          '970': 'Skovbrugsbygning',
          '990': 'Anden landbrugs- eller skovbrugsbygning',
        };

        const usageLabel =
          bbrUsageLabels[properties.bbr_usage_code as string] ||
          `BBR kode ${properties.bbr_usage_code}`;
        data.push({ label: 'BBR anvendelse', value: usageLabel });
      }

      // Show detailed INSPIRE usage as fallback
      else if (properties.inspire_current_use) {
        const usageLabels: Record<string, string> = {
          individualResidence: 'Enfamilieboliger',
          agriculture: 'Landbrugsbygninger',
          collectiveResidence: 'Flerfamilieboliger',
          twoDwellings: 'Tofamiliehuse',
          publicServices: 'Skole og daginstitutioner',
        };
        const usageLabel =
          usageLabels[properties.inspire_current_use as string] ||
          properties.inspire_current_use;
        data.push({ label: 'Anvendelse', value: usageLabel });
      }

      if (properties.building_type) {
        data.push({ label: 'Bygningstype', value: properties.building_type });
      }

      if (properties.inspire_construction_year) {
        data.push({
          label: 'Byggeår',
          value: properties.inspire_construction_year,
        });
      }

      if (properties.inspire_floor_area) {
        data.push({
          label: 'Etageareal',
          value: properties.inspire_floor_area,
          unit: 'm²',
        });
      }

      if (properties.inspire_floors) {
        data.push({ label: 'Etager', value: properties.inspire_floors });
      }

      if (properties.inspire_dwellings) {
        data.push({ label: 'Boliger', value: properties.inspire_dwellings });
      }

      if (properties.distance_m) {
        data.push({
          label: 'Afstand til mark',
          value: properties.distance_m,
          unit: 'm',
        });
      }
    }

    // Show data relevant to current visualization mode
    switch (visualizationMode) {
      case 'total_pesticide_belastning':
        if (properties.total_pesticide_belastning) {
          data.push({
            label: 'Total pesticidbelastning',
            value: properties.total_pesticide_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : '',
          });
        }
        if (properties.total_pesticide_applications) {
          data.push({
            label: 'Antal pesticider',
            value: properties.total_pesticide_applications,
          });
        }
        break;

      case 'pfas_belastning':
        if (properties.total_pfas_belastning) {
          data.push({
            label: 'PFAS belastning',
            value: properties.total_pfas_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : '',
          });
        }
        if (properties.total_pfas_active_ingredient_kg) {
          data.push({
            label: 'PFAS aktivstof',
            value: properties.total_pfas_active_ingredient_kg,
            unit: 'kg',
          });
        }
        if (properties.pfas_applications) {
          data.push({
            label: 'PFAS pesticider',
            value: properties.pfas_applications,
          });
        }
        break;

      case 'diquat_belastning':
        if (properties.total_diquat_belastning) {
          data.push({
            label: 'Diquat belastning',
            value: properties.total_diquat_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : '',
          });
        }
        if (properties.diquat_applications) {
          data.push({
            label: 'Diquat pesticider',
            value: properties.diquat_applications,
          });
        }
        break;

      case 'glyphosate_belastning':
        if (properties.total_glyphosate_belastning) {
          data.push({
            label: 'Glyphosate belastning',
            value: properties.total_glyphosate_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : '',
          });
        }
        if (properties.total_glyphosate_active_ingredient_kg) {
          data.push({
            label: 'Glyphosate aktivstof',
            value: properties.total_glyphosate_active_ingredient_kg,
            unit: 'kg',
          });
        }
        if (properties.glyphosate_applications) {
          data.push({
            label: 'Glyphosate pesticider',
            value: properties.glyphosate_applications,
          });
        }
        break;

      case 'applications_count':
        if (properties.total_pesticide_applications) {
          data.push({
            label: 'Total pesticider',
            value: properties.total_pesticide_applications,
          });
        }
        if (properties.unique_pesticide_products) {
          data.push({
            label: 'Unikke produkter',
            value: properties.unique_pesticide_products,
          });
        }
        break;

      case 'area_size':
        if (properties.area_hectares) {
          data.push({
            label: 'Markareal',
            value: properties.area_hectares,
            unit: 'ha',
          });
        }
        break;
    }

    return data.slice(0, 6); // Limit to 6 items
  };

  const relevantData = getRelevantData();

  return (
    <div
      className="border-border bg-background absolute z-[60] max-w-sm rounded-xl border shadow-xl backdrop-blur-sm"
      style={{
        left: x,
        top: y,
        transform: 'translate(-50%, -100%)',
        marginTop: -12,
      }}
    >
      {/* Header with better typography */}
      <div className="border-border border-b px-4 py-3">
        <h3 className="text-foreground text-base leading-tight font-semibold">
          {layerName}
        </h3>
        {properties.site_name ? (
          <p className="text-muted-foreground mt-1 text-sm font-medium">
            {String(properties.site_name)}
          </p>
        ) : null}
        {/* Show indicator if this is an environmental layer with underlying field data */}
        {(layerName === 'BNBO Område' ||
          layerName === 'Lavbundsområde' ||
          layerName === 'Vandprojekt') &&
          Boolean(properties.crop_name) && (
            <p className="text-muted-foreground mt-1 text-xs italic">
              Inkluderer markdata
            </p>
          )}
      </div>

      {/* Content with improved spacing and hierarchy */}
      <div className="px-4 py-3">
        <div className="space-y-2.5">
          {relevantData.map(({ label, value, unit }, index) => (
            <div
              key={index}
              className="flex items-baseline justify-between gap-3"
            >
              <span className="text-muted-foreground text-sm leading-tight font-medium">
                {label}:
              </span>
              <span className="text-foreground text-right text-sm leading-tight font-semibold">
                {formatValue(value, unit)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
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

  // Fallback map style in case external style fails
  const fallbackMapStyle = {
    version: 8 as const,
    sources: {},
    layers: [
      {
        id: 'background',
        type: 'background' as const,
        paint: {
          'background-color': '#f8f9fa',
        },
      },
    ],
  };
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

  // PROPERLY MERGE viewState - merge external with internal defaults
  const currentViewState: ViewState & { width: number; height: number } =
    externalViewState
      ? { ...internalViewState, ...externalViewState }
      : internalViewState;

  // Track map interactions to prevent clicks during dragging
  const isMapDraggingRef = useRef(false);
  const lastMapMoveTimeRef = useRef<number>(0);

  // Optimized throttling using requestAnimationFrame for smooth performance
  const rafIdRef = useRef<number | null>(null);
  const lastViewState = useRef<ViewState | null>(null);
  const finalNotifyTimeout = useRef<NodeJS.Timeout | null>(null);

  // Handle view state changes - OPTIMIZED WITH RAF THROTTLING
  const handleViewStateChange = useCallback(
    (evt: { viewState: ViewState }) => {
      const newViewState = evt.viewState;
      const currentViewState = externalViewState || internalViewState;

      // Only track movement time if the view actually changed significantly
      const hasSignificantChange =
        !currentViewState ||
        Math.abs(newViewState.longitude - (currentViewState.longitude ?? 0)) >
          0.0001 ||
        Math.abs(newViewState.latitude - (currentViewState.latitude ?? 0)) >
          0.0001 ||
        Math.abs(newViewState.zoom - (currentViewState.zoom ?? 0)) > 0.01;

      if (hasSignificantChange) {
        lastMapMoveTimeRef.current = Date.now();
        isMapDraggingRef.current = true;
      }

      // Always update internal state immediately for smooth map movement
      if (!externalViewState) {
        setInternalViewState({
          ...newViewState,
          width: internalViewState.width,
          height: internalViewState.height,
        });
      }

      // Store the latest view state
      lastViewState.current = newViewState;

      // Use RAF throttling for smooth performance during interactions
      if (onViewStateChange && !rafIdRef.current) {
        rafIdRef.current = requestAnimationFrame(() => {
          rafIdRef.current = null;
          if (lastViewState.current) {
            onViewStateChange(lastViewState.current);
          }
        });
      }

      // Schedule final notification when movement stops
      if (finalNotifyTimeout.current) {
        clearTimeout(finalNotifyTimeout.current);
      }
      finalNotifyTimeout.current = setTimeout(() => {
        if (onViewStateChange && lastViewState.current) {
          onViewStateChange(lastViewState.current);
        }
        // Reset dragging state after movement stops
        isMapDraggingRef.current = false;
      }, 150); // Send final position after 150ms of no movement
    },
    [onViewStateChange, externalViewState, internalViewState]
  );

  // Handle location selection from search
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

      // Fly to the selected location
      map.flyTo({
        center: [location.lng, location.lat],
        zoom: 14,
        duration: 1500,
      });

      // Call the parent callback if provided
      onLocationSelect?.(location);
    },
    [onLocationSelect]
  );

  // Stable callback refs to prevent excessive re-creation
  const onMapReadyRef = useRef(onMapReady);
  const pmtilesUrlsRef = useRef(pmtilesUrls);

  // Update refs when props change
  useEffect(() => {
    onMapReadyRef.current = onMapReady;
    pmtilesUrlsRef.current = pmtilesUrls;
  });

  // OPTIMIZED loading timeout with stable dependencies
  const startLoadingTimeout = useCallback(() => {
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    loadingTimeoutRef.current = setTimeout(() => {
      console.warn('Map loading timeout - forcing loading state to false');
      setIsLoading(false);
      onMapReadyRef.current?.();
    }, 3000); // Reduced from 10s to 3s - data loads quickly
  }, []); // No dependencies - uses ref

  // OPTIMIZED - Check if all required sources are loaded
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
  }, [isLoading]); // Only depend on isLoading state

  // Handle source data events to detect when PMTiles are loaded
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
        // Start loading timeout when initialization begins
        startLoadingTimeout();

        // Import MapLibre GL dynamically to avoid SSR issues
        const [maplibregl, { Protocol }] = await Promise.all([
          import('maplibre-gl'),
          import('pmtiles'),
        ]);

        console.log('MapLibre and PMTiles loaded successfully');

        // Register PMTiles protocol with MapLibre (only once globally)
        if (
          !(window as unknown as { __pmtiles_protocol_registered?: boolean })
            .__pmtiles_protocol_registered
        ) {
          const protocol = new Protocol();
          maplibregl.default.addProtocol('pmtiles', protocol.tile);
          (
            window as unknown as { __pmtiles_protocol_registered?: boolean }
          ).__pmtiles_protocol_registered = true;
          console.log('PMTiles protocol registered');
        }

        // Don't set loading false here - let the map load callback handle it
      } catch (err) {
        console.error(
          `Failed to initialize PMTiles (attempt ${retryCount + 1}):`,
          err
        );
        // Retry up to maxRetries times
        if (retryCount < maxRetries) {
          retryCount++;
          console.log(
            `Retrying PMTiles initialization (${retryCount}/${maxRetries})...`
          );
          setTimeout(initializePMTiles, 1000 * retryCount); // Exponential backoff
          return;
        }

        // Final failure after all retries
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

    // Cleanup timeout on unmount
    return () => {
      if (loadingTimeoutRef.current) {
        clearTimeout(loadingTimeoutRef.current);
        loadingTimeoutRef.current = null;
      }
    };
  }, [startLoadingTimeout]); // Added missing dependency

  // Cleanup event listeners and RAF on unmount
  useEffect(() => {
    const currentMapRef = mapRef.current;
    return () => {
      // Cleanup RAF
      if (rafIdRef.current) {
        cancelAnimationFrame(rafIdRef.current);
        rafIdRef.current = null;
      }

      // Cleanup final notify timeout
      if (finalNotifyTimeout.current) {
        clearTimeout(finalNotifyTimeout.current);
        finalNotifyTimeout.current = null;
      }

      // Cleanup map event listeners
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

  // Memoize paint properties to prevent expensive recalculations during drag/zoom
  const fieldsPaintProps = useMemo(() => {
    const { visualizationMode, colorUnit, useDecileColoring } = filterState;
    const colorScheme = getColorScheme(visualizationMode);

    // Handle organic status visualization with symbols
    if (visualizationMode === 'organic_status') {
      return {
        'fill-color': [
          'case',
          ['==', ['get', 'is_organic'], true],
          'transparent', // Transparent fill for organic fields - will use symbols instead
          '#f3f4f6', // Light gray for non-organic
        ],
        'fill-opacity': 0.6,
      };
    }

    // Get the appropriate field name for the visualization mode
    const getFieldName = (mode: FilterState['visualizationMode']) => {
      switch (mode) {
        case 'total_pesticide_belastning':
          return 'total_pesticide_belastning';
        case 'pfas_belastning':
          return 'total_pfas_belastning';
        case 'diquat_belastning':
          return 'total_diquat_belastning';
        case 'glyphosate_belastning':
          return 'total_glyphosate_belastning';
        case 'applications_count':
          return 'total_pesticide_applications';
        case 'area_size':
          return 'area_hectares';
        default:
          return 'total_pesticide_belastning';
      }
    };

    const fieldName = getFieldName(visualizationMode);
    console.log('Color generation:', {
      visualizationMode,
      fieldName,
      colorScheme: colorScheme.name,
    });

    if (useDecileColoring) {
      // Use decile-based coloring with step function
      const breakpoints = getDecileBreakpoints(visualizationMode, colorUnit);
      const colors = colorScheme.colors;

      return {
        'fill-color': [
          'case',
          ['<=', ['coalesce', ['get', fieldName], 0], 0],
          '#f3f4f6', // Light gray for zero/negative values
          [
            'step',
            ['coalesce', ['get', fieldName], 0],
            colors[0], // Base color for lowest values
            breakpoints[0],
            colors[1],
            breakpoints[1],
            colors[2],
            breakpoints[2],
            colors[3],
            breakpoints[3],
            colors[4],
            breakpoints[4],
            colors[5],
            breakpoints[5],
            colors[6],
            breakpoints[6],
            colors[7],
            breakpoints[7],
            colors[8],
            breakpoints[8],
            colors[9],
          ],
        ],
        'fill-opacity': 0.7,
      };
    } else {
      // Use linear interpolation with proper structure
      const colors = colorScheme.colors;
      return {
        'fill-color': [
          'case',
          ['<=', ['coalesce', ['get', fieldName], 0], 0],
          '#f3f4f6', // Light gray for zero/negative values
          [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', fieldName], 0],
            0.1,
            colors[0],
            1,
            colors[2],
            10,
            colors[4],
            50,
            colors[6],
            100,
            colors[8],
            500,
            colors[9],
          ],
        ],
        'fill-opacity': 0.7,
      };
    }
  }, [filterState.visualizationMode, filterState.colorUnit, filterState.useDecileColoring]);

  // Remove field analysis layers
  const removeFieldsLayers = useCallback((map: MapInstance) => {
    const fieldLayerIds = [
      'organic-borders',
      'fields-outline',
      'fields-partial-coverage-pattern',
      'fields-partial-coverage-base',
      'fields-fill',
    ];

    fieldLayerIds.forEach((layerId) => {
      if (map.getLayer(layerId)) {
        map.removeLayer(layerId);
      }
    });
  }, []);

  // Add field analysis layers
  const addFieldsLayers = useCallback(
    (map: MapInstance) => {
      if (map.getSource('fields') && !map.getLayer('fields-fill')) {
        const paintProps = fieldsPaintProps;

        // Create company filter if specified
        const companyFilter: unknown = filterState.companyFilter
          ? ['==', ['get', 'cvr_number'], parseInt(filterState.companyFilter)]
          : null;

        // Create partial coverage pattern - will be updated dynamically
        const createPartialCoveragePattern = async (
          color: string = '#374151'
        ) => {
          try {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            canvas.width = 32;
            canvas.height = 32;

            if (ctx) {
              // Transparent background
              ctx.clearRect(0, 0, 32, 32);

              // Dynamic color diagonal hash pattern
              ctx.strokeStyle = color;
              ctx.lineWidth = 2;
              ctx.globalAlpha = 0.9;
              ctx.beginPath();

              // Diagonal lines going top-left to bottom-right
              for (let i = -32; i <= 64; i += 6) {
                ctx.moveTo(i, 0);
                ctx.lineTo(i + 32, 32);
              }
              ctx.stroke();

              const bitmap = await createImageBitmap(canvas);
              if (!map.hasImage('partial-coverage-pattern')) {
                map.addImage('partial-coverage-pattern', bitmap);
              }
            }
          } catch (error) {
            console.warn('Failed to create partial coverage pattern:', error);
          }
        };

        // Create initial pattern with default color
        createPartialCoveragePattern();

        // Main fields layer with zoom-based opacity
        const fieldsLayer: MapLibreLayer = {
          id: 'fields-fill',
          source: 'fields',
          'source-layer': 'field_analysis',
          type: 'fill',
          paint: {
            'fill-color': paintProps['fill-color'],
            'fill-opacity': [
              'interpolate',
              ['linear'],
              ['zoom'],
              6, 0.3,   // At zoom 6, 30% opacity (less clutter)
              10, 0.7,  // At zoom 10, 70% opacity
              14, 0.7,  // At zoom 14+, normal opacity
            ],
          },
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
          minzoom: 6,  // Don't render individual fields below zoom 6
        };

        // Only add filter if it exists (MapLibre expects array or undefined, not null)
        if (companyFilter) {
          fieldsLayer.filter = companyFilter;
        }

        map.addLayer(fieldsLayer);

        // Partial coverage overlay layer - uses same colors as main layer but with hash pattern
        const partialCoveragePaint = { ...paintProps };

        // Add hash pattern overlay for partial coverage fields
        map.addLayer({
          id: 'fields-partial-coverage-base',
          source: 'fields',
          'source-layer': 'field_analysis',
          type: 'fill',
          paint: partialCoveragePaint,
          filter: companyFilter
            ? [
                'all',
                companyFilter,
                ['==', ['get', 'is_partial_coverage'], true],
              ]
            : ['==', ['get', 'is_partial_coverage'], true],
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
        });

        // Add diagonal hash pattern on top for partial coverage indication
        map.addLayer({
          id: 'fields-partial-coverage-pattern',
          source: 'fields',
          'source-layer': 'field_analysis',
          type: 'fill',
          paint: {
            'fill-pattern': 'partial-coverage-pattern',
            'fill-opacity': 0.7,
          },
          filter: companyFilter
            ? [
                'all',
                companyFilter,
                ['==', ['get', 'is_partial_coverage'], true],
              ]
            : ['==', ['get', 'is_partial_coverage'], true],
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
        });

        // Fields outline with zoom-based line width
        const fieldsOutlineLayer: MapLibreLayer = {
          id: 'fields-outline',
          source: 'fields',
          'source-layer': 'field_analysis',
          type: 'line',
          paint: {
            'line-color': '#374151',
            'line-width': [
              'interpolate',
              ['linear'],
              ['zoom'],
              6, 0,     // No outline at zoom 6
              10, 0.3,  // Thin outline at zoom 10
              14, 0.5,  // Normal outline at zoom 14+
            ],
            'line-opacity': [
              'interpolate',
              ['linear'],
              ['zoom'],
              6, 0,
              10, 0.5,
              14, 0.8,
            ],
          },
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
        };

        // Only add filter if it exists
        if (companyFilter) {
          fieldsOutlineLayer.filter = companyFilter;
        }

        map.addLayer(fieldsOutlineLayer);

        // Add organic borders layer - dashed green borders for organic fields
        let organicFilter: unknown = ['==', ['get', 'is_organic'], true];
        if (companyFilter) {
          organicFilter = ['all', companyFilter, organicFilter];
        }

        map.addLayer({
          id: 'organic-borders',
          source: 'fields',
          'source-layer': 'field_analysis',
          type: 'line',
          filter: organicFilter,
          paint: {
            'line-color': '#16a34a', // Green-600
            'line-width': 3,
            'line-opacity': 0.9,
            'line-dasharray': [2, 2], // Dashed line pattern
          },
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
        });
      }
    },
    [layerVisibility.fields, fieldsPaintProps, filterState.companyFilter]
  );

  // Add BNBO layers with cross-hatch pattern
  const addBNBOLayers = useCallback(
    (map: MapInstance) => {
      console.log('🔍 addBNBOLayers called');
      console.log('🔍 BNBO source exists:', !!map.getSource('bnbo'));
      console.log('🔍 BNBO layer exists:', !!map.getLayer('bnbo-fill'));
      if (map.getSource('bnbo') && !map.getLayer('bnbo-fill')) {
        console.log('✅ Adding BNBO layers...');
        // Create status-based patterns for BNBO
        const createBNBOPatterns = async () => {
          try {
            // Create completed pattern (green with diagonal lines)
            const completedCanvas = document.createElement('canvas');
            const completedCtx = completedCanvas.getContext('2d');
            completedCanvas.width = 32;
            completedCanvas.height = 32;

            if (completedCtx) {
              completedCtx.fillStyle = '#10B981'; // Green background
              completedCtx.fillRect(0, 0, 32, 32);
              completedCtx.strokeStyle = 'rgba(255, 255, 255, 0.3)';
              completedCtx.lineWidth = 2;
              completedCtx.beginPath();
              // Diagonal lines
              for (let i = -32; i <= 64; i += 8) {
                completedCtx.moveTo(i, 0);
                completedCtx.lineTo(i + 32, 32);
              }
              completedCtx.stroke();

              const completedBitmap = await createImageBitmap(completedCanvas);
              if (!map.hasImage('bnbo-completed-pattern')) {
                map.addImage('bnbo-completed-pattern', completedBitmap);
              }
            }

            // Create action required pattern (red with cross-hatch)
            const actionCanvas = document.createElement('canvas');
            const actionCtx = actionCanvas.getContext('2d');
            actionCanvas.width = 32;
            actionCanvas.height = 32;

            if (actionCtx) {
              actionCtx.fillStyle = '#EAB308'; // Yellow background
              actionCtx.fillRect(0, 0, 32, 32);
              actionCtx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
              actionCtx.lineWidth = 2;
              actionCtx.beginPath();
              // Cross-hatch pattern
              for (let i = -32; i <= 64; i += 8) {
                actionCtx.moveTo(i, 0);
                actionCtx.lineTo(i + 32, 32);
                actionCtx.moveTo(i + 32, 0);
                actionCtx.lineTo(i, 32);
              }
              actionCtx.stroke();

              const actionBitmap = await createImageBitmap(actionCanvas);
              if (!map.hasImage('bnbo-action-pattern')) {
                map.addImage('bnbo-action-pattern', actionBitmap);
              }
            }
          } catch (error) {
            console.warn('Failed to create BNBO patterns:', error);
          }
        };

        try {
          map.addLayer({
            id: 'bnbo-fill',
            source: 'bnbo',
            'source-layer': 'bnbo',
            type: 'fill',
            paint: {
              'fill-color': [
                'case',
                // If action is required (yellow)
                ['==', ['get', 'status_category'], 'Action Required'],
                '#EAB308',
                // If completed (green)
                ['==', ['get', 'status_category'], 'Completed'],
                '#10B981',
                // Default blue for general BNBO areas
                '#2563EB',
              ],
              'fill-opacity': 0.6,
            },
            layout: {
              visibility: layerVisibility.bnbo ? 'visible' : 'none',
            },
          });
          console.log('✅ BNBO fill layer added successfully');
        } catch (error) {
          console.error('🚨 Failed to add BNBO fill layer:', error);
        }

        // Create patterns after layer is added
        createBNBOPatterns().then(() => {
          // Apply patterns based on status
          if (map.getLayer('bnbo-fill')) {
            map.setPaintProperty('bnbo-fill', 'fill-pattern', [
              'case',
              ['==', ['get', 'status_category'], 'Action Required'],
              'bnbo-action-pattern',
              ['==', ['get', 'status_category'], 'Completed'],
              'bnbo-completed-pattern',
              '', // No pattern for general areas
            ]);
          }
        });

        try {
          map.addLayer({
            id: 'bnbo-outline',
            source: 'bnbo',
            'source-layer': 'bnbo',
            type: 'line',
            paint: {
              'line-color': [
                'case',
                ['==', ['get', 'status_category'], 'Action Required'],
                '#DC2626', // Darker red outline
                ['==', ['get', 'status_category'], 'Completed'],
                '#059669', // Darker green outline
                '#1D4ED8', // Darker blue outline
              ],
              'line-width': 1.5,
              'line-opacity': 0.9,
            },
            layout: {
              visibility: layerVisibility.bnbo ? 'visible' : 'none',
            },
          });
          console.log('✅ BNBO outline layer added successfully');
        } catch (error) {
          console.error('🚨 Failed to add BNBO outline layer:', error);
        }
      }
    },
    [layerVisibility.bnbo]
  );

  // Add wetlands layers with wave pattern
  const addWetlandsLayers = useCallback(
    (map: MapInstance) => {
      if (map.getSource('wetlands') && !map.getLayer('wetlands-fill')) {
        // Create wave pattern for wetlands
        const createWetlandsPattern = async () => {
          try {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            canvas.width = 24;
            canvas.height = 16;

            if (ctx) {
              // Fill with blue background
              ctx.fillStyle = '#3B82F6';
              ctx.fillRect(0, 0, 24, 16);

              // Add wave pattern
              ctx.strokeStyle = 'rgba(255, 255, 255, 0.5)';
              ctx.lineWidth = 1.5;
              ctx.beginPath();
              // Top wave
              ctx.moveTo(0, 4);
              ctx.quadraticCurveTo(6, 2, 12, 4);
              ctx.quadraticCurveTo(18, 6, 24, 4);
              // Middle wave
              ctx.moveTo(0, 8);
              ctx.quadraticCurveTo(6, 6, 12, 8);
              ctx.quadraticCurveTo(18, 10, 24, 8);
              // Bottom wave
              ctx.moveTo(0, 12);
              ctx.quadraticCurveTo(6, 10, 12, 12);
              ctx.quadraticCurveTo(18, 14, 24, 12);
              ctx.stroke();

              const imageBitmap = await createImageBitmap(canvas);
              if (!map.hasImage('wetlands-pattern')) {
                map.addImage('wetlands-pattern', imageBitmap);
              }

              // Update layer to use pattern
              if (map.getLayer('wetlands-fill')) {
                map.setPaintProperty(
                  'wetlands-fill',
                  'fill-pattern',
                  'wetlands-pattern'
                );
                map.setPaintProperty('wetlands-fill', 'fill-opacity', 0.4);
              }
            }
          } catch (error) {
            console.warn('Failed to create wetlands pattern:', error);
          }
        };

        map.addLayer({
          id: 'wetlands-fill',
          source: 'wetlands',
          'source-layer': 'wetlands',
          type: 'fill',
          paint: {
            'fill-color': '#3B82F6', // Fallback color
            'fill-opacity': 0.4,
          },
          layout: {
            visibility: layerVisibility.wetlands ? 'visible' : 'none',
          },
        });

        // Create pattern after layer is added
        createWetlandsPattern();

        map.addLayer({
          id: 'wetlands-outline',
          source: 'wetlands',
          'source-layer': 'wetlands',
          type: 'line',
          paint: {
            'line-color': '#1E40AF',
            'line-width': 1.5,
            'line-opacity': 0.8,
          },
          layout: {
            visibility: layerVisibility.wetlands ? 'visible' : 'none',
          },
        });
      }
    },
    [layerVisibility.wetlands]
  );

  // Add water projects layers with dot pattern
  const addWaterProjectsLayers = useCallback(
    (map: MapInstance) => {
      if (
        map.getSource('water_projects') &&
        !map.getLayer('water-projects-fill')
      ) {
        // Create dot pattern for water projects
        const createWaterProjectsPattern = async () => {
          try {
            const canvas = document.createElement('canvas');
            const ctx = canvas.getContext('2d');
            canvas.width = 20;
            canvas.height = 20;

            if (ctx) {
              // Fill with teal background
              ctx.fillStyle = '#14B8A6';
              ctx.fillRect(0, 0, 20, 20);

              // Add dot pattern
              ctx.fillStyle = 'rgba(255, 255, 255, 0.6)';
              // Create dots in a grid pattern
              const dotSize = 2;
              const spacing = 6;
              for (let x = spacing / 2; x < 20; x += spacing) {
                for (let y = spacing / 2; y < 20; y += spacing) {
                  ctx.beginPath();
                  ctx.arc(x, y, dotSize, 0, 2 * Math.PI);
                  ctx.fill();
                }
              }

              const imageBitmap = await createImageBitmap(canvas);
              if (!map.hasImage('water-projects-pattern')) {
                map.addImage('water-projects-pattern', imageBitmap);
              }

              // Update layer to use pattern
              if (map.getLayer('water-projects-fill')) {
                map.setPaintProperty(
                  'water-projects-fill',
                  'fill-pattern',
                  'water-projects-pattern'
                );
                map.setPaintProperty(
                  'water-projects-fill',
                  'fill-opacity',
                  0.5
                );
              }
            }
          } catch (error) {
            console.warn('Failed to create water projects pattern:', error);
          }
        };

        map.addLayer({
          id: 'water-projects-fill',
          source: 'water_projects',
          'source-layer': 'water_projects',
          type: 'fill',
          paint: {
            'fill-color': '#14B8A6', // Fallback color
            'fill-opacity': 0.5,
          },
          layout: {
            visibility: layerVisibility.water_projects ? 'visible' : 'none',
          },
        });

        // Create pattern after layer is added
        createWaterProjectsPattern();

        map.addLayer({
          id: 'water-projects-outline',
          source: 'water_projects',
          'source-layer': 'water_projects',
          type: 'line',
          paint: {
            'line-color': '#0F766E',
            'line-width': 2,
            'line-opacity': 0.9,
          },
          layout: {
            visibility: layerVisibility.water_projects ? 'visible' : 'none',
          },
        });
      }
    },
    [layerVisibility.water_projects]
  );

  // Add buildings layers
  const addBuildingsLayers = useCallback(
    (map: MapInstance) => {
      if (map.getSource('buildings') && !map.getLayer('buildings-fill')) {
        console.log('✅ Adding Buildings layers...');
        try {
          map.addLayer({
            id: 'buildings-fill',
            source: 'buildings',
            'source-layer': 'buildings',
            type: 'fill',
            paint: {
              'fill-color': [
                'case',
                // Educational/Public services buildings - Pink
                ['==', ['get', 'building_usage_category'], 'publicServices'],
                '#EC4899', // Pink for schools and daycare
                // Agricultural buildings - Brown
                ['==', ['get', 'building_usage_category'], 'agricultural'],
                '#A16207', // Brown for agricultural buildings
                // Residential buildings - Light blue (default)
                '#4A90E2',
              ],
              'fill-opacity': 0.6,
            },
            layout: {
              visibility: layerVisibility.buildings ? 'visible' : 'none',
            },
          });
          console.log('✅ Buildings fill layer added successfully');
        } catch (error) {
          console.error('🚨 Failed to add Buildings fill layer:', error);
        }

        try {
          map.addLayer({
            id: 'buildings-outline',
            source: 'buildings',
            'source-layer': 'buildings',
            type: 'line',
            paint: {
              'line-color': [
                'case',
                // Educational/Public services buildings - Darker pink
                ['==', ['get', 'building_usage_category'], 'publicServices'],
                '#BE185D', // Darker pink outline
                // Agricultural buildings - Darker brown
                ['==', ['get', 'building_usage_category'], 'agricultural'],
                '#92400E', // Darker brown outline
                // Residential buildings - Default blue
                '#2563EB',
              ],
              'line-width': 1,
              'line-opacity': 0.8,
            },
            layout: {
              visibility: layerVisibility.buildings ? 'visible' : 'none',
            },
          });
          console.log('✅ Buildings outline layer added successfully');
        } catch (error) {
          console.error('🚨 Failed to add Buildings outline layer:', error);
        }
      }
    },
    [layerVisibility.buildings]
  );

  // Handle map load and add sources
  const onMapLoad = useCallback(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap() as MapInstance;
    if (!map) return;

    try {
      // Reset loaded sources tracking
      loadedSourcesRef.current.clear();

      // Add event listener for source data loading
      const mapWithEvents = map as MapInstance & {
        on: (
          event: string,
          handler: (e: { sourceId: string; isSourceLoaded: boolean }) => void
        ) => void;
        once: (event: string, handler: () => void) => void;
      };
      mapWithEvents.on('sourcedata', handleSourceData);

      // Add PMTiles sources with better error handling
      const sourceErrors: string[] = [];
      let sourcesAdded = 0;

      Object.entries(pmtilesUrls).forEach(([layerName, url]) => {
        if (url && !map.getSource(layerName)) {
          try {
            map.addSource(layerName, {
              type: 'vector',
              url: `pmtiles://${url}`,
            });
            console.log(`✅ Added ${layerName} source:`, url);
            sourcesAdded++;

            // Special debugging for BNBO
            if (layerName === 'bnbo') {
              console.log('🔍 BNBO source added - URL:', url);
              console.log('🔍 BNBO layer visibility:', layerVisibility.bnbo);
            }
          } catch (error) {
            const errorMessage = `Failed to add ${layerName} source: ${error}`;
            console.warn(`❌ ${errorMessage}`);
            sourceErrors.push(errorMessage);

            // Special debugging for BNBO
            if (layerName === 'bnbo') {
              console.error('🚨 BNBO source failed to load:', error);
            }
          }
        } else if (!url) {
          console.warn(`⚠️ Empty URL for ${layerName} layer`);
        } else if (map.getSource(layerName)) {
          console.log(`ℹ️ ${layerName} source already exists`);
        }
      });

      // If no sources were added (all URLs empty or sources already exist)
      if (sourcesAdded === 0) {
        console.log('No new sources to add, marking as ready');
        if (loadingTimeoutRef.current) {
          clearTimeout(loadingTimeoutRef.current);
          loadingTimeoutRef.current = null;
        }
        setIsLoading(false);
        onMapReady?.();
        return;
      }

      // If there are source errors, show a warning but continue
      if (sourceErrors.length > 0) {
        console.warn('Some map sources failed to load:', sourceErrors);
        // Don't set error state for source failures - the map can still work with partial data
      }

      // Add layers
      addFieldsLayers(map as unknown as MapInstance);
      addBNBOLayers(map as unknown as MapInstance);
      addWetlandsLayers(map as unknown as MapInstance);
      addWaterProjectsLayers(map as unknown as MapInstance);
      addBuildingsLayers(map as unknown as MapInstance);

      // Listen for 'idle' event AFTER adding sources - fires when map finishes rendering
      // This is more reliable than waiting for individual sourcedata events
      mapWithEvents.once('idle', () => {
        console.log('🎯 Map idle event fired - rendering complete'); // TEMP DEBUG
        // Map has finished loading and rendering all layers
        setTimeout(() => {
          console.log('✅ Clearing loading state via idle event'); // TEMP DEBUG
          if (loadingTimeoutRef.current) {
            clearTimeout(loadingTimeoutRef.current);
            loadingTimeoutRef.current = null;
          }
          setIsLoading(false);
          onMapReadyRef.current?.();
        }, 100); // Small delay to ensure everything is settled
      });

      // Don't set loading to false here - wait for idle event or sourcedata events
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
    addFieldsLayers,
    addBNBOLayers,
    addWetlandsLayers,
    addWaterProjectsLayers,
    addBuildingsLayers,
    onMapReady,
    handleSourceData,
    layerVisibility.bnbo,
  ]);

  // Ensure sources are added when map and URLs are both ready
  useEffect(() => {
    if (!mapRef.current || !pmtilesUrls.fields) return;

    const map = mapRef.current.getMap();
    if (!map || !map.loaded()) return;

    const fieldsSource = map.getSource('fields');

    // If sources don't exist yet, call onMapLoad to set everything up
    if (!fieldsSource) {
      onMapLoad();
      return;
    }
  }, [pmtilesUrls.fields, onMapLoad]);

  // Handle PMTiles URL changes (e.g., year selection) - optimized approach
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Only update if fields source already exists (avoid running on initial load)
    const fieldsSource = map.getSource('fields');
    if (fieldsSource && pmtilesUrls.fields) {
      console.log(
        `Optimized fields source update for year change:`,
        pmtilesUrls.fields
      );

      // Start loading timeout for year change
      startLoadingTimeout();
      setIsLoading(true);

      try {
        // More efficient approach: Update the source URL directly instead of removing/re-adding
        // This preserves the existing layers and only updates the data source
        const newSource = {
          type: 'vector' as const,
          url: `pmtiles://${pmtilesUrls.fields}`,
        };

        // Reset loaded sources tracking for fields
        loadedSourcesRef.current.delete('fields');

        // Remove and re-add source (MapLibre doesn't support direct URL updates)
        // But we do it more efficiently by only affecting the fields source
        // First remove all field layers that depend on the source
        removeFieldsLayers(map as unknown as MapInstance);
        map.removeSource('fields');
        map.addSource('fields', newSource);

        // Re-add only the fields layers (other layers remain unaffected)
        addFieldsLayers(map as unknown as MapInstance);

        // Don't set loading to false here - wait for sourcedata event
        console.log(`Waiting for optimized fields source to load...`);
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
    addFieldsLayers,
    removeFieldsLayers,
    onMapReady,
    startLoadingTimeout,
  ]);

  // Update layer visibility and styling when props change
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Update fields layers
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

      // Update organic symbols visibility
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

    // Update BNBO layers
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

    // Update wetlands layers
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

    // Update water projects layers
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

    // Update buildings layers
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
  }, [layerVisibility, filterState.visualizationMode]); // Only run when visibility or organic mode changes

  // Update filters when company filter changes
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Create company filter if specified
    const companyFilter: unknown = filterState.companyFilter
      ? ['==', ['get', 'cvr_number'], parseInt(filterState.companyFilter)]
      : null;

    // Update filters on existing layers (only set filter if it exists)
    if (map.getLayer('fields-fill')) {
      if (companyFilter) {
        map.setFilter('fields-fill', companyFilter as never);
      } else {
        map.setFilter('fields-fill', null); // Clear filter
      }
    }
    if (map.getLayer('fields-outline')) {
      if (companyFilter) {
        map.setFilter('fields-outline', companyFilter as never);
      } else {
        map.setFilter('fields-outline', null); // Clear filter
      }
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

  // Update field visualization when filterState changes - with RAF batching
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Guard: Only update if paint props actually changed
    const propsString = JSON.stringify(fieldsPaintProps);
    if (prevPaintPropsRef.current === propsString) {
      return;
    }
    prevPaintPropsRef.current = propsString;

    // Batch all paint property updates in a single RAF for performance
    requestAnimationFrame(() => {
      if (map.getLayer('fields-fill')) {
        const paintProps = fieldsPaintProps;

        // Update the fill color - preserve zoom-based opacity
        map.setPaintProperty(
          'fields-fill',
          'fill-color',
          paintProps['fill-color']
        );
        // Note: fill-opacity is now zoom-based, set in layer creation

        // Update partial coverage base layer to match main field colors
        if (map.getLayer('fields-partial-coverage-base')) {
          map.setPaintProperty(
            'fields-partial-coverage-base',
            'fill-color',
            paintProps['fill-color']
          );
        }

        // Handle organic borders layer visibility
        if (map.getLayer('organic-borders')) {
          map.setLayoutProperty(
            'organic-borders',
            'visibility',
            layerVisibility.fields ? 'visible' : 'none'
          );
        }

        // Handle partial coverage layers visibility
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
  }, [fieldsPaintProps, layerVisibility.fields]); // Use memoized paint props

  // Query for field data at a specific coordinate
  const queryFieldDataAtCoordinate = useCallback(
    async (lng: number, lat: number): Promise<FieldAnalysisData | null> => {
      if (!mapRef.current) {
        console.log('🔍 No map ref available for field query');
        return null;
      }

      const map = mapRef.current.getMap();
      const point = map.project([lng, lat]);
      console.log('🔍 Querying field data at coordinate:', { lng, lat, point });

      // Query for field features at this point
      const fieldFeatures = map.queryRenderedFeatures(point, {
        layers: ['fields-fill'],
      });

      console.log('🔍 Field query result:', {
        featuresFound: fieldFeatures.length,
        layerExists: !!map.getLayer('fields-fill'),
        layerVisible: map.getLayer('fields-fill')
          ? map.getLayoutProperty('fields-fill', 'visibility')
          : 'layer not found',
      });

      if (fieldFeatures.length > 0) {
        const fieldData = fieldFeatures[0].properties as FieldAnalysisData;
        fieldData.click_coordinates = { lat, lng };
        console.log('🔍 Found field data:', {
          crop_name: fieldData.crop_name,
          area_hectares: fieldData.area_hectares,
          field_uuid: fieldData.field_uuid,
        });
        return fieldData;
      }

      return null;
    },
    []
  );

  // Query all visible field features in the current map view
  const queryVisibleFields = useCallback((): FieldAnalysisData[] => {
    if (!mapRef.current) {
      console.log('🔍 No map ref available for visible fields query');
      return [];
    }

    const map = mapRef.current.getMap();

    // Check if fields layer exists and is visible
    if (
      !map.getLayer('fields-fill') ||
      map.getLayoutProperty('fields-fill', 'visibility') === 'none'
    ) {
      console.log('🔍 Fields layer not visible, returning empty array');
      return [];
    }

    // Query all rendered field features in the current viewport
    const fieldFeatures = map.queryRenderedFeatures(undefined, {
      layers: ['fields-fill'],
    });

    console.log('🔍 Found', fieldFeatures.length, 'visible field features');

    // Convert features to FieldAnalysisData and remove duplicates
    const uniqueFields: Record<string, FieldAnalysisData> = {};

    fieldFeatures.forEach((feature) => {
      const fieldData = feature.properties as FieldAnalysisData;
      if (fieldData.field_uuid && !uniqueFields[fieldData.field_uuid]) {
        uniqueFields[fieldData.field_uuid] = fieldData;
      }
    });

    const result = Object.values(uniqueFields);
    console.log('🔍 Returning', result.length, 'unique visible fields');

    return result;
  }, []);

  // Expose queryVisibleFields function via ref
  useEffect(() => {
    if (queryVisibleFieldsRef) {
      queryVisibleFieldsRef.current = queryVisibleFields;
    }
  }, [queryVisibleFields, queryVisibleFieldsRef]);

  // Check if mouse is over UI elements (search bar or legend)
  const isOverUIElement = useCallback(
    (x: number, y: number): boolean => {
      // Mobile breakpoint (md: 768px)
      const isMobile = window.innerWidth < 768;
      const screenWidth = window.innerWidth;

      if (isMobile) {
        // Mobile: Search at top ~4-5rem from top, full width minus padding
        // Legend below search at ~8rem or ~25rem when search is active
        const searchTop = 64; // ~4rem
        const searchBottom = 144; // ~9rem (search + dropdown space)
        const legendTop = isSearchActive ? 400 : 128; // ~25rem or ~8rem
        const legendBottom = legendTop + 300; // Approximate legend height

        // Check if over search area (full width with 1rem padding on each side)
        if (
          y >= searchTop &&
          y <= searchBottom &&
          x >= 16 &&
          x <= screenWidth - 16
        ) {
          return true;
        }

        // Check if over legend area
        if (
          y >= legendTop &&
          y <= legendBottom &&
          x >= 16 &&
          x <= screenWidth - 16
        ) {
          return true;
        }
      } else {
        // Desktop: Search at left: 90px, top: 16px, width: 320px (md) / 384px (lg) / 448px (xl)
        const searchLeft = 90;
        const searchTop = 16;
        const searchWidth =
          window.innerWidth >= 1280
            ? 448
            : window.innerWidth >= 1024
              ? 384
              : 320;
        const searchBottom = 80; // Approximate height

        // Legend: same left position, top: 80px (md:top-20) or ~352px when search active
        const legendLeft = 90;
        const legendTop = isSearchActive ? 352 : 80;
        const legendWidth = 384; // max-w-xs
        const legendBottom = legendTop + 400; // Approximate max height

        // Check if over search area
        if (
          x >= searchLeft &&
          x <= searchLeft + searchWidth &&
          y >= searchTop &&
          y <= searchBottom
        ) {
          return true;
        }

        // Check if over legend area
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
      // Don't show tooltip if mouse is over UI elements
      if (isOverUIElement(event.point.x, event.point.y)) {
        setHoverInfo(null);
        return;
      }

      const feature = event.features && event.features[0];
      if (feature) {
        const layerName = getLayerDisplayName(feature.layer.id);
        let properties = feature.properties || {};

        // For environmental layers, try to get underlying field data for enhanced tooltip
        if (
          feature.layer.id.startsWith('bnbo-') ||
          feature.layer.id.startsWith('wetlands-') ||
          feature.layer.id.startsWith('water-projects-')
        ) {
          console.log(
            '🔍 Environmental layer hovered, querying for field data...'
          );
          const underlyingFieldData = await queryFieldDataAtCoordinate(
            event.lngLat.lng,
            event.lngLat.lat
          );

          if (underlyingFieldData) {
            // Merge environmental layer properties with field data
            properties = {
              ...properties, // Environmental layer data first
              ...underlyingFieldData, // Field data second (will override if same keys)
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

  // Handle click events for field selection and coordinate capture
  const onClick = useCallback(
    async (event: MapLayerMouseEvent) => {
      const now = Date.now();

      // If the map was moving recently (within 50ms), consider this a drag end, not a click
      if (now - lastMapMoveTimeRef.current < 50) {
        return;
      }

      const coordinates = {
        lat: event.lngLat.lat,
        lng: event.lngLat.lng,
      };

      const feature = event.features && event.features[0];
      console.log('🔍 Click event:', {
        hasFeature: !!feature,
        layerId: feature?.layer?.id,
        allFeatures:
          event.features?.map((f) => ({ id: f.layer.id, source: f.source })) ||
          [],
      });
      if (feature && feature.layer.id.startsWith('fields-')) {
        // Field click - add coordinates and select field
        console.log('🔍 Field layer clicked:', feature.layer.id);
        console.log('🔍 Environmental data check:', {
          bnbo_area_hectares: feature.properties?.bnbo_area_hectares,
          wetland_area_hectares: feature.properties?.wetland_area_hectares,
          unique_pesticide_products:
            feature.properties?.unique_pesticide_products,
          pesticides_kg_detail: feature.properties?.pesticides_kg_detail,
          pesticides_liters_detail:
            feature.properties?.pesticides_liters_detail,
        });
        const fieldData = feature.properties as FieldAnalysisData;
        fieldData.click_coordinates = coordinates;
        onFieldSelect(fieldData);
      } else if (
        feature &&
        (feature.layer.id.startsWith('bnbo-') ||
          feature.layer.id.startsWith('wetlands-') ||
          feature.layer.id.startsWith('water-projects-'))
      ) {
        // Environmental layer click - try to find underlying field data
        console.log(
          '🔍 Environmental layer clicked:',
          feature.layer.id,
          feature.properties
        );

        // Query for underlying field data at the same coordinate
        const underlyingFieldData = await queryFieldDataAtCoordinate(
          coordinates.lng,
          coordinates.lat
        );

        if (underlyingFieldData) {
          // Found underlying field data - show it in the sidebar
          console.log('🔍 Found underlying field data:', underlyingFieldData);
          onFieldSelect(underlyingFieldData);
        } else {
          // No underlying field data - just show coordinates
          console.log('🔍 No underlying field data found');
          onMapClick?.(coordinates);
        }
      } else if (feature) {
        // Other layer click - log for debugging
        console.log(
          '🔍 Non-environmental layer clicked:',
          feature.layer.id,
          feature.properties
        );
        onMapClick?.(coordinates);
      } else {
        // Empty area click - only show coordinates if no field is selected
        onMapClick?.(coordinates);
      }
    },
    [onFieldSelect, onMapClick, queryFieldDataAtCoordinate]
  );

  // Get display name for layer
  const getLayerDisplayName = (layerId: string): string => {
    if (layerId.startsWith('fields-')) return 'Landbrugsmark';
    if (layerId.startsWith('bnbo-')) return 'BNBO Område';
    if (layerId.startsWith('wetlands-')) return 'Lavbundsområde';
    if (layerId.startsWith('water-projects-')) return 'Vandprojekt';
    if (layerId.startsWith('buildings-')) return 'Bygning';
    return 'Ukendt lag';
  };

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

  // Interactive layer IDs for hover/click events
  const interactiveLayerIds = [
    'fields-fill',
    'bnbo-fill',
    'wetlands-fill',
    'water-projects-fill',
    'buildings-fill',
  ];

  return (
    <div
      className="relative h-full w-full"
      style={{
        touchAction: 'pan-x pan-y',
        // Ensure proper interaction handling
        cursor: 'grab',
        userSelect: 'none',
        // Prevent any potential overflow issues
        overflow: 'hidden',
      }}
      data-testid="field-analysis-map"
    >
      {/* Search Bar - positioned to avoid sidebar collision */}
      <div
        className={`pointer-events-auto absolute z-30 transition-all duration-200 ${
          // Mobile positioning - below hamburger menu with proper spacing
          'top-[5rem] right-4 left-4 md:top-4 md:right-auto md:left-[90px] md:w-80 lg:w-96 xl:w-[28rem]'
        } ${
          hasRightPanel ? 'md:right-[21rem] xl:right-[29rem]' : 'md:right-4'
        }`}
        style={{
          // Mobile: position below hamburger menu with proper spacing
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
        mapStyle={styleLoadFailed ? fallbackMapStyle : currentMapStyle}
        interactiveLayerIds={interactiveLayerIds}
        onLoad={onMapLoad}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
        onClick={onClick}
        cursor="grab"
        // Explicitly enable map interactions - try different approach
        dragPan={true}
        scrollZoom={true}
        doubleClickZoom={true}
        keyboard={true}
        touchPitch={false}
        // Add debug logging for initialization
        onError={(error: unknown) => {
          console.error('🚨 Map error:', error);
          // If it's a style loading error, try fallback
          const errorMessage =
            (error as { error?: { message?: string } })?.error?.message || '';
          if (
            errorMessage.includes('style') ||
            errorMessage.includes('fetch')
          ) {
            console.warn('🔄 Style loading failed, trying fallback style');
            setStyleLoadFailed(true);
          }
        }}
        onStyleData={() => {
          // Style loaded successfully
        }}
        onSourceData={() => {
          // Source data loaded
        }}
        // Try to ensure the map is properly initialized
        reuseMaps={false}
      >
        <NavigationControl position="top-right" />

        {/* PMTiles sources and layers are added programmatically in onMapLoad */}
      </Map>

      {/* Color Legend - positioned for mobile visibility and desktop sidebar avoidance, pushed down when search is active */}
      <div
        className={`pointer-events-auto absolute right-4 left-4 z-30 max-h-[40vh] max-w-[calc(100vw-2rem)] overflow-auto transition-all duration-200 md:right-auto md:left-[90px] md:max-h-[calc(100vh-12rem)] md:max-w-xs ${
          isSearchActive ? 'md:top-[22rem]' : 'md:top-20'
        }`}
        style={{
          // Mobile: position below search bar, accounting for safe areas
          // When search is active, push it down to make room for the dropdown (max-h-64 = 16rem)
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
