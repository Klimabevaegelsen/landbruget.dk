'use client';

import React, {
  useEffect,
  useRef,
  useState,
  useCallback,
  useMemo,
} from 'react';
import { AlertTriangle } from 'lucide-react';
import Map, {
  MapLayerMouseEvent,
  NavigationControl,
  ViewState,
} from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { LayerVisibility, FilterState, FieldAnalysisData } from './types';
import { getDecileBreakpoints, getColorScheme } from './colorUtils';
import { SearchBar } from './SearchBar';
import { ColorLegend } from './ColorLegend';

// Type for MapLibre map instance
interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown) => void;
  removeLayer: (id: string) => void;
  removeSource: (id: string) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  setPaintProperty: (id: string, prop: string, value: unknown) => void;
  addSource: (id: string, source: unknown) => void;
  addImage: (
    id: string,
    image: HTMLCanvasElement | ImageBitmap | ImageData
  ) => void;
  setFilter: (id: string, filter: unknown) => void;
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
    return String(value);
  };

  const getRelevantData = () => {
    const data: Array<{ label: string; value: unknown; unit?: string }> = [];

    // Always show basic field info
    if (properties.crop_name) {
      data.push({ label: 'Afgrøde', value: properties.crop_name });
    }

    if (properties.area_hectares) {
      data.push({
        label: 'Areal',
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
            label: 'Antal applikationer',
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
            label: 'PFAS applikationer',
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
            label: 'Diquat applikationer',
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
            label: 'Glyphosate applikationer',
            value: properties.glyphosate_applications,
          });
        }
        break;

      case 'applications_count':
        if (properties.total_pesticide_applications) {
          data.push({
            label: 'Total applikationer',
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

export default function FieldAnalysisMap({
  pmtilesUrls,
  layerVisibility,
  filterState,
  onFieldSelect,
  onLocationSelect,
  onMapClick,
  onMapReady,
  viewState: externalViewState,
  onViewStateChange,
}: FieldAnalysisMapProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<{ getMap: () => MapInstance } | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hoverInfo, setHoverInfo] = useState<TooltipInfo | null>(null);
  const loadingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const loadedSourcesRef = useRef<Set<string>>(new Set());

  // Internal view state for controlled map
  const [internalViewState, setInternalViewState] = useState<ViewState>({
    longitude: 9.501785,
    latitude: 56.26392,
    zoom: 7,
    pitch: 0,
    bearing: 0,
    padding: { top: 0, bottom: 0, left: 0, right: 0 },
  });

  // SIMPLIFIED viewState - use external if provided, otherwise internal
  const currentViewState = externalViewState || internalViewState;

  // Handle view state changes - SIMPLIFIED
  const handleViewStateChange = useCallback(
    (evt: { viewState: ViewState }) => {
      const newViewState = evt.viewState;

      // Only update internal state if we're not externally controlled
      if (!externalViewState) {
        setInternalViewState(newViewState);
      }

      // Notify parent immediately - no throttling
      onViewStateChange?.(newViewState);
    },
    [onViewStateChange, externalViewState]
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

  // SIMPLIFIED loading timeout
  const startLoadingTimeout = useCallback(() => {
    if (loadingTimeoutRef.current) {
      clearTimeout(loadingTimeoutRef.current);
    }

    loadingTimeoutRef.current = setTimeout(() => {
      console.warn('Map loading timeout - forcing loading state to false');
      setIsLoading(false);
      onMapReady?.();
    }, 10000);
  }, [onMapReady]);

  // SIMPLIFIED - Check if all required sources are loaded
  const checkAllSourcesLoaded = useCallback(() => {
    const requiredSources = Object.keys(pmtilesUrls).filter(
      (key) => pmtilesUrls[key as keyof typeof pmtilesUrls]
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
      onMapReady?.();
    }
  }, [pmtilesUrls, isLoading, onMapReady]);

  // Handle source data events to detect when PMTiles are loaded
  const handleSourceData = useCallback(
    (e: { sourceId: string; isSourceLoaded: boolean }) => {
      if (e.isSourceLoaded && Object.keys(pmtilesUrls).includes(e.sourceId)) {
        loadedSourcesRef.current.add(e.sourceId);
        console.log(`PMTiles source loaded: ${e.sourceId}`);
        checkAllSourcesLoaded();
      }
    },
    [pmtilesUrls, checkAllSourcesLoaded] // Only depend on checkAllSourcesLoaded
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
        clearLoadingTimeout();
        setError(
          'Kunne ikke indlæse kortdata efter flere forsøg. Prøv at genindlæse siden.'
        );
        setIsLoading(false);
      }
    };

    initializePMTiles();

    // Cleanup timeout on unmount
    return () => {
      clearLoadingTimeout();
    };
  }, [startLoadingTimeout, clearLoadingTimeout]);

  // Cleanup event listeners on unmount
  useEffect(() => {
    const currentMapRef = mapRef.current;
    return () => {
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

  // Generate dynamic paint properties based on visualization mode
  const generateFieldsPaint = useCallback(() => {
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
  }, [filterState]);

  // Add field analysis layers
  const addFieldsLayers = useCallback(
    (map: MapInstance) => {
      if (map.getSource('fields') && !map.getLayer('fields-fill')) {
        const paintProps = generateFieldsPaint();

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
              map.addImage('partial-coverage-pattern', bitmap);
            }
          } catch (error) {
            console.warn('Failed to create partial coverage pattern:', error);
          }
        };

        // Create initial pattern with default color
        createPartialCoveragePattern();

        // Main fields layer
        const fieldsLayer: any = {
          id: 'fields-fill',
          source: 'fields',
          'source-layer': 'fields',
          type: 'fill',
          paint: paintProps,
          layout: {
            visibility: layerVisibility.fields ? 'visible' : 'none',
          },
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
          'source-layer': 'fields',
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
          'source-layer': 'fields',
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

        // Fields outline
        const fieldsOutlineLayer: any = {
          id: 'fields-outline',
          source: 'fields',
          'source-layer': 'fields',
          type: 'line',
          paint: {
            'line-color': '#374151',
            'line-width': 0.5,
            'line-opacity': 0.8,
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
          'source-layer': 'fields',
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
    [layerVisibility.fields, generateFieldsPaint, filterState.companyFilter]
  );

  // Add BNBO layers with cross-hatch pattern
  const addBNBOLayers = useCallback(
    (map: MapInstance) => {
      if (map.getSource('bnbo') && !map.getLayer('bnbo-fill')) {
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
              map.addImage('bnbo-completed-pattern', completedBitmap);
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
              map.addImage('bnbo-action-pattern', actionBitmap);
            }
          } catch (error) {
            console.warn('Failed to create BNBO patterns:', error);
          }
        };

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
              map.addImage('wetlands-pattern', imageBitmap);

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
              map.addImage('water-projects-pattern', imageBitmap);

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
      }
    },
    [layerVisibility.buildings]
  );

  // Handle map load and add sources
  const onMapLoad = useCallback(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    try {
      // Reset loaded sources tracking
      loadedSourcesRef.current.clear();

      // Add event listener for source data loading
      const mapWithEvents = map as MapInstance & {
        on: (
          event: string,
          handler: (e: { sourceId: string; isSourceLoaded: boolean }) => void
        ) => void;
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
            console.log(`Added ${layerName} source:`, url);
            sourcesAdded++;
          } catch (error) {
            const errorMessage = `Failed to add ${layerName} source: ${error}`;
            console.warn(`${errorMessage}`);
            sourceErrors.push(errorMessage);
          }
        }
      });

      // If no sources were added (all URLs empty or sources already exist)
      if (sourcesAdded === 0) {
        console.log('No new sources to add, marking as ready');
        clearLoadingTimeout();
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
      addFieldsLayers(map);
      addBNBOLayers(map);
      addWetlandsLayers(map);
      addWaterProjectsLayers(map);
      addBuildingsLayers(map);

      // Don't set loading to false here - wait for sourcedata events
      console.log(`Waiting for ${sourcesAdded} PMTiles sources to load...`);
    } catch (err) {
      console.error('Error adding map sources/layers:', err);
      clearLoadingTimeout();
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
    clearLoadingTimeout,
    handleSourceData,
  ]);

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
        map.removeSource('fields');
        map.addSource('fields', newSource);

        // Re-add only the fields layers (other layers remain unaffected)
        addFieldsLayers(map);

        // Don't set loading to false here - wait for sourcedata event
        console.log(`Waiting for optimized fields source to load...`);
      } catch (error) {
        console.error('Error updating PMTiles for year:', error);
        clearLoadingTimeout();
        setError('Failed to load data for selected year');
        setIsLoading(false);
      }
    }
  }, [
    pmtilesUrls.fields,
    addFieldsLayers,
    onMapReady,
    startLoadingTimeout,
    clearLoadingTimeout,
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
  }, [layerVisibility, filterState.visualizationMode]);

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
        map.setFilter('fields-fill', companyFilter);
      } else {
        map.setFilter('fields-fill', null); // Clear filter
      }
    }
    if (map.getLayer('fields-outline')) {
      if (companyFilter) {
        map.setFilter('fields-outline', companyFilter);
      } else {
        map.setFilter('fields-outline', null); // Clear filter
      }
    }
    if (map.getLayer('fields-partial-coverage-base')) {
      const partialFilter = companyFilter
        ? ['all', companyFilter, ['==', ['get', 'is_partial_coverage'], true]]
        : ['==', ['get', 'is_partial_coverage'], true];
      map.setFilter('fields-partial-coverage-base', partialFilter);
      map.setFilter('fields-partial-coverage-pattern', partialFilter);
    }
    if (map.getLayer('organic-borders')) {
      let organicFilter: unknown = ['==', ['get', 'is_organic'], true];
      if (companyFilter) {
        organicFilter = ['all', companyFilter, organicFilter];
      }
      map.setFilter('organic-borders', organicFilter);
    }
  }, [filterState.companyFilter]);

  // Update field visualization when filterState changes
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    if (map.getLayer('fields-fill')) {
      const paintProps = generateFieldsPaint();

      // Update the fill color
      map.setPaintProperty(
        'fields-fill',
        'fill-color',
        paintProps['fill-color']
      );
      map.setPaintProperty(
        'fields-fill',
        'fill-opacity',
        paintProps['fill-opacity']
      );

      // Update partial coverage base layer to match main field colors
      if (map.getLayer('fields-partial-coverage-base')) {
        map.setPaintProperty(
          'fields-partial-coverage-base',
          'fill-color',
          paintProps['fill-color']
        );
        map.setPaintProperty(
          'fields-partial-coverage-base',
          'fill-opacity',
          paintProps['fill-opacity']
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
  }, [filterState, layerVisibility.fields, generateFieldsPaint]);

  // Handle hover events
  const onHover = useCallback(
    (event: MapLayerMouseEvent) => {
      const feature = event.features && event.features[0];
      if (feature) {
        const layerName = getLayerDisplayName(feature.layer.id);

        setHoverInfo({
          x: event.point.x,
          y: event.point.y,
          properties: feature.properties || {},
          layerName,
          visualizationMode: filterState.visualizationMode,
          colorUnit: filterState.colorUnit,
        });
      } else {
        setHoverInfo(null);
      }
    },
    [filterState.visualizationMode, filterState.colorUnit]
  );

  // Handle click events for field selection and coordinate capture
  const onClick = useCallback(
    (event: MapLayerMouseEvent) => {
      const coordinates = {
        lat: event.lngLat.lat,
        lng: event.lngLat.lng,
      };

      // Always call onMapClick to capture coordinates
      onMapClick?.(coordinates);

      const feature = event.features && event.features[0];
      if (feature && feature.layer.id.startsWith('fields-')) {
        // Add click coordinates to the field data
        const fieldData = feature.properties as FieldAnalysisData;
        fieldData.click_coordinates = coordinates;
        onFieldSelect(fieldData);
      }
    },
    [onFieldSelect, onMapClick]
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
    <div className="relative h-full w-full touch-manipulation">
      {/* Search Bar - positioned to avoid sidebar collision */}
      <div
        className={`pointer-events-auto absolute top-4 left-4 z-30 transition-all duration-200 md:w-80 lg:w-96 xl:w-[28rem] ${
          hasRightPanel
            ? 'right-[21rem] xl:right-[29rem]'
            : 'right-4 md:right-auto'
        }`}
        style={{ top: 'max(1rem, env(safe-area-inset-top))' }}
      >
        <SearchBar
          onLocationSelect={handleLocationSelect}
          placeholder="Søg efter adresser, byer, regioner..."
          className="w-full"
        />
      </div>

      <Map
        ref={mapRef}
        viewState={currentViewState}
        onMove={handleViewStateChange}
        style={{ width: '100%', height: '100%' }}
        mapStyle={mapStyle}
        interactiveLayerIds={interactiveLayerIds}
        onLoad={onMapLoad}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
        onClick={onClick}
        cursor="default"
      >
        <NavigationControl position="top-right" />

        {/* PMTiles sources and layers are added programmatically in onMapLoad */}
      </Map>

      {/* Color Legend - positioned to avoid mobile controls */}
      <div className="pointer-events-auto absolute bottom-4 left-4 z-30 md:bottom-6 md:left-6">
        <ColorLegend filterState={filterState} />
      </div>

      {hoverInfo && <MapTooltip {...hoverInfo} />}
    </div>
  );
}
