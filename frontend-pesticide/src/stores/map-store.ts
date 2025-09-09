import { create } from 'zustand';
import { devtools } from 'zustand/middleware';

export type DataMode = 'pesticide_total' | 'pfas' | 'diquat' | 'glyphosate';
export type YearSelection = number | 'total';

export interface MapState {
  // Map view state
  zoom: number;
  center: [number, number];
  bearing: number;
  pitch: number;

  // Map instance reference
  mapInstance: unknown | null;

  // Data selection
  selectedYear: YearSelection;
  selectedDataMode: DataMode;

  // Layer visibility (user-controlled)
  showBNBOLayer: boolean;
  showBasemap: boolean;

  // Loading states
  isLoading: boolean;
  isLoadingYear: boolean;
  isLoadingTiles: boolean;
  loadingMessage: string;

  // Error states
  error: string | null;

  // Data availability
  availableYears: number[];
  availableYearOptions: YearSelection[]; // Includes both years and 'total'
  availableResolutions: number[];

  // UI state
  showControls: boolean;
  showTooltip: boolean;
  tooltipData: Record<string, unknown> | null;
  tooltipPosition: { x: number; y: number };

  // Selected cell state
  selectedCell: {
    id: string;
    layer: string;
    properties: Record<string, unknown>;
  } | null;
}

export interface MapActions {
  // Map view actions
  setZoom: (zoom: number) => void;
  setCenter: (center: [number, number]) => void;
  setBearing: (bearing: number) => void;
  setPitch: (pitch: number) => void;
  setViewState: (viewState: Partial<Pick<MapState, 'zoom' | 'center' | 'bearing' | 'pitch'>>) => void;
  setMapInstance: (mapInstance: unknown | null) => void;
  flyToLocation: (location: { lat: number; lng: number; zoom?: number }) => void;

  // Data selection actions
  setSelectedYear: (year: YearSelection) => void;
  setSelectedDataMode: (mode: DataMode) => void;

  // Layer visibility actions
  setShowBNBOLayer: (show: boolean) => void;
  setShowBasemap: (show: boolean) => void;
  toggleBNBOLayer: () => void;

  // Loading state actions
  setIsLoading: (loading: boolean) => void;
  setIsLoadingYear: (loading: boolean) => void;
  setIsLoadingTiles: (loading: boolean) => void;
  setLoadingMessage: (message: string) => void;

  // Error state actions
  setError: (error: string | null) => void;
  clearError: () => void;

  // Data availability actions
  setAvailableYears: (years: number[]) => void;
  setAvailableYearOptions: (yearOptions: YearSelection[]) => void;
  setAvailableResolutions: (resolutions: number[]) => void;

  // UI actions
  setShowControls: (show: boolean) => void;
  setShowTooltip: (show: boolean) => void;
  setTooltipData: (data: Record<string, unknown> | null) => void;
  setTooltipPosition: (position: { x: number; y: number }) => void;
  showTooltipWithData: (data: Record<string, unknown>, position: { x: number; y: number }) => void;
  hideTooltip: () => void;

  // Selected cell actions
  setSelectedCell: (cell: { id: string; layer: string; properties: Record<string, unknown> } | null) => void;
  clearSelectedCell: () => void;

  // Utility actions
  resetToDefaults: () => void;
}

const DEFAULT_CENTER: [number, number] = [10.0, 56.0]; // Center of Denmark
const DEFAULT_ZOOM = 7;
const DEFAULT_YEAR: YearSelection = 2023;
const DEFAULT_DATA_MODE: DataMode = 'pesticide_total';

// Zoom thresholds for layer switching - with overlap zones
const KOMMUNE_TO_H3_ZOOM = 9.0; // Switch from kommune to H3 at this zoom
const H3_RES8_START_FADE = 11.0; // Start fading out res8 at this zoom
const H3_RES10_START_SHOW = 11.5; // Start showing res10 at this zoom
const H3_RES8_FULL_FADE = 12.0; // Fully fade out res8 at this zoom

// H3 resolution based on zoom level - returns primary resolution
const getH3ResolutionForZoom = (zoom: number): number => {
  if (zoom >= H3_RES10_START_SHOW) return 10;
  return 8;
};

// Helper to determine if both H3 resolutions should be visible (overlap zone)
const isInH3OverlapZone = (zoom: number): boolean => {
  return zoom >= H3_RES8_START_FADE && zoom <= H3_RES8_FULL_FADE;
};

// Add zoom threshold for layer visibility updates to prevent excessive updates
const ZOOM_THRESHOLD = 0.1; // Only update layers if zoom changes by more than this amount

export const useMapStore = create<MapState & MapActions>()(
  devtools(
    (set, get) => ({
      // Initial state
      zoom: DEFAULT_ZOOM,
      center: DEFAULT_CENTER,
      bearing: 0,
      pitch: 0,
      mapInstance: null,

      selectedYear: DEFAULT_YEAR,
      selectedDataMode: DEFAULT_DATA_MODE,

      showBNBOLayer: true,
      showBasemap: true,

      isLoading: false,
      isLoadingYear: false,
      isLoadingTiles: false,
      loadingMessage: '',

      error: null,

      availableYears: [],
      availableYearOptions: [],
      availableResolutions: [8, 10], // Only res8 and res10 for H3

      showControls: true,
      showTooltip: false,
      tooltipData: null,
      tooltipPosition: { x: 0, y: 0 },

      selectedCell: null,

      // Map view actions
      setZoom: (zoom) => set({ zoom }),
      setCenter: (center) => set({ center }),
      setBearing: (bearing) => set({ bearing }),
      setPitch: (pitch) => set({ pitch }),
      setViewState: (viewState) => set(viewState),
      setMapInstance: (mapInstance) => set({ mapInstance }),
      flyToLocation: (location) => {
        const { mapInstance } = get();
        console.log('🗺️ flyToLocation called with:', location);
        console.log('🗺️ mapInstance:', mapInstance);

        if (!mapInstance) {
          console.warn('🗺️ No map instance available for flyToLocation');
          return;
        }

        try {
          // Cast to any to access MapLibre methods
          const map = mapInstance as any;

          if (typeof map.flyTo === 'function') {
            console.log('🗺️ Flying to location:', location);
            map.flyTo({
              center: [location.lng, location.lat],
              zoom: location.zoom || 12,
              essential: true,
              duration: 2000
            });
          } else {
            console.warn('🗺️ Map instance does not have flyTo method');
          }
        } catch (error) {
          console.error('🗺️ Error in flyToLocation:', error);
        }
      },

      // Data selection actions
      setSelectedYear: (selectedYear) => set({ selectedYear, isLoadingYear: true }),
      setSelectedDataMode: (selectedDataMode) => set({ selectedDataMode }),

      // Layer visibility actions
      setShowBNBOLayer: (showBNBOLayer) => set({ showBNBOLayer }),
      setShowBasemap: (showBasemap) => set({ showBasemap }),
      toggleBNBOLayer: () => set((state) => ({ showBNBOLayer: !state.showBNBOLayer })),

      // Loading state actions
      setIsLoading: (isLoading) => set({ isLoading }),
      setIsLoadingYear: (isLoadingYear) => set({ isLoadingYear }),
      setIsLoadingTiles: (isLoadingTiles) => set({ isLoadingTiles }),
      setLoadingMessage: (loadingMessage) => set({ loadingMessage }),

      // Error state actions
      setError: (error) => set({ error }),
      clearError: () => set({ error: null }),

      // Data availability actions
      setAvailableYears: (availableYears) => {
        // When years are set, also update year options to include 'total'
        const availableYearOptions: YearSelection[] = [...availableYears, 'total'];
        set({ availableYears, availableYearOptions });
      },
      setAvailableYearOptions: (availableYearOptions) => set({ availableYearOptions }),
      setAvailableResolutions: (availableResolutions) => set({ availableResolutions }),

      // UI actions
      setShowControls: (showControls) => set({ showControls }),
      setShowTooltip: (showTooltip) => set({ showTooltip }),
      setTooltipData: (tooltipData) => set({ tooltipData }),
      setTooltipPosition: (tooltipPosition) => set({ tooltipPosition }),

      showTooltipWithData: (data, position) => {
        set({
          showTooltip: true,
          tooltipData: data,
          tooltipPosition: position,
        });
      },

      hideTooltip: () => {
        set({
          showTooltip: false,
          tooltipData: null,
        });
      },

      // Selected cell actions
      setSelectedCell: (selectedCell) => set({ selectedCell }),
      clearSelectedCell: () => set({ selectedCell: null }),

      // Utility actions
      resetToDefaults: () => {
        set({
          zoom: DEFAULT_ZOOM,
          center: DEFAULT_CENTER,
          bearing: 0,
          pitch: 0,
          selectedYear: DEFAULT_YEAR,
          selectedDataMode: DEFAULT_DATA_MODE,
          showBNBOLayer: true,
          showBasemap: true,
          isLoading: false,
          isLoadingYear: false,
          isLoadingTiles: false,
          loadingMessage: '',
          error: null,
          showControls: true,
          showTooltip: false,
          tooltipData: null,
          tooltipPosition: { x: 0, y: 0 },
          selectedCell: null,
        });
      },
    }),
    {
      name: 'map-store',
      partialize: (state: MapState & MapActions) => ({
        // Persist only essential state
        zoom: state.zoom,
        center: state.center,
        selectedYear: state.selectedYear,
        selectedDataMode: state.selectedDataMode,
        showBNBOLayer: state.showBNBOLayer,
        showControls: state.showControls,
      }),
    }
  )
);

// Computed selectors (these are truly computed and don't cause updates)
export const getComputedLayerVisibility = (zoom: number) => {
  const shouldShowH3 = zoom >= KOMMUNE_TO_H3_ZOOM;
  const shouldShowKommune = zoom < KOMMUNE_TO_H3_ZOOM;
  const inOverlapZone = isInH3OverlapZone(zoom);

  return {
    shouldShowKommune,
    shouldShowH3,
    currentH3Resolution: getH3ResolutionForZoom(zoom),
    // In overlap zone, show both resolutions with different opacities
    shouldShowH3Res8: shouldShowH3 && (zoom < H3_RES8_FULL_FADE),
    shouldShowH3Res10: shouldShowH3 && (zoom >= H3_RES10_START_SHOW),
    inOverlapZone,
    // Calculate opacity for smooth transitions
    res8Opacity: inOverlapZone ? Math.max(0, (H3_RES8_FULL_FADE - zoom) / (H3_RES8_FULL_FADE - H3_RES8_START_FADE)) : (zoom < H3_RES8_START_FADE ? 1 : 0),
    res10Opacity: inOverlapZone ? Math.min(1, (zoom - H3_RES10_START_SHOW) / (H3_RES8_FULL_FADE - H3_RES10_START_SHOW)) : (zoom >= H3_RES8_FULL_FADE ? 1 : 0),
  };
};

// Helper function to determine if layer visibility should be updated based on zoom threshold
export const shouldUpdateLayerVisibility = (oldZoom: number, newZoom: number): boolean => {
  return Math.abs(newZoom - oldZoom) >= ZOOM_THRESHOLD;
};

// Data mode configuration
export const DATA_MODE_CONFIG = {
  pesticide_total: {
    label: 'Total Pesticide Load',
    description: 'Total pesticide load intensity per hectare',
    h3Field: 'pesticide_belastning_per_ha',
    kommuneField: 'pesticide_belastning_per_ha',
    unit: 'kg/ha',
    colorScale: 'white-red',
  },
  pfas: {
    label: 'PFAS Intensity',
    description: 'PFAS-containing pesticide intensity per hectare',
    h3Field: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
    kommuneField: 'pfas_containing_active_ingredient_intensity_grams_per_ha',
    unit: 'g/ha',
    colorScale: 'white-red',
  },
  diquat: {
    label: 'Diquat Intensity',
    description: 'Diquat-containing pesticide intensity per hectare',
    h3Field: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
    kommuneField: 'diquat_containing_active_ingredient_intensity_grams_per_ha',
    unit: 'g/ha',
    colorScale: 'white-red',
  },
  glyphosate: {
    label: 'Glyphosate Intensity',
    description: 'Glyphosate-containing pesticide intensity per hectare',
    h3Field: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha',
    kommuneField: 'glyphosate_containing_active_ingredient_intensity_grams_per_ha',
    unit: 'g/ha',
    colorScale: 'white-red',
  },
} as const;

// BNBO status configuration
export const BNBO_STATUS_CONFIG = {
  'Action Required': {
    color: '#eab308',
    label: 'Action Required',
    description: 'Areas requiring immediate action',
  },
  'Completed': {
    color: '#51cf66',
    label: 'Completed',
    description: 'Areas where action has been completed',
  },
  'Unknown': {
    color: '#868e96',
    label: 'Unknown',
    description: 'Areas with unknown status',
  },
} as const;

// Individual selectors to avoid object recreation
export const useZoom = () => useMapStore((state) => state.zoom);
export const useCenter = () => useMapStore((state) => state.center);
export const useBearing = () => useMapStore((state) => state.bearing);
export const usePitch = () => useMapStore((state) => state.pitch);

export const useSelectedYear = () => useMapStore((state) => state.selectedYear);
export const useSelectedDataMode = () => useMapStore((state) => state.selectedDataMode);
export const useAvailableYears = () => useMapStore((state) => state.availableYears);
export const useAvailableYearOptions = () => useMapStore((state) => state.availableYearOptions);
export const useAvailableResolutions = () => useMapStore((state) => state.availableResolutions);

export const useShowBNBOLayer = () => useMapStore((state) => state.showBNBOLayer);
export const useShowBasemap = () => useMapStore((state) => state.showBasemap);

export const useIsLoading = () => useMapStore((state) => state.isLoading);
export const useIsLoadingYear = () => useMapStore((state) => state.isLoadingYear);
export const useIsLoadingTiles = () => useMapStore((state) => state.isLoadingTiles);
export const useLoadingMessage = () => useMapStore((state) => state.loadingMessage);
export const useError = () => useMapStore((state) => state.error);

export const useShowControls = () => useMapStore((state) => state.showControls);
export const useShowTooltip = () => useMapStore((state) => state.showTooltip);
export const useTooltipData = () => useMapStore((state) => state.tooltipData);
export const useTooltipPosition = () => useMapStore((state) => state.tooltipPosition);

// Stable selectors for complex state combinations
export const useMapViewState = () => {
  const zoom = useZoom();
  const center = useCenter();
  const bearing = useBearing();
  const pitch = usePitch();

  return { zoom, center, bearing, pitch };
};

export const useDataState = () => {
  const selectedYear = useSelectedYear();
  const selectedDataMode = useSelectedDataMode();
  const availableYears = useAvailableYears();
  const availableYearOptions = useAvailableYearOptions();
  const availableResolutions = useAvailableResolutions();

  return { selectedYear, selectedDataMode, availableYears, availableYearOptions, availableResolutions };
};

export const useLayerVisibility = () => {
  const zoom = useZoom();
  const showBNBOLayer = useShowBNBOLayer();
  const showBasemap = useShowBasemap();

  const computed = getComputedLayerVisibility(zoom);

  return {
    showBNBOLayer,
    showBasemap,
    ...computed,
  };
};

export const useLoadingState = () => {
  const isLoading = useIsLoading();
  const isLoadingYear = useIsLoadingYear();
  const isLoadingTiles = useIsLoadingTiles();
  const loadingMessage = useLoadingMessage();
  const error = useError();

  return { isLoading, isLoadingYear, isLoadingTiles, loadingMessage, error };
};

export const useTooltipState = () => {
  const showTooltip = useShowTooltip();
  const tooltipData = useTooltipData();
  const tooltipPosition = useTooltipPosition();

  return { showTooltip, tooltipData, tooltipPosition };
};

export const useSelectedCellState = () => {
  const selectedCell = useMapStore((state) => state.selectedCell);
  const setSelectedCell = useMapStore((state) => state.setSelectedCell);
  const clearSelectedCell = useMapStore((state) => state.clearSelectedCell);

  return { selectedCell, setSelectedCell, clearSelectedCell };
};

// Navigation actions
export const useFlyToLocation = () => useMapStore((state) => state.flyToLocation);
