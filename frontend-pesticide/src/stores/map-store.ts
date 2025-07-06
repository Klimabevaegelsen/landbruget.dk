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
  tooltipData: any;
  tooltipPosition: { x: number; y: number };
}

export interface MapActions {
  // Map view actions
  setZoom: (zoom: number) => void;
  setCenter: (center: [number, number]) => void;
  setBearing: (bearing: number) => void;
  setPitch: (pitch: number) => void;
  setViewState: (viewState: Partial<Pick<MapState, 'zoom' | 'center' | 'bearing' | 'pitch'>>) => void;
  
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
  setTooltipData: (data: any) => void;
  setTooltipPosition: (position: { x: number; y: number }) => void;
  showTooltipWithData: (data: any, position: { x: number; y: number }) => void;
  hideTooltip: () => void;
  
  // Utility actions
  resetToDefaults: () => void;
}

const DEFAULT_CENTER: [number, number] = [10.0, 56.0]; // Center of Denmark
const DEFAULT_ZOOM = 7;
const DEFAULT_YEAR: YearSelection = 2023;
const DEFAULT_DATA_MODE: DataMode = 'pesticide_total';

// Zoom thresholds for layer switching
const KOMMUNE_MAX_ZOOM = 8;
const H3_MIN_ZOOM = 9;

// H3 resolution based on zoom level
const getH3ResolutionForZoom = (zoom: number): number => {
  if (zoom >= 14) return 10;
  if (zoom >= 12) return 9;
  if (zoom >= 10) return 8;
  return 7;
};

export const useMapStore = create<MapState & MapActions>()(
  devtools(
    (set, get) => ({
      // Initial state
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
      
      availableYears: [],
      availableYearOptions: [],
      availableResolutions: [7, 8, 9, 10],
      
      showControls: true,
      showTooltip: false,
      tooltipData: null,
      tooltipPosition: { x: 0, y: 0 },
      
      // Map view actions
      setZoom: (zoom) => set({ zoom }),
      setCenter: (center) => set({ center }),
      setBearing: (bearing) => set({ bearing }),
      setPitch: (pitch) => set({ pitch }),
      setViewState: (viewState) => set(viewState),
      
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
export const getComputedLayerVisibility = (zoom: number) => ({
  shouldShowKommune: zoom <= KOMMUNE_MAX_ZOOM,
  shouldShowH3: zoom >= H3_MIN_ZOOM,
  currentH3Resolution: getH3ResolutionForZoom(zoom),
});

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
    color: '#ff6b6b',
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