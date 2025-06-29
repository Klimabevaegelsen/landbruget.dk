import { create } from 'zustand';

// Type definitions (these will be provided by Developer 1)
interface H3DataPoint {
  h3_id: number;
  year: number;
  total_pesticide_load: number;
  total_pfas_grams: number;
  pesticide_application_count: number;
  field_count: number;
  agricultural_area_ha: number;
  avg_field_coverage: number;
  geometry: string;
  centroid_lon: number;
  centroid_lat: number;
}

interface BNBOArea {
  id: number;
  bnbo_id: string;
  status_code: string;
  status_description: string;
  area_ha: number;
  geometry: string;
  year: number;
}

interface BBRBuilding {
  id: number;
  bbr_id: string;
  building_code: string;
  building_type: string;
  construction_year: number;
  floor_area: number;
  geometry: string;
  address: string;
}

interface DataStore {
  // Cached data
  h3Cache: Map<string, H3DataPoint[]>;
  bnboCache: Map<string, BNBOArea[]>;
  bbrCache: Map<string, BBRBuilding[]>;
  
  // Loading states
  isLoading: boolean;
  loadingProgress: number;
  error: string | null;
  
  // Current data
  currentH3Data: H3DataPoint[];
  currentBNBOData: BNBOArea[];
  currentBBRData: BBRBuilding[];
  
  // Methods (placeholders for Developer 1's implementation)
  fetchH3Data: (year: number, cumulative: boolean) => Promise<H3DataPoint[]>;
  fetchBNBOData: () => Promise<BNBOArea[]>;
  fetchBBRData: () => Promise<BBRBuilding[]>;
  preloadAdjacentYears: () => Promise<void>;
  clearCache: () => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setCurrentData: (h3?: H3DataPoint[], bnbo?: BNBOArea[], bbr?: BBRBuilding[]) => void;
}

export const useDataStore = create<DataStore>((set, get) => ({
  // Initial state
  h3Cache: new Map(),
  bnboCache: new Map(),
  bbrCache: new Map(),
  isLoading: false,
  loadingProgress: 0,
  error: null,
  currentH3Data: [],
  currentBNBOData: [],
  currentBBRData: [],
  
  // Placeholder methods (to be implemented by Developer 1)
  fetchH3Data: async (year: number, cumulative: boolean) => {
    const cacheKey = `${year}_${cumulative}`;
    
    // Check cache first
    if (get().h3Cache.has(cacheKey)) {
      return get().h3Cache.get(cacheKey)!;
    }
    
    set({ isLoading: true, error: null });
    
    try {
      // Placeholder - Developer 1 will implement actual data fetching
      const mockData: H3DataPoint[] = [];
      
      // Update cache
      const newCache = new Map(get().h3Cache);
      newCache.set(cacheKey, mockData);
      
      set({ 
        h3Cache: newCache, 
        currentH3Data: mockData,
        isLoading: false, 
        loadingProgress: 100 
      });
      
      return mockData;
    } catch (error) {
      set({ 
        isLoading: false, 
        loadingProgress: 0, 
        error: error instanceof Error ? error.message : 'Unknown error' 
      });
      throw error;
    }
  },
  
  fetchBNBOData: async () => {
    // Placeholder - Developer 1 will implement
    return [];
  },
  
  fetchBBRData: async () => {
    // Placeholder - Developer 1 will implement
    return [];
  },
  
  preloadAdjacentYears: async () => {
    // Placeholder - Developer 1 will implement
  },
  
  clearCache: () => {
    set({
      h3Cache: new Map(),
      bnboCache: new Map(),
      bbrCache: new Map(),
      currentH3Data: [],
      currentBNBOData: [],
      currentBBRData: [],
    });
  },
  
  setLoading: (loading: boolean) => set({ isLoading: loading }),
  setError: (error: string | null) => set({ error }),
  setCurrentData: (h3?: H3DataPoint[], bnbo?: BNBOArea[], bbr?: BBRBuilding[]) => {
    const updates: Partial<DataStore> = {};
    if (h3) updates.currentH3Data = h3;
    if (bnbo) updates.currentBNBOData = bnbo;
    if (bbr) updates.currentBBRData = bbr;
    set(updates);
  },
})); 