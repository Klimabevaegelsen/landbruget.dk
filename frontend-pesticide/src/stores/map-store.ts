import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { YEARS } from '@/lib/utils';

interface MapState {
  // Selected year state
  selectedYear: number;
  setSelectedYear: (year: number) => void;
  
  // Layer visibility state
  showPesticides: boolean;
  showPFAS: boolean;
  showBNBO: boolean;
  showBBR: boolean;
  setShowPesticides: (show: boolean) => void;
  setShowPFAS: (show: boolean) => void;
  setShowBNBO: (show: boolean) => void;
  setShowBBR: (show: boolean) => void;
  
  // Cumulative mode state
  cumulativeMode: boolean;
  setCumulativeMode: (cumulative: boolean) => void;
  
  // Available years management
  availableYears: number[];
  setAvailableYears: (years: number[]) => void;
  
  // Heatmap toggle (pesticide vs PFAS)
  heatmapMode: 'pesticide' | 'pfas';
  setHeatmapMode: (mode: 'pesticide' | 'pfas') => void;
}

export const useMapStore = create<MapState>()(
  persist(
    (set) => ({
      // Initial state
      selectedYear: 2024,
      showPesticides: true,
      showPFAS: false,
      showBNBO: true,
      showBBR: false,
      cumulativeMode: false,
      availableYears: YEARS,
      heatmapMode: 'pesticide',
      
      // Actions
      setSelectedYear: (year: number) => set({ selectedYear: year }),
      
      setShowPesticides: (show: boolean) => set({ showPesticides: show }),
      setShowPFAS: (show: boolean) => set({ showPFAS: show }),
      setShowBNBO: (show: boolean) => set({ showBNBO: show }),
      setShowBBR: (show: boolean) => set({ showBBR: show }),
      
      setCumulativeMode: (cumulative: boolean) => set({ cumulativeMode: cumulative }),
      
      setAvailableYears: (years: number[]) => set({ availableYears: years }),
      
      setHeatmapMode: (mode: 'pesticide' | 'pfas') => {
        set({ 
          heatmapMode: mode,
          showPesticides: mode === 'pesticide',
          showPFAS: mode === 'pfas'
        });
      },
    }),
    {
      name: 'map-store',
      partialize: (state) => ({
        selectedYear: state.selectedYear,
        showBNBO: state.showBNBO,
        showBBR: state.showBBR,
        cumulativeMode: state.cumulativeMode,
        heatmapMode: state.heatmapMode,
      }),
    }
  )
); 