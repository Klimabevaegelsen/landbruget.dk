import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface UIState {
  // UI state persistence
  sidebarOpen: boolean;
  setSidebarOpen: (open: boolean) => void;
  
  // User preferences
  theme: 'light' | 'dark' | 'system';
  setTheme: (theme: 'light' | 'dark' | 'system') => void;
  
  // Mobile/desktop state
  isMobile: boolean;
  setIsMobile: (isMobile: boolean) => void;
  
  // Panel states
  showDataPanel: boolean;
  setShowDataPanel: (show: boolean) => void;
  
  // Mobile bottom panel state
  showMobilePanel: boolean;
  setShowMobilePanel: (show: boolean) => void;
  
  // Animation preferences
  reduceMotion: boolean;
  setReduceMotion: (reduce: boolean) => void;
  
  // Performance mode
  performanceMode: boolean;
  setPerformanceMode: (enabled: boolean) => void;
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      // Initial state
      sidebarOpen: false,
      theme: 'system',
      isMobile: false,
      showDataPanel: false,
      showMobilePanel: false,
      reduceMotion: false,
      performanceMode: false,
      
      // Actions
      setSidebarOpen: (open: boolean) => set({ sidebarOpen: open }),
      setTheme: (theme: 'light' | 'dark' | 'system') => set({ theme }),
      setIsMobile: (isMobile: boolean) => set({ isMobile }),
      setShowDataPanel: (show: boolean) => set({ showDataPanel: show }),
      setShowMobilePanel: (show: boolean) => set({ showMobilePanel: show }),
      setReduceMotion: (reduce: boolean) => set({ reduceMotion: reduce }),
      setPerformanceMode: (enabled: boolean) => set({ performanceMode: enabled }),
    }),
    {
      name: 'ui-store',
      partialize: (state) => ({
        theme: state.theme,
        showDataPanel: state.showDataPanel,
        reduceMotion: state.reduceMotion,
        performanceMode: state.performanceMode,
      }),
    }
  )
); 