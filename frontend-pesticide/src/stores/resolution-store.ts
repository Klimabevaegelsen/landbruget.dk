import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface ResolutionState {
  // Current resolution
  currentResolution: number
  
  // Auto-resolution settings
  autoResolution: boolean
  
  // Resolution history for smooth transitions
  previousResolution: number | null
  
  // Zoom level tracking
  currentZoom: number
  
  // Actions
  setResolution: (resolution: number) => void
  setAutoResolution: (auto: boolean) => void
  setZoom: (zoom: number) => void
  
  // Utility functions
  getResolutionForZoom: (zoom: number) => number
  shouldUpdateResolution: (newZoom: number) => boolean
  
  // Resolution info
  getResolutionInfo: (resolution: number) => {
    name: string
    description: string
    zoomRange: [number, number]
    cellSize: string
  }
}

export const useResolutionStore = create<ResolutionState>()(
  persist(
    (set, get) => ({
      // Initial state
      currentResolution: 10, // Start with field-level detail
      autoResolution: true,
      previousResolution: null,
      currentZoom: 7,
      
      // Actions
      setResolution: (resolution) => set((state) => ({
        currentResolution: resolution,
        previousResolution: state.currentResolution,
      })),
      
      setAutoResolution: (auto) => set({ autoResolution: auto }),
      
      setZoom: (zoom) => {
        const state = get()
        const newResolution = state.getResolutionForZoom(zoom)
        
        if (state.autoResolution && newResolution !== state.currentResolution) {
          set({
            currentZoom: zoom,
            currentResolution: newResolution,
            previousResolution: state.currentResolution,
          })
        } else {
          set({ currentZoom: zoom })
        }
      },
      
      // Utility functions
      getResolutionForZoom: (zoom) => {
        // Map zoom levels to H3 resolutions - higher zoom = higher resolution
        if (zoom >= 12) return 10  // Field-level detail
        if (zoom >= 10) return 9   // Municipal detail
        if (zoom >= 8) return 8    // Sub-regional
        return 7                   // Regional overview
      },
      
      shouldUpdateResolution: (newZoom) => {
        const state = get()
        if (!state.autoResolution) return false
        
        const newResolution = state.getResolutionForZoom(newZoom)
        return newResolution !== state.currentResolution
      },
      
      getResolutionInfo: (resolution) => {
        const resolutionInfo = {
          7: {
            name: 'Regional',
            description: 'County/regional overview',
            zoomRange: [4, 7] as [number, number],
            cellSize: '~5,000 ha',
          },
          8: {
            name: 'Sub-regional',
            description: 'Large municipal areas',
            zoomRange: [8, 9] as [number, number],
            cellSize: '~700 ha',
          },
          9: {
            name: 'Municipal',
            description: 'Municipal/city detail',
            zoomRange: [10, 11] as [number, number],
            cellSize: '~100 ha',
          },
          10: {
            name: 'Field-level',
            description: 'Individual field analysis',
            zoomRange: [12, 15] as [number, number],
            cellSize: '~15 ha',
          },
        }
        
        return resolutionInfo[resolution as keyof typeof resolutionInfo] || {
          name: 'Unknown',
          description: 'Unknown resolution',
          zoomRange: [0, 15] as [number, number],
          cellSize: 'Unknown',
        }
      },
    }),
    {
      name: 'resolution-store',
      // Persist resolution preferences
      partialize: (state) => ({
        currentResolution: state.currentResolution,
        autoResolution: state.autoResolution,
      }),
    }
  )
) 