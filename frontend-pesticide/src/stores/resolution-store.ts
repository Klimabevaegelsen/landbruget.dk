import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface ResolutionState {
  // Current resolution - 'kommune' | 8 | 10
  currentResolution: 'kommune' | 8 | 10
  
  // Auto-resolution settings
  autoResolution: boolean
  
  // Resolution history for smooth transitions
  previousResolution: 'kommune' | 8 | 10 | null
  
  // Zoom level tracking
  currentZoom: number
  
  // Actions
  setResolution: (resolution: 'kommune' | 8 | 10) => void
  setAutoResolution: (auto: boolean) => void
  setZoom: (zoom: number) => void
  
  // Utility functions
  getResolutionForZoom: (zoom: number) => 'kommune' | 8 | 10
  shouldUpdateResolution: (newZoom: number) => boolean
  
  // Resolution info
  getResolutionInfo: (resolution: 'kommune' | 8 | 10) => {
    name: string
    description: string
    zoomRange: [number, number]
    cellSize: string
  }
}

export const useResolutionStore = create<ResolutionState>()(
  persist(
    (set, get) => ({
      // Initial state - start with kommune
      currentResolution: 'kommune',
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
        // Simplified zoom-to-resolution mapping
        if (zoom >= 12) return 10    // High zoom = field-level detail (res10)
        if (zoom >= 9) return 8      // Medium zoom = sub-regional detail (res8)
        return 'kommune'             // Low zoom = municipal boundaries
      },
      
      shouldUpdateResolution: (newZoom) => {
        const state = get()
        if (!state.autoResolution) return false
        
        const newResolution = state.getResolutionForZoom(newZoom)
        return newResolution !== state.currentResolution
      },
      
      getResolutionInfo: (resolution) => {
        const resolutionInfo = {
          'kommune': {
            name: 'Municipal',
            description: 'Municipal boundaries with aggregated data',
            zoomRange: [4, 8] as [number, number],
            cellSize: '~Municipal area',
          },
          8: {
            name: 'Sub-regional',
            description: 'Large areas for regional analysis',
            zoomRange: [9, 11] as [number, number],
            cellSize: '~700 ha',
          },
          10: {
            name: 'Field-level',
            description: 'Individual field analysis',
            zoomRange: [12, 15] as [number, number],
            cellSize: '~15 ha',
          },
        }
        
        return resolutionInfo[resolution] || {
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