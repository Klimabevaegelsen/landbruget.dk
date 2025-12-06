import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface PMTilesFile {
  year: number
  resolution: number
  timestamp: string
  size: number
  url: string
  lastModified: string
}

interface BNBOFile {
  filename: string
  size: number
  url: string
  lastModified: string
  type: string
}

interface KommuneFile {
  filename: string
  year: number
  timestamp: string
  size: number
  url: string
  lastModified: string
  type: string
}

interface PMTilesMetadata {
  files: PMTilesFile[]
  years: number[]
  resolutions: number[]
  totalFiles: number
  lastUpdated: string
  bnbo?: {
    files: BNBOFile[]
    available: boolean
    status_codes: string[]
    status_colors: Record<string, string>
  }
  kommune?: {
    files: KommuneFile[]
    available: boolean
    years: number[]
    zoom_levels: {
      min: number
      max: number
      base: number
    }
  }
}

interface LoadedPMTiles {
  [key: string]: {
    url: string
    loadedAt: number
    status: 'loading' | 'loaded' | 'error'
    error?: string
  }
}

interface PMTilesState {
  // Metadata
  metadata: PMTilesMetadata | null
  metadataLoading: boolean
  metadataError: string | null
  
  // Loaded tiles tracking
  loadedTiles: LoadedPMTiles
  
  // Actions
  setMetadata: (metadata: PMTilesMetadata) => void
  setMetadataLoading: (loading: boolean) => void
  setMetadataError: (error: string | null) => void
  
  setTileLoading: (key: string, url: string) => void
  setTileLoaded: (key: string) => void
  setTileError: (key: string, error: string) => void
  
  getTileKey: (year: number, resolution: number) => string
  getKommuneTileKey: (year: number) => string
  getTileStatus: (year: number, resolution: number) => 'loading' | 'loaded' | 'error' | 'not-loaded'
  getKommuneTileStatus: (year: number) => 'loading' | 'loaded' | 'error' | 'not-loaded'
  
  // Utility functions
  getAvailableYears: () => number[]
  getAvailableResolutions: () => number[]
  getAvailableKommuneYears: () => number[]
  isYearResolutionAvailable: (year: number, resolution: number) => boolean
  isKommuneYearAvailable: (year: number) => boolean
  
  // Cleanup
  clearOldTiles: () => void
}

export const usePMTilesStore = create<PMTilesState>()(
  persist(
    (set, get) => ({
      // Initial state
      metadata: null,
      metadataLoading: false,
      metadataError: null,
      loadedTiles: {},
      
      // Metadata actions
      setMetadata: (metadata) => set({ metadata, metadataError: null }),
      setMetadataLoading: (loading) => set({ metadataLoading: loading }),
      setMetadataError: (error) => set({ metadataError: error, metadataLoading: false }),
      
      // Tile loading actions
      setTileLoading: (key, url) => set((state) => ({
        loadedTiles: {
          ...state.loadedTiles,
          [key]: {
            url,
            loadedAt: Date.now(),
            status: 'loading',
          },
        },
      })),
      
      setTileLoaded: (key) => set((state) => ({
        loadedTiles: {
          ...state.loadedTiles,
          [key]: {
            ...state.loadedTiles[key],
            status: 'loaded',
          },
        },
      })),
      
      setTileError: (key, error) => set((state) => ({
        loadedTiles: {
          ...state.loadedTiles,
          [key]: {
            ...state.loadedTiles[key],
            status: 'error',
            error,
          },
        },
      })),
      
      // Utility functions
      getTileKey: (year, resolution) => `h3_${year}_${resolution}`,
      getKommuneTileKey: (year) => `kommune_${year}`,
      
      getTileStatus: (year, resolution) => {
        const key = get().getTileKey(year, resolution)
        const tile = get().loadedTiles[key]
        return tile?.status || 'not-loaded'
      },
      
      getKommuneTileStatus: (year) => {
        const key = get().getKommuneTileKey(year)
        const tile = get().loadedTiles[key]
        return tile?.status || 'not-loaded'
      },
      
      getAvailableYears: () => {
        const metadata = get().metadata
        return metadata?.years || []
      },
      
      getAvailableResolutions: () => {
        const metadata = get().metadata
        return metadata?.resolutions || []
      },
      
      getAvailableKommuneYears: () => {
        const metadata = get().metadata
        return metadata?.kommune?.years || []
      },
      
      isYearResolutionAvailable: (year, resolution) => {
        const metadata = get().metadata
        if (!metadata) return false
        
        return metadata.years.includes(year) && metadata.resolutions.includes(resolution)
      },
      
      isKommuneYearAvailable: (year) => {
        const metadata = get().metadata
        if (!metadata?.kommune) return false
        
        return metadata.kommune.years.includes(year)
      },
      
      clearOldTiles: () => {
        const now = Date.now()
        const maxAge = 24 * 60 * 60 * 1000 // 24 hours
        
        set((state) => {
          const filteredTiles = Object.fromEntries(
            Object.entries(state.loadedTiles).filter(
              ([, tile]) => now - tile.loadedAt < maxAge
            )
          )
          
          return { loadedTiles: filteredTiles }
        })
      },
    }),
    {
      name: 'pmtiles-store',
      partialize: (state) => ({
        metadata: state.metadata,
        loadedTiles: state.loadedTiles,
      }),
    }
  )
) 