'use client'

import { useEffect, useState } from 'react'
import { NewPMTilesMap } from '@/components/map/NewPMTilesMap'
import { DataModeSelector } from '@/components/controls/DataModeSelector'
import { StepSlider } from '@/components/controls/StepSlider'
import { LayerToggle } from '@/components/controls/LayerToggle'
import { Legend } from '@/components/controls/Legend'
import { LoadingIndicator } from '@/components/ui/LoadingIndicator'
import { useMapStore } from '@/stores/map-store'
import { pmtilesDiscovery } from '@/services/pmtiles-discovery'

export default function NewMapPage() {
  const [isInitialized, setIsInitialized] = useState(false)
  const [initError, setInitError] = useState<string | null>(null)
  
  const { 
    isLoading, 
    loadingMessage, 
    error, 
    setError, 
    setIsLoading,
    setAvailableYears,
    currentYear,
    setCurrentYear
  } = useMapStore()

  // Initialize the application
  useEffect(() => {
    async function initialize() {
      try {
        setIsLoading(true, 'Initializing application...')
        
        // Get available years
        const years = await pmtilesDiscovery.getAvailableYears()
        setAvailableYears(years)
        
        // Set current year to latest if not already set
        if (years.length > 0 && !years.includes(currentYear)) {
          const latestYear = Math.max(...years)
          setCurrentYear(latestYear)
        }
        
        // Preload current year data
        setIsLoading(true, 'Preloading map data...')
        await pmtilesDiscovery.preloadYearData(currentYear)
        
        setIsInitialized(true)
        setIsLoading(false)
      } catch (error) {
        console.error('Failed to initialize:', error)
        const errorMessage = error instanceof Error ? error.message : 'Failed to initialize application'
        setInitError(errorMessage)
        setError(errorMessage)
        setIsLoading(false)
      }
    }

    initialize()
  }, [currentYear, setAvailableYears, setCurrentYear, setError, setIsLoading])

  // Handle map load
  const handleMapLoad = () => {
    console.log('Map loaded successfully')
  }

  // Handle map error
  const handleMapError = (error: string) => {
    console.error('Map error:', error)
    setError(error)
  }

  // Show initialization error
  if (initError) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-500 text-lg font-semibold mb-2">Initialization Error</div>
          <div className="text-red-400 text-sm max-w-md">{initError}</div>
          <button 
            onClick={() => window.location.reload()} 
            className="mt-4 px-4 py-2 bg-red-500 text-white rounded-md hover:bg-red-600 transition-colors"
          >
            Reload Page
          </button>
        </div>
      </div>
    )
  }

  // Show loading screen during initialization
  if (!isInitialized) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-white border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-white text-sm font-light">{loadingMessage || 'Loading...'}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="h-screen bg-black relative overflow-hidden">
      {/* Map */}
      <NewPMTilesMap 
        className="w-full h-full"
        onLoad={handleMapLoad}
        onError={handleMapError}
      />
      
      {/* Loading Indicator */}
      <LoadingIndicator 
        isLoading={isLoading} 
        message={loadingMessage}
      />
      
      {/* Error Display */}
      {error && (
        <div className="absolute top-4 left-1/2 transform -translate-x-1/2 z-20">
          <div className="bg-red-500/95 backdrop-blur-sm border border-red-400 rounded-lg shadow-lg px-4 py-3 max-w-md">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium text-white">{error}</span>
              <button 
                onClick={() => setError(null)}
                className="ml-3 text-red-200 hover:text-white"
              >
                ×
              </button>
            </div>
          </div>
        </div>
      )}
      
      {/* Controls - Top Left */}
      <div className="absolute top-4 left-4 z-10 space-y-4 max-w-xs">
        <DataModeSelector />
        <StepSlider />
      </div>
      
      {/* Layer Controls - Top Right */}
      <div className="absolute top-4 right-4 z-10 space-y-4 max-w-xs">
        <LayerToggle />
      </div>
      
      {/* Legend - Bottom Left */}
      <div className="absolute bottom-4 left-4 z-10 max-w-xs">
        <Legend />
      </div>
      
      {/* Info Panel - Bottom Right */}
      <div className="absolute bottom-4 right-4 z-10">
        <div className="bg-white/95 backdrop-blur-sm border border-slate-200 rounded-lg shadow-lg p-3 max-w-xs">
          <div className="text-sm font-medium text-slate-700 mb-2">
            New PMTiles Implementation
          </div>
          <div className="text-xs text-slate-500 space-y-1">
            <div>• Automatic latest data discovery</div>
            <div>• Zoom-based layer switching</div>
            <div>• 4 data modes with proper scaling</div>
            <div>• Comprehensive hover tooltips</div>
            <div>• BNBO protected areas overlay</div>
            <div>• Performance optimized</div>
          </div>
        </div>
      </div>
    </div>
  )
} 