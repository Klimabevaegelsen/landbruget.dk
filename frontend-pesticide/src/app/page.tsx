'use client';

import { useEffect, useState } from 'react'
import { PMTilesMap } from '@/components/map/PMTilesMap'
import { ThemeToggle } from '@/components/controls/ThemeToggle'
import { usePMTilesStore } from '@/stores/pmtiles-store'
import { useTemporalStore } from '@/stores/temporal-store'
import { useResolutionStore } from '@/stores/resolution-store'
import { Play, Pause, ChevronLeft, ChevronRight } from 'lucide-react'

type DataLayer = 'pfas' | 'total_pesticide' | 'diquat' | 'glyphosate'

export default function Home() {
  const [isInitialized, setIsInitialized] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [showBNBO, setShowBNBO] = useState(false)
  const [showKommune, setShowKommune] = useState(false)
  const [bnboOpacity, setBnboOpacity] = useState(0.4)
  const [kommuneOpacity, setKommuneOpacity] = useState(0.6)
  const [activeDataLayer, setActiveDataLayer] = useState<DataLayer>('pfas')
  
  // Store hooks
  const { 
    metadata, 
    metadataLoading, 
    metadataError,
    setMetadata, 
    setMetadataLoading, 
    setMetadataError 
  } = usePMTilesStore()
  
  const { 
    currentYear, 
    availableYears, 
    isAnimating, 
    setAvailableYears, 
    setCurrentYear, 
    startAnimation, 
    stopAnimation, 
    goToNextYear, 
    goToPreviousYear, 
    canGoNext, 
    canGoPrevious 
  } = useTemporalStore()
  
  const { setResolution, currentResolution } = useResolutionStore()
  
  // Load PMTiles metadata on mount
  useEffect(() => {
    async function loadMetadata() {
      try {
        setMetadataLoading(true)
        setError(null)
        
        const response = await fetch('/api/metadata')
        if (!response.ok) {
          throw new Error(`Failed to load metadata: ${response.statusText}`)
        }
        
        const data = await response.json()
        
        // Update stores with metadata
        setMetadata(data)
        setAvailableYears(data.years)
        
        // Set initial year to most recent
        if (data.years.length > 0) {
          const latestYear = Math.max(...data.years)
          setCurrentYear(latestYear)
        }
        
        // Set initial resolution to highest available
        if (data.resolutions.length > 0) {
          const highestRes = Math.max(...data.resolutions)
          setResolution(highestRes)
        }
        
        setIsInitialized(true)
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown error'
        setError(errorMessage)
        setMetadataError(errorMessage)
      } finally {
        setMetadataLoading(false)
      }
    }
    
    loadMetadata()
  }, [setMetadata, setMetadataLoading, setMetadataError, setAvailableYears, setCurrentYear, setResolution])
  
  // Loading state
  if (metadataLoading || !isInitialized) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-6 h-6 border border-white border-t-transparent rounded-full animate-spin mx-auto mb-3"></div>
          <p className="text-white text-sm font-light">Loading...</p>
        </div>
      </div>
    )
  }
  
  // Error state
  if (error || metadataError) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-sm mb-3">
            {error || metadataError}
          </div>
          <button
            onClick={() => window.location.reload()}
            className="px-3 py-1 bg-white text-black text-sm rounded hover:bg-gray-100 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    )
  }

  const dataLayerConfig = {
    total_pesticide: {
      name: 'Total Pesticide Load',
      description: 'Total pesticide application load',
      field: 'pesticide_load',
      unit: 'g',
      colors: [
        { color: 'bg-white', label: 'None', value: 0 },
        { color: 'bg-orange-200', label: 'Low', value: 1000 },
        { color: 'bg-orange-400', label: 'Medium', value: 10000 },
        { color: 'bg-orange-600', label: 'High', value: 50000 },
        { color: 'bg-orange-800', label: 'Very High', value: 100000 }
      ]
    },
    pfas: {
      name: 'PFAS Load',
      description: 'PFAS contamination levels',
      field: 'pfas_grams',
      unit: 'g',
      colors: [
        { color: 'bg-white', label: 'None', value: 0 },
        { color: 'bg-red-200', label: 'Low', value: 0.1 },
        { color: 'bg-red-400', label: 'Medium', value: 1 },
        { color: 'bg-red-600', label: 'High', value: 10 },
        { color: 'bg-red-800', label: 'Very High', value: 50 }
      ]
    },
    diquat: {
      name: 'Diquat Load',
      description: 'Diquat herbicide active ingredient',
      field: 'diquat_grams',
      unit: 'g',
      colors: [
        { color: 'bg-white', label: 'None', value: 0 },
        { color: 'bg-blue-200', label: 'Low', value: 0.1 },
        { color: 'bg-blue-400', label: 'Medium', value: 1 },
        { color: 'bg-blue-600', label: 'High', value: 10 },
        { color: 'bg-blue-800', label: 'Very High', value: 100 }
      ]
    },
    glyphosate: {
      name: 'Glyphosate Load',
      description: 'Glyphosate herbicide active ingredient',
      field: 'glyphosate_grams',
      unit: 'g',
      colors: [
        { color: 'bg-white', label: 'None', value: 0 },
        { color: 'bg-green-200', label: 'Low', value: 0.1 },
        { color: 'bg-green-400', label: 'Medium', value: 1 },
        { color: 'bg-green-600', label: 'High', value: 10 },
        { color: 'bg-green-800', label: 'Very High', value: 100 }
      ]
    }
  }
  
  const currentLayerConfig = dataLayerConfig[activeDataLayer]
  
  return (
    <div className="min-h-screen bg-black text-white flex flex-col">
      {/* Top Header */}
      <div className="bg-black/90 backdrop-blur-sm border-b border-white/10 px-6 py-4 z-50">
        <div className="flex items-center justify-between">
          {/* Left: Title */}
          <div>
            <h1 className="text-lg font-semibold">PFAS Environmental Impact</h1>
            <p className="text-sm text-gray-400">Pesticide contamination • Denmark</p>
          </div>
          
          {/* Center: Data Layer Toggle */}
          <div className="flex items-center space-x-2 bg-white/10 rounded-lg p-1">
            {Object.entries(dataLayerConfig).map(([key, config]) => (
              <button
                key={key}
                onClick={() => setActiveDataLayer(key as DataLayer)}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-all ${
                  activeDataLayer === key
                    ? 'bg-white text-black'
                    : 'text-white hover:bg-white/20'
                }`}
              >
                {config.name}
              </button>
            ))}
          </div>
          
          {/* Right: Theme Toggle */}
          <div className="flex items-center space-x-4">
            <ThemeToggle />
            {metadata && (
              <div className="text-right text-xs text-gray-400 font-light">
                <div>{metadata.years.length} years • {metadata.resolutions.length} resolutions</div>
              </div>
            )}
          </div>
        </div>
        
        {/* Year Slider */}
        <div className="mt-4 flex items-center justify-center space-x-6">
          <button
            onClick={goToPreviousYear}
            disabled={!canGoPrevious() || isAnimating}
            className="p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
          
          <button
            onClick={isAnimating ? stopAnimation : startAnimation}
            disabled={availableYears.length <= 1}
            className="p-2 rounded-full bg-white text-black hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
          >
            {isAnimating ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
          </button>
          
          <button
            onClick={goToNextYear}
            disabled={!canGoNext() || isAnimating}
            className="p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
          
          {/* Year Slider */}
          <div className="flex-1 max-w-md">
            <div className="flex justify-between text-xs text-gray-400 mb-2">
              <span>{Math.min(...availableYears)}</span>
              <span className="font-bold text-white">{currentYear}</span>
              <span>{Math.max(...availableYears)}</span>
            </div>
            <div className="relative">
              <input
                type="range"
                min={Math.min(...availableYears)}
                max={Math.max(...availableYears)}
                step="1"
                value={currentYear}
                onChange={(e) => setCurrentYear(parseInt(e.target.value))}
                className="w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer slider"
                disabled={isAnimating}
              />
              <div className="flex justify-between mt-1">
                {availableYears.map((year) => (
                  <div
                    key={year}
                    className={`w-1 h-2 rounded-full ${
                      year === currentYear ? 'bg-white' : 'bg-white/30'
                    }`}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex">
        {/* Left Sidebar - Legend */}
        <div className="w-80 bg-black/90 backdrop-blur-sm border-r border-white/10 p-6 overflow-y-auto">
          <div className="space-y-6">
            {/* Current Layer Info */}
            <div>
              <h3 className="text-lg font-semibold mb-2">{currentLayerConfig.name}</h3>
              <p className="text-sm text-gray-400 mb-4">{currentLayerConfig.description}</p>
              
              {/* Legend */}
              <div className="space-y-2">
                <h4 className="text-sm font-medium text-gray-300">Legend</h4>
                {currentLayerConfig.colors.map((item, index) => (
                  <div key={index} className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                      <div className={`w-4 h-4 ${item.color} rounded border border-white/20`}></div>
                      <span className="text-sm">{item.label}</span>
                    </div>
                    <span className="text-xs text-gray-400">
                      {item.value > 0 ? `${item.value}+ ${currentLayerConfig.unit}` : 'No data'}
                    </span>
                  </div>
                ))}
              </div>
            </div>
            
            {/* Municipality Toggle */}
            <div className="border-t border-white/10 pt-4">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h4 className="text-sm font-medium">Municipality Boundaries</h4>
                  <p className="text-xs text-gray-400">Auto-shown when zoomed out (≤ zoom 8)</p>
                </div>
                <label className="relative inline-flex items-center cursor-pointer">
                  <input
                    type="checkbox"
                    checked={showKommune}
                    onChange={(e) => setShowKommune(e.target.checked)}
                    className="sr-only peer"
                  />
                  <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                </label>
              </div>
              
              <div className="text-xs text-gray-400 mb-3">
                <span className="inline-flex items-center space-x-1">
                  <span className="w-2 h-2 bg-blue-500 rounded-full animate-pulse"></span>
                  <span>Municipalities visible when zoom ≤ 8</span>
                </span>
              </div>
              
              {(showKommune) && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm">Opacity</span>
                    <span className="text-xs text-gray-400">{Math.round(kommuneOpacity * 100)}%</span>
                  </div>
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.1"
                    value={kommuneOpacity}
                    onChange={(e) => setKommuneOpacity(parseFloat(e.target.value))}
                    className="w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer"
                  />
                  
                  <div className="space-y-2 mt-4">
                    <div className="text-xs text-gray-400 mb-2">Municipal PFAS levels:</div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-red-800 rounded border border-white/20"></div>
                      <span className="text-sm">Very High (1000+ g)</span>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-red-600 rounded border border-white/20"></div>
                      <span className="text-sm">High (100-1000 g)</span>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-red-400 rounded border border-white/20"></div>
                      <span className="text-sm">Medium (10-100 g)</span>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-red-200 rounded border border-white/20"></div>
                      <span className="text-sm">Low (0-10 g)</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
            
            {/* BNBO Toggle */}
            <div className="border-t border-white/10 pt-4">
              <div className="flex items-center justify-between mb-3">
                <h4 className="text-sm font-medium">BNBO Protected Areas</h4>
                <label className="relative inline-flex items-center cursor-pointer">
                  <input
                    type="checkbox"
                    checked={showBNBO}
                    onChange={(e) => setShowBNBO(e.target.checked)}
                    className="sr-only peer"
                  />
                  <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
                </label>
              </div>
              
              {showBNBO && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-sm">Opacity</span>
                    <span className="text-xs text-gray-400">{Math.round(bnboOpacity * 100)}%</span>
                  </div>
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.1"
                    value={bnboOpacity}
                    onChange={(e) => setBnboOpacity(parseFloat(e.target.value))}
                    className="w-full h-2 bg-white/20 rounded-lg appearance-none cursor-pointer"
                  />
                  
                  <div className="space-y-2 mt-4">
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-red-500 rounded border border-white/20"></div>
                      <span className="text-sm">Action Required</span>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-green-500 rounded border border-white/20"></div>
                      <span className="text-sm">Completed</span>
                    </div>
                    <div className="flex items-center space-x-3">
                      <div className="w-4 h-4 bg-gray-500 rounded border border-white/20"></div>
                      <span className="text-sm">Unknown</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
            
            {/* Current State Info */}
            <div className="border-t border-white/10 pt-4">
              <h4 className="text-sm font-medium mb-3">Current View</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-400">Year:</span>
                  <span className="font-medium">{currentYear}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Resolution:</span>
                  <span className="font-medium">H3-{currentResolution}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Data Layer:</span>
                  <span className="font-medium">{currentLayerConfig.name}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
        
        {/* Map Container */}
        <div className="flex-1">
          <PMTilesMap 
            className="w-full h-full"
            pmtilesBaseUrl="/api/pmtiles"
            showBNBO={showBNBO}
            bnboOpacity={bnboOpacity}
            showKommune={showKommune}
            kommuneOpacity={kommuneOpacity}
            onBNBOToggle={setShowBNBO}
            onKommuneToggle={setShowKommune}
            activeDataLayer={activeDataLayer}
          />
        </div>
      </div>
    </div>
  )
} 