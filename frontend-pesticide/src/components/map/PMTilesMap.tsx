'use client'

import React, { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { ErrorBoundary } from 'react-error-boundary'
import { MapTooltip } from './MapTooltip'
import { useMapStore, useMapViewState, useDataState, useLayerVisibility, useLoadingState, getComputedLayerVisibility } from '@/stores/map-store'
import { pmtilesDiscovery } from '@/services/pmtiles-discovery'

// Dynamic imports for browser-only modules
const loadMapLibreAndPMTiles = async () => {
  if (typeof window === 'undefined') return null
  
  console.log('🔄 Loading MapLibre and PMTiles...')
  
  const [maplibregl, { Protocol }] = await Promise.all([
    import('maplibre-gl'),
    import('pmtiles')
  ])
  
  console.log('✅ MapLibre and PMTiles loaded successfully')
  
  // Register PMTiles protocol
  let protocolRegistered = false
  if (!protocolRegistered) {
    const protocol = new Protocol()
    maplibregl.default.addProtocol('pmtiles', protocol.tile)
    protocolRegistered = true
    console.log('✅ PMTiles protocol registered')
  }
  
  return maplibregl.default
}

interface PMTilesMapProps {
  className?: string
}

const PMTilesMapInner: React.FC<PMTilesMapProps> = ({ className = 'w-full h-full' }) => {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<any>(null)
  const [mapLibre, setMapLibre] = useState<any>(null)
  const [mapLoaded, setMapLoaded] = useState(false)
  const [pmtilesUrls, setPmtilesUrls] = useState<any>({})
  
  // Store state
  const { zoom, center, bearing, pitch } = useMapViewState()
  const { selectedYear, selectedDataMode, availableYears } = useDataState()
  const showBNBOLayer = useMapStore((state) => state.showBNBOLayer)
  const showBasemap = useMapStore((state) => state.showBasemap)
  const { isLoading, error } = useLoadingState()
  
  // Compute layer visibility based on zoom (stable)
  const shouldShowKommune = zoom <= 8
  const shouldShowH3 = zoom >= 9
  const currentH3Resolution = zoom >= 14 ? 10 : zoom >= 12 ? 9 : zoom >= 10 ? 8 : 7
  
  // Get property name based on data mode - using actual property names from tooltip
  const getPropertyName = (mode: string) => {
    switch (mode) {
      case 'pfas':
        return 'pfas_grams'; // Based on tooltip data
      case 'diquat':
        return 'diquat_grams'; // Based on tooltip data
      case 'glyphosate':
        return 'glyphosate_grams'; // Based on tooltip data
      default:
        return 'pesticide_load'; // Based on tooltip data
    }
  }
  
  // Get current property name for styling
  const currentPropertyName = getPropertyName(selectedDataMode)
  
  // Store actions
  const { 
    setViewState,
    setIsLoading,
    setError, 
    clearError,
    showTooltipWithData,
    hideTooltip,
    setAvailableYears
  } = useMapStore()

  // Load MapLibre and PMTiles
  useEffect(() => {
    let mounted = true
    
    const initMapLibre = async () => {
      try {
        setIsLoading(true)
        const mapLibreInstance = await loadMapLibreAndPMTiles()
        if (mounted && mapLibreInstance) {
          setMapLibre(mapLibreInstance)
        }
      } catch (error) {
        console.error('Error loading MapLibre:', error)
        if (mounted) {
          setError('Failed to load mapping library')
        }
      } finally {
        if (mounted) {
          setIsLoading(false)
        }
      }
    }
    
    initMapLibre()
    
    return () => {
      mounted = false
    }
  }, [])

  // Load PMTiles URLs
  useEffect(() => {
    console.log('🔄 PMTiles useEffect triggered - year:', selectedYear, 'resolution:', currentH3Resolution, 'zoom:', zoom)
    let mounted = true
    
    const loadPMTilesUrls = async () => {
      try {
        setIsLoading(true)
        
        // Discover and validate URLs
        const [validatedUrls, years] = await Promise.all([
          pmtilesDiscovery.discoverAndValidateUrls(selectedYear, currentH3Resolution),
          pmtilesDiscovery.getAvailableYears()
        ])
        
        if (mounted) {
          // Only set URLs that are valid
          const urls: any = {}
          if (validatedUrls.basemap) urls.basemap = validatedUrls.basemap
          if (validatedUrls.kommune) urls.kommune = validatedUrls.kommune
          if (validatedUrls.h3) urls.h3 = validatedUrls.h3
          if (validatedUrls.bnbo) urls.bnbo = validatedUrls.bnbo
          
          setPmtilesUrls(urls)
          // Remove setAvailableYears to prevent potential loop
          // setAvailableYears(years)
          
          // Log what we found
          console.log('🗺️ PMTiles URLs discovered:', {
            basemap: validatedUrls.basemap ? '✅' : '❌',
            kommune: validatedUrls.kommune ? '✅' : '❌',
            h3: validatedUrls.h3 ? '✅' : '❌',
            bnbo: validatedUrls.bnbo ? '✅' : '❌'
          })
          
          // Log actual URLs for debugging
          console.log('📍 Actual URLs:', {
            basemap: validatedUrls.basemap,
            kommune: validatedUrls.kommune,
            h3: validatedUrls.h3,
            bnbo: validatedUrls.bnbo
          })
          
          if (!validatedUrls.basemap) {
            setError('Basemap not available')
          } else {
            clearError()
          }
        }
      } catch (error) {
        console.error('Error loading PMTiles URLs:', error)
        if (mounted) {
          setError('Failed to load data sources')
        }
      } finally {
        if (mounted) {
          setIsLoading(false)
        }
      }
    }
    
    loadPMTilesUrls()
    
    return () => {
      mounted = false
    }
  }, [selectedYear, currentH3Resolution])

  // Initialize map
  useEffect(() => {
    console.log('🔍 Map initialization check:', {
      mapLibre: !!mapLibre,
      mapContainer: !!mapContainer.current,
      basemapUrl: pmtilesUrls.basemap,
      pmtilesUrls: pmtilesUrls
    })
    
    if (!mapLibre || !mapContainer.current || !pmtilesUrls.basemap) return

    try {
      console.log('🚀 Creating map with URLs:', pmtilesUrls)
      
      map.current = new mapLibre.Map({
        container: mapContainer.current,
        style: {
          version: 8,
          sources: {
            'basemap': {
              type: 'vector',
              url: `pmtiles://${pmtilesUrls.basemap}`,
            },
            'kommune': {
              type: 'vector',
              url: `pmtiles://${pmtilesUrls.kommune}`,
            },
            'h3': {
              type: 'vector',
              url: `pmtiles://${pmtilesUrls.h3}`,
            },
            'bnbo': {
              type: 'vector',
              url: `pmtiles://${pmtilesUrls.bnbo}`,
            }
          },
          layers: [
            // Black basemap - use landuse layer for Denmark land areas
            {
              id: 'basemap-fill',
              type: 'fill',
              source: 'basemap',
              'source-layer': 'landuse', // Protomaps landuse layer for land areas
              paint: {
                'fill-color': '#000000',
                'fill-opacity': 1
              }
            },
            // Kommune layer (visible at low zoom) - use actual source-layer name
            {
              id: 'kommune-fill',
              type: 'fill',
              source: 'kommune',
              'source-layer': `kommune_pfas_${selectedYear}`, // Actual source-layer name from pipeline
              layout: {
                visibility: shouldShowKommune ? 'visible' : 'none'
              },
              paint: {
                'fill-color': [
                  'interpolate',
                  ['linear'],
                  ['get', currentPropertyName], // Use dynamic property name
                  0, 'transparent',
                  0.1, '#fee5d9',
                  1, '#fcbba1',
                  5, '#fc9272',
                  10, '#fb6a4a',
                  20, '#ef3b2c',
                  50, '#cb181d',
                  100, '#99000d'
                ],
                'fill-opacity': 0.8
              }
            },
            // Kommune borders
            {
              id: 'kommune-stroke',
              type: 'line',
              source: 'kommune',
              'source-layer': `kommune_pfas_${selectedYear}`, // Actual source-layer name from pipeline
              layout: {
                visibility: shouldShowKommune ? 'visible' : 'none'
              },
              paint: {
                'line-color': '#ffffff',
                'line-width': 0.5,
                'line-opacity': 0.5
              }
            },
            // H3 layer (visible at high zoom)
            {
              id: 'h3-fill',
              type: 'fill',
              source: 'h3',
              'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`, // Actual source-layer name from pipeline
              layout: {
                visibility: shouldShowH3 ? 'visible' : 'none'
              },
              paint: {
                'fill-color': [
                  'interpolate',
                  ['linear'],
                  ['get', currentPropertyName], // Use dynamic property name
                  0, 'transparent',
                  0.1, '#fee5d9',
                  1, '#fcbba1',
                  5, '#fc9272',
                  10, '#fb6a4a',
                  20, '#ef3b2c',
                  50, '#cb181d',
                  100, '#99000d'
                ],
                'fill-opacity': 0.7
              }
            },
            // H3 borders
            {
              id: 'h3-stroke',
              type: 'line',
              source: 'h3',
              'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`, // Actual source-layer name from our pipeline
              layout: {
                visibility: shouldShowH3 ? 'visible' : 'none'
              },
              paint: {
                'line-color': '#ffffff',
                'line-width': 0.2,
                'line-opacity': 0.3
              }
            },
            // BNBO layer (overlay)
            {
              id: 'bnbo-fill',
              type: 'fill',
              source: 'bnbo',
              'source-layer': 'bnbo_areas', // BNBO static layer name
              layout: {
                visibility: showBNBOLayer ? 'visible' : 'none'
              },
              paint: {
                'fill-color': [
                  'match',
                  ['get', 'status'],
                  'Action Required', '#ff6b6b',
                  'Completed', '#51cf66',
                  'Unknown', '#868e96',
                  '#cccccc' // fallback
                ],
                'fill-opacity': 0.6
              }
            },
            // BNBO borders
            {
              id: 'bnbo-stroke',
              type: 'line',
              source: 'bnbo',
              'source-layer': 'bnbo_areas', // BNBO static layer name
              layout: {
                visibility: showBNBOLayer ? 'visible' : 'none'
              },
              paint: {
                'line-color': '#ffffff',
                'line-width': 1,
                'line-opacity': 0.8
              }
            }
          ],
        },
        center: center,
        zoom: zoom,
        bearing: bearing,
        pitch: pitch,
        maxZoom: 15,
        minZoom: 4,
        maxBounds: [
          [7.0, 54.0], // Southwest bounds (Denmark)
          [13.0, 58.0], // Northeast bounds (Denmark)
        ],
      })

      // Add controls
      map.current.addControl(new mapLibre.NavigationControl(), 'top-right')
      map.current.addControl(new mapLibre.ScaleControl(), 'bottom-left')

      // Map event handlers
      map.current.on('load', () => {
        console.log('🎉 Map loaded successfully!')
        
        // Debug: Check available sources and layers
        const style = map.current.getStyle()
        console.log('📊 Map style sources:', Object.keys(style.sources))
        console.log('📊 Map style layers:', style.layers.map((l: any) => ({ id: l.id, type: l.type, source: l.source, 'source-layer': l['source-layer'] })))
        
        // Debug: Try to get source data and inspect tiles
        setTimeout(() => {
          if (!map.current) return
          
          console.log(`📊 Current zoom: ${map.current.getZoom()}, center:`, map.current.getCenter())
          
          // Try to inspect each source more thoroughly
          const sources = ['basemap', 'kommune', 'h3', 'bnbo']
          sources.forEach(sourceId => {
            if (!map.current) return
            const source = map.current.getSource(sourceId)
            if (source) {
              console.log(`📊 Source ${sourceId}:`, source)
              
              // Check if source has loaded tiles
              if (source._tiles) {
                console.log(`📊 ${sourceId} has ${Object.keys(source._tiles).length} loaded tiles`)
              }
              
              // Try to query all features from this source
              try {
                if (!map.current) return
                const allFeatures = map.current.querySourceFeatures(sourceId)
                console.log(`📊 ${sourceId} total features: ${allFeatures.length}`)
                
                if (allFeatures.length > 0) {
                  const sampleFeature = allFeatures[0]
                  console.log(`📊 ${sourceId} sample feature:`, sampleFeature)
                  console.log(`📊 ${sourceId} sample properties:`, sampleFeature.properties)
                  console.log(`📊 ${sourceId} source-layer:`, sampleFeature.sourceLayer)
                }
              } catch (e) {
                console.log(`📊 Could not query ${sourceId} features:`, e)
              }
            }
          })
          
          // Also try to get all rendered features at current view
          try {
            if (!map.current) return
            const allRenderedFeatures = map.current.queryRenderedFeatures()
            console.log(`📊 Total rendered features in view: ${allRenderedFeatures.length}`)
            
            if (allRenderedFeatures.length > 0) {
              const sourceLayerCounts = allRenderedFeatures.reduce((acc: any, f: any) => {
                const key = `${f.source}:${f.sourceLayer}`
                acc[key] = (acc[key] || 0) + 1
                return acc
              }, {})
              console.log(`📊 Rendered features by source:layer:`, sourceLayerCounts)
            }
          } catch (e) {
            console.log(`📊 Could not query rendered features:`, e)
          }
        }, 3000)
        
        setMapLoaded(true)
        clearError()
      })

      map.current.on('move', () => {
        if (!map.current) return
        const { lng, lat } = map.current.getCenter()
        const zoom = map.current.getZoom()
        const bearing = map.current.getBearing()
        const pitch = map.current.getPitch()
        
        setViewState({ 
          center: [lng, lat], 
          zoom, 
          bearing, 
          pitch 
        })
      })

      map.current.on('click', (e: any) => {
        const features = map.current?.queryRenderedFeatures(e.point, {
          layers: ['kommune-fill', 'h3-fill', 'bnbo-fill']
        })
        if (features && features.length > 0) {
          const feature = features[0]
          showTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
        } else {
          hideTooltip()
        }
      })

      map.current.on('mousemove', (e: any) => {
        const features = map.current?.queryRenderedFeatures(e.point, {
          layers: ['kommune-fill', 'h3-fill', 'bnbo-fill']
        })
        if (features && features.length > 0) {
          const feature = features[0]
          showTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
          if (map.current) {
            map.current.getCanvas().style.cursor = 'pointer'
          }
        } else {
          hideTooltip()
          if (map.current) {
            map.current.getCanvas().style.cursor = ''
          }
        }
      })

      map.current.on('mouseleave', () => {
        hideTooltip()
        if (map.current) {
          map.current.getCanvas().style.cursor = ''
        }
      })

      map.current.on('error', (e: any) => {
        console.error('❌ Map error:', e)
        console.error('❌ Map error details:', {
          type: e.type,
          error: e.error,
          sourceId: e.sourceId,
          tile: e.tile
        })
        setError(`Map loading error: ${e.error?.message || 'Unknown error'}`)
      })
      
      map.current.on('sourcedata', (e: any) => {
        if (e.isSourceLoaded) {
          console.log(`📊 Source loaded: ${e.sourceId}`, e)
        }
      })
      
      map.current.on('sourcedataloading', (e: any) => {
        console.log(`📊 Source loading: ${e.sourceId}`, e)
      })

    } catch (err) {
      console.error('❌ Error initializing map:', err)
      setError('Failed to initialize map')
    }

    return () => {
      if (map.current) {
        map.current.remove()
        map.current = null
      }
    }
  }, [mapLibre, pmtilesUrls])

  // Update layer visibility based on zoom
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      const layerVisibility = getComputedLayerVisibility(zoom)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && map.current.getLayer(layerId)) {
          map.current.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
        }
      }
      
      // Update Kommune layer visibility
      updateLayerVisibility('kommune-fill', layerVisibility.shouldShowKommune)
      updateLayerVisibility('kommune-stroke', layerVisibility.shouldShowKommune)
      
      // Update H3 layer visibility
      updateLayerVisibility('h3-fill', layerVisibility.shouldShowH3)
      updateLayerVisibility('h3-stroke', layerVisibility.shouldShowH3)
      
      // Update BNBO layer visibility
      updateLayerVisibility('bnbo-fill', showBNBOLayer)
      updateLayerVisibility('bnbo-stroke', showBNBOLayer)
    } catch (error) {
      console.warn('Error updating layer visibility:', error)
    }
  }, [zoom, showBNBOLayer, mapLoaded])

  // Update paint expressions when data mode changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🎨 Updating paint expressions for data mode:', selectedDataMode, 'property:', currentPropertyName)
      
      // Update Kommune layer paint
      if (map.current.getLayer('kommune-fill')) {
        const newPaint = [
          'interpolate',
          ['linear'],
          ['get', currentPropertyName],
          0, 'transparent',
          0.1, '#fee5d9',
          1, '#fcbba1',
          5, '#fc9272',
          10, '#fb6a4a',
          20, '#ef3b2c',
          50, '#cb181d',
          100, '#99000d'
        ]
        map.current.setPaintProperty('kommune-fill', 'fill-color', newPaint)
        console.log('✅ Updated kommune-fill paint property')
      }
      
      // Update H3 layer paint
      if (map.current.getLayer('h3-fill')) {
        const newPaint = [
          'interpolate',
          ['linear'],
          ['get', currentPropertyName],
          0, 'transparent',
          0.1, '#fee5d9',
          1, '#fcbba1',
          5, '#fc9272',
          10, '#fb6a4a',
          20, '#ef3b2c',
          50, '#cb181d',
          100, '#99000d'
        ]
        map.current.setPaintProperty('h3-fill', 'fill-color', newPaint)
        console.log('✅ Updated h3-fill paint property')
      }
      
      // Debug: Try to get some feature data to see what properties are available
      setTimeout(() => {
        try {
          const features = map.current.queryRenderedFeatures()
          if (features.length > 0) {
            const sampleFeature = features[0]
            console.log('🔍 Sample feature properties:', sampleFeature.properties)
            console.log('🔍 Current property value:', sampleFeature.properties[currentPropertyName])
          }
        } catch (e) {
          console.log('🔍 Could not query features for debugging:', e)
        }
      }, 1000)
      
        } catch (error) {
      console.warn('Error updating paint expressions:', error)
    }
  }, [selectedDataMode, currentPropertyName, mapLoaded])

  // Update source-layer names when year or resolution changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🔄 Updating source-layer names for year:', selectedYear, 'resolution:', currentH3Resolution)
      
      // Remove existing layers
      const layersToUpdate = ['kommune-fill', 'kommune-stroke', 'h3-fill', 'h3-stroke']
      layersToUpdate.forEach(layerId => {
        if (map.current && map.current.getLayer(layerId)) {
          map.current.removeLayer(layerId)
        }
      })
      
      // Re-add Kommune layers with correct source-layer name
      const kommuneSourceLayer = `kommune_pfas_${selectedYear}`
      map.current.addLayer({
        id: 'kommune-fill',
        type: 'fill',
        source: 'kommune',
        'source-layer': kommuneSourceLayer,
        layout: {
          visibility: shouldShowKommune ? 'visible' : 'none'
        },
        paint: {
          'fill-color': [
            'interpolate',
            ['linear'],
            ['get', currentPropertyName],
            0, 'transparent',
            0.1, '#fee5d9',
            1, '#fcbba1',
            5, '#fc9272',
            10, '#fb6a4a',
            20, '#ef3b2c',
            50, '#cb181d',
            100, '#99000d'
          ],
          'fill-opacity': 0.8
        }
      })
      
      map.current.addLayer({
        id: 'kommune-stroke',
        type: 'line',
        source: 'kommune',
        'source-layer': kommuneSourceLayer,
        layout: {
          visibility: shouldShowKommune ? 'visible' : 'none'
        },
        paint: {
          'line-color': '#ffffff',
          'line-width': 0.5,
          'line-opacity': 0.5
        }
      })
      
      // Re-add H3 layers with correct source-layer name
      const h3SourceLayer = `h3_pfas_${selectedYear}_res${currentH3Resolution}`
      map.current.addLayer({
        id: 'h3-fill',
        type: 'fill',
        source: 'h3',
        'source-layer': h3SourceLayer,
        layout: {
          visibility: shouldShowH3 ? 'visible' : 'none'
        },
        paint: {
          'fill-color': [
            'interpolate',
            ['linear'],
            ['get', currentPropertyName],
            0, 'transparent',
            0.1, '#fee5d9',
            1, '#fcbba1',
            5, '#fc9272',
            10, '#fb6a4a',
            20, '#ef3b2c',
            50, '#cb181d',
            100, '#99000d'
          ],
          'fill-opacity': 0.7
        }
      })
      
      map.current.addLayer({
        id: 'h3-stroke',
        type: 'line',
        source: 'h3',
        'source-layer': h3SourceLayer,
        layout: {
          visibility: shouldShowH3 ? 'visible' : 'none'
        },
        paint: {
          'line-color': '#ffffff',
          'line-width': 0.2,
          'line-opacity': 0.3
        }
      })
      
      console.log('✅ Updated source-layer names:', {
        kommune: kommuneSourceLayer,
        h3: h3SourceLayer
      })
      
    } catch (error) {
      console.warn('Error updating source-layer names:', error)
    }
  }, [selectedYear, currentH3Resolution, mapLoaded, shouldShowKommune, shouldShowH3, currentPropertyName])

 

  // Show loading state
  if (!mapLibre || isLoading) {
    return (
      <div className={`relative flex items-center justify-center bg-gray-900 ${className}`}>
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-white text-sm">Loading map...</p>
        </div>
      </div>
    )
  }

  // Show error state
  if (error) {
    return (
      <div className={`relative flex items-center justify-center bg-gray-900 ${className}`}>
        <div className="text-center">
          <div className="text-red-400 text-4xl mb-4">⚠️</div>
          <p className="text-white text-sm mb-2">Map Error</p>
          <p className="text-gray-400 text-xs">{error}</p>
          <button
            onClick={clearError}
            className="mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    )
  }
  
  return (
    <div className={`relative ${className}`}>
      <div ref={mapContainer} className="w-full h-full" />
      
      {/* Loading overlay */}
      {!mapLoaded && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-900/80">
          <div className="text-center">
            <div className="w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-2"></div>
            <p className="text-white text-sm">Initializing map...</p>
          </div>
        </div>
      )}
      
      {/* Map info overlay */}
      <div className="absolute top-4 left-4 bg-black/80 text-white px-3 py-2 rounded text-sm max-w-xs">
        <div>Year: {selectedYear}</div>
        <div>Mode: {selectedDataMode}</div>
        <div>Zoom: {Math.round(zoom * 10) / 10}</div>
        <div>Layers: {shouldShowKommune ? 'Kommune' : shouldShowH3 ? `H3 (${currentH3Resolution})` : 'None'}</div>
        <div className="mt-2 text-xs opacity-75">
          <div>Sources:</div>
          <div>• Basemap: {pmtilesUrls.basemap ? '✅' : '❌'}</div>
          <div>• Kommune: {pmtilesUrls.kommune ? '✅' : '❌'}</div>
          <div>• H3: {pmtilesUrls.h3 ? '✅' : '❌'}</div>
          <div>• BNBO: {pmtilesUrls.bnbo ? '✅' : '❌'}</div>
        </div>
      </div>
      
      {/* Tooltip */}
      <MapTooltip />
    </div>
  )
}

// Error boundary component
const MapErrorFallback: React.FC<{ error: Error; resetErrorBoundary: () => void }> = ({ 
  error, 
  resetErrorBoundary 
}) => (
  <div className="flex items-center justify-center h-full bg-gray-900">
    <div className="text-center">
      <div className="text-red-400 text-6xl mb-4">⚠️</div>
      <h2 className="text-white text-xl font-semibold mb-2">Map Error</h2>
      <p className="text-gray-400 mb-4">{error.message}</p>
      <button
        onClick={resetErrorBoundary}
        className="px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
      >
        Reload Map
      </button>
    </div>
  </div>
)

// Main exported component with error boundary
export const PMTilesMap: React.FC<PMTilesMapProps> = (props) => {
  return (
    <ErrorBoundary FallbackComponent={MapErrorFallback}>
      <PMTilesMapInner {...props} />
    </ErrorBoundary>
  )
}