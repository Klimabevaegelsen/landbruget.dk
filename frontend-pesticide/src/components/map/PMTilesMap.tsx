/* eslint-disable @typescript-eslint/no-explicit-any */
'use client'

import React, { useEffect, useRef, useState } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { ErrorBoundary } from 'react-error-boundary'
import { useMapStore, useMapViewState, useDataState, useLoadingState, getComputedLayerVisibility } from '@/stores/map-store'
import { useUIStore } from '@/stores/ui-store'
import { pmtilesDiscovery } from '@/services/pmtiles-discovery'

// Type definitions
type MapInstance = unknown;
type MapLibreGL = unknown;

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
  const map = useRef<MapInstance | null>(null)
  const [mapLibre, setMapLibre] = useState<MapLibreGL>(null)
  const [mapLoaded, setMapLoaded] = useState(false)
  const [pmtilesUrls, setPmtilesUrls] = useState<Record<string, string>>({})
  
  // Store state
  const { zoom, center, bearing, pitch } = useMapViewState()
  const { selectedYear, selectedDataMode } = useDataState()
  const showBasemap = useMapStore((state) => state.showBasemap)
  const { isLoading, error } = useLoadingState()
  const { isMobile, setShowMobilePanel } = useUIStore()
  
  // Compute layer visibility based on zoom (stable) - use centralized function
  const layerVisibility = getComputedLayerVisibility(zoom)
  const shouldShowKommune = layerVisibility.shouldShowKommune
  const shouldShowH3 = layerVisibility.shouldShowH3
  const currentH3Resolution = layerVisibility.currentH3Resolution
  
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
    setMapInstance,
    setIsLoading,
    setError, 
    clearError,
    showTooltipWithData,
    hideTooltip
  } = useMapStore()

  // Helper function to safely get available interactive layers
  const getAvailableInteractiveLayers = (): string[] => {
    if (!map.current) return []
    
    const layers = []
    try {
      if ((map.current as any).getLayer('kommune-fill')) {
        layers.push('kommune-fill')
      }
    } catch (e) {
      // Layer doesn't exist, ignore
    }
    
    try {
      if ((map.current as any).getLayer('h3-fill')) {
        layers.push('h3-fill')
      }
    } catch (e) {
      // Layer doesn't exist, ignore
    }
    
    try {
      if ((map.current as any).getLayer('bnbo-fill')) {
        layers.push('bnbo-fill')
      }
    } catch (e) {
      // Layer doesn't exist, ignore
    }
    
    return layers
  }

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
  }, [setError, setIsLoading, clearError])

  // Load PMTiles URLs
  useEffect(() => {
    console.log('🔄 PMTiles useEffect triggered - year:', selectedYear, 'resolution:', currentH3Resolution, 'zoom:', zoom)
    let mounted = true
    
    const loadPMTilesUrls = async () => {
      try {
        setIsLoading(true)
        
        console.log('🔍 Starting PMTiles URL discovery for:', { selectedYear, currentH3Resolution })
        
        // Discover and validate URLs
        const validatedUrls = await pmtilesDiscovery.discoverAndValidateUrls(selectedYear, currentH3Resolution)
        
        console.log('🔍 PMTiles URL discovery results:', validatedUrls)
        
        if (mounted) {
          // Only set URLs that are valid
          const urls: Record<string, string> = {}
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
          
          // Test URL accessibility
          if (validatedUrls.basemap) {
            console.log('🔗 Testing basemap URL accessibility...')
            pmtilesDiscovery.testUrl(validatedUrls.basemap).then(isAccessible => {
              console.log('🔗 Basemap URL accessible:', isAccessible)
            }).catch(err => {
              console.error('🔗 Basemap URL test failed:', err)
            })
          }
          
          if (validatedUrls.h3) {
            console.log('🔗 Testing H3 URL accessibility...')
            pmtilesDiscovery.testUrl(validatedUrls.h3).then(isAccessible => {
              console.log('🔗 H3 URL accessible:', isAccessible)
            }).catch(err => {
              console.error('🔗 H3 URL test failed:', err)
            })
          }
          
          if (validatedUrls.kommune) {
            console.log('🔗 Testing Kommune URL accessibility...')
            pmtilesDiscovery.testUrl(validatedUrls.kommune).then(isAccessible => {
              console.log('🔗 Kommune URL accessible:', isAccessible)
            }).catch(err => {
              console.error('🔗 Kommune URL test failed:', err)
            })
          }
          
          if (!validatedUrls.basemap) {
            console.error('❌ No basemap URL available')
            setError('Basemap not available')
          } else {
            clearError()
          }
        }
      } catch (error) {
        console.error('❌ Error loading PMTiles URLs:', error)
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
  }, [selectedYear, currentH3Resolution, zoom, setError, setIsLoading, clearError, setPmtilesUrls])

  // Initialize map
  useEffect(() => {
    console.log('🔍 Map initialization check:', {
      mapLibre: !!mapLibre,
      mapContainer: !!mapContainer.current,
      basemapUrl: pmtilesUrls.basemap,
      pmtilesUrls: pmtilesUrls
    })
    
    if (!mapLibre || !mapContainer.current || !pmtilesUrls.basemap) {
      console.log('⏳ Map initialization skipped - missing dependencies:', {
        mapLibre: !!mapLibre,
        mapContainer: !!mapContainer.current,
        basemapUrl: !!pmtilesUrls.basemap,
        allUrls: Object.keys(pmtilesUrls)
      })
      return
    }

    try {
      console.log('🚀 Creating map with URLs:', pmtilesUrls)
      
      // Validate URLs before creating map
      const requiredSources = ['basemap']
      const optionalSources = ['kommune', 'h3', 'bnbo']
      
      console.log('🔍 Validating required sources:', requiredSources.map(src => ({
        source: src,
        hasUrl: !!pmtilesUrls[src as keyof typeof pmtilesUrls],
        url: pmtilesUrls[src as keyof typeof pmtilesUrls]
      })))
      
      console.log('🔍 Validating optional sources:', optionalSources.map(src => ({
        source: src,
        hasUrl: !!pmtilesUrls[src as keyof typeof pmtilesUrls],
        url: pmtilesUrls[src as keyof typeof pmtilesUrls]
      })))
      
      // Create sources object with only available URLs
      const sources: Record<string, { type: string; url: string }> = {}
      
      if (pmtilesUrls.basemap) {
        sources.basemap = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.basemap}`,
        }
        console.log('✅ Added basemap source')
      }
      
      if (pmtilesUrls.kommune) {
        sources.kommune = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.kommune}`,
        }
        console.log('✅ Added kommune source')
      }
      
      if (pmtilesUrls.h3) {
        sources.h3 = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.h3}`,
        }
        console.log('✅ Added h3 source')
      }
      
      if (pmtilesUrls.bnbo) {
        sources.bnbo = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.bnbo}`,
        }
        console.log('✅ Added bnbo source:', pmtilesUrls.bnbo)
      } else {
        console.log('❌ No BNBO URL found in pmtilesUrls:', pmtilesUrls)
      }
      
      console.log('🗺️ Final sources configuration:', sources)
      
      // Create layers array with only layers for available sources
      // Layer order matters: layers added later appear on top
      // Order: basemap (bottom) -> kommune -> h3 -> bnbo (top)
      const layers: Array<Record<string, string | number | boolean | object>> = []
      
      // Always add basemap layer if available (bottom layer)
      if (sources.basemap) {
        layers.push(
          {
            id: 'basemap-fill',
            type: 'fill',
            source: 'basemap',
            'source-layer': 'earth',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'fill-color': '#1a1a1a',
              'fill-opacity': 1
            }
          },
          {
            id: 'basemap-water',
            type: 'fill',
            source: 'basemap',
            'source-layer': 'water',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'fill-color': '#0f172a',
              'fill-opacity': 1
            }
          },
          {
            id: 'basemap-landuse',
            type: 'fill',
            source: 'basemap',
            'source-layer': 'landuse',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'fill-color': [
                'match',
                ['get', 'kind'],
                'park', '#1e3a2e',
                'forest', '#1a2f1a',
                'residential', '#2a2a2a',
                'commercial', '#2d2d2d',
                'industrial', '#262626',
                'farmland', '#1e2a1e',
                '#222222'
              ],
              'fill-opacity': 0.6
            }
          },
          {
            id: 'basemap-buildings',
            type: 'fill',
            source: 'basemap',
            'source-layer': 'buildings',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'fill-color': '#404040',
              'fill-opacity': 0.8
            }
          },
          {
            id: 'basemap-buildings-stroke',
            type: 'line',
            source: 'basemap',
            'source-layer': 'buildings',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#505050',
              'line-width': 0.5,
              'line-opacity': 0.5
            }
          },
          {
            id: 'basemap-roads-minor',
            type: 'line',
            source: 'basemap',
            'source-layer': 'roads',
            filter: ['in', ['get', 'kind'], ['literal', ['minor_road', 'path']]],
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#3a3a3a',
              'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                8, 0.5,
                12, 1,
                16, 2
              ],
              'line-opacity': 0.6
            }
          },
          {
            id: 'basemap-roads-major',
            type: 'line',
            source: 'basemap',
            'source-layer': 'roads',
            filter: ['in', ['get', 'kind'], ['literal', ['highway', 'major_road']]],
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#4a4a4a',
              'line-width': [
                'interpolate',
                ['linear'],
                ['zoom'],
                6, 1,
                10, 2,
                14, 4,
                16, 6
              ],
              'line-opacity': 0.8
            }
          }
        )
        console.log('✅ Added basemap layers with buildings and roads')
      }
      
      // Add kommune layers if available
      if (sources.kommune) {
        layers.push(
          {
            id: 'kommune-fill',
            type: 'fill',
            source: 'kommune',
            'source-layer': `kommune_pfas_${selectedYear}`,
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
          },
          {
            id: 'kommune-stroke',
            type: 'line',
            source: 'kommune',
            'source-layer': `kommune_pfas_${selectedYear}`,
            layout: {
              visibility: shouldShowKommune ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#ffffff',
              'line-width': 0.5,
              'line-opacity': 0.5
            }
          }
        )
        console.log('✅ Added kommune layers')
      }
      
      // Add H3 layers if available
      if (sources.h3) {
        layers.push(
          {
            id: 'h3-fill',
            type: 'fill',
            source: 'h3',
            'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`,
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
          },
          {
            id: 'h3-stroke',
            type: 'line',
            source: 'h3',
            'source-layer': `h3_pfas_${selectedYear}_res${currentH3Resolution}`,
            layout: {
              visibility: shouldShowH3 ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#ffffff',
              'line-width': 0.2,
              'line-opacity': 0
            }
          }
        )
        console.log('✅ Added H3 layers')
      }
      
      // Add BNBO layers if available - ALWAYS VISIBLE
      if (sources.bnbo) {
        layers.push(
          {
            id: 'bnbo-fill',
            type: 'fill',
            source: 'bnbo',
            'source-layer': 'bnbo', // Fixed: layer name is 'bnbo', not 'bnbo_areas'
            layout: {
              visibility: 'visible'
            },
            paint: {
              'fill-color': [
                'case',
                ['has', 'status_category'], [
                  'match',
                  ['get', 'status_category'],
                  'Action Required', '#ff6b6b',
                  'Completed', '#51cf66',
                  'Unknown', '#868e96',
                  '#cccccc'
                ],
                ['has', 'status'], [
                  'match',
                  ['get', 'status'],
                  'Action Required', '#ff6b6b',
                  'Completed', '#51cf66',
                  'Unknown', '#868e96',
                  '#cccccc'
                ],
                '#ff00ff' // Bright magenta fallback to make any BNBO areas visible
              ],
              'fill-opacity': 0.8 // Increased opacity to make them more visible
            }
          },
          {
            id: 'bnbo-stroke',
            type: 'line',
            source: 'bnbo',
            'source-layer': 'bnbo', // Fixed: layer name is 'bnbo', not 'bnbo_areas'
            layout: {
              visibility: 'visible'
            },
            paint: {
              'line-color': '#ffffff',
              'line-width': 1,
              'line-opacity': 0
            }
          }
        )
        console.log('✅ Added BNBO layers (always visible and on top)')
        console.log('🔍 BNBO source configuration:', sources.bnbo)
      } else {
        console.log('❌ No BNBO source available for layers')
      }
      
      console.log('🗺️ Final layers configuration:', layers.map(l => ({ id: l.id, source: l.source })))
      
      map.current = new (mapLibre as unknown as { Map: unknown }).Map({
        container: mapContainer.current,
        style: {
          version: 8,
          sources: sources,
          layers: layers,
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
      }) as MapInstance

      // Add controls
      (map.current as unknown as Record<string, unknown>).addControl(new (mapLibre as unknown as Record<string, unknown>).NavigationControl(), 'top-right')
      ;(map.current as unknown as Record<string, unknown>).addControl(new (mapLibre as unknown as Record<string, unknown>).ScaleControl(), 'bottom-left')
      
      // Set map instance in store for external control
      setMapInstance(map.current as unknown)

      // Map event handlers
      ;(map.current as unknown as Record<string, unknown>).on('load', () => {
        console.log('🎉 Map loaded successfully!')
        
        // Debug: Check available sources and layers
        const style = (map.current as unknown as Record<string, unknown>).getStyle()
        console.log('📊 Map style sources:', Object.keys(style.sources))
        console.log('📊 Map style layers:', style.layers.map((l: Record<string, unknown>) => ({ id: l.id, type: l.type, source: l.source, 'source-layer': l['source-layer'] })))
        
        // Debug: Try to get source data and inspect tiles
        setTimeout(() => {
          if (!map.current) return
          
          console.log(`📊 Current zoom: ${(map.current as unknown as Record<string, unknown>).getZoom()}, center:`, (map.current as unknown as Record<string, unknown>).getCenter())
          
          // Try to inspect each source more thoroughly
          const sources = ['basemap', 'kommune', 'h3', 'bnbo']
          sources.forEach(sourceId => {
            if (!map.current) return
            const source = (map.current as unknown as Record<string, unknown>).getSource(sourceId)
            if (source) {
              console.log(`📊 Source ${sourceId}:`, source)
              
              // Check if source has loaded tiles
              if (source._tiles) {
                console.log(`📊 ${sourceId} has ${Object.keys(source._tiles).length} loaded tiles`)
              }
              
              // Try to query all features from this source
              try {
                if (!map.current) return
                const allFeatures = (map.current as unknown as Record<string, unknown>).querySourceFeatures(sourceId)
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
            const allRenderedFeatures = (map.current as unknown as Record<string, unknown>).queryRenderedFeatures()
            console.log(`📊 Total rendered features in view: ${allRenderedFeatures.length}`)
            
            if (allRenderedFeatures.length > 0) {
              const sourceLayerCounts = allRenderedFeatures.reduce((acc: Record<string, unknown>, f: Record<string, unknown>) => {
                const key = `${f.source}:${f.sourceLayer}`
                acc[key] = (acc[key] || 0) + 1
                return acc
              }, {})
              console.log(`📊 Rendered features by source:layer:`, sourceLayerCounts)
              
              // Check specifically for BNBO features
              const bnboFeatures = allRenderedFeatures.filter((f: Record<string, unknown>) => f.source === 'bnbo')
              if (bnboFeatures.length > 0) {
                console.log(`🛡️ Found ${bnboFeatures.length} BNBO features:`, bnboFeatures.slice(0, 3))
              } else {
                console.log(`🛡️ No BNBO features found in rendered features`)
              }
            }
          } catch (e) {
            console.log(`📊 Could not query rendered features:`, e)
          }
        }, 3000)
        
        setMapLoaded(true)
        clearError()
      })

      ;(map.current as any).on('move', () => {
        if (!map.current) return
        const { lng, lat } = (map.current as any).getCenter()
        const zoom = (map.current as any).getZoom()
        const bearing = (map.current as any).getBearing()
        const pitch = (map.current as any).getPitch()
        
        setViewState({ 
          center: [lng, lat], 
          zoom, 
          bearing, 
          pitch 
        })
      })

      ;(map.current as any).on('click', (e: any) => {
        // Get available layers before querying
        const availableLayers = getAvailableInteractiveLayers()
        
        if (availableLayers.length === 0) {
          // No interactive layers available, just hide tooltip
          hideTooltip()
          if (isMobile) {
            setShowMobilePanel(false)
          }
          return
        }
        
        try {
          const features = (map.current as any)?.queryRenderedFeatures(e.point, {
            layers: availableLayers
          })
          if (features && features.length > 0) {
            const feature = features[0]
            showTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
            
            // On mobile, also show the mobile panel
            if (isMobile) {
              setShowMobilePanel(true)
            }
          } else {
            hideTooltip()
            
            // On mobile, hide the mobile panel when clicking empty space
            if (isMobile) {
              setShowMobilePanel(false)
            }
          }
        } catch (error) {
          console.warn('Error querying rendered features on click:', error)
          hideTooltip()
          if (isMobile) {
            setShowMobilePanel(false)
          }
        }
      })

      ;(map.current as any).on('mousemove', (e: any) => {
        // Skip hover behavior on mobile devices
        if (isMobile) {
          return
        }
        
        // Get available layers before querying
        const availableLayers = getAvailableInteractiveLayers()
        
        if (availableLayers.length === 0) {
          // No interactive layers available, just hide tooltip
          hideTooltip()
          if (map.current) {
            (map.current as any).getCanvas().style.cursor = ''
          }
          return
        }
        
        try {
          const features = (map.current as any)?.queryRenderedFeatures(e.point, {
            layers: availableLayers
          })
          if (features && features.length > 0) {
            const feature = features[0]
            showTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
            if (map.current) {
              (map.current as any).getCanvas().style.cursor = 'pointer'
            }
          } else {
            hideTooltip()
            if (map.current) {
              (map.current as any).getCanvas().style.cursor = ''
            }
          }
        } catch (error) {
          console.warn('Error querying rendered features on mousemove:', error)
          hideTooltip()
          if (map.current) {
            (map.current as any).getCanvas().style.cursor = ''
          }
        }
      })

      ;(map.current as any).on('mouseleave', () => {
        // Skip hover behavior on mobile devices
        if (isMobile) {
          return
        }
        
        hideTooltip()
        if (map.current) {
          (map.current as any).getCanvas().style.cursor = ''
        }
      })

      ;(map.current as any).on('error', (e: any) => {
        console.error('❌ Map error:', e)
        console.error('❌ Map error details:', {
          type: e.type,
          error: e.error,
          sourceId: e.sourceId,
          tile: e.tile,
          target: e.target,
          originalTarget: e.originalTarget,
          message: e.message,
          stack: e.stack
        })
        
        // Safely log error properties without circular references
        const safeErrorProps = {
          type: e.type,
          message: e.message,
          sourceId: e.sourceId,
          errorMessage: e.error?.message,
          errorStack: e.error?.stack,
          url: e.url
        }
        console.error('❌ Safe error object:', safeErrorProps)
        
        // Try to get more specific error information
        let errorMessage = 'Unknown map error'
        if (e.error && e.error.message) {
          errorMessage = e.error.message
        } else if (e.message) {
          errorMessage = e.message
        } else if (e.sourceId) {
          errorMessage = `Source error: ${e.sourceId}`
        }
        
        // Check if this is a layer query error
        if (errorMessage.includes('kommune-fill') || errorMessage.includes('does not exist')) {
          console.error('❌ Layer query error detected - checking available layers')
          if (map.current) {
            try {
              const style = (map.current as any).getStyle()
              const availableLayers = style.layers.map((l: any) => l.id)
              console.error('❌ Available layers:', availableLayers)
              console.error('❌ Available sources:', Object.keys(style.sources))
              
              // Check if kommune source exists
              if (style.sources.kommune) {
                console.error('❌ Kommune source exists:', style.sources.kommune)
              } else {
                console.error('❌ Kommune source NOT found in style')
              }
            } catch (styleError) {
              console.error('❌ Could not check map style:', styleError)
            }
          }
        }
        
        setError(`Map loading error: ${errorMessage}`)
      })
      
      // Add more specific error handlers
      ;(map.current as any).on('sourceerror', (e: any) => {
        console.error('❌ Source error:', e)
        console.error('❌ Source error details:', {
          sourceId: e.sourceId,
          error: e.error,
          url: e.url,
          message: e.message
        })
        setError(`Source loading error: ${e.sourceId}`)
      })
      
      ;(map.current as any).on('styleerror', (e: any) => {
        console.error('❌ Style error:', e)
        console.error('❌ Style error details:', {
          error: e.error,
          message: e.message
        })
        setError(`Style error: ${e.error?.message || 'Unknown style error'}`)
      })
      
      ;(map.current as any).on('sourcedata', (e: any) => {
        if (e.isSourceLoaded) {
          console.log(`📊 Source loaded: ${e.sourceId}`, e)
        } else if (e.dataType === 'source') {
          console.log(`📊 Source data loading: ${e.sourceId}`, e)
        }
      })
      
      ;(map.current as any).on('sourcedataloading', (e: any) => {
        console.log(`📊 Source loading: ${e.sourceId}`, e)
      })
      
      // Add data event handler to track tile loading
      ;(map.current as any).on('data', (e: any) => {
        if (e.dataType === 'source') {
          console.log(`📊 Data event for source: ${e.sourceId}`, {
            dataType: e.dataType,
            isSourceLoaded: e.isSourceLoaded,
            sourceDataType: e.sourceDataType
          })
        }
      })
      
      // Add tile events to track individual tile loading
      ;(map.current as any).on('dataloading', (e: any) => {
        if (e.dataType === 'source') {
          console.log(`📊 Data loading for source: ${e.sourceId}`)
        }
      })

    } catch (err) {
      console.error('❌ Error initializing map:', err)
      setError('Failed to initialize map')
    }

    return () => {
      if (map.current) {
        (map.current as any).remove()
        map.current = null
      }
      // Clear map instance from store
      setMapInstance(null)
    }
  }, [mapLibre, pmtilesUrls, bearing, center, clearError, currentH3Resolution, currentPropertyName, hideTooltip, isMobile, pitch, selectedYear, setError, setMapInstance, setShowMobilePanel, setViewState, shouldShowH3, shouldShowKommune, showBasemap, showTooltipWithData, zoom])

  // Update layer visibility based on zoom
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      const layerVisibility = getComputedLayerVisibility(zoom)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && (map.current as any).getLayer(layerId)) {
          (map.current as any).setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
          console.log(`✅ Updated ${layerId} visibility to ${visible ? 'visible' : 'none'}`)
        } else {
          console.log(`⚠️ Layer ${layerId} not found, skipping visibility update`)
        }
      }
      
      // Update Kommune layer visibility
      updateLayerVisibility('kommune-fill', layerVisibility.shouldShowKommune)
      updateLayerVisibility('kommune-stroke', layerVisibility.shouldShowKommune)
      
      // Update H3 layer visibility
      updateLayerVisibility('h3-fill', layerVisibility.shouldShowH3)
      updateLayerVisibility('h3-stroke', layerVisibility.shouldShowH3)
      
      // BNBO layers are always visible - no need to update visibility
      // updateLayerVisibility('bnbo-fill', true) // Always visible
      // updateLayerVisibility('bnbo-stroke', true) // Always visible
    } catch (error) {
      console.warn('Error updating layer visibility:', error)
    }
  }, [zoom, mapLoaded])

  // Update basemap visibility when showBasemap changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🗺️ Updating basemap visibility:', showBasemap)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && map.current.getLayer(layerId)) {
          map.current.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
          console.log(`✅ Updated ${layerId} visibility to ${visible ? 'visible' : 'none'}`)
        } else {
          console.log(`⚠️ Layer ${layerId} not found, skipping visibility update`)
        }
      }
      
      // Update all basemap layers
      const basemapLayers = [
        'basemap-fill',
        'basemap-water', 
        'basemap-landuse',
        'basemap-buildings',
        'basemap-buildings-stroke',
        'basemap-roads-minor',
        'basemap-roads-major'
      ]
      
      basemapLayers.forEach(layerId => {
        updateLayerVisibility(layerId, showBasemap)
      })
      
    } catch (error) {
      console.warn('Error updating basemap visibility:', error)
    }
  }, [showBasemap, mapLoaded])

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
      } else {
        console.log('⚠️ kommune-fill layer not found, skipping paint update')
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
      } else {
        console.log('⚠️ h3-fill layer not found, skipping paint update')
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
          console.log(`🗑️ Removed layer: ${layerId}`)
        }
      })
      
      // Re-add Kommune layers with correct source-layer name (only if source exists)
      // Add before BNBO layers to ensure BNBO stays on top
      if (map.current.getSource('kommune')) {
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
        }, 'bnbo-fill') // Add before BNBO fill layer
        
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
        }, 'bnbo-fill') // Add before BNBO fill layer
        
        console.log('✅ Re-added kommune layers with source-layer:', kommuneSourceLayer)
      } else {
        console.log('⚠️ Kommune source not found, skipping layer re-addition')
      }
      
      // Re-add H3 layers with correct source-layer name (only if source exists)
      // Add before BNBO layers to ensure BNBO stays on top
      if (map.current.getSource('h3')) {
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
        }, 'bnbo-fill') // Add before BNBO fill layer
        
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
            'line-opacity': 0
          }
        }, 'bnbo-fill') // Add before BNBO fill layer
        
        console.log('✅ Re-added H3 layers with source-layer:', h3SourceLayer)
      } else {
        console.log('⚠️ H3 source not found, skipping layer re-addition')
      }
      
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
      

      
      {/* Tooltip - Disabled in favor of sidebar */}
      {/* <MapTooltip /> */}
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