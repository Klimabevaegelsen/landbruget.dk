/* eslint-disable @typescript-eslint/no-explicit-any */
'use client'

import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { ErrorBoundary } from 'react-error-boundary'
import { useMapStore, useMapViewState, useDataState, useLoadingState, getComputedLayerVisibility } from '@/stores/map-store'
import { useUIStore } from '@/stores/ui-store'
import { pmtilesDiscovery } from '@/services/pmtiles-discovery'

// Type definitions
type MapInstance = unknown;
type MapLibreGL = unknown;

// Performance monitoring hook for development
const usePerformanceMonitor = () => {
  const [metrics, setMetrics] = useState({
    frameRate: 0,
    eventCount: 0,
    lastUpdate: Date.now()
  });

  const updateMetrics = useCallback(() => {
    setMetrics(prev => ({
      frameRate: Math.round(1000 / (Date.now() - prev.lastUpdate)),
      eventCount: prev.eventCount + 1,
      lastUpdate: Date.now()
    }));
  }, []);

  return { metrics, updateMetrics };
};

// Throttle utility for performance optimization
const throttle = (func: (...args: any[]) => void, delay: number) => {
  let timeoutId: NodeJS.Timeout | null = null;
  let lastExecTime = 0;
  
  return (...args: any[]) => {
    const currentTime = Date.now();
    
    if (currentTime - lastExecTime > delay) {
      func(...args);
      lastExecTime = currentTime;
    } else {
      if (timeoutId) clearTimeout(timeoutId);
      timeoutId = setTimeout(() => {
        func(...args);
        lastExecTime = Date.now();
      }, delay - (currentTime - lastExecTime));
    }
  };
};

// Debounce utility for less frequent updates
const debounce = (func: (...args: any[]) => void, delay: number) => {
  let timeoutId: NodeJS.Timeout | null = null;
  
  return (...args: any[]) => {
    if (timeoutId) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => func(...args), delay);
  };
};

// Dynamic imports for browser-only modules - Next.js 15 compatible
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
  
  // Performance monitoring (development only)
  const { metrics, updateMetrics } = usePerformanceMonitor();
  
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

  // Memoize store functions to prevent unnecessary re-renders
  const memoizedSetError = useCallback(setError, [setError])
  const memoizedClearError = useCallback(clearError, [clearError])
  const memoizedSetMapInstance = useCallback(setMapInstance, [setMapInstance])
  const memoizedShowTooltipWithData = useCallback(showTooltipWithData, [showTooltipWithData])
  const memoizedHideTooltip = useCallback(hideTooltip, [hideTooltip])
  const memoizedSetShowMobilePanel = useCallback(setShowMobilePanel, [setShowMobilePanel])

  // Helper function to safely get available interactive layers
  const getAvailableInteractiveLayers = useCallback((): string[] => {
    if (!map.current) return []
    
    const layers = []
    try {
      if ((map.current as any).getLayer('kommune-fill')) {
        layers.push('kommune-fill')
      }
    } catch (e) {
      // Layer doesn't exist, ignore
    }
    
    // Check for the current H3 resolution layer
    try {
      const h3LayerId = `h3-fill-res${currentH3Resolution}`
      if ((map.current as any).getLayer(h3LayerId)) {
        layers.push(h3LayerId)
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
  }, [currentH3Resolution])

  // Optimized event handlers with throttling and debouncing
  const throttledSetViewState = useMemo(
    () => throttle((viewState: any) => {
      setViewState(viewState);
      updateMetrics();
    }, 16), // ~60fps
    [setViewState, updateMetrics]
  );

  const debouncedHideTooltip = useMemo(
    () => debounce(() => hideTooltip(), 100),
    [hideTooltip]
  );

  const throttledMouseMove = useMemo(
    () => throttle((e: any) => {
      // Skip hover behavior on mobile devices
      if (isMobile) {
        return
      }
      
      // Get available layers before querying
      const availableLayers = getAvailableInteractiveLayers()
      
      if (availableLayers.length === 0) {
        // No interactive layers available, just hide tooltip
        debouncedHideTooltip()
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
          debouncedHideTooltip()
          if (map.current) {
            (map.current as any).getCanvas().style.cursor = ''
          }
        }
      } catch (error) {
        console.warn('Error querying rendered features on mousemove:', error)
        debouncedHideTooltip()
        if (map.current) {
          (map.current as any).getCanvas().style.cursor = ''
        }
      }
    }, 50), // Throttle to 20fps for mouse events
    [isMobile, getAvailableInteractiveLayers, debouncedHideTooltip, showTooltipWithData]
  );

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
  }, [setError, setIsLoading])

  // Load PMTiles URLs
  useEffect(() => {
    let mounted = true
    
    const loadPMTilesUrls = async () => {
      try {
        console.log('🔄 Loading PMTiles URLs...')
        const yearUrls = await pmtilesDiscovery.getYearUrls(selectedYear)
        
        // Convert to the expected format
        const urls: Record<string, string> = {
          basemap: yearUrls.basemap,
          bnbo: yearUrls.bnbo
        }
        
        // Add H3 URL for current resolution
        const h3Key = `${selectedYear}_${currentH3Resolution}`
        if (yearUrls.h3[h3Key]) {
          urls.h3 = yearUrls.h3[h3Key]
        }
        
        // Add kommune URL
        if (yearUrls.kommune[selectedYear.toString()]) {
          urls.kommune = yearUrls.kommune[selectedYear.toString()]
        }
        
        console.log('✅ PMTiles URLs loaded:', urls)
        
        if (mounted) {
          setPmtilesUrls(urls)
        }
      } catch (error) {
        console.error('❌ Error loading PMTiles URLs:', error)
        if (mounted) {
          setError('Failed to load data sources')
        }
      }
    }
    
    loadPMTilesUrls()
    
    return () => {
      mounted = false
    }
  }, [setError, selectedYear, currentH3Resolution])

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
              'line-color': '#606060',
              'line-width': 0.5
            }
          },
          {
            id: 'basemap-roads-minor',
            type: 'line',
            source: 'basemap',
            'source-layer': 'roads',
            filter: ['!=', ['get', 'kind'], 'highway'],
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#333333',
              'line-width': 1
            }
          },
          {
            id: 'basemap-roads-major',
            type: 'line',
            source: 'basemap',
            'source-layer': 'roads',
            filter: ['==', ['get', 'kind'], 'highway'],
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#444444',
              'line-width': 2
            }
          }
        )
      }
      
      // Add kommune layer if available (middle layer)
      if (sources.kommune) {
        layers.push({
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
              0, 'rgba(255,255,255,0.1)',
              1, 'rgba(255,100,100,0.3)',
              10, 'rgba(255,50,50,0.5)',
              50, 'rgba(255,0,0,0.7)',
              100, 'rgba(150,0,0,0.8)'
            ],
            'fill-opacity': 0.7
          }
        })
        
        layers.push({
          id: 'kommune-stroke',
          type: 'line',
          source: 'kommune',
          'source-layer': `kommune_pfas_${selectedYear}`,
          layout: {
            visibility: shouldShowKommune ? 'visible' : 'none'
          },
          paint: {
            'line-color': 'rgba(255,255,255,0.5)',
            'line-width': 1
          }
        })
      }
      
      // Add H3 layers if available (middle layer)
      if (sources.h3) {
        // Add layers for each H3 resolution
        for (let res = 7; res <= 10; res++) {
          const layerVisibility = res === currentH3Resolution && shouldShowH3 ? 'visible' : 'none'
          
          layers.push({
            id: `h3-fill-res${res}`,
            type: 'fill',
            source: 'h3',
            'source-layer': `h3_pfas_${selectedYear}_res${res}`,
            layout: {
              visibility: layerVisibility
            },
            paint: {
              'fill-color': [
                'interpolate',
                ['linear'],
                ['get', currentPropertyName],
                0, 'rgba(255,255,255,0.1)',
                1, 'rgba(255,100,100,0.3)',
                10, 'rgba(255,50,50,0.5)',
                50, 'rgba(255,0,0,0.7)',
                100, 'rgba(150,0,0,0.8)'
              ],
              'fill-opacity': 0.7
            }
          })
          
          layers.push({
            id: `h3-stroke-res${res}`,
            type: 'line',
            source: 'h3',
            'source-layer': `h3_pfas_${selectedYear}_res${res}`,
            layout: {
              visibility: layerVisibility
            },
            paint: {
              'line-color': 'rgba(255,255,255,0.3)',
              'line-width': 0.5
            }
          })
        }
      }
      
      // Add BNBO layer if available (top layer)
      if (sources.bnbo) {
        layers.push({
          id: 'bnbo-fill',
          type: 'fill',
          source: 'bnbo',
          'source-layer': 'default',
          layout: {
            visibility: 'visible'
          },
          paint: {
            'fill-color': [
              'match',
              ['get', 'status'],
              'Action Required', '#ff6b6b',
              'Completed', '#51cf66',
              'Unknown', '#868e96',
              '#868e96'
            ],
            'fill-opacity': 0.3
          }
        })
        
        layers.push({
          id: 'bnbo-stroke',
          type: 'line',
          source: 'bnbo',
          'source-layer': 'default',
          layout: {
            visibility: 'visible'
          },
          paint: {
            'line-color': [
              'match',
              ['get', 'status'],
              'Action Required', '#ff6b6b',
              'Completed', '#51cf66',
              'Unknown', '#868e96',
              '#868e96'
            ],
            'line-width': 1.5
          }
        })
      }
      
      console.log('🗺️ Final layers configuration:', layers.map(l => ({ id: l.id, source: l.source })))
      
      map.current = new (mapLibre as any).Map({
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
        // Optimize for smooth interactions
        scrollZoom: {
          around: 'center'
        },
        doubleClickZoom: true,
        touchZoomRotate: true,
        dragPan: true,
        dragRotate: false,
        keyboard: true,
        // Enable smooth transitions for better UX
        fadeDuration: 300,
        // Performance optimizations
        antialias: true,
        optimizeForTerrain: false,
        renderWorldCopies: false,
        // Interaction options for better performance
        interactive: true,
        trackResize: true,
        cooperativeGestures: false
      }) as MapInstance

      // Add controls with custom options for smoother zoom
      (map.current as any).addControl(
        new (mapLibre as any).NavigationControl({
          showCompass: true,
          showZoom: true,
          visualizePitch: false
        }), 
        'top-right'
      )
      ;(map.current as any).addControl(new (mapLibre as any).ScaleControl(), 'bottom-left')
      
      // Set map instance in store for external control
      memoizedSetMapInstance(map.current as any)

      // Map event handlers with performance optimizations
      ;(map.current as any).on('load', () => {
        console.log('🎉 Map loaded successfully!')
        
        // Debug: Check available sources and layers
        const style = (map.current as any).getStyle()
        console.log('📊 Map style sources:', Object.keys(style.sources))
        console.log('📊 Map style layers:', style.layers.map((l: any) => ({ id: l.id, type: l.type, source: l.source, 'source-layer': l['source-layer'] })))
        
        setMapLoaded(true)
        memoizedClearError()
      })
      
      // Debug drag events
      ;(map.current as any).on('dragstart', (e: any) => {
        console.log('🖱️ Drag started:', e)
      })
      
      ;(map.current as any).on('drag', (e: any) => {
        console.log('🖱️ Dragging:', e)
      })
      
      ;(map.current as any).on('dragend', (e: any) => {
        console.log('🖱️ Drag ended:', e)
      })

      // Use throttled move handler for better performance
      ;(map.current as any).on('move', () => {
        if (!map.current) return
        const { lng, lat } = (map.current as any).getCenter()
        const zoom = (map.current as any).getZoom()
        const bearing = (map.current as any).getBearing()
        const pitch = (map.current as any).getPitch()
        
        throttledSetViewState({ 
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
          memoizedHideTooltip()
          if (isMobile) {
            memoizedSetShowMobilePanel(false)
          }
          return
        }
        
        try {
          const features = (map.current as any)?.queryRenderedFeatures(e.point, {
            layers: availableLayers
          })
          if (features && features.length > 0) {
            const feature = features[0]
            memoizedShowTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
            
            // On mobile, also show the mobile panel
            if (isMobile) {
              memoizedSetShowMobilePanel(true)
            }
          } else {
            memoizedHideTooltip()
            
            // On mobile, hide the mobile panel when clicking empty space
            if (isMobile) {
              memoizedSetShowMobilePanel(false)
            }
          }
        } catch (error) {
          console.warn('Error querying rendered features on click:', error)
          memoizedHideTooltip()
          if (isMobile) {
            memoizedSetShowMobilePanel(false)
          }
        }
      })

      // Use throttled mousemove handler
      ;(map.current as any).on('mousemove', throttledMouseMove)

      ;(map.current as any).on('mouseleave', () => {
        // Skip hover behavior on mobile devices
        if (isMobile) {
          return
        }
        
        memoizedHideTooltip()
        if (map.current) {
          (map.current as any).getCanvas().style.cursor = ''
        }
      })

      ;(map.current as any).on('error', (e: any) => {
        console.error('❌ Map error:', e)
        memoizedSetError(`Map loading error: ${e.message || 'Unknown error'}`)
      })

    } catch (err) {
      console.error('❌ Error initializing map:', err)
      memoizedSetError('Failed to initialize map')
    }

    return () => {
      if (map.current) {
        (map.current as any).remove()
        map.current = null
      }
      // Clear map instance from store
      memoizedSetMapInstance(null)
    }
  }, [mapLibre, pmtilesUrls, selectedYear, currentH3Resolution, currentPropertyName, showBasemap, shouldShowH3, shouldShowKommune])

  // Update layer visibility when zoom changes (optimized)
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🔄 Updating layer visibility for zoom:', zoom)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && (map.current as any).getLayer(layerId)) {
          (map.current as any).setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
        }
      }
      
      // Update kommune layers
      updateLayerVisibility('kommune-fill', shouldShowKommune)
      updateLayerVisibility('kommune-stroke', shouldShowKommune)
      
      // Update H3 layers - only show current resolution
      for (let res = 7; res <= 10; res++) {
        const shouldShow = res === currentH3Resolution && shouldShowH3
        updateLayerVisibility(`h3-fill-res${res}`, shouldShow)
        updateLayerVisibility(`h3-stroke-res${res}`, shouldShow)
      }
      
    } catch (error) {
      console.warn('Error updating layer visibility:', error)
    }
  }, [zoom, shouldShowKommune, shouldShowH3, currentH3Resolution, mapLoaded])

  // Update basemap visibility when showBasemap changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🗺️ Updating basemap visibility:', showBasemap)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && (map.current as any).getLayer(layerId)) {
          (map.current as any).setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
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

  // Update layer styling when data mode changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🎨 Updating layer styling for data mode:', selectedDataMode)
      
      // Update kommune layer styling
      if ((map.current as any).getLayer('kommune-fill')) {
        (map.current as any).setPaintProperty('kommune-fill', 'fill-color', [
          'interpolate',
          ['linear'],
          ['get', currentPropertyName],
          0, 'rgba(255,255,255,0.1)',
          1, 'rgba(255,100,100,0.3)',
          10, 'rgba(255,50,50,0.5)',
          50, 'rgba(255,0,0,0.7)',
          100, 'rgba(150,0,0,0.8)'
        ])
      }
      
      // Update H3 layers styling
      for (let res = 7; res <= 10; res++) {
        const layerId = `h3-fill-res${res}`
        if ((map.current as any).getLayer(layerId)) {
          (map.current as any).setPaintProperty(layerId, 'fill-color', [
            'interpolate',
            ['linear'],
            ['get', currentPropertyName],
            0, 'rgba(255,255,255,0.1)',
            1, 'rgba(255,100,100,0.3)',
            10, 'rgba(255,50,50,0.5)',
            50, 'rgba(255,0,0,0.7)',
            100, 'rgba(150,0,0,0.8)'
          ])
        }
      }
      
    } catch (error) {
      console.warn('Error updating layer styling:', error)
    }
  }, [selectedDataMode, currentPropertyName, mapLoaded])
  
  return (
    <div className={`relative ${className}`}>
      <div 
        ref={mapContainer} 
        className="w-full h-full"
        style={{
          position: 'relative',
          cursor: 'grab',
          touchAction: 'none',
          pointerEvents: 'auto'
        }}
      />
      
      {/* Performance monitor (development only) */}
      {process.env.NODE_ENV === 'development' && (
        <div className="absolute top-4 left-4 bg-black/80 text-white text-xs p-2 rounded font-mono pointer-events-none">
          <div>FPS: {metrics.frameRate}</div>
          <div>Events: {metrics.eventCount}</div>
        </div>
      )}
      
      {/* Loading overlay */}
      {!mapLoaded && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-900/80 pointer-events-none">
          <div className="text-center">
            <div className="w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-2"></div>
            <p className="text-white text-sm">Initializing map...</p>
          </div>
        </div>
      )}
      
      {/* Error overlay */}
      {error && (
        <div className="absolute inset-0 flex items-center justify-center bg-red-900/80 pointer-events-none">
          <div className="text-center text-white p-4">
            <div className="text-4xl mb-2">⚠️</div>
            <p className="text-lg font-semibold mb-2">Map Error</p>
            <p className="text-sm">{error}</p>
          </div>
        </div>
      )}
      
      {/* Tooltip - Disabled in favor of sidebar */}
      {/* <MapTooltip /> */}
    </div>
  )
}

const MapErrorFallback: React.FC<{ error: Error; resetErrorBoundary: () => void }> = ({ 
  error, 
  resetErrorBoundary 
}) => (
  <div className="h-full w-full bg-red-50 flex items-center justify-center">
    <div className="text-center p-8">
      <div className="text-6xl mb-4">⚠️</div>
      <h2 className="text-2xl font-bold text-red-800 mb-4">Map Component Error</h2>
      <p className="text-red-600 mb-4 max-w-md">
        {error.message || 'An unexpected error occurred while loading the map.'}
      </p>
      <button 
        onClick={resetErrorBoundary}
        className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
      >
        Try Again
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