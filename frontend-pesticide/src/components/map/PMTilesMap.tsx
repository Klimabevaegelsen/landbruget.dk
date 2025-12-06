/* eslint-disable @typescript-eslint/no-explicit-any */
'use client'

import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import 'maplibre-gl/dist/maplibre-gl.css'
import { ErrorBoundary } from 'react-error-boundary'
import { useMapStore, useMapViewState, useDataState, useLoadingState, getComputedLayerVisibility, useSelectedCellState } from '@/stores/map-store'
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
  const { selectedCell, setSelectedCell, clearSelectedCell } = useSelectedCellState()
  
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
  const memoizedSetSelectedCell = useCallback(setSelectedCell, [setSelectedCell])
  const memoizedClearSelectedCell = useCallback(clearSelectedCell, [clearSelectedCell])

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

  // Mobile-optimized mouse move handler
  const throttledMouseMove = useMemo(
    () => throttle((e: any) => {
      // Skip hover behavior on mobile devices - mobile uses tap-to-show
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

  // Mobile-optimized touch handlers
  const handleTouchStart = useCallback((e: TouchEvent) => {
    if (!isMobile) return;
    
    // Store touch start time for tap detection
    const touchStartTime = Date.now();
    (e.target as any)._touchStartTime = touchStartTime;
  }, [isMobile]);

  const handleTouchEnd = useCallback((e: TouchEvent) => {
    if (!isMobile) return;
    
    const touchEndTime = Date.now();
    const touchStartTime = (e.target as any)._touchStartTime || 0;
    const touchDuration = touchEndTime - touchStartTime;
    
    // Only treat as tap if touch duration is short (< 300ms)
    if (touchDuration < 300 && touchDuration > 0) {
      // Convert touch to map coordinates
      const touch = e.changedTouches[0];
      const rect = (e.target as HTMLElement).getBoundingClientRect();
      const point = {
        x: touch.clientX - rect.left,
        y: touch.clientY - rect.top
      };
      
      // Query features at touch point
      const availableLayers = getAvailableInteractiveLayers();
      
      if (availableLayers.length === 0) {
        memoizedHideTooltip();
        memoizedSetShowMobilePanel(false);
        return;
      }
      
      try {
        const features = (map.current as any)?.queryRenderedFeatures(point, {
          layers: availableLayers
        });
        
        if (features && features.length > 0) {
          const feature = features[0];
          memoizedShowTooltipWithData(feature.properties, point);
          memoizedSetShowMobilePanel(true);
        } else {
          memoizedHideTooltip();
          memoizedSetShowMobilePanel(false);
        }
      } catch (error) {
        console.warn('Error querying rendered features on touch:', error);
        memoizedHideTooltip();
        memoizedSetShowMobilePanel(false);
      }
    }
  }, [isMobile, getAvailableInteractiveLayers, memoizedShowTooltipWithData, memoizedHideTooltip, memoizedSetShowMobilePanel]);

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
        
        // Add H3 URLs for both resolutions
        const h3Key8 = `${selectedYear}_8`
        const h3Key10 = `${selectedYear}_10`
        if (yearUrls.h3[h3Key8]) {
          urls.h3_res8 = yearUrls.h3[h3Key8]
        }
        if (yearUrls.h3[h3Key10]) {
          urls.h3_res10 = yearUrls.h3[h3Key10]
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
  }, [setError, selectedYear]) // Remove currentH3Resolution dependency to prevent recreation

  // Create map when PMTiles URLs are available
  useEffect(() => {
    if (!mapLibre || !mapContainer.current || !pmtilesUrls.basemap) {
      console.log('⏳ Map creation skipped - missing dependencies:', {
        mapLibre: !!mapLibre,
        mapContainer: !!mapContainer.current,
        basemapUrl: !!pmtilesUrls.basemap
      })
      return
    }

    // Clean up existing map
    if (map.current) {
      (map.current as any).remove()
      map.current = null
      setMapLoaded(false)
    }

    try {
      console.log('🔄 Creating map with PMTiles URLs:', pmtilesUrls)
      
      // Create sources object with available URLs
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
      
      if (pmtilesUrls.h3_res8) {
        sources.h3_res8 = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.h3_res8}`,
        }
        console.log('✅ Added h3_res8 source')
      }
      
      if (pmtilesUrls.h3_res10) {
        sources.h3_res10 = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.h3_res10}`,
        }
        console.log('✅ Added h3_res10 source')
      }
      
      if (pmtilesUrls.bnbo) {
        sources.bnbo = {
          type: 'vector',
          url: `pmtiles://${pmtilesUrls.bnbo}`,
        }
        console.log('✅ Added bnbo source')
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
                'industrial', '#2f2f2f',
                'farmland', '#2a2a1a',
                '#1a1a1a'
              ],
              'fill-opacity': 0.8
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
              'fill-color': '#333333',
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
              'line-color': '#444444',
              'line-width': 0.5
            }
          },
          {
            id: 'basemap-roads',
            type: 'line',
            source: 'basemap',
            'source-layer': 'roads',
            layout: {
              visibility: showBasemap ? 'visible' : 'none'
            },
            paint: {
              'line-color': '#555555',
              'line-width': [
                'case',
                ['==', ['get', 'kind'], 'highway'], 2,
                ['==', ['get', 'kind'], 'major_road'], 1.5,
                ['==', ['get', 'kind'], 'minor_road'], 1,
                0.5
              ]
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
            visibility: 'visible' // Will be updated by layer visibility effect
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
              100, 'rgba(139,0,0,0.8)'
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
            visibility: 'visible' // Will be updated by layer visibility effect
          },
          paint: {
            'line-color': '#ffffff',
            'line-width': 1,
            'line-opacity': 0.8
          }
        })
      }

      // Add H3 layers for both resolutions
      if (sources.h3_res8) {
        layers.push({
          id: 'h3-fill-res8',
          type: 'fill',
          source: 'h3_res8',
          'source-layer': `h3_pfas_${selectedYear}_res8`,
          layout: {
            visibility: 'none' // Will be updated by layer visibility effect
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
              100, 'rgba(139,0,0,0.8)'
            ],
            'fill-opacity': 0.6
          }
        })
        
        layers.push({
          id: 'h3-stroke-res8',
          type: 'line',
          source: 'h3_res8',
          'source-layer': `h3_pfas_${selectedYear}_res8`,
          layout: {
            visibility: 'none' // Will be updated by layer visibility effect
          },
          paint: {
            'line-color': '#ffffff',
            'line-width': 0.5,
            'line-opacity': 0.8
          }
        })
      }
      
      if (sources.h3_res10) {
        layers.push({
          id: 'h3-fill-res10',
          type: 'fill',
          source: 'h3_res10',
          'source-layer': `h3_pfas_${selectedYear}_res10`,
          layout: {
            visibility: 'none' // Will be updated by layer visibility effect
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
              100, 'rgba(139,0,0,0.8)'
            ],
            'fill-opacity': 0.6
          }
        })
        
        layers.push({
          id: 'h3-stroke-res10',
          type: 'line',
          source: 'h3_res10',
          'source-layer': `h3_pfas_${selectedYear}_res10`,
          layout: {
            visibility: 'none' // Will be updated by layer visibility effect
          },
          paint: {
            'line-color': '#ffffff',
            'line-width': 0.5,
            'line-opacity': 0.8
          }
        })
      }

      // Add BNBO layer if available (top overlay)
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
      
      // Add highlight layers for selected cells (on top of all other layers)
      if (sources.kommune) {
        layers.push({
          id: 'kommune-highlight',
          type: 'line',
          source: 'kommune',
          'source-layer': `kommune_pfas_${selectedYear}`,
          layout: {
            visibility: 'none' // Initially hidden
          },
          paint: {
            'line-color': '#00ff00', // Bright green highlight
            'line-width': 4,
            'line-opacity': 1
          },
          filter: ['==', ['get', 'kommune_code'], ''] // Initially no features match
        })
      }
      
      if (sources.h3_res8) {
        layers.push({
          id: 'h3-highlight-res8',
          type: 'line',
          source: 'h3_res8',
          'source-layer': `h3_pfas_${selectedYear}_res8`,
          layout: {
            visibility: 'none' // Initially hidden
          },
          paint: {
            'line-color': '#00ff00', // Bright green highlight
            'line-width': 4,
            'line-opacity': 1
          },
          filter: ['==', ['get', 'h3_id'], ''] // Initially no features match
        })
      }
      
      if (sources.h3_res10) {
        layers.push({
          id: 'h3-highlight-res10',
          type: 'line',
          source: 'h3_res10',
          'source-layer': `h3_pfas_${selectedYear}_res10`,
          layout: {
            visibility: 'none' // Initially hidden
          },
          paint: {
            'line-color': '#00ff00', // Bright green highlight
            'line-width': 4,
            'line-opacity': 1
          },
          filter: ['==', ['get', 'h3_id'], ''] // Initially no features match
        })
      }
      
      if (sources.bnbo) {
        layers.push({
          id: 'bnbo-highlight',
          type: 'line',
          source: 'bnbo',
          'source-layer': 'default',
          layout: {
            visibility: 'none' // Initially hidden
          },
          paint: {
            'line-color': '#00ff00', // Bright green highlight
            'line-width': 4,
            'line-opacity': 1
          },
          filter: ['==', ['get', 'id'], ''] // Initially no features match
        })
      }
      
      // Create the map with sources and layers
      map.current = new (mapLibre as any).Map({
        container: mapContainer.current,
        style: {
          version: 8,
          sources: sources,
          layers: layers
        },
        center: [10.5, 56.0], // Default center for Denmark
        zoom: 7, // Default zoom
        bearing: 0, // Default bearing
        pitch: 0, // Default pitch
        maxZoom: 15,
        minZoom: 4,
        maxBounds: [
          [7.0, 54.0], // Southwest bounds (Denmark)
          [13.0, 58.0], // Northeast bounds (Denmark)
        ],
        // Mobile-optimized interaction options
        scrollZoom: {
          around: 'center'
        },
        doubleClickZoom: true,
        touchZoomRotate: true,
        touchPitch: isMobile ? false : true, // Disable pitch on mobile
        dragPan: true,
        dragRotate: false,
        keyboard: !isMobile, // Disable keyboard on mobile
        // Enable smooth transitions for better UX
        fadeDuration: 300,
        // Performance optimizations
        antialias: true,
        optimizeForTerrain: false,
        renderWorldCopies: false,
        // Interaction options for better performance
        interactive: true,
        trackResize: true,
        cooperativeGestures: false // Disable cooperative gestures for better mobile UX
      }) as MapInstance

      // Add controls with mobile-optimized options
      const navigationControl = new (mapLibre as any).NavigationControl({
        showCompass: !isMobile, // Hide compass on mobile
        showZoom: true,
        visualizePitch: false
      });
      
      (map.current as any).addControl(navigationControl, 'top-right');
      
      // Only add scale control on desktop
      if (!isMobile) {
        (map.current as any).addControl(new (mapLibre as any).ScaleControl(), 'bottom-left');
      }
      
      // Set map instance in store for external control
      setMapInstance(map.current as any)

      // Map event handlers with performance optimizations
      ;(map.current as any).on('load', () => {
        console.log('🎉 Map loaded successfully!')
        setMapLoaded(true)
        clearError()
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
        
        // Use throttled function for performance
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
            // No interactive layers available, clear selection and hide tooltip
            clearSelectedCell()
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
            
            // Set selected cell for highlighting
            const cellId = feature.properties.h3_id || feature.properties.kommune_code || feature.properties.id || 'unknown'
            const layerName = feature.layer.id
            
            setSelectedCell({
              id: String(cellId),
              layer: layerName,
              properties: feature.properties
            })
            
            showTooltipWithData(feature.properties, { x: e.point.x, y: e.point.y })
            
            // On mobile, also show the mobile panel
            if (isMobile) {
              setShowMobilePanel(true)
            }
                      } else {
              // Clear selection when clicking empty space
              clearSelectedCell()
              hideTooltip()
              
              // On mobile, hide the mobile panel when clicking empty space
              if (isMobile) {
                setShowMobilePanel(false)
              }
            }
        } catch (error) {
          console.warn('Error querying rendered features on click:', error)
          clearSelectedCell()
          hideTooltip()
          if (isMobile) {
            setShowMobilePanel(false)
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
        
        hideTooltip()
        if (map.current) {
          (map.current as any).getCanvas().style.cursor = ''
        }
      })

      ;(map.current as any).on('error', (e: any) => {
        console.error('❌ Map error:', e)
        setError(`Map loading error: ${e.message || 'Unknown error'}`)
      })
      
      console.log('🗺️ Created layers:', layers.map(l => ({ id: l.id, source: l.source })))
      
    } catch (err) {
      console.error('❌ Error creating map:', err)
      memoizedSetError('Failed to create map')
    }

    return () => {
      if (map.current) {
        (map.current as any).remove()
        map.current = null
      }
      setMapInstance(null)
    }
  }, [mapLibre, pmtilesUrls, selectedYear, currentPropertyName, showBasemap, isMobile])

  // No longer need dynamic H3 source updates - both sources are loaded initially

  // Update layer visibility when zoom changes (optimized)
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🔄 Updating layer visibility for zoom:', zoom, 'shouldShowKommune:', shouldShowKommune, 'shouldShowH3:', shouldShowH3, 'currentH3Resolution:', currentH3Resolution)
      
      // Helper function to safely update layer visibility
      const updateLayerVisibility = (layerId: string, visible: boolean) => {
        if (map.current && (map.current as any).getLayer(layerId)) {
          (map.current as any).setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none')
          console.log(`✅ Updated ${layerId} visibility to ${visible ? 'visible' : 'none'}`)
        } else {
          console.log(`⚠️ Layer ${layerId} not found, skipping visibility update`)
        }
      }
      
      // Update kommune layers
      updateLayerVisibility('kommune-fill', shouldShowKommune)
      updateLayerVisibility('kommune-stroke', shouldShowKommune)
      
      // Update H3 layers - check both res8 and res10, but only show current resolution
      updateLayerVisibility('h3-fill-res8', shouldShowH3 && currentH3Resolution === 8)
      updateLayerVisibility('h3-stroke-res8', shouldShowH3 && currentH3Resolution === 8)
      updateLayerVisibility('h3-fill-res10', shouldShowH3 && currentH3Resolution === 10)
      updateLayerVisibility('h3-stroke-res10', shouldShowH3 && currentH3Resolution === 10)
      
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
        'basemap-roads'
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
          100, 'rgba(139,0,0,0.8)'
        ])
      }
      
      // Update H3 layer styling for both resolutions
      const h3Layers = ['h3-fill-res8', 'h3-fill-res10']
      h3Layers.forEach(layerId => {
        if ((map.current as any).getLayer(layerId)) {
          (map.current as any).setPaintProperty(layerId, 'fill-color', [
            'interpolate',
            ['linear'],
            ['get', currentPropertyName],
            0, 'rgba(255,255,255,0.1)',
            1, 'rgba(255,100,100,0.3)',
            10, 'rgba(255,50,50,0.5)',
            50, 'rgba(255,0,0,0.7)',
            100, 'rgba(139,0,0,0.8)'
          ])
        }
      })
      
    } catch (error) {
      console.warn('Error updating layer styling:', error)
    }
  }, [selectedDataMode, currentPropertyName, currentH3Resolution, mapLoaded])
  
  // Update cell highlighting when selected cell changes
  useEffect(() => {
    if (!map.current || !mapLoaded) return

    try {
      console.log('🎯 Updating cell highlighting:', selectedCell)
      
      // Helper function to clear all highlight layers
      const clearHighlights = () => {
        // Clear kommune highlight
        if ((map.current as any).getLayer('kommune-highlight')) {
          (map.current as any).setLayoutProperty('kommune-highlight', 'visibility', 'none')
        }
        
        // Clear H3 highlights - only res8 and res10
        for (const res of [8, 10]) {
          const highlightId = `h3-highlight-res${res}`
          if ((map.current as any).getLayer(highlightId)) {
            (map.current as any).setLayoutProperty(highlightId, 'visibility', 'none')
          }
        }
        
        // Clear BNBO highlight
        if ((map.current as any).getLayer('bnbo-highlight')) {
          (map.current as any).setLayoutProperty('bnbo-highlight', 'visibility', 'none')
        }
      }
      
      // Clear all highlights first
      clearHighlights()
      
      // If a cell is selected, highlight it
      if (selectedCell) {
        const { id, layer } = selectedCell
        
        if (layer.includes('kommune')) {
          // Highlight kommune
          if ((map.current as any).getLayer('kommune-highlight')) {
            (map.current as any).setFilter('kommune-highlight', ['==', ['get', 'kommune_code'], Number(id)])
            (map.current as any).setLayoutProperty('kommune-highlight', 'visibility', 'visible')
          }
        } else if (layer.includes('h3')) {
          // Highlight H3 cell - determine resolution from layer name
          const resMatch = layer.match(/res(\d+)/)
          if (resMatch) {
            const res = parseInt(resMatch[1])
            const highlightId = `h3-highlight-res${res}`
            if ((map.current as any).getLayer(highlightId)) {
              (map.current as any).setFilter(highlightId, ['==', ['get', 'h3_id'], id])
              (map.current as any).setLayoutProperty(highlightId, 'visibility', 'visible')
            }
          }
        } else if (layer.includes('bnbo')) {
          // Highlight BNBO area
          if ((map.current as any).getLayer('bnbo-highlight')) {
            (map.current as any).setFilter('bnbo-highlight', ['==', ['get', 'id'], id])
            (map.current as any).setLayoutProperty('bnbo-highlight', 'visibility', 'visible')
          }
        }
      }
      
    } catch (error) {
      console.warn('Error updating cell highlighting:', error)
    }
  }, [selectedCell, mapLoaded])
  
  return (
    <div className={`relative ${className}`}>
      <div 
        ref={mapContainer} 
        className="w-full h-full"
        style={{
          position: 'relative',
          cursor: isMobile ? 'default' : 'grab',
          touchAction: isMobile ? 'pan-x pan-y' : 'none',
          pointerEvents: 'auto'
        }}
      />
      
      {/* Performance monitor (development only) */}
      {process.env.NODE_ENV === 'development' && (
        <div className="absolute top-4 left-4 bg-black/80 text-white text-xs p-2 rounded font-mono pointer-events-none">
          <div>FPS: {metrics.frameRate}</div>
          <div>Events: {metrics.eventCount}</div>
          <div>Mobile: {isMobile ? 'Yes' : 'No'}</div>
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
      
      {/* Mobile tap hint - only show on first load */}
      {isMobile && mapLoaded && (
        <div className="absolute bottom-4 left-4 right-4 bg-black/80 text-white text-xs p-3 rounded-lg pointer-events-none opacity-75">
          <p className="text-center">Tap areas to view details • Pinch to zoom • Drag to pan</p>
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