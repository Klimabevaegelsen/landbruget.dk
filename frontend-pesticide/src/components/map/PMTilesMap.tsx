'use client'

import { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Protocol } from 'pmtiles'
import { usePMTilesStore } from '@/stores/pmtiles-store'
import { useResolutionStore } from '@/stores/resolution-store'
import { useTemporalStore } from '@/stores/temporal-store'
import { useUIStore } from '@/stores/ui-store'
import { getBasemapStyleForTheme } from '@/lib/basemaps'

interface HoverInfo {
  h3_id: string
  year: number
  resolution: number
  pfas_grams: number
  pesticide_load: number
  diquat_grams: number
  glyphosate_grams: number
  applications: number
  pfas_applications: number
  diquat_applications: number
  glyphosate_applications: number
  field_count: number
  coverage: number
  area_ha: number
  pfas_intensity: number
  pesticide_intensity: number
  diquat_intensity: number
  glyphosate_intensity: number
}

interface BNBOHoverInfo {
  bnbo_id: string
  status_code: string
  status_description: string
  area_ha: number
}

interface KommuneHoverInfo {
  kommune_id: string
  kommune_name: string
  pfas_grams: number
  pesticide_load: number
  agricultural_area_ha: number
  kommune_area_ha: number
  field_count: number
  company_count: number
  year: number
}

type DataLayer = 'pfas' | 'total_pesticide' | 'diquat' | 'glyphosate'

interface PMTilesMapProps {
  className?: string
  pmtilesBaseUrl?: string
  initialYear?: number
  initialResolution?: number
  showBNBO?: boolean
  bnboOpacity?: number
  showKommune?: boolean
  kommuneOpacity?: number
  onBNBOToggle?: (visible: boolean) => void
  onKommuneToggle?: (visible: boolean) => void
  activeDataLayer?: DataLayer
}

export function PMTilesMap({ 
  className = '', 
  pmtilesBaseUrl = '/api/pmtiles',
  initialYear = 2023,
  initialResolution = 10,
  showBNBO = false,
  bnboOpacity = 0.4,
  showKommune = false,
  kommuneOpacity = 0.6,
  onBNBOToggle,
  onKommuneToggle,
  activeDataLayer = 'pfas'
}: PMTilesMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [isLoaded, setIsLoaded] = useState(false)
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null)
  const [bnboHoverInfo, setBNBOHoverInfo] = useState<BNBOHoverInfo | null>(null)
  const [kommuneHoverInfo, setKommuneHoverInfo] = useState<KommuneHoverInfo | null>(null)
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 })
  const [currentZoom, setCurrentZoom] = useState(7)
  
  // Store hooks
  const { 
    setTileLoading, 
    setTileLoaded, 
    setTileError, 
    getTileKey,
    getKommuneTileKey,
    getTileStatus,
    getKommuneTileStatus 
  } = usePMTilesStore()
  
  const { 
    currentResolution, 
    setResolution, 
    setZoom,
    getResolutionForZoom 
  } = useResolutionStore()
  
  const { currentYear, setCurrentYear } = useTemporalStore()
  const { theme } = useUIStore()
  
  // Determine if we should use dark mode
  const isDarkMode = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)
  
  // Auto-show municipality layer when zoomed out
  const shouldShowKommune = showKommune || currentZoom <= 8
  
  // Initialize map with PMTiles protocol
  useEffect(() => {
    if (!mapContainer.current || map.current) return
    
    // Register PMTiles protocol
    const protocol = new Protocol()
    maplibregl.addProtocol('pmtiles', protocol.tile)
    
    // Create map with theme-based style
    map.current = new maplibregl.Map({
      container: mapContainer.current,
      style: getBasemapStyleForTheme(theme) as any,
      center: [10.0, 56.0], // Denmark center
      zoom: 7,
      minZoom: 4,
      maxZoom: 15
    })
    
    // Add controls
    map.current.addControl(new maplibregl.NavigationControl(), 'top-right')
    map.current.addControl(new maplibregl.ScaleControl(), 'bottom-left')
    
    map.current.on('load', () => {
      setIsLoaded(true)
    })
    
    // Auto-resolution based on zoom
    map.current.on('zoom', () => {
      if (!map.current) return
      
      const zoom = map.current.getZoom()
      setZoom(zoom)
      setCurrentZoom(zoom)
    })
    
    // Track mouse position for hover tooltip
    map.current.on('mousemove', (e) => {
      setMousePosition({ x: e.point.x, y: e.point.y })
    })
    
    // Cleanup
    return () => {
      if (map.current) {
        map.current.remove()
        map.current = null
      }
      maplibregl.removeProtocol('pmtiles')
    }
  }, [setZoom])

  // Update map style when theme changes
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    // Get new style and preserve existing data sources/layers
    const currentStyle = map.current.getStyle()
    const newBaseStyle = getBasemapStyleForTheme(theme)
    
    // Preserve existing data sources
    const dataSources = Object.fromEntries(
      Object.entries(currentStyle.sources || {}).filter(([key]) => 
        key.includes('h3-') || key.includes('bnbo-')
      )
    )
    
    // Preserve existing data layers
    const dataLayers = Object.values(currentStyle.layers || []).filter((layer: any) => 
      layer.id.includes('h3-') || layer.id.includes('bnbo-')
    )
    
    const updatedStyle = {
      ...newBaseStyle,
      sources: {
        ...newBaseStyle.sources,
        ...dataSources
      },
      layers: [
        ...newBaseStyle.layers,
        ...dataLayers
      ]
    }
    
    map.current.setStyle(updatedStyle as any)
  }, [theme, isLoaded])
  
  // Update PMTiles layer when year, resolution, or data layer changes
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    // Only update H3 layers when not showing municipality layer
    if (!(showKommune || currentZoom <= 8)) {
      updatePMTilesLayer(currentYear, currentResolution, activeDataLayer)
    }
  }, [currentYear, currentResolution, activeDataLayer, isLoaded, pmtilesBaseUrl, currentZoom, showKommune])
  
  // Update BNBO layer when visibility or opacity changes
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    updateBNBOLayer(showBNBO, bnboOpacity)
  }, [showBNBO, bnboOpacity, isLoaded, pmtilesBaseUrl])

  // Update Kommune layer when visibility, opacity, or data layer changes
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    updateKommuneLayer(shouldShowKommune, kommuneOpacity, currentYear)
  }, [shouldShowKommune, kommuneOpacity, currentYear, activeDataLayer, isLoaded, pmtilesBaseUrl])

  function getDataLayerColorExpression(dataLayer: DataLayer): any {
    console.log('Switching to data layer:', dataLayer); // Debug log
    
    // Get the intensity field name for the current data layer (grams per hectare)
    const getIntensityFieldName = (layer: DataLayer): string => {
      switch (layer) {
        case 'pfas': return 'pfas_intensity'
        case 'total_pesticide': return 'pesticide_intensity'
        case 'diquat': return 'diquat_intensity'
        case 'glyphosate': return 'glyphosate_intensity'
        default: return 'pfas_intensity'
      }
    }
    
    const intensityFieldName = getIntensityFieldName(dataLayer)
    
    // Different color scales based on data layer to handle different intensity ranges
    if (dataLayer === 'diquat' || dataLayer === 'glyphosate') {
      // More sensitive scale for diquat and glyphosate (typically lower values)
      return [
        'case',
        ['<=', ['get', intensityFieldName], 0], 'rgba(0, 0, 0, 0)',  // Transparent for 0 or negative values
        [
          'step',
          ['get', intensityFieldName],
          '#fef2f2',           // Very light red for any positive value
          0.001, '#fecaca',    // Light red (0.001-0.01 g/ha)
          0.01, '#f87171',     // Medium light red (0.01-0.05 g/ha)
          0.05, '#ef4444',     // Red (0.05-0.1 g/ha)
          0.1, '#dc2626',      // Dark red (0.1-0.5 g/ha)
          0.5, '#b91c1c',      // Darker red (0.5-1 g/ha)
          1.0, '#991b1b',      // Very dark red (1-2 g/ha)
          2.0, '#7f1d1d',      // Extremely dark red (2-5 g/ha)
          5.0, '#450a0a'       // Almost black red (5+ g/ha)
        ]
      ]
    } else {
      // Standard scale for PFAS and total pesticides (typically higher values)
      return [
        'case',
        ['<=', ['get', intensityFieldName], 0], 'rgba(0, 0, 0, 0)',  // Transparent for 0 or negative values
        [
          'step',
          ['get', intensityFieldName],
          '#fef2f2',         // Very light red for any positive value
          0.01, '#fecaca',   // Light red (0.01-0.1 g/ha)
          0.1, '#f87171',    // Medium light red (0.1-0.5 g/ha)
          0.5, '#ef4444',    // Red (0.5-1 g/ha)
          1.0, '#dc2626',    // Dark red (1-2 g/ha)
          2.0, '#b91c1c',    // Darker red (2-5 g/ha)
          5.0, '#991b1b',    // Very dark red (5-10 g/ha)
          10.0, '#7f1d1d',   // Extremely dark red (10-20 g/ha)
          20.0, '#450a0a'    // Almost black red (20+ g/ha)
        ]
      ]
    }
  }

  function getKommuneColorExpression(dataLayer: DataLayer): any {
    // Municipality data uses absolute grams (not per hectare) due to aggregation across entire municipality
    const getKommuneFieldName = (layer: DataLayer): string => {
      switch (layer) {
        case 'pfas': return 'pfas_grams'
        case 'total_pesticide': return 'pesticide_load'
        case 'diquat': return 'diquat_grams'
        case 'glyphosate': return 'glyphosate_grams'
        default: return 'pfas_grams'
      }
    }
    
    const fieldName = getKommuneFieldName(dataLayer)
    
    // Different scales for municipality data based on typical ranges
    if (dataLayer === 'diquat' || dataLayer === 'glyphosate') {
      // More sensitive scale for diquat and glyphosate at municipality level
      return [
        'case',
        ['<=', ['get', fieldName], 0], 'rgba(0, 0, 0, 0)',  // Transparent for 0 or negative values
        [
          'step',
          ['get', fieldName],
          '#fef2f2',         // Very light red for any positive value
          0.01, '#fecaca',   // Light red for minimal load
          1, '#f87171',      // Medium red for low load (1-10g)
          10, '#ef4444',     // Red for medium load (10-100g)
          100, '#dc2626',    // Dark red for high load (100g-1kg)
          1000, '#b91c1c',   // Very dark red for very high load (1-10kg)
          10000, '#991b1b',  // Extremely dark red for extreme load (10-100kg)
          100000, '#7f1d1d', // Almost black red (100kg+)
          1000000, '#450a0a' // Black red for extreme values
        ]
      ]
    } else {
      // Standard scale for PFAS and total pesticides at municipality level
      return [
        'case',
        ['<=', ['get', fieldName], 0], 'rgba(0, 0, 0, 0)',  // Transparent for 0 or negative values
        [
          'step',
          ['get', fieldName],
          '#fef2f2',          // Very light red for any positive value
          0.1, '#fecaca',     // Light red for minimal load
          100, '#f87171',     // Medium red for low load (100g-1kg)
          1000, '#ef4444',    // Red for medium load (1-10kg)
          10000, '#dc2626',   // Dark red for high load (10-100kg)
          100000, '#b91c1c',  // Very dark red for very high load (100kg-1000kg)
          1000000, '#991b1b', // Extremely dark red for extreme load (1000kg+)
          5000000, '#7f1d1d', // Almost black red
          10000000, '#450a0a' // Black red for extreme values
        ]
      ]
    }
  }

  function updatePMTilesLayer(year: number, resolution: number, dataLayer: DataLayer) {
    if (!map.current) return
    
    console.log('Updating PMTiles layer:', { year, resolution, dataLayer }); // Debug log
    
    // Check if municipality layer should be visible (zoom <= 8)
    const shouldShowKommune = showKommune || currentZoom <= 8
    
    // Hide low resolution H3 layers (7-8) when municipality layer should be visible
    if (shouldShowKommune && resolution <= 8) {
      console.log(`Skipping H3 resolution ${resolution} because municipality layer should be visible at zoom ${currentZoom}`);
      
      // Remove any existing H3 layers when municipality should be visible
      const layerId = 'h3-pfas'
      const sourceId = 'h3-pfas-source'
      if (map.current.getLayer(layerId)) {
        map.current.removeLayer(layerId)
        console.log('Removed H3 layer for municipality visibility')
      }
      if (map.current.getLayer(`${layerId}-outline`)) {
        map.current.removeLayer(`${layerId}-outline`)
        console.log('Removed H3 outline layer for municipality visibility')
      }
      if (map.current.getSource(sourceId)) {
        map.current.removeSource(sourceId)
        console.log('Removed H3 source for municipality visibility')
      }
      return
    }
    
    const layerId = 'h3-pfas'
    const sourceId = 'h3-pfas-source'
    const tileKey = getTileKey(year, resolution)
    
    // Get intensity field name for current data layer (grams per hectare)
    const getIntensityFieldName = (layer: DataLayer): string => {
      switch (layer) {
        case 'pfas': return 'pfas_intensity'
        case 'total_pesticide': return 'pesticide_intensity'
        case 'diquat': return 'diquat_intensity'
        case 'glyphosate': return 'glyphosate_intensity'
        default: return 'pfas_intensity'
      }
    }
    const intensityFieldName = getIntensityFieldName(dataLayer)
    
    // Remove existing layer and source
    if (map.current.getLayer(layerId)) {
      map.current.removeLayer(layerId)
    }
    if (map.current.getLayer(`${layerId}-outline`)) {
      map.current.removeLayer(`${layerId}-outline`)
    }
    if (map.current.getSource(sourceId)) {
      map.current.removeSource(sourceId)
    }
    
    // Add new PMTiles source
    const pmtilesUrl = `pmtiles://${pmtilesBaseUrl}/h3_pfas_${year}_res${resolution}.pmtiles`
    
    // Track loading state
    setTileLoading(tileKey, pmtilesUrl)
    
    try {
      map.current.addSource(sourceId, {
        type: 'vector',
        url: pmtilesUrl
      })
      
      // Add fill layer with resolution-aware styling
      map.current.addLayer({
        id: layerId,
        type: 'fill',
        source: sourceId,
        'source-layer': `h3_pfas_${year}_res${resolution}`,
        paint: {
          'fill-color': getDataLayerColorExpression(dataLayer),
          'fill-opacity': 0.8
        }
      })
      
      // Add outline with resolution-aware width
      map.current.addLayer({
        id: `${layerId}-outline`,
        type: 'line',
        source: sourceId,
        'source-layer': `h3_pfas_${year}_res${resolution}`,
        paint: {
          'line-color': '#64748b',  // slate-500 - subtle outline
          'line-width': resolution <= 8 ? 0.8 : 0.4,
          'line-opacity': 0.4
        }
      })
      
      // Enhanced hover functionality
      map.current.on('mouseenter', layerId, (e) => {
        if (map.current) {
          map.current.getCanvas().style.cursor = 'pointer'
        }
        
        if (e.features && e.features.length > 0) {
          const feature = e.features[0]
          const props = feature.properties
          
          if (props) {
            setHoverInfo({
              h3_id: props.h3_id || '',
              year: props.year || year,
              resolution: props.resolution || resolution,
              pfas_grams: props.pfas_grams || 0,
              pesticide_load: props.pesticide_load || 0,
              diquat_grams: props.diquat_grams || 0,
              glyphosate_grams: props.glyphosate_grams || 0,
              applications: props.applications || 0,
              pfas_applications: props.pfas_applications || 0,
              diquat_applications: props.diquat_applications || 0,
              glyphosate_applications: props.glyphosate_applications || 0,
              field_count: props.field_count || 0,
              coverage: props.coverage || 0,
              area_ha: props.area_ha || 0,
              pfas_intensity: props.pfas_intensity || 0,
              pesticide_intensity: props.pesticide_intensity || 0,
              diquat_intensity: props.diquat_intensity || 0,
              glyphosate_intensity: props.glyphosate_intensity || 0,
            })
          }
        }
      })
      
      map.current.on('mouseleave', layerId, () => {
        if (map.current) {
          map.current.getCanvas().style.cursor = ''
        }
        setHoverInfo(null)
      })
      
      // Remove click handler - we only want hover tooltips
      
      setTileLoaded(tileKey)
    } catch (error) {
      console.error('Failed to load PMTiles:', error)
      setTileError(tileKey, error instanceof Error ? error.message : 'Unknown error')
    }
  }
  
  function updateBNBOLayer(visible: boolean, opacity: number) {
    if (!map.current) return
    
    const layerId = 'bnbo-areas'
    const sourceId = 'bnbo-areas-source'
    
    // Remove existing layer and source
    if (map.current.getLayer(layerId)) {
      map.current.removeLayer(layerId)
    }
    if (map.current.getLayer(`${layerId}-outline`)) {
      map.current.removeLayer(`${layerId}-outline`)
    }
    if (map.current.getSource(sourceId)) {
      map.current.removeSource(sourceId)
    }
    
    if (!visible) return
    
    // Add BNBO PMTiles source - use direct GCS URL
    const pmtilesUrl = `pmtiles://https://storage.googleapis.com/landbrugsdata-raw-data/pmtiles/bnbo_areas.pmtiles`
    
    try {
      map.current.addSource(sourceId, {
        type: 'vector',
        url: pmtilesUrl
      })
      
      // Add fill layer with status-based styling
      map.current.addLayer({
        id: layerId,
        type: 'fill',
        source: sourceId,
        'source-layer': 'bnbo_areas',
        paint: {
          'fill-color': [
            'case',
            ['==', ['get', 'status_code'], 'action_required'], '#ff6b6b',
            ['==', ['get', 'status_code'], 'completed'], '#51cf66',
            '#868e96' // unknown
          ],
          'fill-opacity': opacity
        }
      })
      
      // Add outline
      map.current.addLayer({
        id: `${layerId}-outline`,
        type: 'line',
        source: sourceId,
        'source-layer': 'bnbo_areas',
        paint: {
          'line-color': [
            'case',
            ['==', ['get', 'status_code'], 'action_required'], '#e03131',
            ['==', ['get', 'status_code'], 'completed'], '#37b24d',
            '#495057' // unknown
          ],
          'line-width': 1.5,
          'line-opacity': 0.8
        }
      })
      
      // BNBO hover functionality
      map.current.on('mouseenter', layerId, (e) => {
        if (map.current) {
          map.current.getCanvas().style.cursor = 'pointer'
        }
        
        if (e.features && e.features.length > 0) {
          const feature = e.features[0]
          const props = feature.properties
          
          if (props) {
            setBNBOHoverInfo({
              bnbo_id: props.bnbo_id || '',
              status_code: props.status_code || 'unknown',
              status_description: props.status_description || 'Unknown',
              area_ha: props.area_ha || 0,
            })
          }
        }
      })
      
      map.current.on('mouseleave', layerId, () => {
        if (map.current) {
          map.current.getCanvas().style.cursor = ''
        }
        setBNBOHoverInfo(null)
      })
      
      // BNBO click handler
      map.current.on('click', layerId, (e) => {
        if (!e.features || e.features.length === 0) return
        
        const feature = e.features[0]
        const properties = feature.properties
        
        if (!properties) return
        
        // Create detailed popup
        const popup = new maplibregl.Popup({
          closeButton: true,
          closeOnClick: true,
          maxWidth: '350px'
        })
        
        const statusColor = 
          properties.status_code === 'action_required' ? '#ff6b6b' :
          properties.status_code === 'completed' ? '#51cf66' : '#868e96'
        
        const popupContent = `
          <div class="p-4">
            <div class="border-b pb-2 mb-3">
              <h3 class="font-bold text-lg text-gray-900">BNBO Area</h3>
              <p class="text-sm text-gray-600">Protected Environmental Area</p>
            </div>
            
            <div class="space-y-3 text-sm">
              <div>
                <span class="font-medium text-gray-700">BNBO ID:</span>
                <p class="text-gray-900 font-mono">${properties.bnbo_id || 'N/A'}</p>
              </div>
              <div>
                <span class="font-medium text-gray-700">Area:</span>
                <p class="text-gray-900">${(properties.area_ha || 0).toFixed(2)} ha</p>
              </div>
              <div>
                <span class="font-medium text-gray-700">Status:</span>
                <div class="flex items-center space-x-2 mt-1">
                  <div class="w-3 h-3 rounded-full" style="background-color: ${statusColor}"></div>
                  <span class="text-gray-900">${properties.status_description || 'Unknown'}</span>
                </div>
              </div>
            </div>
          </div>
        `
        
        popup
          .setLngLat(e.lngLat)
          .setHTML(popupContent)
          .addTo(map.current!)
      })
      
    } catch (error) {
      console.error('Failed to load BNBO PMTiles:', error)
    }
  }
  
  function updateKommuneLayer(visible: boolean, opacity: number, year: number) {
    if (!map.current) return
    
    console.log('updateKommuneLayer called:', { visible, opacity, year, currentZoom }); // Debug log
    
    const layerId = 'kommune-areas'
    const sourceId = 'kommune-areas-source'
    
    // Remove existing layer and source
    if (map.current.getLayer(layerId)) {
      map.current.removeLayer(layerId)
      console.log('Removed existing kommune layer'); // Debug log
    }
    if (map.current.getLayer(`${layerId}-outline`)) {
      map.current.removeLayer(`${layerId}-outline`)
      console.log('Removed existing kommune outline layer'); // Debug log
    }
    if (map.current.getSource(sourceId)) {
      map.current.removeSource(sourceId)
      console.log('Removed existing kommune source'); // Debug log
    }
    
    if (!visible) {
      console.log('Kommune layer not visible, returning'); // Debug log
      return
    }
    
    // Add Kommune PMTiles source
    const pmtilesUrl = `pmtiles://${pmtilesBaseUrl}/kommune_pfas_${year}.pmtiles`
    const tileKey = getKommuneTileKey(year)
    
    console.log('Adding kommune source:', pmtilesUrl)
    
    // Track loading state
    setTileLoading(tileKey, pmtilesUrl)
    
    try {
      map.current.addSource(sourceId, {
        type: 'vector',
        url: pmtilesUrl
      })
      
      console.log('Added kommune source successfully'); // Debug log
      
      // Debug: Check what layers are available in the source
      setTimeout(() => {
        if (map.current && map.current.getSource(sourceId)) {
          const source = map.current.getSource(sourceId) as any;
          console.log('Kommune source details:', source);
          
          // Try to get source data to see available layers
          map.current.on('sourcedata', (e) => {
            if (e.sourceId === sourceId && e.isSourceLoaded) {
              console.log('Kommune source loaded, checking layers...');
            }
          });
        }
      }, 1000);
      
      // Find the first H3 layer to insert the kommune layer before it
      let beforeLayerId: string | undefined = undefined
      const layers = map.current.getStyle().layers || []
      for (const layer of layers) {
        if (layer.id.startsWith('h3-pfas')) {
          beforeLayerId = layer.id
          break
        }
      }
      
      // Add the municipality fill layer BEFORE any H3 layers (should be Polygon geometry)
      map.current.addLayer({
        id: layerId,
        type: 'fill',
        source: sourceId,
        'source-layer': `kommune_pfas_${year}`,  // Use the correct layer name with year
        paint: {
          'fill-color': getKommuneColorExpression(activeDataLayer),
          'fill-opacity': opacity
        },
        layout: {
          'visibility': 'visible'   // Force visibility
        }
      }, beforeLayerId)
      
      console.log('Added kommune fill layer successfully with source-layer: kommune_pfas_' + year, 'before layer:', beforeLayerId); // Debug log
      
      // Add outline with municipality boundaries BEFORE any H3 layers
      map.current.addLayer({
        id: `${layerId}-outline`,
        type: 'line',
        source: sourceId,
        'source-layer': `kommune_pfas_${year}`,  // Use the correct layer name with year
        paint: {
          'line-color': '#00ff00',  // Bright green for debugging
          'line-width': 3,          // Thick line for debugging
          'line-opacity': 1.0       // Full opacity for debugging
        },
        layout: {
          'visibility': 'visible'   // Force visibility
        }
      }, beforeLayerId)
      
      console.log('Added kommune outline layer successfully'); // Debug log
      
             // Debug: List all layers to see the current layer stack
       setTimeout(() => {
         if (map.current) {
           const layers = map.current.getStyle().layers || [];
           console.log('Current map layers:', layers.map(l => l.id));
           console.log('Kommune layer visibility:', map.current.getLayoutProperty(layerId, 'visibility'));
           console.log('Kommune outline layer visibility:', map.current.getLayoutProperty(`${layerId}-outline`, 'visibility'));
           
           // Check if layers exist
           const kommuneLayer = map.current.getLayer(layerId);
           const kommuneOutlineLayer = map.current.getLayer(`${layerId}-outline`);
           console.log('Kommune layer exists:', !!kommuneLayer);
           console.log('Kommune outline layer exists:', !!kommuneOutlineLayer);
           
           // Check paint properties
           if (kommuneLayer) {
             console.log('Kommune fill color:', map.current.getPaintProperty(layerId, 'fill-color'));
             console.log('Kommune fill opacity:', map.current.getPaintProperty(layerId, 'fill-opacity'));
           }
           if (kommuneOutlineLayer) {
             console.log('Kommune line color:', map.current.getPaintProperty(`${layerId}-outline`, 'line-color'));
             console.log('Kommune line width:', map.current.getPaintProperty(`${layerId}-outline`, 'line-width'));
           }
         }
       }, 500);
      
      // Debug: Add a sourcedata listener to see when data loads
      map.current.on('sourcedata', (e) => {
        if (e.sourceId === sourceId && e.isSourceLoaded) {
          console.log('Kommune source data loaded successfully');
          
          // Query the source to see what features are available
          setTimeout(() => {
            if (map.current) {
              const features = map.current.querySourceFeatures(sourceId, {
                sourceLayer: `kommune_pfas_${year}`
              });
                             console.log('Kommune features found:', features.length);
               if (features.length > 0) {
                 console.log('Sample kommune feature:', features[0]);
                 console.log('Sample kommune properties:', features[0].properties);
                 
                 // Check all features for geometry types
                 const geometryTypes = new Set();
                 features.slice(0, 10).forEach((f, i) => {
                   geometryTypes.add(f.geometry?.type);
                   console.log(`Feature ${i} geometry type:`, f.geometry?.type);
                   if (f.geometry?.type === 'Polygon' || f.geometry?.type === 'MultiPolygon') {
                     console.log(`Feature ${i} has polygon geometry - should be visible!`);
                   }
                 });
                 console.log('All geometry types found:', Array.from(geometryTypes));
                 
                 // Check if any features have visible geometry in current viewport
                 const bounds = map.current.getBounds();
                 console.log('Current map bounds:', bounds);
                 console.log('Map bounds SW:', bounds.getSouthWest());
                 console.log('Map bounds NE:', bounds.getNorthEast());
                 console.log('Sample feature geometry type:', features[0].geometry?.type);
                 
                 // Get feature bounds to see if they overlap with map viewport
                 const feature = features[0];
                 if (feature.geometry) {
                   console.log('Sample feature geometry type:', feature.geometry.type);
                   
                   // Try to manually check if we're in Denmark area
                   const denmarkBounds = {
                     west: 8.0,
                     east: 15.2,
                     south: 54.5,
                     north: 57.8
                   };
                   
                   const mapSW = bounds.getSouthWest();
                   const mapNE = bounds.getNorthEast();
                   
                   const mapInDenmark = 
                     mapSW.lng >= denmarkBounds.west && mapNE.lng <= denmarkBounds.east &&
                     mapSW.lat >= denmarkBounds.south && mapNE.lat <= denmarkBounds.north;
                   
                   console.log('Map viewport overlaps Denmark bounds:', mapInDenmark);
                   console.log('Map center:', map.current.getCenter());
                   
                   // Test: Try to fit bounds to show all kommune features
                   if (!mapInDenmark) {
                     console.log('Map not in Denmark bounds, trying to fit to features...');
                     setTimeout(() => {
                       if (map.current) {
                         // Try to fit to Denmark bounds
                         map.current.fitBounds([
                           [denmarkBounds.west, denmarkBounds.south],
                           [denmarkBounds.east, denmarkBounds.north]
                         ], { padding: 20 });
                       }
                     }, 1000);
                   }
                 }
               }
            }
          }, 1000);
        }
      });

      // Kommune hover functionality
      map.current.on('mouseenter', layerId, (e) => {
        if (map.current) {
          map.current.getCanvas().style.cursor = 'pointer'
        }
        
        if (e.features && e.features.length > 0) {
          const feature = e.features[0]
          const props = feature.properties
          
          console.log('Kommune feature properties on hover:', props); // Debug log to see actual data
          
          if (props) {
            setKommuneHoverInfo({
              kommune_id: props.kommune_code || '',
              kommune_name: props.kommune_name || 'Unknown Municipality',
              pfas_grams: props.pfas_grams || 0,
              pesticide_load: props.pesticide_load || 0,
              agricultural_area_ha: props.agricultural_area_ha || 0,
              kommune_area_ha: props.kommune_area_ha || 0,
              field_count: props.field_count || 0,
              company_count: props.company_count || 0,
              year: props.year || year,
            })
          }
        }
      })
      
      map.current.on('mouseleave', layerId, () => {
        if (map.current) {
          map.current.getCanvas().style.cursor = ''
        }
        setKommuneHoverInfo(null)
      })
      
      setTileLoaded(tileKey)
      console.log('Kommune layer setup completed successfully')
    } catch (error) {
      console.error('Failed to load Kommune PMTiles:', error)
      setTileError(tileKey, error instanceof Error ? error.message : 'Unknown error')
    }
  }
  
  return (
    <div className={`relative ${className}`}>
      <div ref={mapContainer} className="w-full h-full" />
      

      
      {/* Enhanced Hover tooltip with all data */}
      {(hoverInfo || bnboHoverInfo || kommuneHoverInfo) && (
        <div 
          className="absolute bg-white/95 backdrop-blur-sm border border-slate-200 rounded-lg shadow-xl p-4 pointer-events-none z-10 max-w-sm"
          style={{
            left: mousePosition.x + 15,
            top: mousePosition.y - 15,
            transform: mousePosition.x > window.innerWidth - 400 ? 'translateX(-100%)' : 'none'
          }}
        >
          {hoverInfo && (
            <>
              <div className="flex items-center justify-between mb-3">
                <div>
                  <div className="text-sm font-semibold text-slate-900">H3 Cell Data</div>
                  <div className="text-xs text-slate-500">Resolution {hoverInfo.resolution} • {hoverInfo.year}</div>
                </div>
                <div className="text-xs text-slate-400 font-mono">
                  {hoverInfo.h3_id.substring(0, 8)}...
                </div>
              </div>
              
              <div className="space-y-3">
                {/* Primary data based on active layer - Intensity drives color coding */}
                <div className="bg-slate-50 rounded-md p-3">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm font-medium text-slate-700">
                      {activeDataLayer === 'pfas' ? 'PFAS Intensity' :
                       activeDataLayer === 'total_pesticide' ? 'Pesticide Intensity' :
                       activeDataLayer === 'diquat' ? 'Diquat Intensity' :
                       'Glyphosate Intensity'}
                    </span>
                    <span className="text-sm font-bold text-slate-900">
                      {activeDataLayer === 'pfas' ? `${hoverInfo.pfas_intensity.toFixed(3)} g/ha` :
                       activeDataLayer === 'total_pesticide' ? `${hoverInfo.pesticide_intensity.toFixed(3)} g/ha` :
                       activeDataLayer === 'diquat' ? `${hoverInfo.diquat_intensity.toFixed(3)} g/ha` :
                       `${hoverInfo.glyphosate_intensity.toFixed(3)} g/ha`}
                    </span>
                  </div>
                  <div className="flex justify-between items-center text-xs text-slate-600">
                    <span>Total Load:</span>
                    <span className="font-medium">
                      {activeDataLayer === 'pfas' ? `${hoverInfo.pfas_grams.toFixed(2)} g` :
                       activeDataLayer === 'total_pesticide' ? `${(hoverInfo.pesticide_load * 1000).toFixed(1)} g` :
                       activeDataLayer === 'diquat' ? `${hoverInfo.diquat_grams.toFixed(2)} g` :
                       `${hoverInfo.glyphosate_grams.toFixed(2)} g`}
                    </span>
                  </div>
                </div>
                
                {/* Complete data overview - ALL fields */}
                <div className="space-y-2">
                  <div className="text-xs font-medium text-slate-700 border-b pb-1">Complete Cell Data</div>
                  
                  <div className="grid grid-cols-1 gap-2 text-xs">
                    {/* Chemical loads - all in grams */}
                    <div className="bg-red-50 rounded p-2">
                      <div className="text-xs font-medium text-red-700 mb-1">Chemical Loads (grams)</div>
                      <div className="grid grid-cols-2 gap-1">
                        <div className="flex justify-between">
                          <span className="text-red-600">PFAS:</span>
                          <span className="font-medium text-red-900">{hoverInfo.pfas_grams.toFixed(2)} g</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-red-600">Total Pesticides:</span>
                          <span className="font-medium text-red-900">{(hoverInfo.pesticide_load * 1000).toFixed(1)} g</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-red-600">Diquat:</span>
                          <span className="font-medium text-red-900">{hoverInfo.diquat_grams.toFixed(2)} g</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-red-600">Glyphosate:</span>
                          <span className="font-medium text-red-900">{hoverInfo.glyphosate_grams.toFixed(2)} g</span>
                        </div>
                      </div>
                    </div>
                    
                    {/* Application counts */}
                    <div className="bg-orange-50 rounded p-2">
                      <div className="text-xs font-medium text-orange-700 mb-1">Application Counts</div>
                      <div className="grid grid-cols-2 gap-1">
                        <div className="flex justify-between">
                          <span className="text-orange-600">Total:</span>
                          <span className="font-medium text-orange-900">{hoverInfo.applications}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-orange-600">PFAS:</span>
                          <span className="font-medium text-orange-900">{hoverInfo.pfas_applications}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-orange-600">Diquat:</span>
                          <span className="font-medium text-orange-900">{hoverInfo.diquat_applications}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-orange-600">Glyphosate:</span>
                          <span className="font-medium text-orange-900">{hoverInfo.glyphosate_applications}</span>
                        </div>
                      </div>
                    </div>
                    
                    {/* Agricultural data */}
                    <div className="bg-green-50 rounded p-2">
                      <div className="text-xs font-medium text-green-700 mb-1">Agricultural Data</div>
                      <div className="grid grid-cols-2 gap-1">
                        <div className="flex justify-between">
                          <span className="text-green-600">Field Count:</span>
                          <span className="font-medium text-green-900">{hoverInfo.field_count}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-green-600">Area:</span>
                          <span className="font-medium text-green-900">{hoverInfo.area_ha.toFixed(1)} ha</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-green-600">Coverage:</span>
                          <span className="font-medium text-green-900">{(hoverInfo.coverage * 100).toFixed(0)}%</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-green-600">Quality:</span>
                          <span className="font-medium text-green-900">{hoverInfo.coverage > 0.8 ? 'High' : hoverInfo.coverage > 0.5 ? 'Medium' : 'Low'}</span>
                        </div>
                      </div>
                    </div>
                    
                    {/* Intensity metrics (grams per hectare) */}
                    <div className="bg-blue-50 rounded p-2">
                      <div className="text-xs font-medium text-blue-700 mb-1">Intensity Metrics (g/ha)</div>
                      <div className="grid grid-cols-2 gap-1">
                        <div className="flex justify-between">
                          <span className="text-blue-600">PFAS:</span>
                          <span className="font-medium text-blue-900">{hoverInfo.pfas_intensity.toFixed(3)} g/ha</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-blue-600">Pesticides:</span>
                          <span className="font-medium text-blue-900">{hoverInfo.pesticide_intensity.toFixed(3)} g/ha</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-blue-600">Diquat:</span>
                          <span className="font-medium text-blue-900">{hoverInfo.diquat_intensity.toFixed(3)} g/ha</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-blue-600">Glyphosate:</span>
                          <span className="font-medium text-blue-900">{hoverInfo.glyphosate_intensity.toFixed(3)} g/ha</span>
                        </div>
                      </div>
                    </div>
                    
                    {/* Technical info */}
                    <div className="bg-gray-50 rounded p-2">
                      <div className="text-xs font-medium text-gray-700 mb-1">Technical Info</div>
                      <div className="grid grid-cols-2 gap-1">
                        <div className="flex justify-between">
                          <span className="text-gray-600">H3 ID:</span>
                          <span className="font-medium text-gray-900 font-mono text-xs">{hoverInfo.h3_id.substring(0, 12)}...</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600">Resolution:</span>
                          <span className="font-medium text-gray-900">{hoverInfo.resolution}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600">Year:</span>
                          <span className="font-medium text-gray-900">{hoverInfo.year}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-gray-600">Data Quality:</span>
                          <span className="font-medium text-gray-900">{hoverInfo.coverage > 0.8 ? 'High' : hoverInfo.coverage > 0.5 ? 'Medium' : 'Low'}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </>
          )}
          
          {bnboHoverInfo && (
            <>
              <div className="text-sm font-semibold text-slate-900 mb-2">
                BNBO Protected Area
              </div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">ID:</span>
                  <span className="font-medium text-slate-900 font-mono text-xs">{bnboHoverInfo.bnbo_id}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Status:</span>
                  <span className="font-medium text-slate-900">{bnboHoverInfo.status_description}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Area:</span>
                  <span className="font-medium text-slate-900">{bnboHoverInfo.area_ha.toFixed(1)} ha</span>
                </div>
              </div>
            </>
          )}
          
          {kommuneHoverInfo && (
            <>
              <div className="text-sm font-semibold text-slate-900 mb-2">
                {kommuneHoverInfo.kommune_name}
              </div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Municipality ID:</span>
                  <span className="font-medium text-slate-900 font-mono text-xs">{kommuneHoverInfo.kommune_id}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Total PFAS:</span>
                  <span className="font-medium text-slate-900">{kommuneHoverInfo.pfas_grams.toFixed(1)} g</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Pesticides:</span>
                  <span className="font-medium text-slate-900">{kommuneHoverInfo.pesticide_load.toFixed(1)} kg</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Fields:</span>
                  <span className="font-medium text-slate-900">{kommuneHoverInfo.field_count}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Companies:</span>
                  <span className="font-medium text-slate-900">{kommuneHoverInfo.company_count}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-slate-600">Agricultural Area:</span>
                  <span className="font-medium text-slate-900">{kommuneHoverInfo.agricultural_area_ha.toFixed(0)} ha</span>
                </div>
              </div>
            </>
          )}
        </div>
      )}
      

      
      {/* Loading indicator */}
      {!isLoaded && (
        <div className="absolute inset-0 bg-gray-100 flex items-center justify-center">
          <div className="text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-2"></div>
            <p className="text-sm text-gray-600">Loading map...</p>
          </div>
        </div>
      )}
    </div>
  )
}