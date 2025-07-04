'use client'

import React, { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Protocol } from 'pmtiles'
import { useMapStore } from '@/stores/map-store'
import { useDataStore } from '@/stores/data-store'
import { useUIStore } from '@/stores/ui-store'

interface PMTilesMapProps {
  className?: string
  availableYears: number[]
  pmtilesBaseUrl: string
}

interface HoverInfo {
  h3_id: string
  h3_resolution: number
  year: number
  pfas_grams: number
  pesticide_load: number
  applications: number
  field_count: number
  coverage: number
  area_ha: number
  pfas_intensity: number
  pesticide_intensity: number
  cell_count: number
  avg_pfas_per_field: number
  avg_pesticide_per_application: number
  summary: string
  zoom_class: string
}

export function PMTilesMap({ 
  className = '', 
  availableYears = [2023],
  pmtilesBaseUrl = '/pmtiles'
}: PMTilesMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [isLoaded, setIsLoaded] = useState(false)
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null)
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 })
  
  // Local viewport state
  const [viewport, setViewport] = useState({
    latitude: 56.0,
    longitude: 10.0,
    zoom: 6
  })
  
  // Store hooks
  const { selectedYear, setSelectedYear, showPFAS, heatmapMode, setHeatmapMode } = useMapStore()
  const { } = useDataStore()
  const { performanceMode } = useUIStore()
  
  // Initialize map
  useEffect(() => {
    if (!mapContainer.current || map.current) return
    
    // Register PMTiles protocol
    const protocol = new Protocol()
    maplibregl.addProtocol('pmtiles', protocol.tile)
    
    // Create map
    map.current = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {},
        layers: [
          {
            id: 'background',
            type: 'background',
            paint: {
              'background-color': '#f8f9fa'
            }
          }
        ]
      },
      center: [viewport.longitude, viewport.latitude],
      zoom: viewport.zoom,
      minZoom: 4,
      maxZoom: 16
    })
    
    // Add controls
    map.current.addControl(new maplibregl.NavigationControl(), 'top-right')
    map.current.addControl(new maplibregl.ScaleControl(), 'bottom-left')
    
    // Map event handlers
    map.current.on('load', () => {
      setIsLoaded(true)
    })
    
    map.current.on('moveend', () => {
      if (!map.current) return
      
      const center = map.current.getCenter()
      const zoom = map.current.getZoom()
      
      setViewport({
        longitude: center.lng,
        latitude: center.lat,
        zoom: zoom
      })
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
  }, [])
  
  // Update H3 PFAS layer when year changes
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    const layerId = 'h3-pfas'
    const sourceId = 'h3-pfas-source'
    
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
    
    // Add new source and layer for selected year
    const pmtilesUrl = `pmtiles://${pmtilesBaseUrl}/h3_pfas_${selectedYear}.pmtiles`
    
    map.current.addSource(sourceId, {
      type: 'vector',
      url: pmtilesUrl
    })
    
    // Add layer based on visualization mode
    if (heatmapMode === 'pfas') {
      // Heatmap visualization
      map.current.addLayer({
        id: layerId,
        type: 'heatmap',
        source: sourceId,
        'source-layer': `h3_pfas_${selectedYear}`,
        paint: {
          // Heatmap weight based on PFAS intensity
          'heatmap-weight': [
            'interpolate',
            ['linear'],
            ['get', 'pfas_intensity'],
            0, 0,
            1, 0.2,
            10, 0.6,
            100, 1
          ],
          // Heatmap intensity based on zoom
          'heatmap-intensity': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, 0.8,
            12, 1.2
          ],
          // Heatmap radius based on zoom and resolution
          'heatmap-radius': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, ['case', ['<=', ['get', 'h3_resolution'], 7], 15, 8],
            8, ['case', ['<=', ['get', 'h3_resolution'], 7], 25, 15],
            12, ['case', ['<=', ['get', 'h3_resolution'], 7], 35, 25]
          ],
          // Heatmap color
          'heatmap-color': [
            'interpolate',
            ['linear'],
            ['heatmap-density'],
            0, 'rgba(33,102,172,0)',
            0.2, 'rgb(103,169,207)',
            0.4, 'rgb(209,229,240)',
            0.6, 'rgb(253,219,199)',
            0.8, 'rgb(239,138,98)',
            1, 'rgb(178,24,43)'
          ]
        }
      })
    } else {
      // Choropleth visualization with resolution-aware styling
      map.current.addLayer({
        id: layerId,
        type: 'fill',
        source: sourceId,
        'source-layer': `h3_pfas_${selectedYear}`,
        paint: {
          'fill-color': [
            'case',
            ['>', ['get', 'pfas_grams'], 100], '#d73027',
            ['>', ['get', 'pfas_grams'], 50], '#f46d43',
            ['>', ['get', 'pfas_grams'], 10], '#fdae61',
            ['>', ['get', 'pfas_grams'], 1], '#fee08b',
            ['>', ['get', 'pfas_grams'], 0], '#e6f598',
            '#abdda4'
          ],
          'fill-opacity': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, 0.7,
            8, 0.8,
            12, 0.9
          ]
        }
      })
      
      // Add outline with resolution-aware width
      map.current.addLayer({
        id: `${layerId}-outline`,
        type: 'line',
        source: sourceId,
        'source-layer': `h3_pfas_${selectedYear}`,
        paint: {
          'line-color': '#ffffff',
          'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, ['case', ['<=', ['get', 'h3_resolution'], 7], 0.5, 0.1],
            8, ['case', ['<=', ['get', 'h3_resolution'], 7], 1.0, 0.3],
            12, ['case', ['<=', ['get', 'h3_resolution'], 7], 1.5, 0.5]
          ],
          'line-opacity': 0.6
        }
      })
    }
    
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
            h3_id: props.h3_id,
            h3_resolution: props.h3_resolution,
            year: props.year,
            pfas_grams: props.pfas_grams,
            pesticide_load: props.pesticide_load,
            applications: props.applications,
            field_count: props.field_count,
            coverage: props.coverage,
            area_ha: props.area_ha,
            pfas_intensity: props.pfas_intensity,
            pesticide_intensity: props.pesticide_intensity,
            cell_count: props.cell_count,
            avg_pfas_per_field: props.avg_pfas_per_field,
            avg_pesticide_per_application: props.avg_pesticide_per_application,
            summary: props.summary,
            zoom_class: props.zoom_class
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
    
    // Click handler for detailed popup
    map.current.on('click', layerId, (e) => {
      if (!e.features || e.features.length === 0) return
      
      const feature = e.features[0]
      const properties = feature.properties
      
      if (!properties) return
      
      // Create detailed popup
      const popup = new maplibregl.Popup({
        closeButton: true,
        closeOnClick: true,
        maxWidth: '400px'
      })
      
      const popupContent = `
        <div class="p-4">
          <div class="border-b pb-2 mb-3">
            <h3 class="font-bold text-lg text-gray-900">H3 Cell Details</h3>
            <p class="text-sm text-gray-600">${properties.summary}</p>
          </div>
          
          <div class="grid grid-cols-2 gap-3 text-sm">
            <div>
              <span class="font-medium text-gray-700">H3 ID:</span>
              <p class="text-gray-900 font-mono text-xs">${properties.h3_id}</p>
            </div>
            <div>
              <span class="font-medium text-gray-700">Resolution:</span>
              <p class="text-gray-900">${properties.h3_resolution}</p>
            </div>
            <div>
              <span class="font-medium text-gray-700">Year:</span>
              <p class="text-gray-900">${properties.year}</p>
            </div>
            <div>
              <span class="font-medium text-gray-700">Area:</span>
              <p class="text-gray-900">${properties.area_ha.toFixed(2)} ha</p>
            </div>
          </div>
          
          <div class="mt-3 pt-3 border-t">
            <h4 class="font-medium text-gray-900 mb-2">PFAS Data</h4>
            <div class="grid grid-cols-2 gap-2 text-sm">
              <div>
                <span class="text-gray-600">Total PFAS:</span>
                <p class="font-medium">${properties.pfas_grams.toFixed(3)} g</p>
              </div>
              <div>
                <span class="text-gray-600">Intensity:</span>
                <p class="font-medium">${properties.pfas_intensity.toFixed(3)} g/ha</p>
              </div>
              ${properties.field_count > 0 ? `
                <div>
                  <span class="text-gray-600">Avg per field:</span>
                  <p class="font-medium">${properties.avg_pfas_per_field.toFixed(3)} g</p>
                </div>
              ` : ''}
            </div>
          </div>
          
          <div class="mt-3 pt-3 border-t">
            <h4 class="font-medium text-gray-900 mb-2">Agricultural Data</h4>
            <div class="grid grid-cols-2 gap-2 text-sm">
              <div>
                <span class="text-gray-600">Pesticide load:</span>
                <p class="font-medium">${properties.pesticide_load.toFixed(2)} kg</p>
              </div>
              <div>
                <span class="text-gray-600">Applications:</span>
                <p class="font-medium">${properties.applications}</p>
              </div>
              <div>
                <span class="text-gray-600">Field count:</span>
                <p class="font-medium">${properties.field_count}</p>
              </div>
              <div>
                <span class="text-gray-600">Coverage:</span>
                <p class="font-medium">${(properties.coverage * 100).toFixed(1)}%</p>
              </div>
            </div>
          </div>
          
          ${properties.cell_count > 1 ? `
            <div class="mt-3 pt-3 border-t">
              <p class="text-xs text-gray-500">
                This cell aggregates data from ${properties.cell_count} smaller cells
              </p>
            </div>
          ` : ''}
        </div>
      `
      
      popup
        .setLngLat(e.lngLat)
        .setHTML(popupContent)
        .addTo(map.current!)
    })
    
  }, [selectedYear, heatmapMode, isLoaded, pmtilesBaseUrl])
  
  // Update layer visibility
  useEffect(() => {
    if (!map.current || !isLoaded) return
    
    const layerId = 'h3-pfas'
    const outlineLayerId = `${layerId}-outline`
    
    if (map.current.getLayer(layerId)) {
      map.current.setLayoutProperty(
        layerId,
        'visibility',
        showPFAS ? 'visible' : 'none'
      )
    }
    
    if (map.current.getLayer(outlineLayerId)) {
      map.current.setLayoutProperty(
        outlineLayerId,
        'visibility',
        showPFAS ? 'visible' : 'none'
      )
    }
  }, [showPFAS, isLoaded])
  
  return (
    <div className={`relative ${className}`}>
      <div ref={mapContainer} className="w-full h-full" />
      
      {/* Hover Tooltip */}
      {hoverInfo && (
        <div 
          className="absolute bg-white rounded-lg shadow-lg p-3 pointer-events-none z-20 max-w-xs"
          style={{
            left: mousePosition.x + 10,
            top: mousePosition.y - 10,
            transform: mousePosition.x > window.innerWidth - 300 ? 'translateX(-100%)' : 'none'
          }}
        >
          <div className="text-sm">
            <div className="font-medium text-gray-900 mb-1">
              {hoverInfo.summary}
            </div>
            <div className="space-y-1 text-xs text-gray-600">
              <div className="flex justify-between">
                <span>PFAS:</span>
                <span className="font-medium">{hoverInfo.pfas_grams.toFixed(2)} g</span>
              </div>
              <div className="flex justify-between">
                <span>Intensity:</span>
                <span className="font-medium">{hoverInfo.pfas_intensity.toFixed(3)} g/ha</span>
              </div>
              <div className="flex justify-between">
                <span>Fields:</span>
                <span className="font-medium">{hoverInfo.field_count}</span>
              </div>
              <div className="flex justify-between">
                <span>Area:</span>
                <span className="font-medium">{hoverInfo.area_ha.toFixed(1)} ha</span>
              </div>
              {hoverInfo.cell_count > 1 && (
                <div className="text-xs text-gray-500 mt-1 pt-1 border-t">
                  Aggregated from {hoverInfo.cell_count} cells
                </div>
              )}
            </div>
          </div>
        </div>
      )}
      
      {/* Map Controls */}
      <div className="absolute top-4 left-4 z-10">
        <div className="bg-white rounded-lg shadow-lg p-4 space-y-4">
          {/* Year Selector */}
          <div>
            <label className="block text-sm font-medium mb-2">Year</label>
            <select
              value={selectedYear}
              onChange={(e) => setSelectedYear(parseInt(e.target.value))}
              className="w-full p-2 border rounded-md text-sm"
            >
              {availableYears.map(year => (
                <option key={year} value={year}>{year}</option>
              ))}
            </select>
          </div>
          
          {/* Visualization Mode Toggle */}
          <div>
            <label className="block text-sm font-medium mb-2">Mode</label>
            <select
              value={heatmapMode}
              onChange={(e) => setHeatmapMode(e.target.value as 'pesticide' | 'pfas')}
              className="w-full p-2 border rounded-md text-sm"
            >
              <option value="pesticide">Pesticide</option>
              <option value="pfas">PFAS</option>
            </select>
          </div>
          
          {/* Layer Controls */}
          <div>
            <label className="block text-sm font-medium mb-2">Layers</label>
            <div className="space-y-2">
              <label className="flex items-center text-sm">
                <input
                  type="checkbox"
                  checked={showPFAS}
                  onChange={(e) => setHeatmapMode(e.target.checked ? 'pfas' : 'pesticide')}
                  className="mr-2"
                />
                Show PFAS Data
              </label>
            </div>
          </div>
        </div>
      </div>
      
      {/* Enhanced Legend */}
      <div className="absolute bottom-4 left-4 z-10">
        <div className="bg-white rounded-lg shadow-lg p-4">
          <h3 className="font-bold text-sm mb-2">PFAS Concentration</h3>
          <div className="space-y-1 text-xs">
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#d73027] mr-2"></div>
              <span>&gt; 100g</span>
            </div>
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#f46d43] mr-2"></div>
              <span>50-100g</span>
            </div>
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#fdae61] mr-2"></div>
              <span>10-50g</span>
            </div>
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#fee08b] mr-2"></div>
              <span>1-10g</span>
            </div>
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#e6f598] mr-2"></div>
              <span>0-1g</span>
            </div>
            <div className="flex items-center">
              <div className="w-4 h-4 bg-[#abdda4] mr-2"></div>
              <span>No data</span>
            </div>
          </div>
          
          <div className="mt-3 pt-3 border-t text-xs text-gray-600">
            <p className="mb-1">Resolution levels:</p>
            <div className="space-y-1">
              <div>7-8: Regional/County view</div>
              <div>9: Municipal view</div>
              <div>10: Field level detail</div>
            </div>
          </div>
        </div>
      </div>
      
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