'use client';

import { Suspense } from 'react';
import { ResponsiveLayout } from '@/components/ui/responsive-layout';
import { MapPlaceholder } from '@/components/map/MapPlaceholder';
import { TimeControls } from '@/components/map/TimeControls';
import { HeatmapToggle } from '@/components/map/HeatmapToggle';
import { LayerControls } from '@/components/map/LayerControls';
import { DataPanel } from '@/components/overlays/DataPanel';
import { LoadingSpinner } from '@/components/ui/loading-spinner';
import { PMTilesMap } from '@/components/map/PMTilesMap'
import { KeplerMap } from '@/components/map/KeplerMap'
import { useState } from 'react'

function MapSection({ 
  visualizationMode, 
  availableYears 
}: { 
  visualizationMode: 'pmtiles' | 'kepler'
  availableYears: number[]
}) {
  
  return (
    <div className="map-container">
      {/* Placeholder for the actual map component (Developer 2's responsibility) */}
      <Suspense fallback={<LoadingSpinner />}>
        {visualizationMode === 'pmtiles' ? (
          <PMTilesMap
            className="w-full h-full"
            availableYears={availableYears}
            pmtilesBaseUrl="/pmtiles" // Adjust based on your CDN setup
          />
        ) : (
          <KeplerMap 
            selectedYear={2023}
            showPesticides={false}
            showPFAS={true}
            showBNBO={false}
            showBBR={false}
            cumulativeMode={false}
          />
        )}
      </Suspense>
      
      {/* UI Controls - Developer 3's responsibility */}
      <HeatmapToggle />
      <LayerControls />
      <TimeControls />
      <DataPanel />
    </div>
  );
}

export default function HomePage() {
  const [visualizationMode, setVisualizationMode] = useState<'pmtiles' | 'kepler'>('pmtiles')
  
  // Available years for H3 PFAS data
  const availableYears = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
  
  return (
    <ResponsiveLayout>
      <div className="flex flex-col h-screen">
        {/* Header */}
        <header className="bg-white shadow-sm border-b p-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">
                H3 PFAS Exposure Visualization
              </h1>
              <p className="text-gray-600">
                Explore PFAS contamination patterns across Denmark
              </p>
            </div>
            
            {/* Visualization Mode Toggle */}
            <div className="flex items-center space-x-4">
              <label className="text-sm font-medium text-gray-700">
                Visualization Mode:
              </label>
              <select
                value={visualizationMode}
                onChange={(e) => setVisualizationMode(e.target.value as 'pmtiles' | 'kepler')}
                className="px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="pmtiles">PMTiles (Fast)</option>
                <option value="kepler">Kepler.gl (Flexible)</option>
              </select>
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="flex-1 relative">
          <MapSection 
            visualizationMode={visualizationMode}
            availableYears={availableYears}
          />
          
          {/* Info Panel */}
          <div className="absolute top-4 right-4 bg-white rounded-lg shadow-lg p-4 max-w-sm z-10">
            <h3 className="font-bold text-lg mb-2">About This Visualization</h3>
            <div className="space-y-2 text-sm text-gray-600">
              <p>
                This map shows PFAS (Per- and polyfluoroalkyl substances) contamination 
                across Denmark using H3 hexagonal grid system.
              </p>
              
              {visualizationMode === 'pmtiles' ? (
                <div className="bg-blue-50 p-3 rounded-md">
                  <p className="font-medium text-blue-900">PMTiles Mode</p>
                  <p className="text-blue-700">
                    Ultra-fast rendering using vector tiles. Perfect for smooth 
                    pan/zoom exploration of large datasets.
                  </p>
                </div>
              ) : (
                <div className="bg-green-50 p-3 rounded-md">
                  <p className="font-medium text-green-900">Kepler.gl Mode</p>
                  <p className="text-green-700">
                    Flexible analysis with dynamic filtering and advanced 
                    visualization options.
                  </p>
                </div>
              )}
              
              <div className="pt-2 border-t">
                <p className="text-xs text-gray-500">
                  Data covers {availableYears.length} years ({availableYears[0]}-{availableYears[availableYears.length - 1]}) 
                  with ~1.9M hexagons per year.
                </p>
              </div>
            </div>
          </div>
        </main>

        {/* Footer */}
        <footer className="bg-gray-50 border-t p-4">
          <div className="flex items-center justify-between text-sm text-gray-600">
            <div>
              <p>
                Data source: H3 PFAS Exposure Pipeline | 
                Visualization: {visualizationMode === 'pmtiles' ? 'MapLibre GL + PMTiles' : 'Kepler.gl'}
              </p>
            </div>
            <div className="flex items-center space-x-4">
              <span>Performance: {visualizationMode === 'pmtiles' ? '🚀 Fast' : '⚡ Flexible'}</span>
              <span>Scale: {visualizationMode === 'pmtiles' ? '♾️ Unlimited' : '👥 Limited'}</span>
            </div>
          </div>
        </footer>
      </div>
    </ResponsiveLayout>
  );
} 