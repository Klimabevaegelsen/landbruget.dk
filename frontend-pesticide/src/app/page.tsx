'use client';

import { useEffect, useState } from 'react';
import { PMTilesMap } from '@/components/map/PMTilesMap';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { useMapStore, useDataState, useLayerVisibility, useLoadingState, type YearSelection } from '@/stores/map-store';
import { pmtilesDiscovery } from '@/services/pmtiles-discovery';
import { Settings, Eye, EyeOff } from 'lucide-react';

export default function Home() {
  const [isInitialized, setIsInitialized] = useState(false);
  const [isInitializing, setIsInitializing] = useState(true);
  const [showControls, setShowControls] = useState(true);
  
  // Store state
  const { selectedYear, selectedDataMode, availableYearOptions } = useDataState();
  const { showBNBOLayer } = useLayerVisibility();
  const { error } = useLoadingState();
  
  // Store actions
  const { 
    setAvailableYearOptions, 
    toggleBNBOLayer,
    setError: mapSetError,
    clearError: mapClearError
  } = useMapStore();

  // Initialize the application
  useEffect(() => {
    const initialize = async () => {
      try {
        console.log('🚀 Starting initialization...');
        setIsInitializing(true);
        
        console.log('📡 Getting data availability...');
        const availability = await pmtilesDiscovery.getDataAvailability();
        console.log('✅ Data availability:', availability);
        
        // Create year options including 'total' option
        const yearOptions: YearSelection[] = [...availability.years, 'total'];
        setAvailableYearOptions(yearOptions);
        
        setIsInitialized(true);
        mapClearError();
        console.log('✅ Initialization complete');
      } catch (err) {
        console.error('❌ Error initializing application:', err);
        mapSetError('Failed to initialize application');
      } finally {
        console.log('🏁 Setting loading to false');
        setIsInitializing(false);
      }
    };

    initialize();
  }, [setAvailableYearOptions, mapSetError, mapClearError]);

  // Loading state
  if (isInitializing || !isInitialized) {
    console.log('🔄 Still loading - isInitializing:', isInitializing, 'isInitialized:', isInitialized);
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-white text-lg font-medium">Loading PMTiles Map...</p>
          <p className="text-gray-400 text-sm mt-2">Discovering latest data from GCS bucket</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-center max-w-md mx-auto p-6">
          <div className="text-red-400 text-6xl mb-4">⚠️</div>
          <h2 className="text-white text-xl font-semibold mb-2">Something went wrong</h2>
          <p className="text-gray-400 mb-4">{error}</p>
          <button
            onClick={() => window.location.reload()}
            className="px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
          >
            Reload Application
          </button>
        </div>
      </div>
    );
  }

  // Get year count for display
  const yearCount = availableYearOptions.filter(year => typeof year === 'number').length;

  return (
    <div className="min-h-screen bg-gray-900 text-white flex flex-col">
      {/* Top Header */}
      <div className="bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-4 z-50">
        <div className="flex items-center justify-between">
          {/* Left: Title */}
          <div>
            <h1 className="text-xl font-bold text-white">Danish Agricultural Pesticide Analysis</h1>
            <p className="text-sm text-gray-400">PMTiles visualization • {yearCount} years of data + cumulative</p>
          </div>
          
          {/* Center: Year Selection */}
          <div className="flex items-center">
            <StepSlider />
          </div>
          
          {/* Right: Controls Toggle */}
          <div className="flex items-center space-x-4">
            <button
              onClick={toggleBNBOLayer}
              className={`flex items-center space-x-2 px-3 py-2 rounded-lg transition-all ${
                showBNBOLayer 
                  ? 'bg-green-600 hover:bg-green-700' 
                  : 'bg-gray-700 hover:bg-gray-600'
              }`}
            >
              {showBNBOLayer ? <Eye className="w-4 h-4" /> : <EyeOff className="w-4 h-4" />}
              <span className="text-sm">BNBO</span>
            </button>
            
            <button
              onClick={() => setShowControls(!showControls)}
              className="p-2 rounded-lg bg-gray-700 hover:bg-gray-600 transition-all"
            >
              <Settings className="w-5 h-5" />
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex relative">
        {/* Left Sidebar - Controls */}
        {showControls && (
          <div className="w-80 bg-gray-800/95 backdrop-blur-sm border-r border-gray-700 overflow-y-auto">
            <DataModeSelector className="m-4" />
            
            {/* Additional Info Panel */}
            <div className="m-4 p-4 bg-gray-700/50 rounded-lg">
              <h3 className="text-sm font-semibold text-white mb-3">Current View</h3>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-400">Year:</span>
                  <span className="font-medium text-white">
                    {selectedYear === 'total' ? 'Cumulative (All Years)' : selectedYear}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">Data Mode:</span>
                  <span className="font-medium text-white capitalize">{selectedDataMode.replace('_', ' ')}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-400">BNBO Layer:</span>
                  <span className={`font-medium ${showBNBOLayer ? 'text-green-400' : 'text-gray-400'}`}>
                    {showBNBOLayer ? 'Visible' : 'Hidden'}
                  </span>
                </div>
              </div>
              
              <div className="mt-4 pt-3 border-t border-gray-600">
                <h4 className="text-xs font-semibold text-gray-300 mb-2">Usage Tips</h4>
                <ul className="text-xs text-gray-400 space-y-1">
                  <li>• Zoom out: Municipality-level data</li>
                  <li>• Zoom in: H3 cell-level detail</li>
                  <li>• Hover for detailed information</li>
                  <li>• Click for expanded data view</li>
                  <li>• Use "Total" for cumulative analysis</li>
                </ul>
              </div>
            </div>
          </div>
        )}
        
        {/* Map Container */}
        <div className="flex-1 relative">
          <PMTilesMap className="w-full h-full" />
          
          {/* Map Controls Overlay */}
          <div className="absolute top-4 right-4 z-40">
            <div className="bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg">
              <div className="text-xs text-gray-600 text-center">
                <div className="font-medium">Zoom Level</div>
                <div className="text-gray-500">Auto-switching layers</div>
              </div>
            </div>
          </div>
          
          {/* Legend Overlay */}
          <div className="absolute bottom-4 left-4 z-40">
            <div className="bg-white/90 backdrop-blur-sm rounded-lg p-3 shadow-lg max-w-xs">
              <div className="text-xs text-gray-800">
                <div className="font-medium mb-1">Layer Switching</div>
                <div className="space-y-1">
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 bg-blue-500 rounded"></div>
                    <span>Kommune (zoom 4-8)</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="w-3 h-3 bg-red-500 rounded"></div>
                    <span>H3 Cells (zoom 9+)</span>
                  </div>
                  {showBNBOLayer && (
                    <div className="flex items-center space-x-2">
                      <div className="w-3 h-3 bg-green-500 rounded"></div>
                      <span>BNBO Protected Areas</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
} 