'use client';

import { useEffect, useState } from 'react';
import { PMTilesMap } from '@/components/map/PMTilesMap';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { DataSidebar } from '@/components/overlays/DataSidebar';
import { useMapStore, useDataState, useLayerVisibility, useLoadingState, useTooltipState, type YearSelection } from '@/stores/map-store';
import { pmtilesDiscovery } from '@/services/pmtiles-discovery';
import { Settings, Eye, EyeOff } from 'lucide-react';
import { BasemapToggle } from '@/components/controls/BasemapToggle';

// Define HoverInfo interface to match the sidebar component
interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: any;
  coordinate: [number, number];
  pixel: [number, number];
}

export default function Home() {
  const [isInitialized, setIsInitialized] = useState(false);
  const [isInitializing, setIsInitializing] = useState(true);
  const [showControls, setShowControls] = useState(false); // Start with controls hidden like London Underground
  const [showSidebar, setShowSidebar] = useState(false);
  
  // Store state
  const { selectedYear, selectedDataMode, availableYearOptions } = useDataState();
  const { error } = useLoadingState();
  const { showTooltip, tooltipData, tooltipPosition } = useTooltipState();
  
  // Store actions
  const { 
    setAvailableYearOptions, 
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

  // Convert tooltip data to HoverInfo format for sidebar
  const convertToHoverInfo = (tooltipData: any, position: { x: number; y: number }): HoverInfo | null => {
    if (!tooltipData) return null;
    
    // Determine layer type based on data
    let layer: 'h3' | 'bnbo' | 'bbr';
    if (tooltipData.bnbo_id || tooltipData.status) {
      layer = 'bnbo';
    } else if (tooltipData.kommune_code || tooltipData.kommune_name) {
      layer = 'h3'; // Kommune data is shown as h3 for now
    } else {
      layer = 'h3';
    }
    
    return {
      layer,
      data: tooltipData,
      coordinate: [0, 0], // We don't have coordinate from tooltip
      pixel: [position.x, position.y]
    };
  };

  // Handle tooltip changes to show/hide sidebar
  useEffect(() => {
    if (showTooltip && tooltipData) {
      setShowSidebar(true);
    } else {
      setShowSidebar(false);
    }
  }, [showTooltip, tooltipData]);

  const handleCloseSidebar = () => {
    setShowSidebar(false);
  };

  // Get current hover info for sidebar
  const hoverInfo = convertToHoverInfo(tooltipData, tooltipPosition);

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
      {/* Top Header - Inspired by London Underground Live */}
      <div className="bg-gray-800/95 backdrop-blur-sm border-b border-gray-700 px-6 py-3 z-50">
        <div className="flex items-center justify-between">
          {/* Left: Title */}
          <div className="flex items-center space-x-4">
            <div>
              <h1 className="text-lg font-bold text-white">Danish Agricultural Pesticide Analysis</h1>
              <p className="text-xs text-gray-400">PMTiles visualization • {yearCount} years of data + cumulative</p>
            </div>
          </div>
          
          {/* Center: Data Mode Selector */}
          <div className="flex items-center space-x-6">
            <DataModeSelector variant="topbar" />
            <div className="h-6 w-px bg-gray-600"></div>
            <StepSlider />
          </div>
          
          {/* Right: Controls Toggle */}
          <div className="flex items-center space-x-4">
            <button
              onClick={() => setShowControls(!showControls)}
              className={`p-2 rounded-lg transition-all ${
                showControls 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-gray-700 hover:bg-gray-600 text-gray-300'
              }`}
            >
              <Settings className="w-4 h-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex relative">
        {/* Left Sidebar - Advanced Controls (hidden by default) */}
        {showControls && (
          <div className="w-80 bg-gray-800/95 backdrop-blur-sm border-r border-gray-700 overflow-y-auto">
            <div className="p-4 space-y-6">
              <h3 className="text-lg font-semibold text-white mb-4">Advanced Controls</h3>
              
              {/* Data Mode Selector */}
              <div>
                <h4 className="text-sm font-medium text-gray-300 mb-2">Data Mode</h4>
                <DataModeSelector variant="sidebar" />
              </div>
              
              {/* Layer Visibility Controls */}
              <div>
                <h4 className="text-sm font-medium text-gray-300 mb-3">Layer Visibility</h4>
                <div className="space-y-3">
                  {/* Basemap Toggle */}
                  <BasemapToggle />
                </div>
              </div>
            </div>
          </div>
        )}
        
        {/* Map Container */}
        <div className="flex-1 relative">
          <PMTilesMap className="w-full h-full" />
        </div>

        {/* Right Sidebar - Data Details */}
        <DataSidebar 
          hoverInfo={hoverInfo}
          onClose={handleCloseSidebar}
          isVisible={showSidebar}
        />
      </div>
    </div>
  );
} 