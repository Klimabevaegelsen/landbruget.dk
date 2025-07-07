'use client';

import { useEffect, useState } from 'react';
import { PMTilesMap } from '@/components/map/PMTilesMap';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { SearchBar } from '@/components/controls/SearchBar';
import { DataSidebar } from '@/components/overlays/DataSidebar';
import { MobileBottomPanel } from '@/components/overlays/MobileBottomPanel';
import { useMapStore, useDataState, useLoadingState, useTooltipState, type YearSelection } from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import { pmtilesDiscovery } from '@/services/pmtiles-discovery';
import { Settings, PanelRightOpen, PanelRightClose } from 'lucide-react';

// Define HoverInfo interface to match the sidebar component
interface HoverInfo {
  layer: 'h3' | 'bnbo' | 'bbr';
  data: Record<string, unknown>;
  coordinate: [number, number];
  pixel: [number, number];
}

export default function Home() {
  const [isInitialized, setIsInitialized] = useState(false);
  const [isInitializing, setIsInitializing] = useState(true);
  const [showControls, setShowControls] = useState(false); // Start with controls hidden like London Underground
  const [showSidebar, setShowSidebar] = useState(true); // Start with sidebar visible
  
  // Store state
  const { selectedYear } = useDataState();
  const { error } = useLoadingState();
  const { showTooltip, tooltipData, tooltipPosition } = useTooltipState();
  const { isMobile, setIsMobile, showMobilePanel, setShowMobilePanel } = useUIStore();
  
  // Store actions
  const { 
    setAvailableYearOptions, 
    setError: mapSetError,
    clearError: mapClearError,
    flyToLocation
  } = useMapStore();

  // Detect mobile viewport
  useEffect(() => {
    const checkMobile = () => {
      const isMobileViewport = window.innerWidth < 768;
      setIsMobile(isMobileViewport);
      
      // Close sidebar on mobile, close mobile panel on desktop
      if (isMobileViewport) {
        setShowSidebar(false);
      } else {
        setShowMobilePanel(false);
      }
    };

    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, [setIsMobile, setShowMobilePanel]);

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
  const convertToHoverInfo = (tooltipData: Record<string, unknown>, position: { x: number; y: number }): HoverInfo | null => {
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

  // Get current hover info for sidebar/mobile panel - only when there's tooltip data
  const hoverInfo = showTooltip && tooltipData ? convertToHoverInfo(tooltipData, tooltipPosition) : null;

  const handleToggleSidebar = () => {
    if (isMobile) {
      setShowMobilePanel(!showMobilePanel);
    } else {
      setShowSidebar(!showSidebar);
    }
  };

  const handleCloseSidebar = () => {
    if (isMobile) {
      setShowMobilePanel(false);
    } else {
      setShowSidebar(false);
    }
  };

  const handleCloseMobilePanel = () => {
    setShowMobilePanel(false);
  };

  const handleLocationSelect = (location: { lat: number; lng: number; address: string }) => {
    console.log('Selected location:', location);
    // Navigate to the selected location on the map
    flyToLocation({
      lat: location.lat,
      lng: location.lng,
      zoom: 12 // Zoom to a reasonable level to see the area
    });
  };

  // Loading state
  if (isInitializing || !isInitialized) {
    console.log('🔄 Still loading - isInitializing:', isInitializing, 'isInitialized:', isInitialized);
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-white/40 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
          <p className="text-white text-lg font-medium">Loading PMTiles Map...</p>
          <p className="text-white/60 text-sm mt-2">Discovering latest data from GCS bucket</p>
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="text-red-400 text-4xl mb-4">⚠️</div>
          <p className="text-white text-lg font-medium">Error Loading Map</p>
          <p className="text-white/60 text-sm mt-2">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-black flex flex-col">
      {/* Top Bar - London Underground Style */}
      <div className="bg-black/90 backdrop-blur-md border-b border-white/10 px-4 py-3 flex items-center justify-between">
        <div className="flex items-center space-x-6">
          <div className="flex items-center space-x-3">
            <h1 className="text-xl font-bold text-white">PFAS Exposure Analysis</h1>
            <div className="text-sm text-gray-400">
              {selectedYear === 'total' ? 'All Years' : `Year ${selectedYear}`}
            </div>
          </div>
          
          {/* Search Bar - London Underground Style */}
          <div className="flex-1 max-w-md">
            <SearchBar 
              onLocationSelect={handleLocationSelect}
              placeholder="Search Danish addresses..."
              className="w-full"
            />
          </div>
          
          {/* Data Mode Selector - Top Bar Version */}
          <DataModeSelector variant="topbar" />
          
          {/* Step Slider for Year Selection */}
          <StepSlider />
        </div>
        
        <div className="flex items-center space-x-2">
          {/* Sidebar Toggle Button */}
          <button
            onClick={handleToggleSidebar}
            className={`p-2 rounded-lg transition-colors ${
              (isMobile ? showMobilePanel : showSidebar)
                ? 'bg-white/20 text-white border border-white/30' 
                : 'bg-black/20 text-white/60 border border-white/10 hover:bg-white/10 hover:text-white hover:border-white/20'
            }`}
            title={
              isMobile 
                ? (showMobilePanel ? 'Hide Details Panel' : 'Show Details Panel')
                : (showSidebar ? 'Hide Details Panel' : 'Show Details Panel')
            }
          >
            {(isMobile ? showMobilePanel : showSidebar) ? <PanelRightClose className="w-5 h-5" /> : <PanelRightOpen className="w-5 h-5" />}
          </button>
          
          {/* Advanced Controls Toggle */}
          <button
            onClick={() => setShowControls(!showControls)}
            className={`p-2 rounded-lg transition-colors ${
              showControls 
                ? 'bg-white/20 text-white border border-white/30' 
                : 'bg-black/20 text-white/60 border border-white/10 hover:bg-white/10 hover:text-white hover:border-white/20'
            }`}
            title={showControls ? 'Hide Advanced Controls' : 'Show Advanced Controls'}
          >
            <Settings className="w-5 h-5" />
          </button>
        </div>
      </div>

      <div className="flex-1 flex relative">
        {/* Left Sidebar - Advanced Controls (hidden by default) */}
        {showControls && (
          <div className="w-80 bg-black/80 backdrop-blur-md border-r border-white/10 overflow-y-auto">
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
                  {/* Basemap controls would go here */}
                  <div className="text-sm text-gray-400">Basemap controls coming soon...</div>
                </div>
              </div>
            </div>
          </div>
        )}
        
        {/* Map Container */}
        <div className={`flex-1 relative transition-all duration-300 ease-in-out ${
          !isMobile && showSidebar ? 'mr-96' : 'mr-0'
        }`}>
          <PMTilesMap className="w-full h-full" />
        </div>

        {/* Right Sidebar - Data Details (Desktop Only) */}
        {!isMobile && (
          <DataSidebar 
            hoverInfo={hoverInfo}
            onClose={handleCloseSidebar}
            isVisible={showSidebar}
          />
        )}
      </div>

      {/* Mobile Bottom Panel */}
      {isMobile && (
        <MobileBottomPanel 
          hoverInfo={hoverInfo}
          onClose={handleCloseMobilePanel}
          isVisible={showMobilePanel}
        />
      )}
    </div>
  );
} 