'use client';

import { useEffect, useState } from 'react';
import { PMTilesMap } from '@/components/map/PMTilesMap';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { TopBar } from '@/components/layout/TopBar';
import { DataSidebar } from '@/components/overlays/DataSidebar';
import { MobileBottomPanel } from '@/components/overlays/MobileBottomPanel';
import { useMapStore, useDataState, useLoadingState, useTooltipState, type YearSelection } from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import { pmtilesDiscovery } from '@/services/pmtiles-discovery';

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
      <TopBar
        showControls={showControls}
        setShowControls={setShowControls}
        showSidebar={showSidebar}
        setShowSidebar={setShowSidebar}
        showMobilePanel={showMobilePanel}
        setShowMobilePanel={setShowMobilePanel}
        onLocationSelect={handleLocationSelect}
      />

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