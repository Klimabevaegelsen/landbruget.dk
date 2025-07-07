'use client';

import { useState } from 'react';
import { SearchBar } from '@/components/controls/SearchBar';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { useDataState } from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import { Settings, PanelRightOpen, PanelRightClose, Menu, X } from 'lucide-react';

interface TopBarProps {
  showControls: boolean;
  setShowControls: (show: boolean) => void;
  showSidebar: boolean;
  setShowSidebar: (show: boolean) => void;
  showMobilePanel: boolean;
  setShowMobilePanel: (show: boolean) => void;
  onLocationSelect: (location: { lat: number; lng: number; address: string }) => void;
}

export function TopBar({
  showControls,
  setShowControls,
  showSidebar,
  setShowSidebar,
  showMobilePanel,
  setShowMobilePanel,
  onLocationSelect
}: TopBarProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const { selectedYear } = useDataState();
  const { isMobile } = useUIStore();

  const handleToggleSidebar = () => {
    if (isMobile) {
      setShowMobilePanel(!showMobilePanel);
    } else {
      setShowSidebar(!showSidebar);
    }
  };

  const currentSidebarState = isMobile ? showMobilePanel : showSidebar;

  return (
    <>
      {/* Clean, Professional Top Bar - Search Focused */}
      <div className="bg-slate-900 border-b border-slate-700 px-6 py-4 shadow-lg">
        <div className="flex items-center justify-between max-w-none">
          
          {/* Left: Compact Brand */}
          <div className="flex items-center space-x-3 flex-shrink-0">
            <div className="w-10 h-10 bg-red-600 rounded-lg flex items-center justify-center shadow-sm">
              <span className="text-white font-bold text-lg">P</span>
            </div>
            <h1 className="text-lg font-semibold text-white hidden sm:block">Pesticidkortet</h1>
          </div>

          {/* Center: Prominent Search Bar */}
          <div className="flex-1 max-w-3xl mx-6">
            <SearchBar 
              onLocationSelect={onLocationSelect}
              placeholder="Search Danish addresses, cities, regions..."
              className="w-full"
            />
          </div>

          {/* Right: Essential Actions Only */}
          <div className="flex items-center space-x-2 flex-shrink-0">
            {/* Menu Button */}
            <button
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
              className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition-colors"
            >
              <Menu className="w-5 h-5" />
            </button>

            {/* Sidebar Toggle */}
            <button
              onClick={handleToggleSidebar}
              className={`p-2 rounded-lg transition-colors ${
                currentSidebarState
                  ? 'bg-blue-600 text-white' 
                  : 'bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white'
              }`}
            >
              {currentSidebarState ? <PanelRightClose className="w-5 h-5" /> : <PanelRightOpen className="w-5 h-5" />}
            </button>
          </div>
        </div>
      </div>

      {/* Secondary Controls Bar */}
      <div className="bg-slate-800 border-b border-slate-700 px-6 py-3">
        <div className="flex items-center justify-between max-w-none">
          
          {/* Left: Data Mode */}
          <div className="flex items-center space-x-4">
            <span className="text-sm font-medium text-slate-300">Data Mode:</span>
            <DataModeSelector variant="topbar" />
          </div>

          {/* Right: Year Controls */}
          <div className="flex items-center space-x-4">
            <span className="text-sm font-medium text-slate-300">Year:</span>
            <StepSlider />
          </div>
        </div>
      </div>

      {/* Mobile Menu */}
      {isMobileMenuOpen && (
        <div className="bg-slate-700 border-b border-slate-600 px-6 py-4">
          <div className="space-y-4">
            
            {/* Current Status */}
            <div className="bg-slate-600 rounded-lg p-3">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-slate-300">Current View</span>
                <span className="text-sm font-semibold text-white">
                  {selectedYear === 'total' ? 'All Years' : `Year ${selectedYear}`}
                </span>
              </div>
            </div>
            
            {/* Settings Toggle */}
            <button
              onClick={() => {
                setShowControls(!showControls);
                setIsMobileMenuOpen(false);
              }}
              className={`w-full flex items-center justify-center space-x-2 p-3 rounded-lg transition-colors ${
                showControls 
                  ? 'bg-blue-600 text-white' 
                  : 'bg-slate-600 text-slate-300 hover:bg-slate-500'
              }`}
            >
              <Settings className="w-5 h-5" />
              <span className="text-sm font-medium">
                {showControls ? 'Hide Advanced Controls' : 'Show Advanced Controls'}
              </span>
            </button>
          </div>
        </div>
      )}
    </>
  );
} 