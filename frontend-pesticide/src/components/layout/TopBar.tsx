'use client';

import { useState } from 'react';
import { SearchBar } from '@/components/controls/SearchBar';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { useDataState } from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import { Settings, PanelRightOpen, PanelRightClose, Menu, X, ChevronDown, ChevronUp } from 'lucide-react';

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
  const [showMobileControls, setShowMobileControls] = useState(false);
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
      {/* Main Top Bar - Mobile Optimized */}
      <div className="bg-slate-900 border-b border-slate-700 shadow-lg">
        <div className="px-4 sm:px-6 py-3 sm:py-4">
          <div className="flex items-center justify-between">
            
            {/* Left: Brand - Mobile Optimized */}
            <div className="flex items-center space-x-2 sm:space-x-3 flex-shrink-0">
              <div className="w-8 h-8 sm:w-10 sm:h-10 bg-red-600 rounded-lg flex items-center justify-center shadow-sm">
                <span className="text-white font-bold text-sm sm:text-lg">P</span>
              </div>
              <h1 className="text-sm sm:text-lg font-semibold text-white hidden xs:block">
                <span className="sm:hidden">Pesticide</span>
                <span className="hidden sm:inline">Pesticidkortet</span>
              </h1>
            </div>

            {/* Center: Search Bar - Mobile Optimized */}
            <div className="flex-1 mx-3 sm:mx-6 max-w-md sm:max-w-3xl">
              <SearchBar 
                onLocationSelect={onLocationSelect}
                placeholder={isMobile ? "Search locations..." : "Search Danish addresses, cities, regions..."}
                className="w-full"
              />
            </div>

            {/* Right: Actions - Mobile Optimized */}
            <div className="flex items-center space-x-1 sm:space-x-2 flex-shrink-0">
              {/* Mobile Controls Toggle - Mobile Only */}
              {isMobile && (
                <button
                  onClick={() => setShowMobileControls(!showMobileControls)}
                  className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition-colors touch-manipulation"
                >
                  {showMobileControls ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                </button>
              )}

              {/* Menu Button - Desktop and Mobile */}
              <button
                onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition-colors touch-manipulation"
              >
                {isMobileMenuOpen ? <X className="w-4 h-4 sm:w-5 sm:h-5" /> : <Menu className="w-4 h-4 sm:w-5 sm:h-5" />}
              </button>

              {/* Sidebar Toggle */}
              <button
                onClick={handleToggleSidebar}
                className={`p-2 rounded-lg transition-colors touch-manipulation ${
                  currentSidebarState
                    ? 'bg-blue-600 text-white' 
                    : 'bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white'
                }`}
              >
                {currentSidebarState ? <PanelRightClose className="w-4 h-4 sm:w-5 sm:h-5" /> : <PanelRightOpen className="w-4 h-4 sm:w-5 sm:h-5" />}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Controls Bar - Only shown on mobile when toggled */}
        {isMobile && showMobileControls && (
          <div className="bg-slate-800 border-t border-slate-700 px-4 py-3">
            <div className="space-y-3">
              {/* Data Mode */}
              <div className="flex flex-col space-y-2">
                <span className="text-xs font-medium text-slate-300 uppercase tracking-wide">Data Mode</span>
                <DataModeSelector variant="topbar" />
              </div>

              {/* Year Controls */}
              <div className="flex flex-col space-y-2">
                <span className="text-xs font-medium text-slate-300 uppercase tracking-wide">Year</span>
                <StepSlider />
              </div>
            </div>
          </div>
        )}

        {/* Desktop Controls Bar - Hidden on mobile */}
        {!isMobile && (
          <div className="bg-slate-800 border-t border-slate-700 px-6 py-3">
            <div className="flex items-center justify-between">
              
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
        )}
      </div>

      {/* Advanced Menu - Mobile and Desktop */}
      {isMobileMenuOpen && (
        <div className="bg-slate-700 border-b border-slate-600 px-4 sm:px-6 py-4">
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
              className={`w-full flex items-center justify-center space-x-2 p-3 rounded-lg transition-colors touch-manipulation ${
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