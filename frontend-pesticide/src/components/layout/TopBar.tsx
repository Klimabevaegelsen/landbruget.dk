'use client';

import { useState } from 'react';
import { SearchBar } from '@/components/controls/SearchBar';
import { DataModeSelector } from '@/components/controls/DataModeSelector';
import { StepSlider } from '@/components/controls/StepSlider';
import { useDataState } from '@/stores/map-store';
import { useUIStore } from '@/stores/ui-store';
import {
  Settings,
  PanelRightOpen,
  PanelRightClose,
  Menu,
  X,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';

interface TopBarProps {
  showControls: boolean;
  setShowControls: (show: boolean) => void;
  showSidebar: boolean;
  setShowSidebar: (show: boolean) => void;
  showMobilePanel: boolean;
  setShowMobilePanel: (show: boolean) => void;
  onLocationSelect: (location: {
    lat: number;
    lng: number;
    address: string;
  }) => void;
}

export function TopBar({
  showControls,
  setShowControls,
  showSidebar,
  setShowSidebar,
  showMobilePanel,
  setShowMobilePanel,
  onLocationSelect,
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
      <div className="border-b border-slate-700 bg-slate-900 shadow-lg">
        <div className="px-4 py-3 sm:px-6 sm:py-4">
          <div className="flex items-center justify-between">
            {/* Left: Brand - Mobile Optimized */}
            <div className="flex flex-shrink-0 items-center space-x-2 sm:space-x-3">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-red-600 shadow-sm sm:h-10 sm:w-10">
                <span className="text-sm font-bold text-white sm:text-lg">
                  P
                </span>
              </div>
              <h1 className="xs:block hidden text-sm font-semibold text-white sm:text-lg">
                <span className="sm:hidden">Pesticide</span>
                <span className="hidden sm:inline">Pesticidkortet</span>
              </h1>
            </div>

            {/* Center: Search Bar - Mobile Optimized */}
            <div className="mx-3 max-w-md flex-1 sm:mx-6 sm:max-w-3xl">
              <SearchBar
                onLocationSelect={onLocationSelect}
                placeholder={
                  isMobile
                    ? 'Search locations...'
                    : 'Search Danish addresses, cities, regions...'
                }
                className="w-full"
              />
            </div>

            {/* Right: Actions - Mobile Optimized */}
            <div className="flex flex-shrink-0 items-center space-x-1 sm:space-x-2">
              {/* Mobile Controls Toggle - Mobile Only */}
              {isMobile && (
                <button
                  onClick={() => setShowMobileControls(!showMobileControls)}
                  className="touch-manipulation rounded-lg bg-slate-800 p-2 text-slate-300 transition-colors hover:bg-slate-700 hover:text-white"
                >
                  {showMobileControls ? (
                    <ChevronUp className="h-4 w-4" />
                  ) : (
                    <ChevronDown className="h-4 w-4" />
                  )}
                </button>
              )}

              {/* Menu Button - Desktop and Mobile */}
              <button
                onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                className="touch-manipulation rounded-lg bg-slate-800 p-2 text-slate-300 transition-colors hover:bg-slate-700 hover:text-white"
              >
                {isMobileMenuOpen ? (
                  <X className="h-4 w-4 sm:h-5 sm:w-5" />
                ) : (
                  <Menu className="h-4 w-4 sm:h-5 sm:w-5" />
                )}
              </button>

              {/* Sidebar Toggle */}
              <button
                onClick={handleToggleSidebar}
                className={`touch-manipulation rounded-lg p-2 transition-colors ${
                  currentSidebarState
                    ? 'bg-blue-600 text-white'
                    : 'bg-slate-800 text-slate-300 hover:bg-slate-700 hover:text-white'
                }`}
              >
                {currentSidebarState ? (
                  <PanelRightClose className="h-4 w-4 sm:h-5 sm:w-5" />
                ) : (
                  <PanelRightOpen className="h-4 w-4 sm:h-5 sm:w-5" />
                )}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Controls Bar - Only shown on mobile when toggled */}
        {isMobile && showMobileControls && (
          <div className="border-t border-slate-700 bg-slate-800 px-4 py-3">
            <div className="space-y-3">
              {/* Data Mode */}
              <div className="flex flex-col space-y-2">
                <span className="text-xs font-medium tracking-wide text-slate-300 uppercase">
                  Data Mode
                </span>
                <DataModeSelector variant="topbar" />
              </div>

              {/* Year Controls */}
              <div className="flex flex-col space-y-2">
                <span className="text-xs font-medium tracking-wide text-slate-300 uppercase">
                  Year
                </span>
                <StepSlider />
              </div>
            </div>
          </div>
        )}

        {/* Desktop Controls Bar - Hidden on mobile */}
        {!isMobile && (
          <div className="border-t border-slate-700 bg-slate-800 px-6 py-3">
            <div className="flex items-center justify-between">
              {/* Left: Data Mode */}
              <div className="flex items-center space-x-4">
                <span className="text-sm font-medium text-slate-300">
                  Data Mode:
                </span>
                <DataModeSelector variant="topbar" />
              </div>

              {/* Right: Year Controls */}
              <div className="flex items-center space-x-4">
                <span className="text-sm font-medium text-slate-300">
                  Year:
                </span>
                <StepSlider />
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Advanced Menu - Mobile and Desktop */}
      {isMobileMenuOpen && (
        <div className="border-b border-slate-600 bg-slate-700 px-4 py-4 sm:px-6">
          <div className="space-y-4">
            {/* Current Status */}
            <div className="rounded-lg bg-slate-600 p-3">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-slate-300">
                  Current View
                </span>
                <span className="text-sm font-semibold text-white">
                  {selectedYear === 'total'
                    ? 'All Years'
                    : `Year ${selectedYear}`}
                </span>
              </div>
            </div>

            {/* Settings Toggle */}
            <button
              onClick={() => {
                setShowControls(!showControls);
                setIsMobileMenuOpen(false);
              }}
              className={`flex w-full touch-manipulation items-center justify-center space-x-2 rounded-lg p-3 transition-colors ${
                showControls
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-600 text-slate-300 hover:bg-slate-500'
              }`}
            >
              <Settings className="h-5 w-5" />
              <span className="text-sm font-medium">
                {showControls
                  ? 'Hide Advanced Controls'
                  : 'Show Advanced Controls'}
              </span>
            </button>
          </div>
        </div>
      )}
    </>
  );
}
