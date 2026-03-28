'use client';

import React from 'react';

interface MobileControlsPanelProps {
  isOpen: boolean;
  onToggle: () => void;
  onClose: () => void;
  headerTitle: string;
  headerSubtitle?: string;
  children: React.ReactNode;
}

export function MobileControlsPanel({
  isOpen,
  onToggle,
  onClose,
  headerTitle,
  headerSubtitle,
  children,
}: MobileControlsPanelProps) {
  return (
    <>
      {/* Mobile Toggle Button */}
      <div className="pointer-events-auto absolute top-[max(1rem,env(safe-area-inset-top))] left-4 z-40 lg:hidden">
        <button
          onClick={onToggle}
          data-testid="toggle-mobile-controls-button"
          className="bg-background hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] touch-manipulation items-center justify-center rounded-lg p-3 shadow-lg transition-colors"
          aria-label="Toggle controls"
        >
          <svg
            className="h-6 w-6"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 6h16M4 12h16M4 18h16"
            />
          </svg>
        </button>
      </div>

      {/* Overlay Panel */}
      <div
        className={` ${isOpen ? 'block pt-[env(safe-area-inset-top)] pb-[env(safe-area-inset-bottom)]' : 'hidden'} bg-background fixed inset-0 z-50 h-full w-full overflow-y-auto shadow-lg lg:relative lg:inset-auto lg:z-10 lg:block lg:h-full lg:w-80 lg:pt-0 lg:pb-0 lg:shadow-lg`}
      >
        {/* Mobile close header */}
        <div className="bg-background sticky top-0 z-10 flex items-center justify-between border-b p-4 lg:hidden">
          <div>
            <h2 className="text-lg font-semibold">{headerTitle}</h2>
            {headerSubtitle && (
              <p className="text-muted-foreground text-sm">{headerSubtitle}</p>
            )}
          </div>
          <button
            onClick={onClose}
            data-testid="close-mobile-controls-button"
            className="hover:bg-muted/50 active:bg-muted flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2"
            aria-label="Luk kontrolpanel"
          >
            <svg
              className="h-5 w-5"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>

        {/* Desktop header */}
        <div className="bg-background hidden border-b p-4 lg:block">
          <h2 className="text-lg font-semibold">{headerTitle}</h2>
          {headerSubtitle && (
            <p className="text-muted-foreground text-sm">{headerSubtitle}</p>
          )}
        </div>

        {children}
      </div>

      {/* Backdrop */}
      {isOpen && (
        <div
          className="bg-opacity-50 bg-background fixed inset-0 z-40 lg:hidden"
          onClick={onClose}
        />
      )}
    </>
  );
}
