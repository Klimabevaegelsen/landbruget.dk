"use client";

import React, { useState } from "react";
import { formatWgs84Coordinates, generateSkraafotoUrl, copyCoordinatesToClipboard } from "./coordinateUtils";

interface CoordinatePanelProps {
  coordinates: { lat: number; lng: number };
  onClose: () => void;
}

export function CoordinatePanel({ coordinates, onClose }: CoordinatePanelProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  // Handle coordinate copying
  const handleCopyCoordinates = async () => {
    const success = await copyCoordinatesToClipboard(coordinates.lat, coordinates.lng);
    
    if (success) {
      setCopiedCoordinates(true);
      setTimeout(() => setCopiedCoordinates(false), 2000);
    }
  };

  // Handle swipe gestures for mobile
  const handleTouchStart = (e: React.TouchEvent) => {
    const touch = e.touches[0];
    const startX = touch.clientX;

    const handleTouchMove = (moveEvent: TouchEvent) => {
      const currentTouch = moveEvent.touches[0];
      const deltaX = currentTouch.clientX - startX;

      // Swipe right to close (threshold: 100px)
      if (deltaX > 100) {
        onClose();
        document.removeEventListener('touchmove', handleTouchMove);
        document.removeEventListener('touchend', handleTouchEnd);
      }
    };

    const handleTouchEnd = () => {
      document.removeEventListener('touchmove', handleTouchMove);
      document.removeEventListener('touchend', handleTouchEnd);
    };

    document.addEventListener('touchmove', handleTouchMove);
    document.addEventListener('touchend', handleTouchEnd);
  };

  return (
    <div className="p-4 lg:p-6 h-full overflow-y-auto" onTouchStart={handleTouchStart}>
      {/* Mobile swipe indicator */}
      <div className="lg:hidden w-12 h-1 bg-gray-300 rounded-full mx-auto mb-4"></div>

      {/* Header */}
      <div className="flex items-center justify-between mb-4 lg:mb-6">
        <h2 className="text-lg lg:text-xl font-bold text-gray-900">Koordinater</h2>
        <button
          onClick={onClose}
          className="p-2 hover:bg-gray-100 active:bg-gray-200 rounded-full transition-colors min-h-[44px] min-w-[44px] flex items-center justify-center"
          aria-label="Luk panel"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* GPS Coordinates and Links */}
      <div className="mb-4">
        <div className="bg-blue-50 rounded-lg p-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-blue-800">📍 GPS Position</span>
            <button
              onClick={handleCopyCoordinates}
              className="text-xs bg-blue-100 hover:bg-blue-200 active:bg-blue-300 text-blue-700 px-2 py-1 rounded transition-colors min-h-[32px] flex items-center"
              title="Kopier koordinater"
            >
              {copiedCoordinates ? "✓ Kopieret!" : "📋 Kopier"}
            </button>
          </div>
          <div className="text-xs text-blue-700 font-mono mb-3">
            {formatWgs84Coordinates(coordinates.lat, coordinates.lng)}
          </div>
          <div className="space-y-2">
            <a
              href={generateSkraafotoUrl(coordinates.lat, coordinates.lng)}
              target="_blank"
              rel="noopener noreferrer"
              className="block text-xs bg-blue-600 hover:bg-blue-700 active:bg-blue-800 text-white px-3 py-2 rounded transition-colors text-center font-medium min-h-[36px] flex items-center justify-center"
            >
              🛩️ Åbn i Skråfoto
            </a>
            <button
              onClick={() => {
                const googleMapsUrl = `https://www.google.com/maps?q=${coordinates.lat},${coordinates.lng}`;
                window.open(googleMapsUrl, '_blank');
              }}
              className="w-full text-xs bg-green-600 hover:bg-green-700 active:bg-green-800 text-white px-3 py-2 rounded transition-colors text-center font-medium min-h-[36px] flex items-center justify-center"
            >
              🗺️ Google Maps
            </button>
          </div>
        </div>
      </div>

      <div className="text-xs text-gray-500 italic">
        Klik på en landbrugsmark for at se detaljerede oplysninger.
      </div>
    </div>
  );
}
