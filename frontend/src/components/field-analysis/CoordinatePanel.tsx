'use client';

import React, { useState } from 'react';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  copyCoordinatesToClipboard,
} from './coordinateUtils';
import { MapPin, Copy, Check, Plane, Map } from 'lucide-react';

interface CoordinatePanelProps {
  coordinates: { lat: number; lng: number };
  onClose: () => void;
}

export function CoordinatePanel({
  coordinates,
  onClose,
}: CoordinatePanelProps) {
  const [copiedCoordinates, setCopiedCoordinates] = useState(false);

  // Handle coordinate copying
  const handleCopyCoordinates = async () => {
    const success = await copyCoordinatesToClipboard(
      coordinates.lat,
      coordinates.lng
    );

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
    <div
      className="h-full overflow-y-auto p-4 lg:p-6"
      onTouchStart={handleTouchStart}
    >
      {/* Mobile swipe indicator */}
      <div className="bg-muted mx-auto mb-4 h-1 w-12 rounded-full lg:hidden"></div>

      {/* Header */}
      <div className="mb-4 flex items-center justify-between lg:mb-6">
        <h2 className="text-foreground text-lg font-bold lg:text-xl">
          Koordinater
        </h2>
        <button
          onClick={onClose}
          className="hover:bg-muted active:bg-accent flex min-h-[44px] min-w-[44px] items-center justify-center rounded-full p-2 transition-colors"
          aria-label="Luk panel"
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

      {/* GPS Coordinates and Links */}
      <div className="mb-4">
        <div className="bg-primary/10 rounded-lg p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-primary flex items-center text-sm font-medium">
              <MapPin className="mr-1 h-4 w-4" />
              GPS Position
            </span>
            <button
              onClick={handleCopyCoordinates}
              className="bg-primary/20 text-primary hover:bg-primary/30 active:bg-primary/40 flex min-h-[32px] items-center rounded px-2 py-1 text-xs transition-colors"
              title="Kopier koordinater"
            >
              {copiedCoordinates ? (
                <>
                  <Check className="mr-1 h-3 w-3" />
                  Kopieret!
                </>
              ) : (
                <>
                  <Copy className="mr-1 h-3 w-3" />
                  Kopier
                </>
              )}
            </button>
          </div>
          <div className="text-primary/80 mb-3 font-mono text-xs">
            {formatWgs84Coordinates(coordinates.lat, coordinates.lng)}
          </div>
          <div className="space-y-2">
            <a
              href={generateSkraafotoUrl(coordinates.lat, coordinates.lng)}
              target="_blank"
              rel="noopener noreferrer"
              className="bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 block flex min-h-[36px] items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors"
            >
              <Plane className="mr-1 h-3 w-3" />
              Åbn i Skråfoto
            </a>
            <button
              onClick={() => {
                const googleMapsUrl = `https://www.google.com/maps?q=${coordinates.lat},${coordinates.lng}`;
                window.open(googleMapsUrl, '_blank');
              }}
              className="bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex min-h-[36px] w-full items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors"
            >
              <Map className="mr-1 h-3 w-3" />
              Google Maps
            </button>
          </div>
        </div>
      </div>

      <div className="text-muted-foreground text-xs italic">
        Klik på en landbrugsmark for at se detaljerede oplysninger.
      </div>
    </div>
  );
}
