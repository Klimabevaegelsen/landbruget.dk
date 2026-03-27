'use client';

import React, { useState } from 'react';
import { MapPin, Copy, Check, Plane, Map } from 'lucide-react';
import { Card } from '@/components/ui/card';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  generateGoogleMapsUrl,
  copyCoordinatesToClipboard,
} from '@/components/field-analysis/coordinateUtils';

interface CoordinatesCardProps {
  coordinates: { lat: number; lng: number };
}

export function CoordinatesCard({ coordinates }: CoordinatesCardProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    const success = await copyCoordinatesToClipboard(
      coordinates.lat,
      coordinates.lng
    );
    if (success) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <Card className="p-4 lg:p-6">
      <h3 className="text-foreground mb-3 flex items-center gap-2 text-base font-semibold lg:text-lg">
        <MapPin className="h-4 w-4 lg:h-5 lg:w-5" />
        Koordinater
      </h3>
      <div className="bg-primary/10 rounded-lg p-3 lg:p-4">
        <div className="mb-2 flex items-center justify-between">
          <span className="text-primary text-sm font-medium lg:text-base">
            GPS Koordinater
          </span>
          <button
            onClick={handleCopy}
            data-testid="copy-coordinates-button"
            className="touch-target bg-primary/20 text-primary hover:bg-primary/30 active:bg-primary/40 flex items-center rounded px-2 py-1 text-xs transition-colors lg:px-3 lg:py-2 lg:text-sm"
            title="Kopier koordinater"
          >
            {copied ? (
              <>
                <Check className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                Kopieret!
              </>
            ) : (
              <>
                <Copy className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
                Kopier
              </>
            )}
          </button>
        </div>
        <div className="text-primary/80 mb-3 font-mono text-xs lg:text-sm">
          {formatWgs84Coordinates(coordinates.lat, coordinates.lng)}
        </div>
        <div className="flex space-x-2">
          <a
            href={generateSkraafotoUrl(coordinates.lat, coordinates.lng)}
            target="_blank"
            rel="noopener noreferrer"
            data-testid="skraafoto-link"
            className="touch-target bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex flex-1 items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors lg:text-sm"
          >
            <Plane className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
            Skråfoto
          </a>
          <a
            href={generateGoogleMapsUrl(coordinates.lat, coordinates.lng)}
            target="_blank"
            rel="noopener noreferrer"
            data-testid="google-maps-link"
            className="touch-target bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex flex-1 items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors lg:text-sm"
          >
            <Map className="mr-1 h-3 w-3 lg:h-4 lg:w-4" />
            Google Maps
          </a>
        </div>
      </div>
    </Card>
  );
}
