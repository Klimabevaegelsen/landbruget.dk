'use client';

import { useState } from 'react';
import { MapPin, Copy, Check, Plane, Map } from 'lucide-react';
import {
  formatWgs84Coordinates,
  generateSkraafotoUrl,
  generateGoogleMapsUrl,
  copyCoordinatesToClipboard,
} from './coordinateUtils';

interface FieldCoordinatesProps {
  coordinates: { lat: number; lng: number };
  showHeading: boolean;
  /** Whether layout uses side-by-side buttons (true) or stacked (false) */
  inlineButtons: boolean;
}

export function FieldCoordinates({
  coordinates,
  showHeading,
  inlineButtons,
}: FieldCoordinatesProps) {
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

  const btnClass = `bg-primary text-primary-foreground hover:bg-primary/90 active:bg-primary/80 flex min-h-[36px] items-center justify-center rounded px-3 py-2 text-center text-xs font-medium transition-colors`;

  return (
    <div className="mb-6">
      {showHeading && (
        <h3 className="text-foreground mb-4 text-lg font-semibold">
          Koordinater
        </h3>
      )}
      <div className="bg-primary/10 rounded-lg p-4">
        <div className="mb-2 flex items-center justify-between">
          <span className="text-primary flex items-center text-sm font-medium">
            <MapPin className="mr-1 h-4 w-4" />
            GPS Position
          </span>
          <button
            onClick={handleCopy}
            data-testid="copy-coordinates-button"
            className="bg-primary/20 text-primary hover:bg-primary/30 active:bg-primary/40 flex min-h-[32px] items-center rounded px-2 py-1 text-xs transition-colors"
            title="Kopier koordinater"
          >
            {copied ? (
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
        <div className="text-primary/80 mb-2 font-mono text-xs">
          {formatWgs84Coordinates(coordinates.lat, coordinates.lng)}
        </div>
        <div className={inlineButtons ? 'flex space-x-2' : 'space-y-2'}>
          <a
            href={generateSkraafotoUrl(coordinates.lat, coordinates.lng)}
            target="_blank"
            rel="noopener noreferrer"
            className={`${btnClass} ${inlineButtons ? 'flex-1' : 'block'}`}
          >
            <Plane className="mr-1 h-3 w-3" />
            Åbn i Skråfoto
          </a>
          <button
            onClick={() => {
              window.open(
                generateGoogleMapsUrl(coordinates.lat, coordinates.lng),
                '_blank'
              );
            }}
            data-testid="open-google-maps-button"
            className={`${btnClass} ${inlineButtons ? 'flex-1' : 'w-full'}`}
          >
            <Map className="mr-1 h-3 w-3" />
            Google Maps
          </button>
        </div>
      </div>
    </div>
  );
}
