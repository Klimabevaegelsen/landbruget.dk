'use client';

import { Marker } from '@vis.gl/react-maplibre';
import { cn } from '@/lib/utils';

interface MapLabelProps {
  lng: number;
  lat: number;
  text: string;
  subtext?: string;
  variant?: 'default' | 'muted' | 'alert';
}

export function MapLabel({
  lng,
  lat,
  text,
  subtext,
  variant = 'default',
}: MapLabelProps) {
  return (
    <Marker longitude={lng} latitude={lat} anchor="center">
      <div
        className={cn(
          'pointer-events-none rounded-md px-2 py-1 text-center shadow-sm backdrop-blur-sm',
          variant === 'default' && 'bg-card/90 border',
          variant === 'muted' && 'bg-muted/80 border',
          variant === 'alert' && 'bg-destructive/90 text-destructive-foreground'
        )}
      >
        <p className="text-foreground text-[11px] leading-tight font-semibold">
          {text}
        </p>
        {subtext && (
          <p className="text-muted-foreground text-[9px] leading-tight">
            {subtext}
          </p>
        )}
      </div>
    </Marker>
  );
}
