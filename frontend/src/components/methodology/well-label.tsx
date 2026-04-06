'use client';

import { Marker } from '@vis.gl/react-maplibre';

interface WellLabelProps {
  lng: number;
  lat: number;
  dgu: string;
  value: string;
  unit: string;
  depth?: string;
}

export function WellLabel({
  lng,
  lat,
  dgu,
  value,
  unit,
  depth,
}: WellLabelProps) {
  return (
    <Marker longitude={lng} latitude={lat} anchor="left" offset={[12, 0]}>
      <div className="bg-card/90 pointer-events-none rounded-md border px-2 py-1 shadow-sm backdrop-blur-sm">
        <p className="text-foreground text-[11px] leading-tight font-semibold">
          DGU {dgu}
        </p>
        <p className="text-destructive font-mono text-[10px] leading-tight">
          {value} {unit}
          {depth && <span className="text-muted-foreground"> · {depth}</span>}
        </p>
      </div>
    </Marker>
  );
}
