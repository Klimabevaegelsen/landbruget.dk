'use client';

import { Marker } from '@vis.gl/react-maplibre';

interface AddressMarkerProps {
  lat: number;
  lng: number;
}

export function AddressMarker({ lat, lng }: AddressMarkerProps) {
  return (
    <Marker longitude={lng} latitude={lat} anchor="center">
      <div className="bg-primary border-background h-4 w-4 rounded-full border-2 shadow-md" />
    </Marker>
  );
}
