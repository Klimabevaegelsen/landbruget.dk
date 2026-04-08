import { useState, useRef } from 'react';
import type { FieldHoverData } from '@/components/pesticidkort/types';
import { setupFieldHoverHandlers } from '@/components/pesticidkort/map-events';
import type { Map as MaplibreMap } from 'maplibre-gl';

export function useFieldHover() {
  const [hoverData, setHoverData] = useState<FieldHoverData | null>(null);
  const hoverRef = useRef(setHoverData);
  hoverRef.current = setHoverData;

  const attachHover = (map: MaplibreMap) => {
    setupFieldHoverHandlers(map, hoverRef);
  };

  return { hoverData, attachHover };
}
