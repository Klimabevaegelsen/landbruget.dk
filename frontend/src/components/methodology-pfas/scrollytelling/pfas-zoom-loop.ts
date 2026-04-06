import { useEffect } from 'react';
import type { MapRef } from '@vis.gl/react-maplibre';

export function useZoomLoop(
  mapRef: React.RefObject<MapRef | null>,
  active: boolean
) {
  useEffect(() => {
    if (!active) return;
    const map = mapRef.current?.getMap();
    if (!map) return;
    let rafId: number;
    const tick = () => {
      const z = map.getZoom();
      if (z > 5.5) map.setZoom(z - 0.002);
      else map.setZoom(6.5);
      rafId = requestAnimationFrame(tick);
    };
    rafId = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafId);
  }, [mapRef, active]);
}
