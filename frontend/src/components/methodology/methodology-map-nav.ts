import type { MapRef } from '@vis.gl/react-maplibre';
import {
  VIEWS,
  type DisaggStepId,
} from '@/components/methodology/scrolly-disagg-views';
import { updateStepPaint } from '@/components/methodology/disagg-map-layers';

/** Animate the methodology map to the view for the given step. */
export function animateToStep(
  mapRef: React.RefObject<MapRef | null>,
  step: DisaggStepId,
  isMobile: boolean
) {
  const map = mapRef.current?.getMap();
  if (!map) return;
  map.stop();
  const v = VIEWS[step];
  const dur = (ms: number) => (isMobile ? ms * 0.5 : ms);
  if (v.bounds) {
    map.fitBounds(v.bounds, { padding: 60, duration: dur(1600) });
  } else {
    map.flyTo({
      center: [v.lng, v.lat],
      zoom: v.zoom,
      pitch: v.pitch ?? 0,
      bearing: v.bearing ?? 0,
      duration: dur(step === 'scale' ? 2000 : 1600),
      essential: true,
      curve: 1.2,
    });
  }
  try {
    updateStepPaint(map, step);
  } catch (err) {
    console.warn('[MethodologyMap] Paint update failed:', err);
  }
}
