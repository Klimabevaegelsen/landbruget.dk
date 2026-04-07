import { featureToFieldSummary } from '@/components/pesticidkort/map-utils';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import type { Map as MaplibreMap } from 'maplibre-gl';

/** Wire up click + cursor handlers for field and overview layers. */
export function setupFieldClickHandlers(
  map: MaplibreMap,
  lat: number,
  lng: number,
  onFieldClick: React.RefObject<
    ((uuid: string, data: NearbyFieldSummary) => void) | undefined
  >
) {
  // Detail layer (high zoom, full properties)
  map.on('click', 'fields-fill', (e) => {
    const feat = e.features?.[0];
    if (!onFieldClick.current || !feat) return;
    const uuid = String(feat.properties.field_uuid ?? '');
    if (!uuid) return;
    const fieldData = featureToFieldSummary(
      feat.properties as Record<string, unknown>,
      lat,
      lng,
      e.lngLat.lat,
      e.lngLat.lng
    );
    onFieldClick.current(uuid, fieldData);
  });

  // Overview layer (low zoom) — zoom in to show detail
  map.on('click', 'fields-overview-fill', (e) => {
    if (!e.features?.[0]) return;
    map.flyTo({
      center: [e.lngLat.lng, e.lngLat.lat],
      zoom: 13,
      duration: 800,
    });
  });

  for (const layer of ['fields-fill', 'fields-overview-fill']) {
    map.on('mouseenter', layer, () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', layer, () => {
      map.getCanvas().style.cursor = '';
    });
  }
}
