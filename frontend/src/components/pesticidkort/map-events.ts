import { featureToFieldSummary } from '@/components/pesticidkort/map-utils';
import type {
  NearbyFieldSummary,
  FieldHoverData,
} from '@/components/pesticidkort/types';
import type { Map as MaplibreMap } from 'maplibre-gl';

/** Update a PMTiles source URL on an existing map. */
export function updateSourceUrl(map: MaplibreMap, name: string, url: string) {
  const src = map.getSource(name);
  if (src && 'setUrl' in src)
    (src as unknown as { setUrl: (u: string) => void }).setUrl(
      `pmtiles://${url}`
    );
}

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

/** Wire up mousemove/leave on the detail layer for hover tooltip. */
export function setupFieldHoverHandlers(
  map: MaplibreMap,
  setHoverData: React.RefObject<
    ((data: FieldHoverData | null) => void) | undefined
  >
) {
  let rafPending = false;

  map.on('mousemove', 'fields-fill', (e) => {
    if (rafPending || !setHoverData.current) return;
    rafPending = true;
    requestAnimationFrame(() => {
      rafPending = false;
      const feat = e.features?.[0];
      if (!feat || !setHoverData.current) return;
      const p = feat.properties;
      setHoverData.current({
        cropName: String(p.crop_name ?? 'Ukendt'),
        cvrNumber: p.cvr_number ? String(p.cvr_number) : undefined,
        burden: Number(p.total_pesticide_belastning ?? 0),
        x: e.point.x,
        y: e.point.y,
      });
    });
  });

  map.on('mouseleave', 'fields-fill', () => {
    setHoverData.current?.(null);
  });
}
