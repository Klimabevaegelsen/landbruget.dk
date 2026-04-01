import type { MapRef } from '@vis.gl/react-maplibre';

const HIGHLIGHT_LAYER = 'fields-highlight';
const HIGHLIGHT_SOURCE = 'fields';
const SOURCE_LAYER = 'field_analysis';

/**
 * Add or update a highlight outline on a specific field by UUID.
 * Pass `null` to clear the highlight.
 */
export function highlightField(
  mapRef: MapRef | null,
  fieldUuid: string | null
) {
  if (!mapRef) return;
  const map = mapRef.getMap();

  if (!map.getLayer(HIGHLIGHT_LAYER)) {
    map.addLayer({
      id: HIGHLIGHT_LAYER,
      source: HIGHLIGHT_SOURCE,
      'source-layer': SOURCE_LAYER,
      type: 'line',
      paint: {
        'line-color': '#3a9d5d',
        'line-width': 3,
        'line-opacity': 1,
      },
      filter: ['==', 'field_uuid', ''],
    });
  }

  map.setFilter(
    HIGHLIGHT_LAYER,
    fieldUuid ? ['==', 'field_uuid', fieldUuid] : ['==', 'field_uuid', '']
  );
}

/** Fly the map to a field's rendered features by UUID. */
export function flyToField(mapRef: MapRef | null, fieldUuid: string) {
  if (!mapRef) return;
  const map = mapRef.getMap();
  const features = map.queryRenderedFeatures(undefined, {
    layers: ['fields-fill'],
    filter: ['==', 'field_uuid', fieldUuid],
  });
  if (features.length === 0) return;

  const coords = features.flatMap((f) => {
    const g = f.geometry;
    if (g.type === 'Polygon') return g.coordinates.flat();
    if (g.type === 'MultiPolygon') return g.coordinates.flat(2);
    return [];
  });

  if (coords.length === 0) return;

  let minLng = Infinity,
    minLat = Infinity,
    maxLng = -Infinity,
    maxLat = -Infinity;
  for (const [lng, lat] of coords) {
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  }

  map.flyTo({
    center: [(minLng + maxLng) / 2, (minLat + maxLat) / 2],
    zoom: Math.max(map.getZoom(), 15),
    duration: 800,
  });
}
