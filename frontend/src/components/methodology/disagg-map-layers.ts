import type { Map as MaplibreMap } from 'maplibre-gl';
import type { DisaggStepId } from '@/components/methodology/scrolly-disagg-views';

const HIGHLIGHT_STEPS = new Set<DisaggStepId>([
  'fields',
  'match',
  'virkelighed',
  'scale',
]);
const BURDEN_STEPS = new Set<DisaggStepId>(['virkelighed', 'scale']);
const BURDEN_COLOR = [
  'interpolate',
  ['linear'],
  ['coalesce', ['get', 'total_pesticide_belastning'], 0],
  0,
  '#6abf69',
  2,
  '#d4c54a',
  5,
  '#d89135',
  10,
  '#c4512c',
];

type MapInstance = MaplibreMap;

/** Add the highlight-fill and highlight-line layers for example fields. */
export function addHighlightLayers(map: MapInstance, uuids: string[]) {
  const src = { source: 'fields', 'source-layer': 'field_analysis' } as const;
  const t = { duration: 600, delay: 0 };

  for (const id of ['fields-fill', 'fields-overview-fill']) {
    if (!map.getLayer(id)) continue;
    map.setPaintProperty(id, 'fill-color', '#a3b18a');
    map.setPaintProperty(id, 'fill-opacity', 0);
    map.setPaintProperty(id, 'fill-opacity-transition', t);
  }
  for (const id of ['fields-outline', 'fields-overview-outline']) {
    if (!map.getLayer(id)) continue;
    map.setPaintProperty(id, 'line-opacity', 0);
  }

  map.addLayer({
    id: 'method-highlight-fill',
    ...src,
    type: 'fill',
    filter: ['in', 'field_uuid', ...uuids],
    paint: {
      'fill-color': '#6abf69',
      'fill-opacity': 0,
      'fill-opacity-transition': t,
    },
  });
  map.addLayer({
    id: 'method-highlight',
    ...src,
    type: 'line',
    filter: ['in', 'field_uuid', ...uuids],
    paint: {
      'line-color': '#f59e0b',
      'line-width': 3,
      'line-opacity': 0,
      'line-opacity-transition': { duration: 500, delay: 0 },
    },
  });
}

/** Update layer paint properties for the current scrolly step. */
export function updateStepPaint(map: MapInstance, step: DisaggStepId) {
  const highlight = HIGHLIGHT_STEPS.has(step);
  const burden = BURDEN_STEPS.has(step);
  const color = burden ? BURDEN_COLOR : '#6abf69';
  const isScale = step === 'scale';
  const closeUp = highlight && !isScale;

  // Surrounding fields: light green fill + visible dark-green borders in close-up
  const fillColor = isScale ? BURDEN_COLOR : '#d4e4c1';
  const fillOpacity = isScale ? 0.6 : closeUp ? 0.5 : 0.08;
  for (const id of ['fields-fill', 'fields-overview-fill']) {
    if (!map.getLayer(id)) continue;
    map.setPaintProperty(id, 'fill-color', fillColor as unknown as string);
    map.setPaintProperty(id, 'fill-opacity', fillOpacity);
  }
  const lineColor = closeUp ? '#4a6741' : '#5f6b80';
  const lineOpacity = isScale ? 0.4 : closeUp ? 0.8 : 0.06;
  const lineWidth = closeUp ? 1.5 : 0.5;
  for (const id of ['fields-outline', 'fields-overview-outline']) {
    if (!map.getLayer(id)) continue;
    map.setPaintProperty(id, 'line-color', lineColor);
    map.setPaintProperty(id, 'line-opacity', lineOpacity);
    map.setPaintProperty(id, 'line-width', lineWidth);
  }

  // Highlighted example fields
  map.setPaintProperty(
    'method-highlight-fill',
    'fill-color',
    color as unknown as string
  );
  map.setPaintProperty(
    'method-highlight-fill',
    'fill-opacity',
    highlight ? 0.7 : isScale ? 0 : 0.12
  );
  map.setPaintProperty('method-highlight', 'line-opacity', highlight ? 1 : 0);
}
