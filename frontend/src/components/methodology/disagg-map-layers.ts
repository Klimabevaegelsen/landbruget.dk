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

type MapInstance = maplibregl.Map;

/** Add the highlight-fill and highlight-line layers for example fields. */
export function addHighlightLayers(map: MapInstance, uuids: string[]) {
  const src = { source: 'fields', 'source-layer': 'field_analysis' } as const;
  const t = { duration: 600, delay: 0 };

  map.setPaintProperty('fields-fill', 'fill-color', '#a3b18a');
  map.setPaintProperty('fields-fill', 'fill-opacity', 0);
  map.setPaintProperty('fields-fill', 'fill-opacity-transition', t);
  map.setPaintProperty('fields-outline', 'line-opacity', 0);

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

  map.setPaintProperty('fields-fill', 'fill-opacity', highlight ? 0.1 : 0.08);
  map.setPaintProperty(
    'fields-outline',
    'line-opacity',
    highlight ? 0.15 : 0.06
  );
  map.setPaintProperty('fields-outline', 'line-width', 0.5);
  map.setPaintProperty('method-highlight-fill', 'fill-color', color);
  map.setPaintProperty(
    'method-highlight-fill',
    'fill-opacity',
    highlight ? 0.7 : step === 'scale' ? 0.4 : 0.12
  );
  map.setPaintProperty('method-highlight', 'line-opacity', highlight ? 1 : 0);
}
