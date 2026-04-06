import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import type { LngLatBoundsLike } from 'maplibre-gl';

const C = EXAMPLE.center;

export type DisaggStepId =
  | 'context'
  | 'location'
  | 'record'
  | 'overview'
  | 'fields'
  | 'match'
  | 'result'
  | 'summary'
  | 'scale';

export interface DisaggViewState {
  lng: number;
  lat: number;
  zoom: number;
  bounds?: LngLatBoundsLike;
}

/** Bounds enclosing all 3 example fields with padding for polygon extent */
const FIELD_BOUNDS: LngLatBoundsLike = [
  [9.374, 55.248],
  [9.401, 55.273],
];

export const VIEWS: Record<DisaggStepId, DisaggViewState> = {
  context: { lng: 10.5, lat: 56.0, zoom: 6.5 },
  location: { lng: 9.5, lat: 55.3, zoom: 9 },
  record: { lng: C[0], lat: C[1], zoom: 11 },
  overview: { lng: C[0], lat: C[1], zoom: 12.5 },
  fields: { lng: C[0], lat: C[1], zoom: 14, bounds: FIELD_BOUNDS },
  match: { lng: C[0], lat: C[1], zoom: 14, bounds: FIELD_BOUNDS },
  result: { lng: C[0], lat: C[1], zoom: 13.5, bounds: FIELD_BOUNDS },
  summary: { lng: C[0], lat: C[1], zoom: 12 },
  scale: { lng: 9.5, lat: 55.5, zoom: 10 },
};
