import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import type { LngLatBoundsLike } from 'maplibre-gl';

const C = EXAMPLE.center;

export type DisaggStepId =
  | 'context'
  | 'location'
  | 'regneark'
  | 'fields'
  | 'match'
  | 'virkelighed'
  | 'scale';

export interface DisaggViewState {
  lng: number;
  lat: number;
  zoom: number;
  pitch?: number;
  bearing?: number;
  bounds?: LngLatBoundsLike;
}

/** Bounds enclosing all 3 example fields with padding for polygon extent */
const FIELD_BOUNDS: LngLatBoundsLike = [
  [9.374, 55.248],
  [9.401, 55.273],
];

export const VIEWS: Record<DisaggStepId, DisaggViewState> = {
  context: { lng: 10.5, lat: 56.0, zoom: 6, pitch: 0, bearing: 0 },
  location: { lng: 9.5, lat: 55.26, zoom: 10 },
  regneark: { lng: C[0] - 0.02, lat: C[1], zoom: 11 },
  fields: { lng: C[0], lat: C[1], zoom: 14.5, bounds: FIELD_BOUNDS },
  match: { lng: C[0], lat: C[1], zoom: 14.5, bounds: FIELD_BOUNDS },
  virkelighed: { lng: C[0], lat: C[1], zoom: 14, bounds: FIELD_BOUNDS },
  scale: { lng: 10.5, lat: 56.0, zoom: 6 },
};

export const DISAGG_STEP_ORDER: DisaggStepId[] = [
  'context',
  'location',
  'regneark',
  'fields',
  'match',
  'virkelighed',
  'scale',
];
