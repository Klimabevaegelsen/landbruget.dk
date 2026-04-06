import { EXAMPLE } from '@/components/methodology/scrolly-example-data';

const C = EXAMPLE.center;

export type DisaggStepId =
  | 'context'
  | 'location'
  | 'record'
  | 'overview'
  | 'fields'
  | 'match'
  | 'result'
  | 'summary';

export interface DisaggViewState {
  lng: number;
  lat: number;
  zoom: number;
}

export const VIEWS: Record<DisaggStepId, DisaggViewState> = {
  context: { lng: 10.5, lat: 56.0, zoom: 6.5 },
  location: { lng: 9.5, lat: 55.3, zoom: 9 },
  record: { lng: C[0], lat: C[1], zoom: 11 },
  overview: { lng: C[0], lat: C[1], zoom: 12.5 },
  fields: { lng: C[0], lat: C[1], zoom: 14 },
  match: { lng: C[0], lat: C[1], zoom: 14.5 },
  result: { lng: C[0], lat: C[1], zoom: 13.5 },
  summary: { lng: C[0], lat: C[1], zoom: 12 },
};
