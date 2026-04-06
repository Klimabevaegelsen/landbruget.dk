/**
 * Constants for groundwater scrollytelling — well positions, view states, and story data.
 * All coordinates verified from R2: silver/geus_clean_pesticides, silver/grukos
 */

export const ESPE_WELL = {
  dgu: '155. 1899',
  lng: 10.42178,
  lat: 55.20651,
  depthM: 6,
  detection: { date: '2019-05-10', conc: 490, unit: 'µg/L' },
} as const;

export const VEJEN_WELL = {
  dgu: '132. 1056',
  lng: 9.15636,
  lat: 55.49367,
  depthM: 19.3,
  detection: { date: '2021-10-14', conc: 3.2, unit: 'µg/L' },
} as const;

export type ScrollyStepId =
  | 'intro'
  | 'espe'
  | 'kilden'
  | 'rejsen'
  | 'fundet'
  | 'mcpa'
  | 'fortidslevn'
  | 'billede';

export interface ScrollyViewState {
  lng: number;
  lat: number;
  zoom: number;
  pitch?: number;
  bearing?: number;
}

export const VIEWS: Record<ScrollyStepId, ScrollyViewState> = {
  intro: { lng: 10.5, lat: 56.0, zoom: 6.5, pitch: 0, bearing: 0 },
  espe: { lng: 10.454, lat: 55.205, zoom: 12, pitch: 0, bearing: 0 },
  kilden: { lng: 10.454, lat: 55.205, zoom: 12.5, pitch: 0, bearing: 0 },
  rejsen: { lng: 10.46, lat: 55.204, zoom: 12.5, pitch: 50, bearing: -15 },
  fundet: {
    lng: ESPE_WELL.lng,
    lat: ESPE_WELL.lat,
    zoom: 13.5,
    pitch: 50,
    bearing: -15,
  },
  mcpa: {
    lng: VEJEN_WELL.lng,
    lat: VEJEN_WELL.lat,
    zoom: 12,
    pitch: 50,
    bearing: -15,
  },
  fortidslevn: {
    lng: VEJEN_WELL.lng,
    lat: VEJEN_WELL.lat,
    zoom: 13,
    pitch: 50,
    bearing: -15,
  },
  billede: { lng: 10.5, lat: 56.0, zoom: 6.5, pitch: 0, bearing: 0 },
};

/** Steps where each case-study's layers are visible on the map. */
export const ESPE_STEPS = new Set<ScrollyStepId>([
  'espe',
  'kilden',
  'rejsen',
  'fundet',
  'billede',
]);
export const VEJEN_STEPS = new Set<ScrollyStepId>([
  'mcpa',
  'fortidslevn',
  'billede',
]);
export const ESPE_WELL_STEPS = new Set<ScrollyStepId>(['fundet', 'billede']);
export const VEJEN_WELL_STEPS = new Set<ScrollyStepId>([
  'fortidslevn',
  'billede',
]);

/** Steps where 3D pitch is active. */
export const PITCHED_STEPS = new Set<ScrollyStepId>([
  'rejsen',
  'fundet',
  'mcpa',
  'fortidslevn',
]);

/** Per-step flyTo animation overrides (duration in ms, curve). */
export const FLY_TO_OVERRIDES: Partial<
  Record<ScrollyStepId, { duration?: number; curve?: number }>
> = {
  espe: { duration: 2200, curve: 1.0 },
  kilden: { duration: 1400, curve: 1.4 },
  rejsen: { duration: 2000, curve: 1.2 },
  mcpa: { duration: 2800, curve: 1.6 },
  billede: { duration: 2000, curve: 1.2 },
};
