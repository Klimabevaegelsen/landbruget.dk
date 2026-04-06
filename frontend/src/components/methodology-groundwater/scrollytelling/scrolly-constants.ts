/**
 * Constants for groundwater scrollytelling — well positions, view states, and story data.
 * All coordinates verified from R2: silver/geus_clean_pesticides, silver/grukos
 */

export const ESPE_WELL = {
  dgu: '155. 1899',
  lng: 10.42178,
  lat: 55.20651,
  depthM: 6,
  detection: { date: '2019-05-10', conc: 490, unit: '\u00b5g/L' },
} as const;

export const VEJEN_WELL = {
  dgu: '132. 1056',
  lng: 9.15636,
  lat: 55.49367,
  depthM: 19.3,
  detection: { date: '2021-10-14', conc: 3.2, unit: '\u00b5g/L' },
} as const;

export type ScrollyStepId =
  | 'intro'
  | 'location'
  | 'opland'
  | 'fields'
  | 'ingredient'
  | 'soil'
  | 'vadose'
  | 'detection'
  | 'doseresponse'
  | 'transition'
  | 'metabolite'
  | 'conclusion';

export interface ScrollyViewState {
  lng: number;
  lat: number;
  zoom: number;
}

export const VIEWS: Record<ScrollyStepId, ScrollyViewState> = {
  intro: { lng: 10.5, lat: 56.0, zoom: 6.5 },
  location: { lng: 10.46, lat: 55.25, zoom: 9 },
  opland: { lng: 10.454, lat: 55.205, zoom: 11.5 },
  fields: { lng: 10.463, lat: 55.201, zoom: 11.8 },
  ingredient: { lng: 10.46, lat: 55.202, zoom: 14 },
  soil: { lng: 10.46, lat: 55.205, zoom: 13.5 },
  vadose: { lng: 10.44, lat: 55.204, zoom: 12.5 },
  detection: { lng: ESPE_WELL.lng, lat: ESPE_WELL.lat, zoom: 14.5 },
  doseresponse: { lng: 10.46, lat: 55.22, zoom: 11 },
  transition: { lng: 9.8, lat: 55.35, zoom: 9 },
  metabolite: { lng: VEJEN_WELL.lng, lat: VEJEN_WELL.lat, zoom: 12 },
  conclusion: { lng: 9.8, lat: 55.35, zoom: 8 },
};

/** Steps where each case-study's layers are visible on the map. */
export const ESPE_STEPS = new Set<ScrollyStepId>([
  'opland',
  'fields',
  'ingredient',
  'soil',
  'vadose',
  'detection',
  'doseresponse',
  'conclusion',
]);
export const VEJEN_STEPS = new Set<ScrollyStepId>(['metabolite', 'conclusion']);
export const ESPE_WELL_STEPS = new Set<ScrollyStepId>([
  'detection',
  'doseresponse',
  'conclusion',
]);
export const VEJEN_WELL_STEPS = new Set<ScrollyStepId>([
  'metabolite',
  'conclusion',
]);

/** Per-step flyTo animation overrides (duration in ms, curve). */
export const FLY_TO_OVERRIDES: Partial<
  Record<ScrollyStepId, { duration?: number; curve?: number }>
> = {
  opland: { duration: 2200, curve: 1.0 },
  fields: { duration: 1400, curve: 1.4 },
  doseresponse: { duration: 2000 },
  transition: { duration: 2800, curve: 1.6 },
};
