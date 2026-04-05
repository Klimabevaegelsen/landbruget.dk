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
  | 'fields'
  | 'ingredient'
  | 'soil'
  | 'vadose'
  | 'detection'
  | 'doseresponse'
  | 'metabolite'
  | 'conclusion';

export interface ScrollyViewState {
  lng: number;
  lat: number;
  zoom: number;
}

export const VIEWS: Record<ScrollyStepId, ScrollyViewState> = {
  fields: { lng: 10.46, lat: 55.202, zoom: 13 },
  ingredient: { lng: 10.46, lat: 55.202, zoom: 14 },
  soil: { lng: 10.46, lat: 55.202, zoom: 13 },
  vadose: { lng: 10.46, lat: 55.202, zoom: 13 },
  detection: { lng: ESPE_WELL.lng, lat: ESPE_WELL.lat, zoom: 14.5 },
  doseresponse: { lng: ESPE_WELL.lng, lat: ESPE_WELL.lat, zoom: 13 },
  metabolite: { lng: VEJEN_WELL.lng, lat: VEJEN_WELL.lat, zoom: 12 },
  conclusion: { lng: 9.8, lat: 55.35, zoom: 8 },
};
