/**
 * Constants for PFAS scrollytelling — well positions, view states, and story data.
 * All coordinates verified from R2: silver/geus_clean_all, silver/grukos, silver/bmd
 */

export const SKRYDSTRUP_WELL = {
  dgu: '151. 2426',
  lng: 9.250338,
  lat: 55.247566,
  depthM: 5,
  detection: { date: '2021-09-24', conc: 4.88, unit: 'µg/L' },
} as const;

export const WELLS = [
  { dgu: '151. 2426', lng: 9.250338, lat: 55.247566, tfa: 4.88, depth: 5 },
  { dgu: '151. 2429', lng: 9.252405, lat: 55.248693, tfa: 2.01, depth: 4 },
  { dgu: '151. 2428', lng: 9.251949, lat: 55.248744, tfa: 1.43, depth: 6 },
  { dgu: '151. 2432', lng: 9.250884, lat: 55.248018, tfa: 0.71, depth: 5 },
  { dgu: '151. 2455', lng: 9.252073, lat: 55.248732, tfa: 0.69, depth: 21 },
  { dgu: '151. 2456', lng: 9.25128, lat: 55.248089, tfa: 0.41, depth: 10.3 },
  { dgu: '151. 1291', lng: 9.24638, lat: 55.247298, tfa: 0.075, depth: 89 },
] as const;

export const EXAMPLE_FIELD = {
  uuid: '534793F5-84D4-5FBF-8BAC-91C63E494474',
  crop: 'Vinterhvede',
  areaHa: 62.9,
  products: [
    { name: 'DFF', ingredient: 'diflufenican' },
    { name: 'Propulse SE 250', ingredient: 'fluopyram + prothioconazol' },
    { name: 'Mavrik 2F', ingredient: 'tau-fluvalinat' },
  ],
} as const;

export const STATS = {
  totalCatchments: 5826,
  monitored: 2113,
  unmonitored: 3713,
  pctMonitored: 36.3,
  pfasAnalyses: 441589,
  tfaDetectionRate: 100.0,
  tfaWells: 4789,
  fluorinatedProducts: 132,
  tfaFormingIngredients: 35,
} as const;

export type PfasScrollyStepId =
  | 'field'
  | 'product'
  | 'molecule'
  | 'tfa'
  | 'detection'
  | 'everywhere'
  | 'blindspot'
  | 'conclusion';

import type { ScrollyViewState } from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';

export const VIEWS: Record<PfasScrollyStepId, ScrollyViewState> = {
  field: { lng: 9.251, lat: 55.248, zoom: 13.5 },
  product: { lng: 9.251, lat: 55.248, zoom: 14 },
  molecule: { lng: 9.251, lat: 55.248, zoom: 13 },
  tfa: { lng: 9.251, lat: 55.248, zoom: 13 },
  detection: { lng: SKRYDSTRUP_WELL.lng, lat: SKRYDSTRUP_WELL.lat, zoom: 15 },
  everywhere: { lng: 10.5, lat: 56.0, zoom: 6.5 },
  blindspot: { lng: 8.82, lat: 55.91, zoom: 11 },
  conclusion: { lng: 10.5, lat: 56.0, zoom: 6.5 },
};
