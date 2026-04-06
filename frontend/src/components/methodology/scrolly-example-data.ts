/**
 * Real disaggregation example extracted from R2 gold layer.
 *
 * Source: gold/pesticide_disaggregation_2022_2023 (row 268465)
 * Extracted: 2026-04-05 via backend/scripts/extract_methodology_example.py
 */
export const EXAMPLE = {
  year: 2022,
  fieldYear: 2023,
  sjiRow: 268465,
  cvr: '41996528',
  cropName: 'Silomajs med græsudlæg',
  municipality: 'Haderslev',
  pesticide: {
    name: 'Roundup PowerMax',
    fullName: 'Roundup PowerMax (tid. MON 79991)',
    regNr: '48-47',
    totalDose: 51.165,
    unit: 'L',
    reportedAreaHa: 38.47,
    dosePerHa: 1.33,
  },
  fields: [
    {
      uuid: '0D402F86-D47F-5D56-8777-0B0A65FEEC9F',
      areaHa: 16.24,
      dose: 21.599,
      centroid: [9.389477, 55.264266] as const,
    },
    {
      uuid: '9F926B9C-1622-52CD-8C0C-26FA79A790DA',
      areaHa: 14.43,
      dose: 19.192,
      centroid: [9.380717, 55.253974] as const,
    },
    {
      uuid: '9E860EB7-9194-515A-8B9A-AD21D62D2736',
      areaHa: 7.8,
      dose: 10.374,
      centroid: [9.394685, 55.266776] as const,
    },
  ],
  /** Map center (average of field centroids) */
  center: [9.388293, 55.261672] as const,
  confidence: 1.0,
  areaDeviationPct: 0.0,
  allocationMethod: 'Marker_ApplicationAreaToTotalFieldArea_FieldProportional',
} as const;

export type ExampleField = (typeof EXAMPLE.fields)[number];

/** Real SJI journal rows from CVR 41996528, extracted from R2 gold layer. */
export const SJI_JOURNAL_ROWS = [
  { post: 268458, product: 'Balaya', dose: 3.65, unit: 'L', area: 6.08 },
  { post: 268459, product: 'Prosaro', dose: 12.03, unit: 'L', area: 34.82 },
  {
    post: 268460,
    product: 'Harmony 50 SX',
    dose: 0.22,
    unit: 'L',
    area: 38.47,
  },
  { post: 268461, product: 'Balaya', dose: 24.37, unit: 'L', area: 34.82 },
  { post: 268462, product: 'Mavrik', dose: 1.4, unit: 'L', area: 17.54 },
  { post: 268463, product: 'Kerb 400 SC', dose: 9.8, unit: 'L', area: 7.84 },
  { post: 268464, product: 'Signum', dose: 5.45, unit: 'L', area: 6.42 },
  {
    post: 268465,
    product: 'Roundup PowerMax',
    dose: 51.17,
    unit: 'L',
    area: 38.47,
  },
  {
    post: 268466,
    product: 'Harmony 50 SX',
    dose: 0.06,
    unit: 'L',
    area: 10.36,
  },
  { post: 268467, product: 'Cossack OD', dose: 8.0, unit: 'L', area: 34.82 },
  { post: 268468, product: 'Agil 100 EC', dose: 5.8, unit: 'L', area: 7.84 },
  { post: 268469, product: 'Pictor Active', dose: 5.49, unit: 'L', area: 7.84 },
  { post: 268470, product: 'Caryx', dose: 5.49, unit: 'L', area: 7.84 },
  { post: 268471, product: 'Primus XL', dose: 13.0, unit: 'L', area: 34.82 },
  { post: 268472, product: 'Lamdex', dose: 4.7, unit: 'L', area: 7.84 },
  {
    post: 268473,
    product: 'Propulse SE 250',
    dose: 2.74,
    unit: 'L',
    area: 7.84,
  },
  { post: 268474, product: 'Betanal', dose: 6.6, unit: 'L', area: 6.42 },
  { post: 268475, product: 'Primus XL', dose: 4.56, unit: 'L', area: 6.08 },
  { post: 268476, product: 'MaisTer', dose: 0.72, unit: 'L', area: 38.47 },
  { post: 268477, product: 'Kerb 400 SC', dose: 1.82, unit: 'L', area: 6.08 },
  {
    post: 268478,
    product: 'Roundup PowerMax',
    dose: 5.8,
    unit: 'L',
    area: 6.42,
  },
] as const;

export const TARGET_POST = EXAMPLE.sjiRow;
