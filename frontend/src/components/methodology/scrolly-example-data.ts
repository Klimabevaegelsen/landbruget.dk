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
  cropName: 'Silomajs med gr\u00e6sudl\u00e6g',
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
