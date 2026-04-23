export const env = {
  NEXT_PUBLIC_DATA_URL:
    process.env.NEXT_PUBLIC_DATA_URL ?? 'https://api.landbruget.dk/api/v1',
  NEXT_PUBLIC_PMTILES_BASE_URL:
    process.env.NEXT_PUBLIC_PMTILES_BASE_URL ?? 'https://api.landbruget.dk',
} as const;

/** Convenience re-export for common env var */
export const DATA_URL = env.NEXT_PUBLIC_DATA_URL;
export const PMTILES_BASE_URL = env.NEXT_PUBLIC_PMTILES_BASE_URL;
