export const env = {
  NEXT_PUBLIC_DATA_URL:
    process.env.NEXT_PUBLIC_DATA_URL ?? 'https://data.pesticidkortet.dk/api/v1',
} as const;

/** Convenience re-export for common env var */
export const DATA_URL = env.NEXT_PUBLIC_DATA_URL;
