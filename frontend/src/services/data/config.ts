import { env } from '@/lib/env';

/**
 * Base URL for pre-computed JSON API files on R2 CDN.
 * All data is static, updated weekly by the api_export pipeline.
 */
const DATA_URL = env.NEXT_PUBLIC_DATA_URL;

/**
 * Fetch pre-computed JSON from R2 CDN.
 * Uses force-cache since data is static and updates weekly.
 */
export async function dataFetch<T>(path: string): Promise<T> {
  const url = `${DATA_URL}${path}`;
  const response = await fetch(url, {
    cache: 'force-cache',
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Data fetch failed: ${url} (${response.status})`);
  }

  return response.json() as Promise<T>;
}
