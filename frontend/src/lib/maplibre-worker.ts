import { setWorkerUrl } from 'maplibre-gl';

/**
 * maplibre-gl v6 loads its Web Worker via `new URL(..., import.meta.url)`. Under
 * Next.js + Turbopack, `import.meta.url` is not an http(s) URL, so maplibre's
 * default `getWorkerUrl()` returns an empty string and the worker is created
 * with the page URL — it fails to start, the style never finishes loading, and
 * the map `load` event never fires.
 *
 * Point maplibre at the worker bundle served from `/public/maplibre/` (copied
 * there by `scripts/copy-maplibre-worker.mjs`). The worker imports its sibling
 * `./maplibre-gl-shared.mjs`, which is served from the same directory.
 *
 * Imported for its side effect at module load, before any `<Map>` mounts.
 */
if (typeof window !== 'undefined') {
  setWorkerUrl('/maplibre/maplibre-gl-worker.mjs');
}
