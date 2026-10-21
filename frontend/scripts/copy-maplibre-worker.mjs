// Copies maplibre-gl v6's Web Worker bundle into public/ so it can be served at
// a stable URL and referenced via `setWorkerUrl()` (see src/lib/maplibre-worker.ts).
//
// maplibre-gl v6 loads its worker via `new URL(..., import.meta.url)`, which
// Turbopack/Next cannot resolve — the worker ends up pointing at the page URL
// and never starts, so maps never fire their `load` event. Serving the worker
// ourselves sidesteps that. Running this on prebuild/predev keeps the copy in
// lockstep with the installed maplibre-gl version (no committed vendor drift).

import { copyFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const distDir = join(
  dirname(require.resolve('maplibre-gl/package.json')),
  'dist'
);
const outDir = join(process.cwd(), 'public', 'maplibre');

// The worker imports its sibling `./maplibre-gl-shared.mjs`, so both must be
// served from the same directory.
const files = ['maplibre-gl-worker.mjs', 'maplibre-gl-shared.mjs'];

mkdirSync(outDir, { recursive: true });
for (const file of files) {
  copyFileSync(join(distDir, file), join(outDir, file));
}

process.stdout.write(
  `[copy-maplibre-worker] copied ${files.join(', ')} -> public/maplibre/\n`
);
