import { fileURLToPath } from 'node:url';

import tsconfigPaths from 'vite-tsconfig-paths';
import { defineConfig } from 'vitest/config';

const rootDir = fileURLToPath(new URL('.', import.meta.url));

export default defineConfig({
  plugins: [tsconfigPaths()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  test: {
    name: 'routes',
    environment: 'node',
    include: ['tests/routes/**/*.test.ts'],
    setupFiles: ['./tests/routes/setup.ts'],
    restoreMocks: true,
    clearMocks: true,
    unstubEnvs: true,
    unstubGlobals: true,
    coverage: {
      provider: 'v8',
      reportsDirectory: fileURLToPath(
        new URL('./coverage/routes', import.meta.url)
      ),
    },
  },
  server: {
    fs: {
      allow: [rootDir],
    },
  },
});
