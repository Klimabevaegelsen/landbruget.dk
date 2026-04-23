import { afterEach, vi } from 'vitest';

afterEach(() => {
  delete process.env.CLOUDFLARE_ZONE_ID;
  delete process.env.CLOUDFLARE_API_TOKEN;
  vi.useRealTimers();
});
