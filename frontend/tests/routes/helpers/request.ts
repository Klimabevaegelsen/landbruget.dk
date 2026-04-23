import type { NextRequest } from 'next/server';

export function createRouteRequest(
  url: string,
  init?: RequestInit
): NextRequest {
  return new Request(url, init) as NextRequest;
}
