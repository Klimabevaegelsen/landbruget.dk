/**
 * Security utilities for landbruget.dk public API
 * Implements rate limiting, CORS, and input validation for public endpoints
 */

// Rate limiting configuration
const RATE_LIMIT_WINDOW = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 100; // requests per window
const RATE_LIMIT_STORAGE = new Map<string, { count: number; resetTime: number }>();

/**
 * Rate limiter for public API endpoints
 */
export function rateLimit(request: Request): { allowed: boolean; remaining: number } {
  const clientIP = request.headers.get('x-forwarded-for') ||
                   request.headers.get('x-real-ip') ||
                   'unknown';

  const now = Date.now();
  const key = `rate_limit:${clientIP}`;

  const current = RATE_LIMIT_STORAGE.get(key);

  if (!current || now > current.resetTime) {
    // New window or expired window
    RATE_LIMIT_STORAGE.set(key, {
      count: 1,
      resetTime: now + RATE_LIMIT_WINDOW
    });
    return { allowed: true, remaining: RATE_LIMIT_MAX_REQUESTS - 1 };
  }

  if (current.count >= RATE_LIMIT_MAX_REQUESTS) {
    return { allowed: false, remaining: 0 };
  }

  current.count++;
  RATE_LIMIT_STORAGE.set(key, current);

  return { allowed: true, remaining: RATE_LIMIT_MAX_REQUESTS - current.count };
}

/**
 * CORS headers for public API
 */
export const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Max-Age': '86400', // 24 hours
};

/**
 * Security headers for all responses
 */
export const SECURITY_HEADERS = {
  ...CORS_HEADERS,
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'X-XSS-Protection': '1; mode=block',
  'Referrer-Policy': 'strict-origin-when-cross-origin',
  'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline';",
};

/**
 * Input validation for company IDs (UUIDs)
 */
export function validateCompanyId(id: string | null): { valid: boolean; error?: string } {
  if (!id) {
    return { valid: false, error: 'Company ID is required' };
  }

  // UUID v4/v5 regex pattern
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  if (!uuidRegex.test(id)) {
    return { valid: false, error: 'Invalid company ID format' };
  }

  return { valid: true };
}

/**
 * Input sanitization for text parameters
 */
export function sanitizeTextInput(input: string | null, maxLength = 100): string | null {
  if (!input) return null;

  // Remove potentially dangerous characters
  const sanitized = input
    .replace(/[<>'"&]/g, '') // Remove HTML/JS injection chars
    .replace(/[;\-]/g, '')   // Remove SQL injection chars
    .trim()
    .substring(0, maxLength);

  return sanitized.length > 0 ? sanitized : null;
}

/**
 * Create standardized error response
 */
export function createErrorResponse(
  error: string,
  status: number = 400,
  rateLimitInfo?: { remaining: number }
): Response {
  const headers = { ...SECURITY_HEADERS, 'Content-Type': 'application/json' };

  if (rateLimitInfo) {
    headers['X-RateLimit-Remaining'] = rateLimitInfo.remaining.toString();
  }

  return new Response(
    JSON.stringify({ error, timestamp: new Date().toISOString() }),
    { status, headers }
  );
}

/**
 * Create standardized success response
 */
export function createSuccessResponse(
  data: any,
  rateLimitInfo?: { remaining: number }
): Response {
  const headers = { ...SECURITY_HEADERS, 'Content-Type': 'application/json' };

  if (rateLimitInfo) {
    headers['X-RateLimit-Remaining'] = rateLimitInfo.remaining.toString();
  }

  return new Response(
    JSON.stringify(data),
    { status: 200, headers }
  );
}

/**
 * Handle CORS preflight requests
 */
export function handleCORSPreflight(): Response {
  return new Response('ok', { headers: SECURITY_HEADERS });
}

/**
 * Security middleware wrapper for Edge Functions
 */
export function withSecurity(
  handler: (request: Request, rateLimitInfo: { remaining: number }) => Promise<Response>
) {
  return async (request: Request): Promise<Response> => {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return handleCORSPreflight();
    }

    // Apply rate limiting
    const rateLimitResult = rateLimit(request);
    if (!rateLimitResult.allowed) {
      return createErrorResponse(
        'Rate limit exceeded. Please try again later.',
        429,
        { remaining: 0 }
      );
    }

    try {
      return await handler(request, { remaining: rateLimitResult.remaining });
    } catch (error) {
      console.error('API Error:', error);
      return createErrorResponse(
        'Internal server error',
        500,
        { remaining: rateLimitResult.remaining }
      );
    }
  };
}
