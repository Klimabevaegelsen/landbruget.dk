const AUTH_COOKIE = 'data_auth';
const MAX_AGE_SECONDS = 60 * 60 * 24 * 7;

function getFirstDefined(...values: Array<string | undefined>): string | undefined {
  return values.find((value) => typeof value === 'string' && value.length > 0);
}

function deriveCookieValue(password: string): string {
  // Deterministic fallback so AUTH_COOKIE_VALUE is optional.
  let hash = 5381;
  for (let i = 0; i < password.length; i += 1) {
    hash = (hash * 33) ^ password.charCodeAt(i);
  }
  return `pw_${(hash >>> 0).toString(16)}`;
}

export function getAuthConfig() {
  const sitePassword = getFirstDefined(
    process.env.SITE_PASSWORD,
    process.env.DATA_EXPLORER_PASSWORD
  );
  const explicitCookieValue = getFirstDefined(
    process.env.AUTH_COOKIE_VALUE,
    process.env.DATA_EXPLORER_AUTH_COOKIE_VALUE
  );
  const cookieValue = explicitCookieValue ?? (sitePassword ? deriveCookieValue(sitePassword) : undefined);

  return {
    sitePassword,
    cookieValue,
    authCookieName: AUTH_COOKIE,
    cookieMaxAge: MAX_AGE_SECONDS,
    isConfigured: Boolean(sitePassword && cookieValue),
  };
}
