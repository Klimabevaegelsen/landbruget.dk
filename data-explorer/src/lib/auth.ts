const AUTH_COOKIE = 'data_auth';
const MAX_AGE_SECONDS = 60 * 60 * 24 * 7;

function getFirstDefined(...values: Array<string | undefined>): string | undefined {
  return values.find((value) => typeof value === 'string' && value.length > 0);
}

export function getAuthConfig() {
  const sitePassword = getFirstDefined(
    process.env.SITE_PASSWORD,
    process.env.DATA_EXPLORER_PASSWORD
  );
  const cookieValue = getFirstDefined(
    process.env.AUTH_COOKIE_VALUE,
    process.env.DATA_EXPLORER_AUTH_COOKIE_VALUE
  );

  return {
    sitePassword,
    cookieValue,
    authCookieName: AUTH_COOKIE,
    cookieMaxAge: MAX_AGE_SECONDS,
    isConfigured: Boolean(sitePassword && cookieValue),
  };
}
