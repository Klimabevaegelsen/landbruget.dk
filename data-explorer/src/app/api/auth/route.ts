import { NextRequest, NextResponse } from 'next/server';

const AUTH_COOKIE = 'data_auth';
// Cookie lives for 7 days
const MAX_AGE = 60 * 60 * 24 * 7;

export async function POST(request: NextRequest) {
  const { password } = await request.json();

  const sitePassword = process.env.SITE_PASSWORD;
  const cookieValue = process.env.AUTH_COOKIE_VALUE;

  if (!sitePassword || !cookieValue) {
    return NextResponse.json({ error: 'Server misconfigured' }, { status: 500 });
  }

  if (password !== sitePassword) {
    return NextResponse.json({ error: 'Incorrect password' }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set(AUTH_COOKIE, cookieValue, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: MAX_AGE,
    path: '/',
  });

  return response;
}
