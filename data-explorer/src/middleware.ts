import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

const AUTH_COOKIE = 'data_auth';
const LOGIN_PATH = '/login';

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow login page and auth API through unconditionally
  if (pathname === LOGIN_PATH || pathname.startsWith('/api/auth')) {
    return NextResponse.next();
  }

  // Allow health check through
  if (pathname === '/api/health' || pathname === '/healthz') {
    return NextResponse.next();
  }

  // Allow Next.js internals
  if (pathname.startsWith('/_next') || pathname.startsWith('/favicon')) {
    return NextResponse.next();
  }

  const cookie = request.cookies.get(AUTH_COOKIE);

  if (cookie?.value === process.env.AUTH_COOKIE_VALUE) {
    return NextResponse.next();
  }

  const loginUrl = request.nextUrl.clone();
  loginUrl.pathname = LOGIN_PATH;
  loginUrl.searchParams.set('from', pathname);
  return NextResponse.redirect(loginUrl);
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
