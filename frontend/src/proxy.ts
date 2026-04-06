import { NextRequest, NextResponse } from 'next/server';

export function proxy(request: NextRequest) {
  const hostname = request.headers.get('host') ?? '';

  // pesticidkortet.dk root → rewrite to /pesticidkort
  if (
    hostname.includes('pesticidkortet.dk') &&
    request.nextUrl.pathname === '/'
  ) {
    const url = request.nextUrl.clone();
    url.pathname = '/pesticidkort';
    return NextResponse.rewrite(url);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
