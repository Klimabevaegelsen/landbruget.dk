import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { getAuthConfig } from "@/lib/auth";

const LOGIN_PATH = "/login";

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const { authCookieName, cookieValue } = getAuthConfig();

  // Allow login page and auth API through unconditionally
  if (pathname === LOGIN_PATH || pathname.startsWith("/api/auth")) {
    return NextResponse.next();
  }

  // Allow health check through
  if (pathname === "/api/health" || pathname === "/healthz") {
    return NextResponse.next();
  }

  // Allow Next.js internals
  if (pathname.startsWith("/_next") || pathname.startsWith("/favicon")) {
    return NextResponse.next();
  }

  const cookie = request.cookies.get(authCookieName);

  if (cookieValue && cookie?.value === cookieValue) {
    return NextResponse.next();
  }

  const loginUrl = request.nextUrl.clone();
  loginUrl.pathname = LOGIN_PATH;
  loginUrl.searchParams.set("from", pathname);
  return NextResponse.redirect(loginUrl);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
