import { NextRequest, NextResponse } from "next/server";
import { getAuthConfig } from "@/lib/auth";

export async function POST(request: NextRequest) {
  const { password } = await request.json();

  const { sitePassword, cookieValue, authCookieName, cookieMaxAge, isConfigured } = getAuthConfig();

  if (!isConfigured || !sitePassword || !cookieValue) {
    return NextResponse.json({ error: "Server misconfigured" }, { status: 500 });
  }

  if (password !== sitePassword) {
    return NextResponse.json({ error: "Incorrect password" }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set(authCookieName, cookieValue, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    maxAge: cookieMaxAge,
    path: "/",
  });

  return response;
}
