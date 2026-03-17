import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET() {
  const health = {
    status: "healthy",
    timestamp: new Date().toISOString(),
    environment: process.env.NEXT_PUBLIC_VERCEL_ENV || "development",
    checks: {
      r2_configured: Boolean(process.env.NEXT_PUBLIC_R2_BASE_URL),
      gemini_configured: Boolean(process.env.GOOGLE_API_KEY),
    },
  };

  // Return 503 if critical services are not configured
  const isHealthy = health.checks.r2_configured && health.checks.gemini_configured;

  return NextResponse.json(health, {
    status: isHealthy ? 200 : 503,
    headers: {
      "Cache-Control": "no-cache, no-store, must-revalidate",
      Pragma: "no-cache",
      Expires: "0",
    },
  });
}
