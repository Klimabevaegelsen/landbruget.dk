export const env = {
  NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_SUPABASE_URL,
  NEXT_PUBLIC_API_KEY: process.env.NEXT_PUBLIC_API_KEY,
} as const;
