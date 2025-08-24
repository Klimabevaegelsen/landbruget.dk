import { env } from "@/lib/env";

export const apiFetch = async (
  path: string,
  options?: {
    method?: string;
    body?: BodyInit;
    headers?: HeadersInit;
    cache?: RequestCache;
  }
) => {
  const response = await fetch(`${env.NEXT_PUBLIC_API_URL}${path}`, {
    method: options?.method || "GET",
    body: options?.body,
    headers: {
      ...options?.headers,
      Authorization: `Bearer ${env.NEXT_PUBLIC_API_KEY}`,
    },
    cache: options?.cache || "force-cache",
  });
  return response;
};
