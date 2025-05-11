export const apiFetch = async (
  path: string,
  options?: {
    method?: string;
    body?: BodyInit;
    headers?: HeadersInit;
    cache?: RequestCache;
  }
) => {
  const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/${path}`, {
    method: options?.method || "GET",
    body: options?.body,
    headers: {
      ...options?.headers,
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
    cache: options?.cache || "force-cache",
  });
  return response;
};
