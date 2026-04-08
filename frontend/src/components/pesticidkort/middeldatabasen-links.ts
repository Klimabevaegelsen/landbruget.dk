/** Lazy-loaded lookup: product name → middeldatabasen.dk ProductID. */
let cache: Record<string, string> | null = null;

async function loadMapping(): Promise<Record<string, string>> {
  if (cache) return cache;
  const res = await fetch('/data/middeldatabasen-product-ids.json');
  cache = res.ok ? ((await res.json()) as Record<string, string>) : {};
  return cache;
}

const SEARCH_URL = 'https://www.middeldatabasen.dk/Middelvalg.asp';
const PRODUCT_URL = 'https://www.middeldatabasen.dk/Product.asp';

/** Returns a direct Product.asp link if the product is known, otherwise the search page. */
export function getProductUrl(
  name: string,
  lookup: Record<string, string> | null
): string {
  const id = lookup?.[name];
  return id ? `${PRODUCT_URL}?ProductID=${id}` : SEARCH_URL;
}

export { loadMapping };
