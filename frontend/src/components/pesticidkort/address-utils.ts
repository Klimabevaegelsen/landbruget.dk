import { env } from '@/lib/env';
import { addressResultFromDetails } from '@/components/pesticidkort/address-coordinates';
import type { AddressResult } from '@/components/pesticidkort/types';

const ADRESSEVAELGER_API = env.NEXT_PUBLIC_ADRESSEVAELGER_API_URL;
const ADRESSEVAELGER_TOKEN = env.NEXT_PUBLIC_ADRESSEVAELGER_TOKEN;

export interface AdressevaelgerResult {
  id: string;
  type: string;
  titel: string;
}

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null;
}

function parseSuggestion(value: unknown): AdressevaelgerResult | null {
  if (!isObject(value)) return null;
  if (
    typeof value.id !== 'string' ||
    typeof value.type !== 'string' ||
    typeof value.titel !== 'string'
  ) {
    return null;
  }
  return { id: value.id, type: value.type, titel: value.titel };
}

export function isSelectableAddress(result: AdressevaelgerResult): boolean {
  return result.type === 'adresse' || result.type === 'husnummer';
}

export async function searchAddresses(
  query: string,
  signal?: AbortSignal
): Promise<AdressevaelgerResult[]> {
  const trimmedQuery = query.trim();
  if (trimmedQuery.length < 2) return [];
  if (trimmedQuery.length > 73) {
    throw new Error('Adressevælger accepts search text up to 73 characters.');
  }

  const url = new URL(`${ADRESSEVAELGER_API}/adresser/soeg`);
  url.searchParams.set('tekst', trimmedQuery);
  url.searchParams.set('token', ADRESSEVAELGER_TOKEN);
  url.searchParams.set('maksimum', '8');

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Adressevælger search failed (${response.status}).`);
  }

  const data: unknown = await response.json();
  if (!isObject(data)) {
    throw new Error('Adressevælger returned an invalid search response.');
  }
  if (data.status === 'fejl') {
    const description =
      typeof data.beskrivelse === 'string' ? data.beskrivelse : null;
    throw new Error(description || 'Adressevælger search failed.');
  }

  const fund: unknown = data.fund;
  if (!Array.isArray(fund)) return [];
  return fund
    .map((item: unknown) => parseSuggestion(item))
    .filter((result): result is AdressevaelgerResult => result !== null);
}

export async function resolveCoordinates(
  result: AdressevaelgerResult,
  signal?: AbortSignal
): Promise<AddressResult | null> {
  if (!isSelectableAddress(result)) return null;

  const endpoint = result.type === 'husnummer' ? 'husnumre' : 'adresser';
  const url = new URL(
    `${ADRESSEVAELGER_API}/${endpoint}/${encodeURIComponent(result.id)}`
  );
  url.searchParams.set('token', ADRESSEVAELGER_TOKEN);

  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
    signal,
  });
  if (!response.ok) {
    throw new Error(
      `Adressevælger address lookup failed (${response.status}).`
    );
  }

  return addressResultFromDetails(await response.json(), result.titel);
}
