export interface ProximityEntry {
  address: string;
  distanceM: number;
  buildingType?: string;
}

export function parseProximityList(
  raw: string | null | undefined
): ProximityEntry[] {
  if (!raw) return [];

  // One plot can hold several residential buildings sharing an address; keep
  // the nearest occurrence so counts reflect addresses, not buildings.
  const seen = new Set<string>();
  return raw
    .split('\n')
    .map((line) => parseProximityLine(line))
    .filter((entry): entry is ProximityEntry => {
      if (entry === null || seen.has(entry.address)) return false;
      seen.add(entry.address);
      return true;
    });
}

export function parseDistanceM(raw: string | null | undefined): number | null {
  if (!raw) return null;

  const match = raw.trim().match(/^(\d+(?:[.,]\d+)?)\s*m$/i);
  if (!match) return null;

  const distance = Number(match[1].replace(',', '.'));
  return Number.isFinite(distance) ? distance : null;
}

export function formatDistanceM(m: number): string {
  return `${Math.round(m)} m`;
}

/** "7 m fra marken", or "grænser op til marken" when it rounds to 0 m. */
export function formatDistanceFromField(m: number): string {
  return Math.round(m) === 0
    ? 'grænser op til marken'
    : `${formatDistanceM(m)} fra marken`;
}

export function formatProximityListForCsv(
  raw: string | null | undefined
): string {
  return parseProximityList(raw)
    .map((entry) => {
      const type = entry.buildingType ? ` – ${entry.buildingType}` : '';
      return `${entry.address}${type} (${formatDistanceM(entry.distanceM)})`;
    })
    .join('; ');
}

function parseProximityLine(line: string): ProximityEntry | null {
  const trimmed = line.trim();
  if (!trimmed) return null;

  const separatorIndex = trimmed.lastIndexOf(':');
  if (separatorIndex === -1) return null;

  const addressAndType = trimmed.slice(0, separatorIndex).trim();
  const distanceM = parseDistanceM(trimmed.slice(separatorIndex + 1));
  if (!addressAndType || distanceM === null) return null;

  const { address, buildingType } = splitAddressAndType(addressAndType);
  if (!address) return null;

  return {
    address,
    distanceM,
    ...(buildingType ? { buildingType } : {}),
  };
}

function splitAddressAndType(value: string): {
  address: string;
  buildingType?: string;
} {
  const separatorIndex = value.lastIndexOf('|');
  if (separatorIndex === -1) return { address: value.trim() };

  const address = value.slice(0, separatorIndex).trim();
  const buildingType = value.slice(separatorIndex + 1).trim();
  return {
    address,
    ...(buildingType ? { buildingType } : {}),
  };
}
